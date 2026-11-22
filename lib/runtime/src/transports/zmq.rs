// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ZMQ Transport
//!
//! This module provides a ZMQ transport for the [crate::DistributedRuntime].
//!
//! The [Server] and [Client] communicate over OMQ ROUTER and DEALER sockets.
//!
//! The distributed service pattern we will use is based on the Harmony pattern described in
//! [Chapter 8: A Framework for Distributed Computing](https://zguide.zeromq.org/docs/chapter8/#True-Peer-Connectivity-Harmony-Pattern).
//!
//! This is similar to the TCP implementation; however, the TCP implementation used a direct
//! connection between the client and server per stream. The ZMQ transport will enable the
//! equivalent of a connection pool per upstream service at the cost of needing an extra internal
//! routing step per service endpoint.

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use derive_getters::Dissolve;
use omq_tokio::{Context as OmqContext, ContextConfig, Options, Socket, SocketType};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub type MultipartMessage = Vec<Vec<u8>>;

/// Returns whether `endpoint` needs `ZMQ_IPV6`.
///
/// Rejects unbracketed IPv6 literals before a socket starts reconnecting.
pub fn ipv6_option_for(endpoint: &str) -> Result<bool> {
    let Some(address) = endpoint.strip_prefix("tcp://") else {
        return Ok(false);
    };
    if address.starts_with('[') {
        return Ok(true);
    }
    if let Some((host, _port)) = address.rsplit_once(':')
        && host.parse::<std::net::Ipv6Addr>().is_ok()
    {
        anyhow::bail!(
            "Invalid ZMQ endpoint '{endpoint}': IPv6 addresses must be bracketed, for example tcp://[{host}]:<port>"
        );
    }
    Ok(false)
}

/// Create an OMQ socket for Dynamo transports on the caller's runtime,
/// or on a shared I/O runtime when `DYN_ZMQ_IO_THREADS` is configured.
pub fn new_socket(socket_type: SocketType, options: Options) -> Result<Socket> {
    match configured_context()? {
        Some(context) => Ok(context.socket(socket_type, options)),
        None => Ok(Socket::try_new(socket_type, options)?),
    }
}

/// Create a socket that can outlive the runtime that creates it.
pub(super) fn new_owned_socket(socket_type: SocketType, options: Options) -> Result<Socket> {
    static DEFAULT_CONTEXT: OnceCell<OmqContext> = OnceCell::new();
    let context =
        configured_context()?.unwrap_or_else(|| DEFAULT_CONTEXT.get_or_init(|| new_context(4)));
    Ok(context.socket(socket_type, options))
}

fn configured_context() -> Result<Option<&'static OmqContext>> {
    static CONTEXT: OnceCell<Option<OmqContext>> = OnceCell::new();
    let context = CONTEXT.get_or_try_init(|| {
        let value = std::env::var_os("DYN_ZMQ_IO_THREADS");
        let threads = value.as_deref().map(parse_io_threads).transpose()?;
        Ok::<_, anyhow::Error>(threads.map(new_context))
    })?;
    Ok(context.as_ref())
}

fn new_context(io_threads: usize) -> OmqContext {
    OmqContext::with_config_and_name(ContextConfig { io_threads }, "dynamo-omq")
}

fn parse_io_threads(value: &OsStr) -> Result<usize> {
    let threads = value
        .to_str()
        .context("DYN_ZMQ_IO_THREADS must be valid UTF-8")?
        .parse::<i32>()
        .context("DYN_ZMQ_IO_THREADS must be a positive integer")?;
    anyhow::ensure!(threads > 0, "DYN_ZMQ_IO_THREADS must be a positive integer");
    Ok(threads as usize)
}

/// Connect an OMQ socket using the Dynamo endpoint conventions.
pub async fn connect_socket(socket: &Socket, endpoint: &str) -> Result<()> {
    ipv6_option_for(endpoint)?;
    socket
        .connect(endpoint)
        .await
        .with_context(|| format!("Failed to connect ZMQ socket to {endpoint}"))
}

/// Bind an OMQ socket and return its resolved endpoint.
pub async fn bind_socket(socket: &Socket, endpoint: &str) -> Result<String> {
    ipv6_option_for(endpoint)?;
    let endpoint = socket
        .bind(endpoint)
        .await
        .with_context(|| format!("Failed to bind ZMQ socket to {endpoint}"))?;
    Ok(endpoint.to_string())
}

// Core message types
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ControlMessage {
    Cancel { request_id: String },
    CancelAck { request_id: String },
    Error { request_id: String, error: String },
    Complete { request_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum MessageType {
    Data(Vec<u8>),
    Control(ControlMessage),
}

enum StreamAction {
    SendEager(usize),
    SendDelayed(usize),
    Close,
}

// Router state management
struct RouterState {
    active_streams: HashMap<String, mpsc::Sender<Bytes>>,
    control_channels: HashMap<String, mpsc::Sender<ControlMessage>>,
}

impl RouterState {
    fn new() -> Self {
        Self {
            active_streams: HashMap::new(),
            control_channels: HashMap::new(),
        }
    }

    fn register_stream(
        &mut self,
        request_id: String,
        data_tx: mpsc::Sender<Bytes>,
        control_tx: mpsc::Sender<ControlMessage>,
    ) {
        self.active_streams.insert(request_id.clone(), data_tx);
        self.control_channels.insert(request_id, control_tx);
    }

    fn remove_stream(&mut self, request_id: &str) {
        self.active_streams.remove(request_id);
        self.control_channels.remove(request_id);
    }
}

// Server implementation
#[derive(Clone, Dissolve)]
pub struct Server {
    state: Arc<Mutex<RouterState>>,
    cancel_token: CancellationToken,
    endpoint: String,
}

impl Server {
    /// Create a new [Server] and bind its OMQ ROUTER socket to the address.
    ///
    /// If the event loop processing the router fails with an error, the signal is propagated through the [CancellationToken]
    /// by issuing a [CancellationToken::cancel].
    ///
    /// The [Server] is how you interact with the running instance.
    ///
    /// The [ServerExecutionHandle] is the handle for background task executing the [Server].
    pub async fn new(
        context: &OmqContext,
        address: &str,
        cancel_token: CancellationToken,
    ) -> Result<(Self, ServerExecutionHandle)> {
        let router = context.socket(SocketType::Router, Options::default());
        let endpoint = bind_socket(&router, address).await?;
        let state = Arc::new(Mutex::new(RouterState::new()));

        // can cancel the router's event loop
        let child = cancel_token.child_token();
        let primary_task = tokio::spawn(Self::run(router, state.clone(), child.child_token()));

        // this task captures the primary cancellation token, so if an error occurs, we can cancel the router's event loop
        // but we also propagate the error to the caller's cancellation token
        let watch_task = tokio::spawn(async move {
            let result = primary_task.await.inspect_err(|e| {
                tracing::error!("zmq server/router task failed: {e}");
                cancel_token.cancel();
            })?;
            result.inspect_err(|e| {
                tracing::error!("zmq server/router task failed: {e}");
                cancel_token.cancel();
            })
        });

        let handle = ServerExecutionHandle {
            task: watch_task,
            cancel_token: child.clone(),
        };

        Ok((
            Self {
                state,
                cancel_token: child,
                endpoint,
            },
            handle,
        ))
    }

    // pub async fn register_stream(&)

    async fn run(
        router: Socket,
        state: Arc<Mutex<RouterState>>,
        token: CancellationToken,
    ) -> Result<()> {
        loop {
            let frames = tokio::select! {
                biased;
                _ = token.cancelled() => break,
                frames = router.recv() => frames?,
            };

            // we should have at least 3 frames
            // 0: identity
            // 1: request_id
            // 2: message type

            // if the contract is broken, we should exit
            if frames.len() != 3 {
                anyhow::bail!(
                    "Fatal Error -- Broken contract -- Expected 3 frames, got {}",
                    frames.len()
                );
            }

            let request_id = String::from_utf8_lossy(&frames[1]).to_string();
            let message = frames[2].to_vec();
            let message_size = message.len();

            if let Some(tx) = state.lock().await.active_streams.get(&request_id) {
                // first we try to send the data eagerly without blocking
                let action = match tx.try_send(message.into()) {
                    Ok(_) => {
                        tracing::trace!(
                            request_id,
                            "response data sent eagerly to stream: {} bytes",
                            message_size
                        );
                        StreamAction::SendEager(message_size)
                    }
                    Err(e) => match e {
                        mpsc::error::TrySendError::Closed(_) => {
                            tracing::info!(request_id, "response stream was closed");
                            StreamAction::Close
                        }
                        mpsc::error::TrySendError::Full(data) => {
                            tracing::warn!(
                                request_id,
                                "response stream is full; backpressure alert"
                            );
                            // todo - add timeout - we are blocking all other streams
                            if (tx.send(data).await).is_err() {
                                StreamAction::Close
                            } else {
                                StreamAction::SendDelayed(message_size)
                            }
                        }
                    },
                };

                match action {
                    StreamAction::SendEager(_size) => {
                        // increment bytes_received
                        // increment messages_received
                        // increment eager_messages_received
                    }
                    StreamAction::SendDelayed(_size) => {
                        // increment bytes_received
                        // increment messages_received
                        // increment delayed_messages_received
                    }
                    StreamAction::Close => {
                        state.lock().await.active_streams.remove(&request_id);
                    }
                }
            } else {
                // increment bytes_dropped
                // increment messages_dropped
                tracing::trace!(request_id, "no active stream for request_id");
            }
        }

        Ok(())
    }
}

/// The [ServerExecutionHandle] is the handle for background task executing the [Server].
///
/// You can use this to check if the server is finished or cancelled.
///
/// You can also join on the task to wait for it to finish.
pub struct ServerExecutionHandle {
    task: JoinHandle<Result<()>>,
    cancel_token: CancellationToken,
}

impl ServerExecutionHandle {
    /// Check if the task awaiting on the [Server]s background event loop has finished.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Check if the server's event loop has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// Cancel the server's event loop.
    ///
    /// This will signal the server to stop processing requests and exit.
    ///
    /// This will not wait for the server to finish, it will exit immediately.
    ///
    /// This will not propagate to the [CancellationToken] used to start the [Server]
    /// unless an error happens during the shutdown process.
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// Join on the task awaiting on the [Server]s background event loop.
    ///
    /// This will return the result of the [Server]s background event loop.
    pub async fn join(self) -> Result<()> {
        self.task.await?
    }
}

// Client implementation
pub struct Client {
    dealer: Socket,
}

impl Client {
    /// Connect a DEALER to a ZMQ request server.
    pub async fn new(context: &OmqContext, address: &str) -> Result<Self> {
        let dealer = context.socket(SocketType::Dealer, Options::default());
        connect_socket(&dealer, address).await?;
        Ok(Self { dealer })
    }

    /// Access the DEALER socket for multipart requests.
    pub fn dealer(&self) -> &Socket {
        &self.dealer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[test]
    fn parses_io_thread_override() {
        assert_eq!(parse_io_threads(OsStr::new("4")).unwrap(), 4);
        for value in ["0", "invalid", "2147483648"] {
            assert!(parse_io_threads(OsStr::new(value)).is_err());
        }
    }

    #[test]
    fn ipv6_option_follows_endpoint_address_family() {
        for (endpoint, expected) in [
            ("tcp://[::1]:5555", true),
            ("tcp://[::]:*", true),
            ("tcp://[2001:db8::10]:5555", true),
            ("tcp://127.0.0.1:5555", false),
            ("tcp://0.0.0.0:*", false),
            ("tcp://localhost:5555", false),
            ("tcp://*:5555", false),
            ("inproc://events", false),
        ] {
            assert_eq!(ipv6_option_for(endpoint).unwrap(), expected, "{endpoint}");
        }

        for endpoint in ["tcp://::1:5555", "tcp://2001:db8::10:5555"] {
            let error = ipv6_option_for(endpoint).unwrap_err().to_string();
            assert!(
                error.contains("IPv6 addresses must be bracketed"),
                "{endpoint}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn test_basic_communication() -> Result<()> {
        let context = OmqContext::current();
        let address = "tcp://127.0.0.1:0";
        let token = CancellationToken::new();

        // Start server
        let (server, handle) = Server::new(&context, address, token.clone()).await?;
        let state = server.state.clone();

        let id = "test-request".to_string();
        let (tx, mut rx) = tokio::sync::mpsc::channel(512);
        state.lock().await.active_streams.insert(id.clone(), tx);

        // Create client
        let client = Client::new(&context, &server.endpoint).await?;

        client
            .dealer()
            .send(omq_tokio::Message::multipart([
                id.as_bytes().to_vec(),
                id.as_bytes().to_vec(),
            ]))
            .await?;

        let receive_result = timeout(std::time::Duration::from_secs(3), rx.recv()).await?;

        let received = receive_result.unwrap();

        // convert to string
        let received_str = String::from_utf8_lossy(&received).to_string();
        assert_eq!(received_str, "test-request");

        drop(client);

        handle.cancel();
        handle.join().await?;

        println!("done");

        Ok(())
    }

    // #[tokio::test]
    // async fn test_multiple_streams() -> Result<()> {
    //     // Similar to above but with multiple clients/streams
    //     Ok(())
    // }

    // #[tokio::test]
    // async fn test_error_handling() -> Result<()> {
    //     // Test various error conditions
    //     Ok(())
    // }
}
