// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Native OMQ sockets for KV-event ingest, replay, and replica sync.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Result, anyhow};
use omq_tokio::{Context, ContextConfig, KeepAlive, Options, ReconnectPolicy, Socket, SocketType};

fn new_socket(socket_type: SocketType) -> Result<Socket> {
    static CONTEXT: OnceLock<Option<Context>> = OnceLock::new();
    let context = CONTEXT.get_or_init(|| {
        let value = std::env::var_os("DYN_ZMQ_IO_THREADS")?;
        match value.to_str().and_then(|value| value.parse::<i32>().ok()) {
            Some(threads) if threads > 0 => Some(Context::with_config_and_name(
                ContextConfig {
                    io_threads: threads as usize,
                },
                "dynamo-kv-router-omq",
            )),
            _ => {
                tracing::warn!(
                    ?value,
                    "DYN_ZMQ_IO_THREADS must be a positive integer; using the Tokio runtime"
                );
                None
            }
        }
    });
    let options = Options::default()
        .linger(Duration::ZERO)
        .reconnect(ReconnectPolicy::Exponential {
            min: Duration::from_millis(100),
            max: Duration::from_secs(5),
        })
        .tcp_keepalive(KeepAlive::Enabled {
            idle: Duration::from_secs(7200),
            intvl: Duration::from_secs(75),
            cnt: 9,
        })
        .heartbeat_interval(Duration::from_secs(5))
        .heartbeat_timeout(Duration::from_secs(15))
        .heartbeat_ttl(Duration::from_secs(15));
    match context {
        Some(context) => Ok(context.socket(socket_type, options)),
        None => Ok(Socket::try_new(socket_type, options)?),
    }
}

pub async fn create_sub_socket_topics(topics: &[&[u8]]) -> Result<Socket> {
    let socket = new_socket(SocketType::Sub)?;
    for topic in topics {
        socket.subscribe(topic.to_vec()).await?;
    }
    Ok(socket)
}

#[cfg(feature = "ingest-zmq")]
pub async fn connect_sub_socket(endpoint: &str) -> Result<Socket> {
    let socket = create_sub_socket_topics(&[b""]).await?;
    socket.connect(endpoint).await?;
    Ok(socket)
}

#[cfg(feature = "ingest-zmq")]
pub async fn connect_dealer_socket(endpoint: &str) -> Result<Socket> {
    let socket = new_socket(SocketType::Dealer)?;
    socket.connect(endpoint).await?;
    Ok(socket)
}

#[cfg(any(feature = "replica-sync-zmq", test))]
pub async fn create_bound_pub_socket(endpoint: &str) -> Result<(Socket, String)> {
    let socket = new_socket(SocketType::Pub)?;
    let endpoint = socket.bind(endpoint).await?.to_string();
    Ok((socket, endpoint))
}

pub fn validate_endpoint(endpoint: &str) -> Result<()> {
    let (scheme, address) = endpoint
        .split_once("://")
        .ok_or_else(|| anyhow!("invalid ZMQ endpoint `{endpoint}`: missing scheme"))?;

    if address.is_empty() {
        return Err(anyhow!(
            "invalid ZMQ endpoint `{endpoint}`: missing address"
        ));
    }

    match scheme {
        "tcp" => {
            let (host, port) = address
                .rsplit_once(':')
                .ok_or_else(|| anyhow!("invalid ZMQ endpoint `{endpoint}`: missing TCP port"))?;
            if host.is_empty() {
                return Err(anyhow!(
                    "invalid ZMQ endpoint `{endpoint}`: missing TCP host"
                ));
            }
            if host.starts_with('[') {
                if !host.ends_with(']') {
                    return Err(anyhow!(
                        "invalid ZMQ endpoint `{endpoint}`: missing closing `]`"
                    ));
                }
            } else if host.contains(':') {
                return Err(anyhow!(
                    "invalid ZMQ endpoint `{endpoint}`: missing TCP port"
                ));
            }
            port.parse::<u16>().map_err(|error| {
                anyhow!("invalid ZMQ endpoint `{endpoint}`: invalid TCP port: {error}")
            })?;
            Ok(())
        }
        "ipc" | "inproc" => Ok(()),
        other => Err(anyhow!(
            "invalid ZMQ endpoint `{endpoint}`: unsupported scheme `{other}`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_tokio::Message;

    #[tokio::test]
    async fn bracketed_ipv6_endpoints_connect() {
        if let Err(error) = std::net::TcpListener::bind("[::1]:0") {
            eprintln!("Skipping IPv6 ZMQ test: {error}");
            return;
        }
        let (publisher, endpoint) = create_bound_pub_socket("tcp://[::1]:0").await.unwrap();
        validate_endpoint(&endpoint).unwrap();
        let subscriber = create_sub_socket_topics(&[b""]).await.unwrap();
        subscriber.connect(&endpoint).await.unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                publisher.send(Message::from("ipv6")).await.unwrap();
                if let Ok(frames) =
                    tokio::time::timeout(Duration::from_millis(50), subscriber.recv()).await
                {
                    let frames = frames.unwrap();
                    assert_eq!(frames.len(), 1);
                    assert_eq!(&frames[0], b"ipv6");
                    break;
                }
            }
        })
        .await
        .expect("IPv6 SUB socket should receive the message");
    }
}
