// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use dynamo_runtime::transports::zmq::{bind_socket, connect_socket, new_socket};
use omq_tokio::{KeepAlive, MonitorStream, Options, ReconnectPolicy, Socket, SocketType};

pub(crate) type SubSocket = Socket;
pub(crate) type PullSocket = Socket;

pub(crate) fn socket_options() -> Options {
    Options::default()
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
}

pub(crate) async fn connect_sub_socket(endpoint: &str, topic: Option<&str>) -> Result<SubSocket> {
    let socket = new_socket(SocketType::Sub, socket_options())?;
    socket
        .subscribe(Bytes::copy_from_slice(topic.unwrap_or("").as_bytes()))
        .await?;
    connect_socket(&socket, endpoint).await?;
    Ok(socket)
}

/// Attach the monitor before connecting so recovery sees the first handshake.
pub(crate) async fn connect_sub_socket_with_monitor(
    endpoint: &str,
    topic: &str,
) -> Result<(SubSocket, MonitorStream)> {
    let socket = new_socket(SocketType::Sub, socket_options())?;
    socket
        .subscribe(Bytes::copy_from_slice(topic.as_bytes()))
        .await?;
    let monitor = socket.monitor();
    connect_socket(&socket, endpoint).await?;
    Ok((socket, monitor))
}

#[cfg(test)]
pub(crate) async fn bind_pub_socket(endpoint: &str) -> Result<(Socket, String)> {
    let socket = new_socket(SocketType::Pub, socket_options())?;
    let endpoint = bind_socket(&socket, endpoint).await?;
    Ok((socket, endpoint))
}

pub(crate) async fn bind_pull_socket(endpoint: &str) -> Result<PullSocket> {
    let socket = new_socket(SocketType::Pull, socket_options())?;
    bind_socket(&socket, endpoint).await?;
    Ok(socket)
}

#[cfg(test)]
pub(crate) async fn connect_push_socket(endpoint: &str) -> Result<Socket> {
    let socket = new_socket(SocketType::Push, socket_options())?;
    connect_socket(&socket, endpoint).await?;
    Ok(socket)
}

#[cfg(test)]
pub(crate) async fn send_multipart(socket: &Socket, frames: Vec<Vec<u8>>) -> Result<()> {
    socket.send(omq_tokio::Message::multipart(frames)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bracketed_ipv6_endpoints_connect() {
        if let Err(error) = std::net::TcpListener::bind("[::1]:0") {
            eprintln!("Skipping IPv6 ZMQ test: {error}");
            return;
        }
        let pull = new_socket(SocketType::Pull, socket_options()).unwrap();
        let endpoint = bind_socket(&pull, "tcp://[::1]:0").await.unwrap();
        let push = connect_push_socket(&endpoint).await.unwrap();
        send_multipart(&push, vec![b"ipv6".to_vec()]).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(&received[0], b"ipv6");
    }
}
