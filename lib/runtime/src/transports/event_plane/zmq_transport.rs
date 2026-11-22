// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Uses ZMQ PUB/SUB pattern for one-way event broadcasting:
//! - Publishers bind to endpoints and broadcast events
//! - Subscribers connect to endpoints and receive events
//! - Topic-based filtering at socket level for efficiency
//!
//! ## Message Format
//!
//! ZMQ multipart message:
//! - Frame 0: Topic (string) - for ZMQ subscription filtering
//! - Frame 1: publisher_id (8 bytes, u64 big-endian) - for fast deduplication
//! - Frame 2: sequence (8 bytes, u64 big-endian) - for fast deduplication
//! - Frame 3: Binary frame (5-byte header + EventEnvelope payload)

use anyhow::{Context as _, Result, anyhow};
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use omq_tokio::{Message, Options, Payload, Socket, SocketType};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_util::task::AbortOnDropHandle;

use super::codec::{Codec, MsgpackCodec};
use super::frame::Frame;
use super::transport::{EventTransportRx, EventTransportTx, WireStream};
use crate::discovery::EventTransportKind;
use crate::transports::zmq::{bind_socket, connect_socket, new_owned_socket, new_socket};

const ZMQ_SNDHWM: i32 = 100_000;
const ZMQ_RCVHWM: i32 = 100_000;

fn publisher_options() -> Options {
    Options::default().send_hwm(ZMQ_SNDHWM as u32)
}

/// ZMQ PUB transport for publishing events.
pub struct ZmqPubTransport {
    socket: Socket,
    topic: String,
    topic_frame: Payload,
}

impl ZmqPubTransport {
    /// Create a new ZMQ publisher by binding to an endpoint.
    ///
    /// If the TCP port is 0, ZMQ allocates and reserves an ephemeral port
    /// on the publisher socket itself.
    ///
    /// Returns the transport and the actual bound endpoint.
    pub async fn bind(endpoint: &str, topic: &str) -> Result<(Self, String)> {
        let socket = new_socket(SocketType::Pub, publisher_options())?;
        Self::bind_on(socket, endpoint, topic).await
    }

    /// A shared publisher must survive the runtime that initialized it.
    pub(super) async fn bind_shared(endpoint: &str, topic: &str) -> Result<(Self, String)> {
        let socket = new_owned_socket(SocketType::Pub, publisher_options())?;
        Self::bind_on(socket, endpoint, topic).await
    }

    async fn bind_on(socket: Socket, endpoint: &str, topic: &str) -> Result<(Self, String)> {
        let actual_endpoint = bind_socket(&socket, endpoint).await?;

        tracing::info!(
            endpoint = %actual_endpoint,
            topic = %topic,
            sndhwm = ZMQ_SNDHWM,
            "ZMQ PUB transport bound with configured HWM"
        );

        Ok((
            Self {
                socket,
                topic: topic.to_string(),
                topic_frame: Payload::from_slice(topic.as_bytes()),
            },
            actual_endpoint,
        ))
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Connect to single broker XSUB endpoint (broker mode)
    pub async fn connect(xsub_endpoint: &str, topic: &str) -> Result<Self> {
        let socket = new_socket(SocketType::Pub, publisher_options())?;
        connect_socket(&socket, xsub_endpoint).await?;

        tracing::info!(
            endpoint = %xsub_endpoint,
            topic = %topic,
            sndhwm = ZMQ_SNDHWM,
            "ZMQ PUB transport connected to broker XSUB"
        );

        Ok(Self {
            socket,
            topic: topic.to_string(),
            topic_frame: Payload::from_slice(topic.as_bytes()),
        })
    }

    /// Connect to multiple broker XSUB endpoints (HA mode)
    pub async fn connect_multiple(xsub_endpoints: &[String], topic: &str) -> Result<Self> {
        let mut endpoints = xsub_endpoints.iter();
        let Some(first_endpoint) = endpoints.next() else {
            anyhow::bail!("Cannot connect to zero endpoints");
        };

        let socket = new_socket(SocketType::Pub, publisher_options())?;
        connect_socket(&socket, first_endpoint).await?;

        for endpoint in endpoints {
            connect_socket(&socket, endpoint).await?;
            tracing::debug!(endpoint = %endpoint, "ZMQ PUB connected to broker XSUB");
        }

        tracing::info!(
            num_endpoints = xsub_endpoints.len(),
            topic = %topic,
            sndhwm = ZMQ_SNDHWM,
            "ZMQ PUB transport connected to multiple broker XSUBs with configured HWM"
        );

        Ok(Self {
            socket,
            topic: topic.to_string(),
            topic_frame: Payload::from_slice(topic.as_bytes()),
        })
    }
}

#[async_trait]
impl EventTransportTx for ZmqPubTransport {
    async fn publish(&self, _subject: &str, envelope_bytes: Bytes) -> Result<()> {
        let codec = MsgpackCodec;
        let (publisher_id, sequence) = codec.decode_envelope_identity(&envelope_bytes)?;

        let message = Message::multipart_payloads([
            self.topic_frame.clone(),
            Payload::from_slice(&publisher_id.to_be_bytes()),
            Payload::from_slice(&sequence.to_be_bytes()),
            Payload::from(Frame::new(envelope_bytes).encode()),
        ]);
        self.socket.send(message).await?;

        Ok(())
    }

    fn kind(&self) -> EventTransportKind {
        EventTransportKind::Zmq
    }
}

/// ZMQ SUB transport for subscribing to events.
///
/// Uses a background async reader to fan out frames to multiple local subscribers.
pub struct ZmqSubTransport {
    broadcast_tx: broadcast::Sender<Bytes>,
    socket_pump_handle: Arc<AbortOnDropHandle<()>>,
}

/// One validated multipart message from a direct ZMQ publisher.
pub struct ZmqWireMessage {
    pub publisher_id: u64,
    pub sequence: u64,
    pub payload: Bytes,
}

pub type ZmqWireStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<ZmqWireMessage>> + Send>>;

/// One dynamically managed ZMQ SUB socket connected to several publishers.
pub struct DynamicZmqSubSocket {
    socket: Socket,
    expected_topic: Bytes,
}

impl DynamicZmqSubSocket {
    /// Create a SUB socket. Add its first endpoint before receiving.
    pub fn new(topic: &str, rcvhwm: i32) -> Result<Self> {
        anyhow::ensure!(rcvhwm > 0, "ZMQ receive HWM must be greater than zero");
        let socket = new_socket(SocketType::Sub, Options::default().recv_hwm(rcvhwm as u32))?;
        Ok(Self {
            socket,
            expected_topic: Bytes::copy_from_slice(topic.as_bytes()),
        })
    }

    /// Connect a new SUB socket with an explicit receive high-water mark.
    pub async fn connect_with_rcvhwm(endpoint: &str, topic: &str, rcvhwm: i32) -> Result<Self> {
        let socket = Self::new(topic, rcvhwm)?;
        socket.connect_initial(endpoint).await?;
        Ok(socket)
    }

    /// Subscribe to the initial topic and connect to the first publisher.
    pub async fn connect_initial(&self, endpoint: &str) -> Result<()> {
        self.socket.subscribe(self.expected_topic.clone()).await?;
        self.add_endpoint(endpoint).await
    }

    /// Connect this SUB socket to one more publisher endpoint.
    pub async fn add_endpoint(&self, endpoint: &str) -> Result<()> {
        connect_socket(&self.socket, endpoint).await
    }

    /// Stop receiving from one publisher endpoint.
    pub async fn remove_endpoint(&self, endpoint: &str) -> Result<()> {
        self.socket.disconnect(endpoint).await?;
        Ok(())
    }

    /// Add a topic prefix filter, for consumers that route by raw topic frame.
    pub async fn subscribe(&self, topic: &[u8]) -> Result<()> {
        self.socket.subscribe(Bytes::copy_from_slice(topic)).await?;
        Ok(())
    }

    /// Remove a topic prefix filter added by [`Self::subscribe`].
    pub async fn unsubscribe(&self, topic: &[u8]) -> Result<()> {
        self.socket
            .unsubscribe(Bytes::copy_from_slice(topic))
            .await?;
        Ok(())
    }

    /// Receive the next multipart message undecoded for raw publishers.
    pub async fn next_frames(&self) -> Option<Result<Message>> {
        Some(self.socket.recv().await.map_err(Into::into))
    }

    /// Receive and decode the next multipart message.
    pub async fn next(&self) -> Option<Result<ZmqWireMessage>> {
        Some(receive_wire_message(&self.socket, &self.expected_topic).await)
    }
}

/// One event envelope whose ZMQ frames and envelope attribution agree.
#[derive(Debug)]
pub struct ValidatedEnvelope {
    pub publisher_id: u64,
    pub sequence: u64,
    pub published_at: u64,
    pub payload: Bytes,
}

/// Failure returned while reading a [`ValidatedZmqSource`].
#[derive(Debug, Error)]
pub enum ValidatedZmqSourceError {
    #[error("direct ZMQ receive failed: {0}")]
    Receive(#[source] anyhow::Error),
    #[error("direct ZMQ envelope decode failed: {0}")]
    EnvelopeDecode(#[source] anyhow::Error),
    #[error(
        "direct ZMQ identity mismatch: expected publisher {expected_publisher_id} topic {expected_topic}, frame publisher {frame_publisher_id} sequence {frame_sequence}, envelope publisher {envelope_publisher_id} sequence {envelope_sequence} topic {envelope_topic}"
    )]
    IdentityMismatch {
        expected_publisher_id: u64,
        expected_topic: String,
        frame_publisher_id: u64,
        frame_sequence: u64,
        envelope_publisher_id: u64,
        envelope_sequence: u64,
        envelope_topic: String,
    },
}

/// A direct ZMQ source that validates frame and envelope attribution once.
/// Uses OMQ sockets and interoperates with existing ZMTP publishers.
pub struct ValidatedZmqSource {
    stream: ZmqWireStream,
    expected_topic: String,
    expected_publisher_id: u64,
    codec: Codec,
}

impl ValidatedZmqSource {
    pub async fn connect_default(
        endpoint: &str,
        topic: &str,
        expected_publisher_id: u64,
    ) -> Result<Self> {
        Self::connect(endpoint, topic, expected_publisher_id, ZMQ_RCVHWM).await
    }

    pub async fn connect(
        endpoint: &str,
        topic: &str,
        expected_publisher_id: u64,
        rcvhwm: i32,
    ) -> Result<Self> {
        let stream =
            ZmqSubTransport::connect_single_consumer_with_rcvhwm(endpoint, topic, rcvhwm).await?;
        Ok(Self {
            stream,
            expected_topic: topic.to_string(),
            expected_publisher_id,
            codec: Codec::default(),
        })
    }

    pub async fn next(
        &mut self,
    ) -> Option<std::result::Result<ValidatedEnvelope, ValidatedZmqSourceError>> {
        let message = match self.stream.next().await? {
            Ok(message) => message,
            Err(error) => return Some(Err(ValidatedZmqSourceError::Receive(error))),
        };
        let envelope = match self.codec.decode_envelope(&message.payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                return Some(Err(ValidatedZmqSourceError::EnvelopeDecode(error)));
            }
        };

        if envelope.publisher_id != self.expected_publisher_id
            || envelope.publisher_id != message.publisher_id
            || envelope.sequence != message.sequence
            || envelope.topic != self.expected_topic
        {
            return Some(Err(ValidatedZmqSourceError::IdentityMismatch {
                expected_publisher_id: self.expected_publisher_id,
                expected_topic: self.expected_topic.clone(),
                frame_publisher_id: message.publisher_id,
                frame_sequence: message.sequence,
                envelope_publisher_id: envelope.publisher_id,
                envelope_sequence: envelope.sequence,
                envelope_topic: envelope.topic,
            }));
        }

        Some(Ok(ValidatedEnvelope {
            publisher_id: envelope.publisher_id,
            sequence: envelope.sequence,
            published_at: envelope.published_at,
            payload: envelope.payload,
        }))
    }
}

impl ZmqSubTransport {
    async fn connect_socket(endpoint: &str, topic: &str, rcvhwm: i32) -> Result<Socket> {
        Ok(
            DynamicZmqSubSocket::connect_with_rcvhwm(endpoint, topic, rcvhwm)
                .await?
                .socket,
        )
    }

    /// Create a new ZMQ subscriber by connecting to a single endpoint.
    pub async fn connect(endpoint: &str, topic: &str) -> Result<Self> {
        let socket = Self::connect_socket(endpoint, topic, ZMQ_RCVHWM).await?;

        tracing::info!(
            endpoint = %endpoint,
            topic = %topic,
            rcvhwm = ZMQ_RCVHWM,
            "ZMQ SUB transport connected with configured HWM"
        );

        let (broadcast_tx, _) = broadcast::channel(1024);
        let pump_handle = Self::start_socket_pump(socket, broadcast_tx.clone());

        Ok(Self {
            broadcast_tx,
            socket_pump_handle: Arc::new(AbortOnDropHandle::new(pump_handle)),
        })
    }

    /// Connect one consumer directly to one ZMQ publisher.
    ///
    /// Unlike [`Self::connect`], this stream owns and polls the socket directly. It
    /// therefore has no background pump or lossy broadcast hop and naturally
    /// applies backpressure at the configured ZMQ receive HWM.
    pub async fn connect_single_consumer(endpoint: &str, topic: &str) -> Result<ZmqWireStream> {
        Self::connect_single_consumer_with_rcvhwm(endpoint, topic, ZMQ_RCVHWM).await
    }

    /// Connect one consumer directly to one ZMQ publisher with an explicit receive HWM.
    pub async fn connect_single_consumer_with_rcvhwm(
        endpoint: &str,
        topic: &str,
        rcvhwm: i32,
    ) -> Result<ZmqWireStream> {
        let socket = Self::connect_socket(endpoint, topic, rcvhwm).await?;
        tracing::info!(
            endpoint,
            topic,
            rcvhwm,
            "ZMQ single-consumer stream connected"
        );
        Self::single_consumer_stream(socket, topic)
    }

    /// Connect one consumer directly to multiple ZMQ endpoints.
    ///
    /// The returned stream owns one SUB socket connected to every endpoint. It
    /// avoids the lossy local broadcast hop used by [`Self::connect_multiple`].
    pub async fn connect_single_consumer_multiple(
        endpoints: &[String],
        topic: &str,
    ) -> Result<ZmqWireStream> {
        let mut endpoint_iter = endpoints.iter();
        let Some(first_endpoint) = endpoint_iter.next() else {
            anyhow::bail!("Cannot connect to zero endpoints");
        };

        let endpoint_count = endpoints.len();
        let socket = Self::connect_socket(first_endpoint, topic, ZMQ_RCVHWM).await?;
        for endpoint in endpoint_iter {
            connect_socket(&socket, endpoint).await?;
        }

        tracing::info!(
            endpoint_count,
            topic,
            rcvhwm = ZMQ_RCVHWM,
            "ZMQ single-consumer stream connected to multiple endpoints"
        );
        tracing::debug!(?endpoints, topic, "ZMQ single-consumer stream endpoints");
        Self::single_consumer_stream(socket, topic)
    }

    fn single_consumer_stream(socket: Socket, topic: &str) -> Result<ZmqWireStream> {
        let expected_topic = Bytes::copy_from_slice(topic.as_bytes());
        Ok(Box::pin(stream! {
            loop {
                let result = receive_wire_message(&socket, &expected_topic).await;
                let closed = result.is_err();
                yield result;
                if closed {
                    break;
                }
            }
        }))
    }

    /// Connect to broker's XPUB endpoint (broker mode)
    pub async fn connect_broker(xpub_endpoint: &str, topic: &str) -> Result<Self> {
        Self::connect(xpub_endpoint, topic).await
    }

    /// Connect to multiple broker XPUB endpoints (HA mode)
    pub async fn connect_broker_multiple(xpub_endpoints: &[String], topic: &str) -> Result<Self> {
        Self::connect_multiple(xpub_endpoints, topic).await
    }

    /// Create a new ZMQ subscriber by connecting to multiple endpoints (fan-in).
    pub async fn connect_multiple(endpoints: &[String], topic: &str) -> Result<Self> {
        let mut endpoints_iter = endpoints.iter();
        let Some(first_endpoint) = endpoints_iter.next() else {
            anyhow::bail!("Cannot connect to zero endpoints");
        };

        let socket = Self::connect_socket(first_endpoint, topic, ZMQ_RCVHWM).await?;

        for endpoint in endpoints_iter {
            connect_socket(&socket, endpoint).await?;
            tracing::debug!(endpoint = %endpoint, "ZMQ SUB connected to endpoint");
        }

        tracing::info!(
            num_endpoints = endpoints.len(),
            topic = %topic,
            rcvhwm = ZMQ_RCVHWM,
            "ZMQ SUB transport connected to multiple endpoints with configured HWM"
        );

        let (broadcast_tx, _) = broadcast::channel(1024);
        let pump_handle = Self::start_socket_pump(socket, broadcast_tx.clone());

        Ok(Self {
            broadcast_tx,
            socket_pump_handle: Arc::new(AbortOnDropHandle::new(pump_handle)),
        })
    }

    fn start_socket_pump(
        socket: Socket,
        broadcast_tx: broadcast::Sender<Bytes>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match receive_wire_message(&socket, &[]).await {
                    Ok(message) => {
                        let _ = broadcast_tx.send(message.payload);
                    }
                    Err(error) => {
                        tracing::error!(%error, "ZMQ receive error in socket pump");
                        break;
                    }
                }
            }
        })
    }
}

async fn receive_wire_message(socket: &Socket, expected_topic: &[u8]) -> Result<ZmqWireMessage> {
    loop {
        tokio::task::consume_budget().await;
        let frames = socket.recv().await?;
        match decode_multipart(frames, expected_topic) {
            Ok(message) => return Ok(message),
            Err(error) => tracing::warn!(%error, "Dropping malformed ZMQ message"),
        }
    }
}

fn decode_multipart(frames: Message, expected_topic: &[u8]) -> Result<ZmqWireMessage> {
    let (publisher_id, sequence) =
        decode_wire_headers(frames.len(), expected_topic, |index| &frames[index])?;
    let payload = frames
        .last_payload()
        .ok_or_else(|| anyhow!("ZMQ multipart message has no payload frame"))?;
    decode_wire_payload(publisher_id, sequence, payload.into_bytes())
}

fn decode_wire_headers<'a>(
    frame_count: usize,
    expected_topic: &[u8],
    frame: impl Fn(usize) -> &'a [u8],
) -> Result<(u64, u64)> {
    if frame_count != 4 {
        anyhow::bail!("unexpected ZMQ multipart frame count: {frame_count}");
    }
    if !expected_topic.is_empty() && frame(0) != expected_topic {
        anyhow::bail!("ZMQ message topic disagrees with the exact subscription topic");
    }

    let publisher_id_bytes = frame(1);
    let publisher_id = publisher_id_bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| {
            anyhow!(
                "invalid ZMQ publisher ID frame length: {}",
                publisher_id_bytes.len()
            )
        })?;
    let sequence_bytes = frame(2);
    let sequence = sequence_bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| {
            anyhow!(
                "invalid ZMQ sequence frame length: {}",
                sequence_bytes.len()
            )
        })?;
    Ok((publisher_id, sequence))
}

fn decode_wire_payload(
    publisher_id: u64,
    sequence: u64,
    frame_bytes: Bytes,
) -> Result<ZmqWireMessage> {
    let frame = Frame::decode(frame_bytes)?;

    Ok(ZmqWireMessage {
        publisher_id,
        sequence,
        payload: frame.payload,
    })
}

#[async_trait]
impl EventTransportRx for ZmqSubTransport {
    async fn subscribe(&self, _subject: &str) -> Result<WireStream> {
        let mut receiver = self.broadcast_tx.subscribe();
        let socket_pump_handle = Arc::clone(&self.socket_pump_handle);

        let stream = stream! {
            // Keep the socket pump alive after the transport is dropped. The
            // final transport or subscription stream aborts the pump, which
            // drops its owned ZMQ socket instead of detaching the task.
            let _socket_pump_handle = socket_pump_handle;
            loop {
                match receiver.recv().await {
                    Ok(payload) => yield Ok(payload),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped = skipped, "Subscriber lagged behind, skipped messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        tracing::info!("Broadcast channel closed");
                        break;
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn kind(&self) -> EventTransportKind {
        EventTransportKind::Zmq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transports::event_plane::{EventEnvelope, MsgpackCodec};
    use crate::transports::zmq::ipv6_option_for;
    use std::collections::HashSet;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn unbracketed_ipv6_endpoints_fail_before_connecting() {
        let error = ZmqSubTransport::connect_broker("tcp://::1:5555", "topic")
            .await
            .err()
            .expect("unbracketed IPv6 endpoint should be rejected");
        assert!(
            error
                .to_string()
                .contains("IPv6 addresses must be bracketed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn failed_additional_connection_names_endpoint() {
        let endpoint = format!("inproc://dynamo-zmq-connect-context-{}", std::process::id());
        let subscriber = DynamicZmqSubSocket::connect_with_rcvhwm(&endpoint, "topic", ZMQ_RCVHWM)
            .await
            .unwrap();
        let error = subscriber
            .add_endpoint("tcp://missing-port")
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("tcp://missing-port"),
            "{error:#}"
        );
    }

    async fn send_raw(publisher: &ZmqPubTransport, frames: Vec<Vec<u8>>) {
        publisher
            .socket
            .send(Message::multipart(frames))
            .await
            .unwrap();
    }

    fn encoded_event(topic: &str, publisher_id: u64, sequence: u64) -> Bytes {
        MsgpackCodec
            .encode_envelope(&EventEnvelope {
                publisher_id,
                sequence,
                published_at: sequence,
                topic: topic.to_string(),
                payload: Bytes::from_static(b"payload"),
            })
            .unwrap()
    }

    async fn receive_publishers(
        subscriber: &DynamicZmqSubSocket,
        topic: &str,
        sequence: u64,
        expected: usize,
        publications: &[(&ZmqPubTransport, &Bytes)],
    ) -> HashSet<u64> {
        timeout(Duration::from_secs(2), async {
            let mut seen = HashSet::new();
            while seen.len() < expected {
                for (publisher, payload) in publications {
                    publisher.publish(topic, (*payload).clone()).await.unwrap();
                }
                if let Ok(Some(Ok(message))) =
                    timeout(Duration::from_millis(25), subscriber.next()).await
                    && message.sequence == sequence
                {
                    seen.insert(message.publisher_id);
                }
            }
            seen
        })
        .await
        .expect("dynamic subscriber should receive expected publishers")
    }

    #[tokio::test]
    async fn test_zmq_pubsub_basic() {
        let topic = "test-topic";

        let (publisher, endpoint) = ZmqPubTransport::bind("tcp://127.0.0.1:0", topic)
            .await
            .expect("Failed to create publisher");
        assert!(endpoint.starts_with("tcp://127.0.0.1:"));

        tokio::time::sleep(Duration::from_millis(100)).await;

        let subscriber = ZmqSubTransport::connect(&endpoint, topic)
            .await
            .expect("Failed to create subscriber");

        let mut stream = subscriber
            .subscribe(topic)
            .await
            .expect("Failed to create subscription");

        // Broker-mode callers retain only the returned stream. It must keep the
        // socket pump alive after the transport itself leaves scope.
        drop(subscriber);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let codec = MsgpackCodec;
        let envelope = EventEnvelope {
            publisher_id: 12345,
            sequence: 1,
            published_at: 1700000000000,
            topic: topic.to_string(),
            payload: Bytes::from("test payload"),
        };

        let envelope_bytes = codec.encode_envelope(&envelope).unwrap();
        publisher.publish(topic, envelope_bytes).await.unwrap();

        let result = timeout(Duration::from_secs(2), stream.next()).await;
        assert!(result.is_ok(), "Timeout waiting for message");

        let received_bytes = result.unwrap().unwrap().unwrap();
        let decoded = codec.decode_envelope(&received_bytes).unwrap();

        assert_eq!(decoded.publisher_id, 12345);
        assert_eq!(decoded.sequence, 1);
        assert_eq!(decoded.topic, topic);
    }

    #[tokio::test]
    async fn single_consumer_applies_explicit_receive_hwm() {
        let endpoint = format!("inproc://dynamo-zmq-explicit-hwm-{}", std::process::id());
        let topic = "explicit-hwm";
        let (_publisher, _) = ZmqPubTransport::bind(&endpoint, topic).await.unwrap();

        ZmqSubTransport::connect_socket(&endpoint, topic, 37)
            .await
            .unwrap();
        assert!(
            ZmqSubTransport::connect_single_consumer_with_rcvhwm(&endpoint, topic, 0)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn validated_source_rejects_bad_envelopes_and_continues_zero_copy() {
        let codec = Codec::default();
        let topic = "validated-source";
        let publisher_id = 7;
        let invalid = ZmqWireMessage {
            publisher_id,
            sequence: 0,
            payload: Bytes::from_static(&[0xc1]),
        };
        let mismatched_payload = codec
            .encode_envelope_parts(8, 1, 11, topic, b"mismatch")
            .unwrap();
        let mismatch = ZmqWireMessage {
            publisher_id,
            sequence: 1,
            payload: mismatched_payload,
        };
        let encoded = codec
            .encode_envelope_parts(publisher_id, 2, 12, topic, b"payload")
            .unwrap();
        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();
        let valid = ZmqWireMessage {
            publisher_id,
            sequence: 2,
            payload: encoded,
        };
        let mut source = ValidatedZmqSource {
            stream: Box::pin(futures::stream::iter(vec![
                Ok(invalid),
                Ok(mismatch),
                Ok(valid),
            ])),
            expected_topic: topic.to_string(),
            expected_publisher_id: publisher_id,
            codec,
        };

        assert!(matches!(
            source.next().await.unwrap(),
            Err(ValidatedZmqSourceError::EnvelopeDecode(_))
        ));
        assert!(matches!(
            source.next().await.unwrap(),
            Err(ValidatedZmqSourceError::IdentityMismatch { .. })
        ));
        let envelope = source.next().await.unwrap().unwrap();
        assert_eq!(envelope.publisher_id, publisher_id);
        assert_eq!(envelope.sequence, 2);
        assert_eq!(envelope.published_at, 12);
        assert_eq!(envelope.payload, Bytes::from_static(b"payload"));
        let payload_ptr = envelope.payload.as_ptr() as usize;
        assert!((encoded_start..encoded_end).contains(&payload_ptr));
        assert!(source.next().await.is_none());
    }

    #[test]
    fn omq_wire_decode_preserves_payload_storage() {
        let framed = Frame::new(Bytes::from(vec![7; 512])).encode();
        let expected = Frame::decode(framed.clone()).unwrap().payload;
        let frames = omq_tokio::Message::multipart([
            Bytes::from_static(b"kv"),
            Bytes::copy_from_slice(&7u64.to_be_bytes()),
            Bytes::copy_from_slice(&12u64.to_be_bytes()),
            framed,
        ]);

        let message = decode_multipart(frames, b"kv").unwrap();
        assert_eq!(message.publisher_id, 7);
        assert_eq!(message.sequence, 12);
        assert_eq!(message.payload, expected);
        assert_eq!(message.payload.as_ptr(), expected.as_ptr());
    }

    #[tokio::test]
    async fn tcp_source_validates_publisher_frames() {
        let topic = "kv";
        let (publisher, endpoint) = ZmqPubTransport::bind("tcp://127.0.0.1:*", topic)
            .await
            .unwrap();
        let endpoint = endpoint.replace("127.0.0.1", "localhost");
        assert!(
            ValidatedZmqSource::connect(&endpoint, topic, 7, 0)
                .await
                .is_err()
        );
        let mut source = ValidatedZmqSource::connect(&endpoint, topic, 7, 37)
            .await
            .unwrap();
        let codec = Codec::default();
        let anchor = codec
            .encode_envelope_parts(7, 0, 11, topic, b"anchor")
            .unwrap();

        let first = timeout(Duration::from_secs(3), async {
            loop {
                publisher.publish(topic, anchor.clone()).await.unwrap();
                if let Ok(message) = timeout(Duration::from_millis(25), source.next()).await {
                    break message.unwrap().unwrap();
                }
            }
        })
        .await
        .expect("subscriber should receive a publication");
        assert_eq!(first.payload, Bytes::from_static(b"anchor"));

        let valid = codec
            .encode_envelope_parts(7, 2, 12, topic, b"valid")
            .unwrap();
        let mismatch = codec
            .encode_envelope_parts(8, 1, 12, topic, b"mismatch")
            .unwrap();
        send_raw(&publisher, vec![topic.as_bytes().to_vec()]).await;
        send_raw(
            &publisher,
            vec![
                b"kv-prefix".to_vec(),
                7u64.to_be_bytes().to_vec(),
                2u64.to_be_bytes().to_vec(),
                Frame::new(valid.clone()).encode().to_vec(),
            ],
        )
        .await;
        send_raw(
            &publisher,
            vec![
                topic.as_bytes().to_vec(),
                7u64.to_be_bytes().to_vec(),
                1u64.to_be_bytes().to_vec(),
                Frame::new(mismatch).encode().to_vec(),
            ],
        )
        .await;
        publisher.publish(topic, valid).await.unwrap();

        timeout(Duration::from_secs(3), async {
            loop {
                match source.next().await.unwrap() {
                    Ok(envelope) if envelope.sequence == 0 => continue,
                    Err(ValidatedZmqSourceError::IdentityMismatch { .. }) => break,
                    result => panic!("expected a publisher identity mismatch, got {result:?}"),
                }
            }
            let envelope = source.next().await.unwrap().unwrap();
            assert_eq!(
                (
                    envelope.publisher_id,
                    envelope.sequence,
                    envelope.published_at
                ),
                (7, 2, 12)
            );
            assert_eq!(envelope.payload, Bytes::from_static(b"valid"));
        })
        .await
        .expect("valid TCP stream should continue after rejected frames");
    }

    #[tokio::test]
    async fn single_consumer_preserves_wire_identity_and_exact_topic() {
        let endpoint = format!("inproc://dynamo-zmq-single-consumer-{}", std::process::id());
        let topic = "single-consumer";
        let (publisher, _) = ZmqPubTransport::bind(&endpoint, topic).await.unwrap();
        let mut stream = ZmqSubTransport::connect_single_consumer(&endpoint, topic)
            .await
            .unwrap();
        let codec = MsgpackCodec;
        let anchor = EventEnvelope {
            publisher_id: 41,
            sequence: 1,
            published_at: 1,
            topic: topic.to_string(),
            payload: Bytes::from_static(b"anchor"),
        };
        let anchor_bytes = codec.encode_envelope(&anchor).unwrap();

        let wire = timeout(Duration::from_secs(2), async {
            loop {
                publisher
                    .publish(topic, anchor_bytes.clone())
                    .await
                    .unwrap();
                if let Ok(Some(Ok(message))) =
                    timeout(Duration::from_millis(25), stream.next()).await
                {
                    break message;
                }
            }
        })
        .await
        .expect("single-consumer socket should become ready");
        assert_eq!(wire.publisher_id, anchor.publisher_id);
        assert_eq!(wire.sequence, anchor.sequence);
        assert_eq!(
            codec.decode_envelope(&wire.payload).unwrap().payload,
            anchor.payload
        );

        let sentinel = EventEnvelope {
            publisher_id: 41,
            sequence: 2,
            published_at: 2,
            topic: topic.to_string(),
            payload: Bytes::from_static(b"sentinel"),
        };
        let sentinel_bytes = codec.encode_envelope(&sentinel).unwrap();
        let framed = Frame::new(sentinel_bytes.clone()).encode().to_vec();
        send_raw(
            &publisher,
            vec![
                format!("{topic}-prefix-collision").into_bytes(),
                sentinel.publisher_id.to_be_bytes().to_vec(),
                sentinel.sequence.to_be_bytes().to_vec(),
                framed,
            ],
        )
        .await;
        publisher.publish(topic, sentinel_bytes).await.unwrap();

        let wire = timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("valid event should follow an exact-topic rejection")
            .expect("single-consumer stream should remain open")
            .expect("valid event should decode");
        assert_eq!(wire.publisher_id, sentinel.publisher_id);
        assert_eq!(wire.sequence, sentinel.sequence);
        assert_eq!(
            codec.decode_envelope(&wire.payload).unwrap().payload,
            sentinel.payload
        );
    }

    #[tokio::test]
    async fn broker_hostname_connections_reach_ipv4_peers() {
        let topic = "broker-hostname";
        let xsub = new_socket(SocketType::XSub, Options::default()).unwrap();
        let xpub = new_socket(SocketType::XPub, Options::default()).unwrap();
        let xsub_endpoint = xsub
            .bind("tcp://127.0.0.1:0")
            .await
            .unwrap()
            .to_string()
            .replace("127.0.0.1", "localhost");
        let xpub_endpoint = xpub
            .bind("tcp://127.0.0.1:0")
            .await
            .unwrap()
            .to_string()
            .replace("127.0.0.1", "localhost");
        let publisher = ZmqPubTransport::connect(&xsub_endpoint, topic)
            .await
            .unwrap();
        let mut publisher_monitor = publisher.socket.monitor();
        let subscriber = ZmqSubTransport::connect_broker(&xpub_endpoint, topic)
            .await
            .unwrap();
        let mut stream = subscriber.subscribe(topic).await.unwrap();
        let encoded = encoded_event(topic, 101, 1);

        timeout(Duration::from_secs(5), async {
            let subscription = xpub.recv().await.unwrap();
            assert_eq!(&subscription[0], b"\x01broker-hostname");
            xsub.send(subscription).await.unwrap();
            loop {
                if let omq_tokio::MonitorEvent::SubscribeReceived { prefix, .. } =
                    publisher_monitor.recv().await.unwrap()
                {
                    assert_eq!(prefix.as_ref(), topic.as_bytes());
                    break;
                }
            }
            publisher.publish(topic, encoded.clone()).await.unwrap();
            xpub.send(xsub.recv().await.unwrap()).await.unwrap();
            assert_eq!(stream.next().await.unwrap().unwrap(), encoded);
        })
        .await
        .expect("hostname connections should deliver through IPv4-only peers");
    }

    #[rstest::rstest]
    #[case("inproc", "inproc")]
    #[case("tcp://127.0.0.1:0", "tcp://[::1]:0")]
    #[case("tcp://[::1]:0", "tcp://127.0.0.1:0")]
    #[tokio::test]
    async fn dynamic_sub_socket_adds_and_removes_publishers(
        #[case] endpoint_a: &str,
        #[case] endpoint_b: &str,
    ) {
        if (ipv6_option_for(endpoint_a).unwrap() || ipv6_option_for(endpoint_b).unwrap())
            && let Err(error) = std::net::TcpListener::bind("[::1]:0")
        {
            eprintln!("Skipping mixed IPv4/IPv6 subscriber test: {error}");
            return;
        }
        let process = std::process::id();
        let (endpoint_a, endpoint_b) = if endpoint_a == "inproc" {
            (
                format!("inproc://dynamo-zmq-dynamic-a-{process}"),
                format!("inproc://dynamo-zmq-dynamic-b-{process}"),
            )
        } else {
            (endpoint_a.to_string(), endpoint_b.to_string())
        };
        let topic = "dynamic-subscriber";
        let (publisher_a, endpoint_a) = ZmqPubTransport::bind(&endpoint_a, topic).await.unwrap();
        let (publisher_b, endpoint_b) = ZmqPubTransport::bind(&endpoint_b, topic).await.unwrap();
        let endpoint_a = endpoint_a.replace("127.0.0.1", "localhost");
        let endpoint_b = endpoint_b.replace("127.0.0.1", "localhost");
        let subscriber = DynamicZmqSubSocket::connect_with_rcvhwm(&endpoint_a, topic, ZMQ_RCVHWM)
            .await
            .unwrap();
        subscriber.add_endpoint(&endpoint_b).await.unwrap();

        let encoded_a = encoded_event(topic, 101, 1);
        let encoded_b = encoded_event(topic, 202, 1);
        let seen = receive_publishers(
            &subscriber,
            topic,
            1,
            2,
            &[(&publisher_a, &encoded_a), (&publisher_b, &encoded_b)],
        )
        .await;
        assert_eq!(seen, HashSet::from([101, 202]));

        subscriber.remove_endpoint(&endpoint_a).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let encoded_a_after_removal = encoded_event(topic, 101, 2);
        let encoded_b_after_removal = encoded_event(topic, 202, 2);
        let publications = [
            (&publisher_a, &encoded_a_after_removal),
            (&publisher_b, &encoded_b_after_removal),
        ];
        assert_eq!(
            receive_publishers(&subscriber, topic, 2, 1, &publications).await,
            HashSet::from([202])
        );

        let removed_delivery = timeout(Duration::from_millis(250), async {
            loop {
                publisher_a
                    .publish(topic, encoded_a_after_removal.clone())
                    .await
                    .unwrap();
                if let Some(Ok(message)) = subscriber.next().await
                    && message.publisher_id == 101
                    && message.sequence == 2
                {
                    return;
                }
            }
        })
        .await;
        assert!(
            removed_delivery.is_err(),
            "removed publisher delivered data"
        );
    }

    #[tokio::test]
    async fn test_zmq_socket_pump_stops_with_last_owner() {
        let endpoint = format!("inproc://dynamo-zmq-pump-lifetime-{}", std::process::id());
        let topic = "pump-lifetime";

        let (_publisher, _) = ZmqPubTransport::bind(&endpoint, topic).await.unwrap();
        let subscriber = ZmqSubTransport::connect(&endpoint, topic).await.unwrap();
        let pump_handle = subscriber.socket_pump_handle.abort_handle();
        let stream = subscriber.subscribe(topic).await.unwrap();

        drop(subscriber);
        tokio::task::yield_now().await;
        assert!(
            !pump_handle.is_finished(),
            "subscription stream should keep the socket pump alive"
        );

        drop(stream);
        timeout(Duration::from_secs(1), async {
            while !pump_handle.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("socket pump should stop when its final owner is dropped");
    }

    #[tokio::test]
    async fn test_zmq_multiple_messages() {
        let port = 25556;
        let endpoint = format!("tcp://127.0.0.1:{port}");
        let topic = "multi-test";

        let (publisher, _) = ZmqPubTransport::bind(&endpoint, topic).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let subscriber = ZmqSubTransport::connect(&endpoint, topic).await.unwrap();
        let mut stream = subscriber.subscribe(topic).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let codec = MsgpackCodec;

        for i in 0..5 {
            let envelope = EventEnvelope {
                publisher_id: 99999,
                sequence: i,
                published_at: 1700000000000 + i,
                topic: topic.to_string(),
                payload: Bytes::from(format!("message {i}")),
            };

            let bytes = codec.encode_envelope(&envelope).unwrap();
            publisher.publish(topic, bytes).await.unwrap();
        }

        for i in 0..5 {
            let result = timeout(Duration::from_secs(2), stream.next()).await;
            assert!(result.is_ok(), "Timeout on message {i}");

            let received = result.unwrap().unwrap().unwrap();
            let decoded = codec.decode_envelope(&received).unwrap();
            assert_eq!(decoded.sequence, i);
            assert_eq!(decoded.topic, topic);
        }
    }

    #[tokio::test]
    async fn test_zmq_socket_pump_continues_after_malformed_messages() {
        let endpoint = format!("inproc://dynamo-zmq-malformed-{}", std::process::id());
        let topic = "malformed-test";

        let (publisher, _) = ZmqPubTransport::bind(&endpoint, topic).await.unwrap();
        let subscriber = ZmqSubTransport::connect(&endpoint, topic).await.unwrap();
        let mut stream = subscriber.subscribe(topic).await.unwrap();

        let codec = MsgpackCodec;
        let anchor = EventEnvelope {
            publisher_id: 12345,
            sequence: 0,
            published_at: 1700000000000,
            topic: topic.to_string(),
            payload: Bytes::from_static(b"anchor"),
        };
        let anchor_bytes = codec.encode_envelope(&anchor).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let received_anchor = loop {
            publisher
                .publish(topic, anchor_bytes.clone())
                .await
                .unwrap();
            if let Ok(Some(Ok(bytes))) = timeout(Duration::from_millis(25), stream.next()).await {
                break bytes;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timeout waiting for subscriber readiness anchor"
            );
        };
        assert_eq!(
            codec.decode_envelope(&received_anchor).unwrap().payload,
            anchor.payload
        );

        let topic_frame = topic.as_bytes().to_vec();
        let publisher_frame = 12345_u64.to_be_bytes().to_vec();
        let sequence_frame = 1_u64.to_be_bytes().to_vec();
        let empty_frame = Frame::new(Bytes::new()).encode().to_vec();

        send_raw(
            &publisher,
            vec![
                topic_frame.clone(),
                publisher_frame.clone(),
                sequence_frame.clone(),
            ],
        )
        .await;
        send_raw(
            &publisher,
            vec![
                topic_frame.clone(),
                publisher_frame.clone(),
                sequence_frame.clone(),
                empty_frame.clone(),
                b"extra".to_vec(),
            ],
        )
        .await;
        send_raw(
            &publisher,
            vec![
                topic_frame.clone(),
                vec![0; 7],
                sequence_frame.clone(),
                empty_frame.clone(),
            ],
        )
        .await;
        send_raw(
            &publisher,
            vec![
                topic_frame.clone(),
                publisher_frame.clone(),
                vec![0; 7],
                empty_frame,
            ],
        )
        .await;
        send_raw(
            &publisher,
            vec![
                topic_frame,
                publisher_frame,
                sequence_frame,
                vec![99, 0, 0, 0, 0],
            ],
        )
        .await;

        let sentinel = EventEnvelope {
            publisher_id: 12345,
            sequence: 2,
            published_at: 1700000000000,
            topic: topic.to_string(),
            payload: Bytes::from_static(b"sentinel"),
        };
        publisher
            .publish(topic, codec.encode_envelope(&sentinel).unwrap())
            .await
            .unwrap();

        let decoded = timeout(Duration::from_secs(2), async {
            loop {
                let received = stream.next().await.unwrap().unwrap();
                let decoded = codec.decode_envelope(&received).unwrap();
                if decoded.sequence == sentinel.sequence {
                    break decoded;
                }
            }
        })
        .await
        .expect("timeout waiting for valid message after malformed messages");
        assert_eq!(decoded.publisher_id, sentinel.publisher_id);
        assert_eq!(decoded.sequence, sentinel.sequence);
        assert_eq!(decoded.payload, sentinel.payload);
    }
}
