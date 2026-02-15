//! Signaling interface for WebRTC connection establishment.
//!
//! [`SignalingClient`] is a trait that allows pluggable signaling implementations.
//! A built-in WebSocket implementation ([`WsSignalingClient`]) is provided.

use crate::error::{Result, WebRtcError};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

/// A signaling message exchanged between peers to establish a WebRTC connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SignalingMessage {
    /// SDP offer from the offerer.
    #[serde(rename = "offer")]
    Offer { sdp: String },

    /// SDP answer from the answerer.
    #[serde(rename = "answer")]
    Answer { sdp: String },

    /// ICE candidate for connectivity checks.
    #[serde(rename = "ice_candidate")]
    IceCandidate {
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    },
}

/// Trait for signaling channel implementations.
///
/// Signaling is used to exchange SDP offers/answers and ICE candidates
/// between peers before the direct WebRTC connection is established.
#[async_trait]
pub trait SignalingClient: Send + Sync {
    /// Send a signaling message to the remote peer.
    async fn send(&self, msg: SignalingMessage) -> Result<()>;

    /// Receive the next signaling message from the remote peer.
    async fn recv(&self) -> Result<SignalingMessage>;
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// WebSocket-based signaling client.
///
/// Connects to a WebSocket signaling server that relays messages
/// between peers.
pub struct WsSignalingClient {
    write: Mutex<futures::stream::SplitSink<WsStream, Message>>,
    read: Mutex<futures::stream::SplitStream<WsStream>>,
}

impl WsSignalingClient {
    /// Connect to a WebSocket signaling server.
    pub async fn connect(url: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| WebRtcError::Signaling(e.to_string()))?;

        let (write, read) = ws_stream.split();
        Ok(Self {
            write: Mutex::new(write),
            read: Mutex::new(read),
        })
    }
}

#[async_trait]
impl SignalingClient for WsSignalingClient {
    async fn send(&self, msg: SignalingMessage) -> Result<()> {
        let json = serde_json::to_string(&msg)?;
        self.write
            .lock()
            .await
            .send(Message::Text(json))
            .await
            .map_err(|e| WebRtcError::Signaling(e.to_string()))?;
        Ok(())
    }

    async fn recv(&self) -> Result<SignalingMessage> {
        let mut read = self.read.lock().await;
        loop {
            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    let msg: SignalingMessage = serde_json::from_str(&text)?;
                    return Ok(msg);
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err(WebRtcError::ChannelClosed);
                }
                Some(Err(e)) => {
                    return Err(WebRtcError::Signaling(e.to_string()));
                }
                _ => continue, // Skip ping/pong/binary
            }
        }
    }
}
