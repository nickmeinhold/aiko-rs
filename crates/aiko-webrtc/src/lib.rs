//! Aiko WebRTC - WebRTC transport layer for the Aiko distributed pipeline framework.
//!
//! This crate provides WebRTC-based peer-to-peer communication:
//!
//! - [`WebRtcTransport`](transport::WebRtcTransport) - WebRTC client wrapper
//! - [`SignalingClient`](signaling::SignalingClient) - Pluggable signaling interface
//! - [`FrameEnvelope`](codec::FrameEnvelope) - Frame serialization for network transport
//! - Data channels for typed message exchange
//! - Media tracks for audio/video streams
//!
//! # Example
//!
//! ```rust,ignore
//! use aiko_webrtc::prelude::*;
//! use aiko_webrtc::signaling::WsSignalingClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = WebRtcConfig::new()
//!         .with_role(PeerRole::Offerer);
//!
//!     let signaling = Box::new(WsSignalingClient::connect("ws://localhost:8080").await?);
//!     let (transport, event_loop) = WebRtcTransport::connect(config, signaling).await?;
//!
//!     // Run event loop in background
//!     tokio::spawn(event_loop.run());
//!
//!     // Open a data channel and send messages
//!     transport.open_channel("chat").await?;
//!     transport.send("chat", b"hello").await?;
//!
//!     Ok(())
//! }
//! ```

pub mod codec;
pub mod config;
pub mod data_channel;
pub mod error;
pub mod media;
pub mod peer;
#[cfg(any(feature = "video", feature = "audio"))]
pub mod pipeline;
pub mod reconnect;
pub mod signaling;
pub mod transport;

/// Convenient re-exports of commonly used types.
pub mod prelude {
    pub use crate::codec::{AudioSample, CodecError, FrameEnvelope, NetworkSerializable};
    pub use crate::config::{IceServer, PeerRole, WebRtcConfig};
    pub use crate::data_channel::IncomingMessage;
    pub use crate::error::WebRtcError;
    pub use crate::media::{LocalTrack, LocalTrackConfig, MediaKind, RemoteTrack};
    pub use crate::peer::{PeerEvent, PeerState};
    pub use crate::signaling::{SignalingClient, SignalingMessage};
    pub use crate::transport::{WebRtcEventLoop, WebRtcTransport};
}
