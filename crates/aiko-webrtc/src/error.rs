//! Error types for WebRTC transport.

use thiserror::Error;

/// Errors that can occur with WebRTC operations.
#[derive(Error, Debug)]
pub enum WebRtcError {
    #[error("WebRTC error: {0}")]
    WebRtc(#[from] webrtc::Error),

    #[error("Signaling error: {0}")]
    Signaling(String),

    #[error("Codec error: {0}")]
    Codec(#[from] crate::codec::CodecError),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Decoding error: {0}")]
    Decoding(String),

    #[error("Track error: {0}")]
    Track(String),

    #[error("Data channel error: {0}")]
    DataChannel(String),

    #[error("Operation timed out: {0}")]
    Timeout(String),

    #[error("Channel closed")]
    ChannelClosed,

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type alias for WebRTC operations.
pub type Result<T> = std::result::Result<T, WebRtcError>;
