//! Pipeline elements for WebRTC media streams.
//!
//! Bridges the pipeline framework with WebRTC transport, providing
//! [`SinkElement`] and [`SourceElement`] implementations that encode/decode
//! media for WebRTC tracks.
//!
//! # Feature flags
//!
//! - `video` — Enables [`WebRtcVideoSink`] and [`WebRtcVideoSource`] (requires `openh264`)

#[cfg(feature = "video")]
mod video_sink;

#[cfg(feature = "video")]
mod video_source;

#[cfg(feature = "video")]
mod h264;

#[cfg(feature = "video")]
pub use video_sink::{WebRtcVideoSink, WebRtcVideoSinkConfig};

#[cfg(feature = "video")]
pub use video_source::WebRtcVideoSource;
