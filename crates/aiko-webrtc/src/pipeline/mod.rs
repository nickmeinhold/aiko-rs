//! Pipeline elements for WebRTC media streams.
//!
//! Bridges the pipeline framework with WebRTC transport, providing
//! [`SinkElement`] and [`SourceElement`] implementations that encode/decode
//! media for WebRTC tracks.
//!
//! # Feature flags
//!
//! - `video` — Enables [`WebRtcVideoSink`] and [`WebRtcVideoSource`] (requires `openh264`)
//! - `audio` — Enables [`WebRtcAudioSink`] and [`WebRtcAudioSource`] (requires `audiopus` / libopus)

#[cfg(feature = "video")]
mod video_sink;

#[cfg(feature = "video")]
mod video_source;

#[cfg(feature = "video")]
mod h264;

#[cfg(feature = "audio")]
mod audio_sink;

#[cfg(feature = "audio")]
mod audio_source;

#[cfg(feature = "video")]
pub use video_sink::{WebRtcVideoSink, WebRtcVideoSinkConfig};

#[cfg(feature = "video")]
pub use video_source::WebRtcVideoSource;

#[cfg(feature = "audio")]
pub use audio_sink::{OpusApplication, WebRtcAudioSink, WebRtcAudioSinkConfig};

#[cfg(feature = "audio")]
pub use audio_source::WebRtcAudioSource;
