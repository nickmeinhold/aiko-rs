//! Frame serialization for network transport.
//!
//! Re-exports shared codec types from `aiko-core` and provides
//! WebRTC-specific data types.

// Re-export shared codec types from aiko-core
pub use aiko_core::codec::*;

use serde::{Deserialize, Serialize};

/// Audio sample data for media pipelines.
///
/// Deprecated: prefer `aiko_core::media::AudioFrame` for new code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioSample {
    pub sample_rate: u32,
    pub channels: u16,
    pub data: Vec<u8>,
}

impl AudioSample {
    /// Create a new audio sample.
    pub fn new(sample_rate: u32, channels: u16, data: Vec<u8>) -> Self {
        Self {
            sample_rate,
            channels,
            data,
        }
    }
}

impl NetworkSerializable for AudioSample {
    fn type_name() -> &'static str {
        "audio_sample"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aiko_core::frame::{FrameId, StreamId};

    #[test]
    fn test_frame_envelope_roundtrip() {
        let frame =
            aiko_core::frame::Frame::new(StreamId::new(), FrameId(42), "hello world".to_string());

        let envelope = FrameEnvelope::from_frame(frame).unwrap();
        assert_eq!(envelope.payload_type, "string");

        let bytes = envelope.to_bytes().unwrap();
        let envelope2 = FrameEnvelope::from_bytes(&bytes).unwrap();

        let recovered: aiko_core::frame::Frame<String> = envelope2.into_frame().unwrap();
        assert_eq!(recovered.payload, "hello world");
        assert_eq!(recovered.frame_id(), FrameId(42));
    }

    #[test]
    fn test_audio_sample() {
        let sample = AudioSample::new(48000, 2, vec![0u8; 960]);
        assert_eq!(sample.sample_rate, 48000);
        assert_eq!(sample.channels, 2);
        assert_eq!(sample.data.len(), 960);
    }
}
