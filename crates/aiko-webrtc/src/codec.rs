//! Frame serialization for network transport.
//!
//! Duplicated from `aiko-mqtt` to avoid cross-transport dependency.
//! A future refactor could move these types to `aiko-core`.

use aiko_core::frame::{Frame, FrameMetadata};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur during codec operations.
#[derive(Error, Debug)]
pub enum CodecError {
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Invalid payload")]
    InvalidPayload,
}

/// Trait for types that can be serialized for network transport.
pub trait NetworkSerializable:
    Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static
{
    /// Return the type name for identification.
    fn type_name() -> &'static str;
}

/// Serializable frame envelope for network transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameEnvelope {
    pub metadata: FrameMetadata,
    pub payload_type: String,
    pub payload: Vec<u8>,
}

impl FrameEnvelope {
    /// Create an envelope from a typed frame.
    pub fn from_frame<T: NetworkSerializable>(frame: Frame<T>) -> Result<Self, CodecError> {
        let payload = bincode::serialize(&frame.payload)?;
        Ok(Self {
            metadata: frame.metadata,
            payload_type: T::type_name().to_string(),
            payload,
        })
    }

    /// Attempt to deserialize into a typed frame.
    pub fn into_frame<T: NetworkSerializable>(self) -> Result<Frame<T>, CodecError> {
        if self.payload_type != T::type_name() {
            return Err(CodecError::TypeMismatch {
                expected: T::type_name().to_string(),
                actual: self.payload_type,
            });
        }
        let payload: T = bincode::deserialize(&self.payload)?;
        Ok(Frame {
            metadata: self.metadata,
            payload,
        })
    }

    /// Serialize the envelope to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        Ok(bincode::serialize(self)?)
    }

    /// Deserialize an envelope from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(bincode::deserialize(bytes)?)
    }
}

// Implement NetworkSerializable for common types

impl NetworkSerializable for Vec<u8> {
    fn type_name() -> &'static str {
        "bytes"
    }
}

impl NetworkSerializable for String {
    fn type_name() -> &'static str {
        "string"
    }
}

impl NetworkSerializable for f32 {
    fn type_name() -> &'static str {
        "f32"
    }
}

impl NetworkSerializable for f64 {
    fn type_name() -> &'static str {
        "f64"
    }
}

impl NetworkSerializable for serde_json::Value {
    fn type_name() -> &'static str {
        "json"
    }
}

/// Audio sample data for media pipelines.
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
        let frame = Frame::new(StreamId::new(), FrameId(42), "hello world".to_string());

        let envelope = FrameEnvelope::from_frame(frame).unwrap();
        assert_eq!(envelope.payload_type, "string");

        let bytes = envelope.to_bytes().unwrap();
        let envelope2 = FrameEnvelope::from_bytes(&bytes).unwrap();

        let recovered: Frame<String> = envelope2.into_frame().unwrap();
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
