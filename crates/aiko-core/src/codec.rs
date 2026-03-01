//! Frame serialization for network transport.
//!
//! Provides the shared codec abstraction used by both `aiko-mqtt` and `aiko-webrtc`
//! to serialize typed [`Frame`]s for transmission over the network.

use crate::frame::{Frame, FrameMetadata};
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
///
/// Implementors provide a unique `type_name()` used to tag serialized payloads
/// so the receiver can verify it is deserializing the correct type.
///
/// # Example
///
/// ```rust
/// use serde::{Serialize, Deserialize};
/// use aiko_core::codec::NetworkSerializable;
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct SensorReading {
///     temperature: f64,
///     humidity: f64,
/// }
///
/// impl NetworkSerializable for SensorReading {
///     fn type_name() -> &'static str {
///         "sensor_reading"
///     }
/// }
/// ```
pub trait NetworkSerializable:
    Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static
{
    /// Return the type name for identification.
    fn type_name() -> &'static str;
}

/// Serializable frame envelope for network transport.
///
/// Wraps a typed [`Frame<T>`] into a type-erased binary representation that can
/// be sent over MQTT topics, WebRTC data channels, or any byte-oriented transport.
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

impl NetworkSerializable for i32 {
    fn type_name() -> &'static str {
        "i32"
    }
}

impl NetworkSerializable for i64 {
    fn type_name() -> &'static str {
        "i64"
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{FrameId, StreamId};

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
    fn test_type_mismatch_error() {
        let frame = Frame::new(StreamId::new(), FrameId(0), 42i32);
        let envelope = FrameEnvelope::from_frame(frame).unwrap();

        let result = envelope.into_frame::<String>();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CodecError::TypeMismatch { .. }));
    }

    #[test]
    fn test_bytes_roundtrip() {
        let data = vec![1u8, 2, 3, 4, 5];
        let frame = Frame::new(StreamId::new(), FrameId(0), data.clone());
        let envelope = FrameEnvelope::from_frame(frame).unwrap();
        let recovered: Frame<Vec<u8>> = envelope.into_frame().unwrap();
        assert_eq!(recovered.payload, data);
    }

    // Note: serde_json::Value cannot roundtrip through bincode (bincode doesn't
    // support deserialize_any). The NetworkSerializable impl for Value is useful
    // with self-describing formats but not tested here.
}
