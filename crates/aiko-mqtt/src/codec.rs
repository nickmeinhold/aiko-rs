//! Frame serialization for network transport.

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

/// Image data type for ML pipelines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageData {
    pub width: u32,
    pub height: u32,
    pub channels: u8,
    pub format: ImageFormat,
    pub data: Vec<u8>,
}

impl ImageData {
    /// Create a new image with the given dimensions.
    pub fn new(width: u32, height: u32, channels: u8, format: ImageFormat) -> Self {
        let size = (width * height * channels as u32) as usize;
        Self {
            width,
            height,
            channels,
            format,
            data: vec![0u8; size],
        }
    }

    /// Create an image from raw data.
    pub fn from_raw(
        width: u32,
        height: u32,
        channels: u8,
        format: ImageFormat,
        data: Vec<u8>,
    ) -> Self {
        Self {
            width,
            height,
            channels,
            format,
            data,
        }
    }
}

impl NetworkSerializable for ImageData {
    fn type_name() -> &'static str {
        "image"
    }
}

/// Image format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageFormat {
    Rgb8,
    Rgba8,
    Gray8,
    Bgr8,
}

/// Detection results from ML models.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Detections {
    pub boxes: Vec<BoundingBox>,
    pub scores: Vec<f32>,
    pub class_ids: Vec<u32>,
    pub class_names: Vec<String>,
}

impl Detections {
    /// Create empty detections.
    pub fn empty() -> Self {
        Self {
            boxes: Vec::new(),
            scores: Vec::new(),
            class_ids: Vec::new(),
            class_names: Vec::new(),
        }
    }

    /// Add a detection.
    pub fn add(
        &mut self,
        bbox: BoundingBox,
        score: f32,
        class_id: u32,
        class_name: impl Into<String>,
    ) {
        self.boxes.push(bbox);
        self.scores.push(score);
        self.class_ids.push(class_id);
        self.class_names.push(class_name.into());
    }

    /// Get the number of detections.
    pub fn len(&self) -> usize {
        self.boxes.len()
    }

    /// Check if there are no detections.
    pub fn is_empty(&self) -> bool {
        self.boxes.is_empty()
    }
}

impl NetworkSerializable for Detections {
    fn type_name() -> &'static str {
        "detections"
    }
}

/// Bounding box for object detection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BoundingBox {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

impl BoundingBox {
    /// Create a new bounding box.
    pub fn new(x: f32, y: f32, width: f32, height: f32) -> Self {
        Self {
            x,
            y,
            width,
            height,
        }
    }

    /// Get the center point.
    pub fn center(&self) -> (f32, f32) {
        (self.x + self.width / 2.0, self.y + self.height / 2.0)
    }

    /// Get the area.
    pub fn area(&self) -> f32 {
        self.width * self.height
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
    fn test_image_data() {
        let image = ImageData::new(640, 480, 3, ImageFormat::Rgb8);
        assert_eq!(image.data.len(), 640 * 480 * 3);
    }

    #[test]
    fn test_detections() {
        let mut detections = Detections::empty();
        detections.add(BoundingBox::new(100.0, 100.0, 50.0, 80.0), 0.95, 0, "person");

        assert_eq!(detections.len(), 1);
        assert_eq!(detections.class_names[0], "person");
    }
}
