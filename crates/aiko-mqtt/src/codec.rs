//! Frame serialization for network transport.
//!
//! Re-exports shared codec types from `aiko-core` and provides
//! ML-specific data types for MQTT pipelines.

// Re-export shared codec types from aiko-core
pub use aiko_core::codec::*;

use serde::{Deserialize, Serialize};

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
