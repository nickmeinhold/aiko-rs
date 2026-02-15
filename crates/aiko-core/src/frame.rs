//! Frame types for data flowing through pipelines.

use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Unique identifier for a stream of frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId(pub Uuid);

impl StreamId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for StreamId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a frame within a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct FrameId(pub u64);

impl FrameId {
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl std::fmt::Display for FrameId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metadata attached to every frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameMetadata {
    pub stream_id: StreamId,
    pub frame_id: FrameId,
    pub timestamp_ns: u64,
    pub source_element: Option<String>,
    pub properties: HashMap<String, String>,
}

impl FrameMetadata {
    pub fn new(stream_id: StreamId, frame_id: FrameId) -> Self {
        Self {
            stream_id,
            frame_id,
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            source_element: None,
            properties: HashMap::new(),
        }
    }

    /// Set the source element name.
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source_element = Some(source.into());
        self
    }

    /// Add a property to the metadata.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// A frame is a unit of data flowing through the pipeline.
/// Generic over the payload type T for compile-time type safety.
#[derive(Debug, Clone)]
pub struct Frame<T> {
    pub metadata: FrameMetadata,
    pub payload: T,
}

impl<T> Frame<T> {
    /// Create a new frame with the given stream ID, frame ID, and payload.
    pub fn new(stream_id: StreamId, frame_id: FrameId, payload: T) -> Self {
        Self {
            metadata: FrameMetadata::new(stream_id, frame_id),
            payload,
        }
    }

    /// Create a frame with existing metadata.
    pub fn with_metadata(metadata: FrameMetadata, payload: T) -> Self {
        Self { metadata, payload }
    }

    /// Transform the payload while preserving metadata.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Frame<U> {
        Frame {
            metadata: self.metadata,
            payload: f(self.payload),
        }
    }

    /// Transform with a fallible function.
    pub fn try_map<U, E, F: FnOnce(T) -> Result<U, E>>(self, f: F) -> Result<Frame<U>, E> {
        Ok(Frame {
            metadata: self.metadata,
            payload: f(self.payload)?,
        })
    }

    /// Get the stream ID.
    pub fn stream_id(&self) -> StreamId {
        self.metadata.stream_id
    }

    /// Get the frame ID.
    pub fn frame_id(&self) -> FrameId {
        self.metadata.frame_id
    }
}

/// Type-erased frame for heterogeneous collections.
pub struct AnyFrame {
    pub metadata: FrameMetadata,
    payload: Box<dyn Any + Send + Sync>,
    type_id: TypeId,
    type_name: &'static str,
}

impl AnyFrame {
    /// Create a new type-erased frame from a typed frame.
    pub fn new<T: Send + Sync + 'static>(frame: Frame<T>) -> Self {
        Self {
            metadata: frame.metadata,
            payload: Box::new(frame.payload),
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>(),
        }
    }

    /// Attempt to downcast to a typed frame.
    pub fn downcast<T: 'static>(self) -> Option<Frame<T>> {
        if self.type_id == TypeId::of::<T>() {
            let payload = self.payload.downcast::<T>().ok()?;
            Some(Frame {
                metadata: self.metadata,
                payload: *payload,
            })
        } else {
            None
        }
    }

    /// Attempt to get a reference to the payload.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.payload.downcast_ref()
    }

    /// Get the type name of the payload (for debugging).
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    /// Check if the payload is of a specific type.
    pub fn is<T: 'static>(&self) -> bool {
        self.type_id == TypeId::of::<T>()
    }
}

impl std::fmt::Debug for AnyFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnyFrame")
            .field("metadata", &self.metadata)
            .field("type_name", &self.type_name)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_creation() {
        let stream_id = StreamId::new();
        let frame_id = FrameId(0);
        let frame = Frame::new(stream_id, frame_id, "hello".to_string());

        assert_eq!(frame.stream_id(), stream_id);
        assert_eq!(frame.frame_id(), frame_id);
        assert_eq!(frame.payload, "hello");
    }

    #[test]
    fn test_frame_map() {
        let frame = Frame::new(StreamId::new(), FrameId(0), 42i32);
        let mapped = frame.map(|x| x * 2);
        assert_eq!(mapped.payload, 84);
    }

    #[test]
    fn test_any_frame_downcast() {
        let frame = Frame::new(StreamId::new(), FrameId(0), "test".to_string());
        let any_frame = AnyFrame::new(frame);

        assert!(any_frame.is::<String>());
        assert!(!any_frame.is::<i32>());

        let recovered = any_frame.downcast::<String>().unwrap();
        assert_eq!(recovered.payload, "test");
    }
}
