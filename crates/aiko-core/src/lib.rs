//! Aiko Core - Core traits and types for the Aiko distributed pipeline framework.
//!
//! This crate provides the foundational building blocks:
//!
//! - [`Frame`](frame::Frame) - A unit of data flowing through pipelines
//! - [`Element`](element::Element) - A processing unit that transforms frames
//! - [`SourceElement`](element::SourceElement) - An element that produces frames
//! - [`SinkElement`](element::SinkElement) - An element that consumes frames
//! - Actor message types for distributed communication
//!
//! # Example
//!
//! ```rust
//! use aiko_core::prelude::*;
//!
//! // Create a simple frame
//! let frame = Frame::new(StreamId::new(), FrameId(0), "hello world".to_string());
//!
//! // Transform it
//! let upper = frame.map(|s| s.to_uppercase());
//! assert_eq!(upper.payload, "HELLO WORLD");
//! ```

pub mod element;
pub mod error;
pub mod frame;
pub mod message;

/// Convenient re-exports of commonly used types.
pub mod prelude {
    pub use crate::element::{
        Element, ElementConfig, ElementContext, FilterElement, MapElement, PassThrough,
        PipelineData, SinkElement, SourceElement,
    };
    pub use crate::error::{ActorError, ElementError, PipelineError};
    pub use crate::frame::{AnyFrame, Frame, FrameId, FrameMetadata, StreamId};
    pub use crate::message::{ActorId, ActorMessage, ControlMessage, StopReason, SystemMessage};
}
