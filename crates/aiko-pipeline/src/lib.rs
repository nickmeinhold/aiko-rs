//! Aiko Pipeline - Type-safe pipeline builder for the Aiko framework.
//!
//! This crate provides the pipeline construction and execution:
//!
//! - [`Pipeline`](builder::Pipeline) - Builder for constructing pipelines
//! - Type-safe element chaining with compile-time validation
//! - Actor-based execution runtime
//!
//! # Type Safety
//!
//! The pipeline builder uses Rust's type system to ensure that element
//! connections are valid at compile time:
//!
//! ```rust,ignore
//! let pipeline = Pipeline::new("example")
//!     .source(MySource::new())           // () -> ImageData
//!     .then(ResizeElement::new(640, 480))  // ImageData -> ImageData
//!     .then(YoloDetector::new())           // ImageData -> Detections
//!     .sink(DisplaySink::new());           // Detections -> ()
//!
//! // This would FAIL to compile:
//! // .then(AudioEncoder::new())  // ERROR: expected ImageData, found AudioFrame
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use aiko_pipeline::prelude::*;
//! use aiko_core::element::MapElement;
//!
//! let pipeline = Pipeline::new("double_to_string")
//!     .input::<i32>()
//!     .then(MapElement::new("double", |x| x * 2))
//!     .then(MapElement::new("to_string", |x: i32| x.to_string()))
//!     .build();
//!
//! println!("Pipeline: {}", pipeline.name());
//! println!("Elements: {:?}", pipeline.element_names());
//! ```

pub mod builder;
pub mod executor;

/// Convenient re-exports of commonly used types.
pub mod prelude {
    pub use crate::builder::{
        ErasedElement, InputPipeline, InputPipelineBuilder, OpenPipeline, Pipeline,
        PipelineBuilder, SourcedPipeline, SourcedPipelineBuilder,
    };
    pub use crate::executor::{ElementActor, SinkActor, SourceActor, SyncExecutor};
}
