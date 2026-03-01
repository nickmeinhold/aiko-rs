//! Type-safe pipeline builder with compile-time validation.
//!
//! The builder uses Rust's type system to ensure that element connections
//! are valid at compile time. If you try to connect elements with incompatible
//! types, you'll get a compile error rather than a runtime error.
//!
//! # Example
//!
//! ```rust,ignore
//! let pipeline = Pipeline::builder("my_pipeline")
//!     .source(MySource::new())           // () -> String
//!     .then(MyTransform::new())          // String -> i32
//!     .then(MyFilter::new())             // i32 -> i32
//!     .sink(MySink::new());              // i32 -> ()
//!
//! // This would fail to compile:
//! // .then(WrongElement::new())  // Error: expected i32, found bool
//! ```

use aiko_core::element::{Element, PipelineData, SinkElement, SourceElement};
use std::marker::PhantomData;

/// Internal trait for type-erased element storage.
pub trait ErasedElement: Send + Sync {
    fn name(&self) -> &str;
    fn input_type_name(&self) -> &'static str;
    fn output_type_name(&self) -> &'static str;
}

/// Wrapper to store typed elements in a type-erased collection.
struct ElementWrapper<E> {
    element: E,
    input_type: &'static str,
    output_type: &'static str,
}

impl<E: Element> ErasedElement for ElementWrapper<E> {
    fn name(&self) -> &str {
        self.element.name()
    }

    fn input_type_name(&self) -> &'static str {
        self.input_type
    }

    fn output_type_name(&self) -> &'static str {
        self.output_type
    }
}

/// Wrapper for sink elements.
struct SinkWrapper<S> {
    sink: S,
    input_type: &'static str,
}

impl<S: SinkElement> ErasedElement for SinkWrapper<S> {
    fn name(&self) -> &str {
        self.sink.name()
    }

    fn input_type_name(&self) -> &'static str {
        self.input_type
    }

    fn output_type_name(&self) -> &'static str {
        "()"
    }
}

/// Initial builder state before any elements are added.
pub struct PipelineBuilder {
    name: String,
}

impl PipelineBuilder {
    /// Create a new pipeline builder with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Start the pipeline with a source element.
    ///
    /// The source element produces frames of type `S::Output`.
    pub fn source<S: SourceElement>(self, source: S) -> SourcedPipelineBuilder<S::Output> {
        SourcedPipelineBuilder {
            name: self.name,
            elements: vec![Box::new(SourceElementWrapper {
                source,
                output_type: std::any::type_name::<S::Output>(),
            })],
            _phantom: PhantomData,
        }
    }

    /// Start the pipeline expecting external input of type `T`.
    ///
    /// Use this when frames will be pushed into the pipeline from outside.
    pub fn input<T: PipelineData>(self) -> InputPipelineBuilder<T> {
        InputPipelineBuilder {
            name: self.name,
            elements: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

/// Wrapper for source elements.
struct SourceElementWrapper<S> {
    source: S,
    output_type: &'static str,
}

impl<S: SourceElement> ErasedElement for SourceElementWrapper<S> {
    fn name(&self) -> &str {
        self.source.name()
    }

    fn input_type_name(&self) -> &'static str {
        "()"
    }

    fn output_type_name(&self) -> &'static str {
        self.output_type
    }
}

/// Builder for a pipeline that has an external input type.
pub struct InputPipelineBuilder<CurrentOutput: PipelineData> {
    name: String,
    elements: Vec<Box<dyn ErasedElement>>,
    _phantom: PhantomData<CurrentOutput>,
}

impl<CurrentOutput: PipelineData> InputPipelineBuilder<CurrentOutput> {
    /// Add an element that transforms `CurrentOutput` -> `E::Output`.
    ///
    /// This method enforces at compile time that the element's input
    /// matches the current pipeline output type.
    pub fn then<E>(mut self, element: E) -> InputPipelineBuilder<E::Output>
    where
        E: Element<Input = CurrentOutput>,
    {
        self.elements.push(Box::new(ElementWrapper {
            element,
            input_type: std::any::type_name::<E::Input>(),
            output_type: std::any::type_name::<E::Output>(),
        }));

        InputPipelineBuilder {
            name: self.name,
            elements: self.elements,
            _phantom: PhantomData,
        }
    }

    /// Terminate the pipeline with a sink element.
    pub fn sink<S>(mut self, sink: S) -> InputPipeline<CurrentOutput>
    where
        S: SinkElement<Input = CurrentOutput>,
    {
        self.elements.push(Box::new(SinkWrapper {
            sink,
            input_type: std::any::type_name::<S::Input>(),
        }));

        InputPipeline {
            name: self.name,
            elements: self.elements,
            _phantom: PhantomData,
        }
    }

    /// Build the pipeline without a sink (outputs are returned).
    pub fn build(self) -> OpenPipeline<CurrentOutput> {
        OpenPipeline {
            name: self.name,
            elements: self.elements,
            _phantom: PhantomData,
        }
    }
}

/// Builder for a pipeline that has a source element.
///
/// The `CurrentOutput` tracks the type that would be produced by the last element in the chain.
pub struct SourcedPipelineBuilder<CurrentOutput: PipelineData> {
    name: String,
    elements: Vec<Box<dyn ErasedElement>>,
    _phantom: PhantomData<CurrentOutput>,
}

impl<CurrentOutput: PipelineData> SourcedPipelineBuilder<CurrentOutput> {
    /// Add an element that transforms `CurrentOutput` -> `E::Output`.
    pub fn then<E>(mut self, element: E) -> SourcedPipelineBuilder<E::Output>
    where
        E: Element<Input = CurrentOutput>,
    {
        self.elements.push(Box::new(ElementWrapper {
            element,
            input_type: std::any::type_name::<E::Input>(),
            output_type: std::any::type_name::<E::Output>(),
        }));

        SourcedPipelineBuilder {
            name: self.name,
            elements: self.elements,
            _phantom: PhantomData,
        }
    }

    /// Terminate the pipeline with a sink element.
    pub fn sink<Sink>(mut self, sink: Sink) -> SourcedPipeline
    where
        Sink: SinkElement<Input = CurrentOutput>,
    {
        self.elements.push(Box::new(SinkWrapper {
            sink,
            input_type: std::any::type_name::<Sink::Input>(),
        }));

        SourcedPipeline {
            name: self.name,
            elements: self.elements,
        }
    }

    /// Build the pipeline without a sink (outputs are returned).
    pub fn build(self) -> OpenPipeline<CurrentOutput> {
        OpenPipeline {
            name: self.name,
            elements: self.elements,
            _phantom: PhantomData,
        }
    }
}

/// A complete pipeline with external input and a sink.
pub struct InputPipeline<Input: PipelineData> {
    name: String,
    elements: Vec<Box<dyn ErasedElement>>,
    _phantom: PhantomData<Input>,
}

impl<Input: PipelineData> InputPipeline<Input> {
    /// Get the pipeline name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the names of all elements in the pipeline.
    pub fn element_names(&self) -> Vec<&str> {
        self.elements.iter().map(|e| e.name()).collect()
    }

    /// Get the type flow for debugging.
    pub fn type_flow(&self) -> Vec<(&'static str, &'static str)> {
        self.elements
            .iter()
            .map(|e| (e.input_type_name(), e.output_type_name()))
            .collect()
    }
}

/// A pipeline without a sink (outputs are returned).
pub struct OpenPipeline<Output: PipelineData> {
    name: String,
    elements: Vec<Box<dyn ErasedElement>>,
    _phantom: PhantomData<Output>,
}

impl<Output: PipelineData> OpenPipeline<Output> {
    /// Get the pipeline name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the names of all elements in the pipeline.
    pub fn element_names(&self) -> Vec<&str> {
        self.elements.iter().map(|e| e.name()).collect()
    }
}

/// A complete pipeline with a source and sink.
pub struct SourcedPipeline {
    name: String,
    elements: Vec<Box<dyn ErasedElement>>,
}

impl SourcedPipeline {
    /// Get the pipeline name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the names of all elements in the pipeline.
    pub fn element_names(&self) -> Vec<&str> {
        self.elements.iter().map(|e| e.name()).collect()
    }
}

/// Convenience type alias for the main entry point.
pub type Pipeline = PipelineBuilder;

#[cfg(test)]
mod tests {
    use super::*;
    use aiko_core::element::{ElementContext, MapElement};
    use aiko_core::error::ElementError;
    use aiko_core::frame::{Frame, FrameId, StreamId};
    use async_trait::async_trait;

    // Test source
    struct TestSource;

    #[async_trait]
    impl SourceElement for TestSource {
        type Output = i32;
        type Config = ();

        fn name(&self) -> &str {
            "test_source"
        }

        async fn next(
            &mut self,
            _: &mut ElementContext,
        ) -> Result<Option<Frame<i32>>, ElementError> {
            Ok(Some(Frame::new(StreamId::new(), FrameId(0), 42)))
        }
    }

    // Test sink
    struct TestSink;

    #[async_trait]
    impl SinkElement for TestSink {
        type Input = String;
        type Config = ();

        fn name(&self) -> &str {
            "test_sink"
        }

        async fn consume(
            &mut self,
            _: Frame<String>,
            _: &mut ElementContext,
        ) -> Result<(), ElementError> {
            Ok(())
        }
    }

    #[test]
    fn test_pipeline_builder_compiles() {
        // This test verifies that valid pipelines compile
        let _pipeline = Pipeline::new("test")
            .source(TestSource)
            .then(MapElement::new("double", |x: i32| x * 2))
            .then(MapElement::new("to_string", |x: i32| x.to_string()))
            .sink(TestSink);
    }

    #[test]
    fn test_input_pipeline_builder() {
        let pipeline = Pipeline::new("test")
            .input::<i32>()
            .then(MapElement::new("double", |x: i32| x * 2))
            .then(MapElement::new("to_string", |x: i32| x.to_string()))
            .sink(TestSink);

        assert_eq!(pipeline.name(), "test");
        assert_eq!(
            pipeline.element_names(),
            vec!["double", "to_string", "test_sink"]
        );
    }

    // This test demonstrates compile-time type safety.
    // Uncommenting the following would cause a compile error:
    //
    // #[test]
    // fn test_type_mismatch_fails_to_compile() {
    //     struct WrongElement;
    //
    //     impl Element for WrongElement {
    //         type Input = bool;  // Expects bool, but pipeline has String
    //         type Output = f64;
    //         type Config = ();
    //         fn name(&self) -> &str { "wrong" }
    //         async fn process(&mut self, _: Frame<bool>, _: &mut ElementContext)
    //             -> Result<Option<Frame<f64>>, ElementError> { Ok(None) }
    //     }
    //
    //     let _pipeline = Pipeline::new("test")
    //         .source(TestSource)
    //         .then(MapElement::new("to_string", |x: i32| x.to_string()))
    //         .then(WrongElement);  // ERROR: expected String, found bool
    // }
}
