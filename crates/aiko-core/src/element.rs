//! Element traits for pipeline processing units.

use crate::error::ElementError;
use crate::frame::Frame;
use async_trait::async_trait;

/// Marker trait for types that can flow through pipelines.
pub trait PipelineData: Send + Sync + 'static {}

// Blanket implementation for all compatible types.
impl<T: Send + Sync + 'static> PipelineData for T {}

/// Trait for element configuration types.
pub trait ElementConfig: Send + Sync + Clone + Default + 'static {}

// Implement for unit type (no config).
impl ElementConfig for () {}

/// Context provided to elements during processing.
pub struct ElementContext {
    pub element_name: String,
    pub pipeline_name: String,
}

impl ElementContext {
    pub fn new(element_name: impl Into<String>, pipeline_name: impl Into<String>) -> Self {
        Self {
            element_name: element_name.into(),
            pipeline_name: pipeline_name.into(),
        }
    }
}

/// The core Element trait - defines a processing unit in a pipeline.
///
/// Elements are typed on their Input and Output, enabling compile-time
/// verification of pipeline connections.
///
/// # Example
///
/// ```ignore
/// struct DoubleElement;
///
/// #[async_trait]
/// impl Element for DoubleElement {
///     type Input = i32;
///     type Output = i32;
///     type Config = ();
///
///     fn name(&self) -> &str { "double" }
///
///     async fn process(
///         &mut self,
///         frame: Frame<i32>,
///         _ctx: &mut ElementContext,
///     ) -> Result<Option<Frame<i32>>, ElementError> {
///         Ok(Some(frame.map(|x| x * 2)))
///     }
/// }
/// ```
#[async_trait]
pub trait Element: Send + Sync + 'static {
    /// The type of data this element accepts.
    type Input: PipelineData;

    /// The type of data this element produces.
    type Output: PipelineData;

    /// Configuration type for this element.
    type Config: ElementConfig;

    /// Human-readable name for this element.
    fn name(&self) -> &str;

    /// Initialize the element with configuration.
    async fn init(&mut self, _config: Self::Config) -> Result<(), ElementError> {
        Ok(())
    }

    /// Process a single frame.
    ///
    /// Returns `Ok(Some(frame))` to pass the frame downstream,
    /// `Ok(None)` to filter out the frame,
    /// or `Err` on failure.
    async fn process(
        &mut self,
        frame: Frame<Self::Input>,
        ctx: &mut ElementContext,
    ) -> Result<Option<Frame<Self::Output>>, ElementError>;

    /// Called when the element is being shut down.
    async fn shutdown(&mut self) -> Result<(), ElementError> {
        Ok(())
    }

    /// Whether this element can process frames concurrently.
    /// Stateless elements can be parallelized.
    fn is_stateless(&self) -> bool {
        false
    }
}

/// Trait for source elements that produce frames without input.
#[async_trait]
pub trait SourceElement: Send + Sync + 'static {
    /// The type of data this source produces.
    type Output: PipelineData;

    /// Configuration type for this source.
    type Config: ElementConfig;

    /// Human-readable name for this source.
    fn name(&self) -> &str;

    /// Initialize the source with configuration.
    async fn init(&mut self, _config: Self::Config) -> Result<(), ElementError> {
        Ok(())
    }

    /// Produce the next frame, or None if the source is exhausted.
    async fn next(
        &mut self,
        ctx: &mut ElementContext,
    ) -> Result<Option<Frame<Self::Output>>, ElementError>;

    /// Called when the source is being shut down.
    async fn shutdown(&mut self) -> Result<(), ElementError> {
        Ok(())
    }
}

/// Trait for sink elements that consume frames without output.
#[async_trait]
pub trait SinkElement: Send + Sync + 'static {
    /// The type of data this sink accepts.
    type Input: PipelineData;

    /// Configuration type for this sink.
    type Config: ElementConfig;

    /// Human-readable name for this sink.
    fn name(&self) -> &str;

    /// Initialize the sink with configuration.
    async fn init(&mut self, _config: Self::Config) -> Result<(), ElementError> {
        Ok(())
    }

    /// Consume a frame.
    async fn consume(
        &mut self,
        frame: Frame<Self::Input>,
        ctx: &mut ElementContext,
    ) -> Result<(), ElementError>;

    /// Called when the sink is being shut down.
    async fn shutdown(&mut self) -> Result<(), ElementError> {
        Ok(())
    }
}

/// A pass-through element that doesn't modify the data.
/// Useful as a placeholder or for debugging.
pub struct PassThrough<T> {
    name: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> PassThrough<T> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: PipelineData> Element for PassThrough<T> {
    type Input = T;
    type Output = T;
    type Config = ();

    fn name(&self) -> &str {
        &self.name
    }

    async fn process(
        &mut self,
        frame: Frame<Self::Input>,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<Self::Output>>, ElementError> {
        Ok(Some(frame))
    }

    fn is_stateless(&self) -> bool {
        true
    }
}

/// An element that transforms data using a closure.
pub struct MapElement<I, O, F>
where
    F: FnMut(I) -> O + Send + Sync,
{
    name: String,
    f: F,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> MapElement<I, O, F>
where
    F: FnMut(I) -> O + Send + Sync,
{
    pub fn new(name: impl Into<String>, f: F) -> Self {
        Self {
            name: name.into(),
            f,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<I, O, F> Element for MapElement<I, O, F>
where
    I: PipelineData,
    O: PipelineData,
    F: FnMut(I) -> O + Send + Sync + 'static,
{
    type Input = I;
    type Output = O;
    type Config = ();

    fn name(&self) -> &str {
        &self.name
    }

    async fn process(
        &mut self,
        frame: Frame<Self::Input>,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<Self::Output>>, ElementError> {
        Ok(Some(frame.map(&mut self.f)))
    }

    fn is_stateless(&self) -> bool {
        true
    }
}

/// An element that filters frames based on a predicate.
pub struct FilterElement<T, F>
where
    F: FnMut(&T) -> bool + Send + Sync,
{
    name: String,
    predicate: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> FilterElement<T, F>
where
    F: FnMut(&T) -> bool + Send + Sync,
{
    pub fn new(name: impl Into<String>, predicate: F) -> Self {
        Self {
            name: name.into(),
            predicate,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T, F> Element for FilterElement<T, F>
where
    T: PipelineData,
    F: FnMut(&T) -> bool + Send + Sync + 'static,
{
    type Input = T;
    type Output = T;
    type Config = ();

    fn name(&self) -> &str {
        &self.name
    }

    async fn process(
        &mut self,
        frame: Frame<Self::Input>,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<Self::Output>>, ElementError> {
        if (self.predicate)(&frame.payload) {
            Ok(Some(frame))
        } else {
            Ok(None)
        }
    }

    fn is_stateless(&self) -> bool {
        true
    }
}
