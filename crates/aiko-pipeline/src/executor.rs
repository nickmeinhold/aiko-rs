//! Pipeline execution runtime.

use aiko_actor::prelude::*;
use aiko_core::element::{Element, ElementContext, SinkElement, SourceElement};
use aiko_core::error::ElementError;
use aiko_core::frame::{AnyFrame, Frame, FrameId, StreamId};
use aiko_core::message::{ActorMessage, ControlMessage};
use async_trait::async_trait;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Actor that wraps a Source element.
pub struct SourceActor<S: SourceElement> {
    source: S,
    stream_id: StreamId,
    frame_counter: u64,
    next_actor: Option<ActorRef>,
    running: bool,
}

impl<S: SourceElement> SourceActor<S> {
    pub fn new(source: S, next_actor: Option<ActorRef>) -> Self {
        Self {
            source,
            stream_id: StreamId::new(),
            frame_counter: 0,
            next_actor,
            running: false,
        }
    }
}

#[async_trait]
impl<S: SourceElement> Actor for SourceActor<S>
where
    S::Config: Default,
{
    type Config = ();
    type State = ();

    async fn pre_start(
        &mut self,
        _config: Self::Config,
        ctx: &mut ActorContext,
    ) -> Result<Self::State, ActorError> {
        self.source
            .init(S::Config::default())
            .await
            .map_err(|e| ActorError::InitFailed(e.to_string()))?;
        info!(source = self.source.name(), "Source actor started");
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: ActorMessage,
        _state: &mut Self::State,
        ctx: &mut ActorContext,
    ) -> Result<(), ActorError> {
        match msg {
            ActorMessage::Control(ControlMessage::Start) => {
                self.running = true;
                // Start producing frames
                self.produce_frames(ctx).await?;
            }
            ActorMessage::Control(ControlMessage::Pause) => {
                self.running = false;
            }
            ActorMessage::Control(ControlMessage::Resume) => {
                self.running = true;
                self.produce_frames(ctx).await?;
            }
            ActorMessage::Control(ControlMessage::Stop) => {
                self.running = false;
            }
            _ => {}
        }
        Ok(())
    }

    async fn post_stop(
        &mut self,
        _state: &mut Self::State,
        _ctx: &mut ActorContext,
    ) -> Result<(), ActorError> {
        self.source
            .shutdown()
            .await
            .map_err(|e| ActorError::ProcessingError(e.to_string()))?;
        Ok(())
    }
}

impl<S: SourceElement> SourceActor<S> {
    async fn produce_frames(&mut self, ctx: &mut ActorContext) -> Result<(), ActorError> {
        let mut elem_ctx = ElementContext::new(self.source.name(), "pipeline");

        while self.running {
            match self.source.next(&mut elem_ctx).await {
                Ok(Some(frame)) => {
                    self.frame_counter += 1;
                    if let Some(next) = &self.next_actor {
                        let any_frame = AnyFrame::new(frame);
                        next.send(ActorMessage::ProcessFrame(any_frame)).await?;
                    }
                }
                Ok(None) => {
                    // Source exhausted
                    info!(source = self.source.name(), "Source exhausted");
                    break;
                }
                Err(e) => {
                    error!(source = self.source.name(), error = %e, "Source error");
                    return Err(ActorError::ProcessingError(e.to_string()));
                }
            }
        }
        Ok(())
    }
}

/// Actor that wraps an Element.
pub struct ElementActor<E: Element> {
    element: E,
    next_actor: Option<ActorRef>,
}

impl<E: Element> ElementActor<E> {
    pub fn new(element: E, next_actor: Option<ActorRef>) -> Self {
        Self { element, next_actor }
    }
}

#[async_trait]
impl<E: Element> Actor for ElementActor<E>
where
    E::Config: Default,
{
    type Config = ();
    type State = ();

    async fn pre_start(
        &mut self,
        _config: Self::Config,
        _ctx: &mut ActorContext,
    ) -> Result<Self::State, ActorError> {
        self.element
            .init(E::Config::default())
            .await
            .map_err(|e| ActorError::InitFailed(e.to_string()))?;
        info!(element = self.element.name(), "Element actor started");
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: ActorMessage,
        _state: &mut Self::State,
        ctx: &mut ActorContext,
    ) -> Result<(), ActorError> {
        match msg {
            ActorMessage::ProcessFrame(any_frame) => {
                // Downcast to expected input type
                if let Some(frame) = any_frame.downcast::<E::Input>() {
                    let mut elem_ctx = ElementContext::new(self.element.name(), "pipeline");

                    match self.element.process(frame, &mut elem_ctx).await {
                        Ok(Some(output_frame)) => {
                            // Forward to next element
                            if let Some(next) = &self.next_actor {
                                let any_output = AnyFrame::new(output_frame);
                                next.send(ActorMessage::ProcessFrame(any_output)).await?;
                            }
                        }
                        Ok(None) => {
                            // Frame filtered out
                            debug!(element = self.element.name(), "Frame filtered");
                        }
                        Err(e) => {
                            error!(element = self.element.name(), error = %e, "Element error");
                        }
                    }
                } else {
                    error!(
                        element = self.element.name(),
                        expected = std::any::type_name::<E::Input>(),
                        "Type mismatch in element"
                    );
                }
            }
            ActorMessage::Control(ControlMessage::Stop) => {
                self.element.shutdown().await.ok();
            }
            _ => {}
        }
        Ok(())
    }

    async fn post_stop(
        &mut self,
        _state: &mut Self::State,
        _ctx: &mut ActorContext,
    ) -> Result<(), ActorError> {
        self.element
            .shutdown()
            .await
            .map_err(|e| ActorError::ProcessingError(e.to_string()))?;
        Ok(())
    }
}

/// Actor that wraps a Sink element.
pub struct SinkActor<S: SinkElement> {
    sink: S,
    frames_consumed: u64,
}

impl<S: SinkElement> SinkActor<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            frames_consumed: 0,
        }
    }
}

#[async_trait]
impl<S: SinkElement> Actor for SinkActor<S>
where
    S::Config: Default,
{
    type Config = ();
    type State = ();

    async fn pre_start(
        &mut self,
        _config: Self::Config,
        _ctx: &mut ActorContext,
    ) -> Result<Self::State, ActorError> {
        self.sink
            .init(S::Config::default())
            .await
            .map_err(|e| ActorError::InitFailed(e.to_string()))?;
        info!(sink = self.sink.name(), "Sink actor started");
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: ActorMessage,
        _state: &mut Self::State,
        _ctx: &mut ActorContext,
    ) -> Result<(), ActorError> {
        match msg {
            ActorMessage::ProcessFrame(any_frame) => {
                if let Some(frame) = any_frame.downcast::<S::Input>() {
                    let mut elem_ctx = ElementContext::new(self.sink.name(), "pipeline");

                    match self.sink.consume(frame, &mut elem_ctx).await {
                        Ok(()) => {
                            self.frames_consumed += 1;
                            debug!(
                                sink = self.sink.name(),
                                frames = self.frames_consumed,
                                "Frame consumed"
                            );
                        }
                        Err(e) => {
                            error!(sink = self.sink.name(), error = %e, "Sink error");
                        }
                    }
                } else {
                    error!(
                        sink = self.sink.name(),
                        expected = std::any::type_name::<S::Input>(),
                        "Type mismatch in sink"
                    );
                }
            }
            ActorMessage::Control(ControlMessage::Stop) => {
                self.sink.shutdown().await.ok();
            }
            _ => {}
        }
        Ok(())
    }

    async fn post_stop(
        &mut self,
        _state: &mut Self::State,
        _ctx: &mut ActorContext,
    ) -> Result<(), ActorError> {
        self.sink
            .shutdown()
            .await
            .map_err(|e| ActorError::ProcessingError(e.to_string()))?;
        info!(
            sink = self.sink.name(),
            frames = self.frames_consumed,
            "Sink stopped"
        );
        Ok(())
    }
}

/// Simple synchronous pipeline executor for testing.
pub struct SyncExecutor;

impl SyncExecutor {
    /// Execute a chain of elements synchronously on a single frame.
    pub async fn run_once<I, O, F>(
        input: Frame<I>,
        mut process: F,
    ) -> Result<Option<Frame<O>>, ElementError>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: FnMut(Frame<I>) -> Result<Option<Frame<O>>, ElementError>,
    {
        process(input)
    }
}
