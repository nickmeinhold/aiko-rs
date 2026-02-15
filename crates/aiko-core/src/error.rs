//! Error types for the Aiko framework.

use thiserror::Error;

/// Errors that can occur during element processing.
#[derive(Error, Debug)]
pub enum ElementError {
    #[error("Processing error: {0}")]
    Processing(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Shutdown requested")]
    Shutdown,

    #[error("Channel closed")]
    ChannelClosed,
}

/// Errors that can occur in the actor system.
#[derive(Error, Debug)]
pub enum ActorError {
    #[error("Actor mailbox is full")]
    MailboxFull,

    #[error("Actor mailbox is closed")]
    MailboxClosed,

    #[error("Actor initialization failed: {0}")]
    InitFailed(String),

    #[error("Actor processing error: {0}")]
    ProcessingError(String),

    #[error("Supervision error: {0}")]
    SupervisionError(String),

    #[error("Actor not found: {0}")]
    NotFound(String),
}

/// Errors that can occur during pipeline execution.
#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("Pipeline build error: {0}")]
    Build(String),

    #[error("Pipeline execution error: {0}")]
    Execution(String),

    #[error("Element error: {0}")]
    Element(#[from] ElementError),

    #[error("Actor error: {0}")]
    Actor(#[from] ActorError),
}
