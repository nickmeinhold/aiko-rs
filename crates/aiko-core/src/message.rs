//! Message types for actor communication.

use crate::frame::AnyFrame;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorId(pub Uuid);

impl ActorId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ActorId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Messages that can be sent between actors.
#[derive(Debug)]
pub enum ActorMessage {
    /// A frame to process.
    ProcessFrame(AnyFrame),

    /// Control messages.
    Control(ControlMessage),

    /// System messages.
    System(SystemMessage),
}

/// Control messages for managing actor lifecycle and behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    /// Start processing.
    Start,

    /// Pause processing.
    Pause,

    /// Resume processing.
    Resume,

    /// Stop and shutdown gracefully.
    Stop,

    /// Update configuration with JSON value.
    Configure(serde_json::Value),
}

/// System messages for actor coordination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemMessage {
    /// Health check ping.
    Ping { reply_to: ActorId },

    /// Health check response.
    Pong { from: ActorId, timestamp_ns: u64 },

    /// Actor has started.
    Started { actor_id: ActorId, name: String },

    /// Actor has stopped.
    Stopped { actor_id: ActorId, reason: StopReason },

    /// Actor encountered an error.
    Error { actor_id: ActorId, error: String },
}

/// Reason for an actor stopping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StopReason {
    /// Normal shutdown.
    Normal,

    /// Shutdown due to error.
    Error(String),

    /// Forcefully killed.
    Killed,
}

/// Output from an element actor.
#[derive(Debug)]
pub enum ElementOutput {
    /// A processed frame.
    Frame(AnyFrame),

    /// Processing is complete (source exhausted).
    Complete,

    /// An error occurred.
    Error(String),
}

/// Envelope for network-serializable messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkEnvelope {
    /// Unique message ID.
    pub id: Uuid,

    /// Source node identifier.
    pub source_node: String,

    /// Target node identifier.
    pub target_node: String,

    /// Target actor ID.
    pub target_actor: ActorId,

    /// Timestamp in nanoseconds.
    pub timestamp_ns: u64,

    /// Serialized message payload.
    pub payload: Vec<u8>,

    /// Type identifier for the payload.
    pub payload_type: String,
}

impl NetworkEnvelope {
    /// Create a new network envelope.
    pub fn new(
        source_node: impl Into<String>,
        target_node: impl Into<String>,
        target_actor: ActorId,
        payload: Vec<u8>,
        payload_type: impl Into<String>,
    ) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        Self {
            id: Uuid::new_v4(),
            source_node: source_node.into(),
            target_node: target_node.into(),
            target_actor,
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            payload,
            payload_type: payload_type.into(),
        }
    }
}
