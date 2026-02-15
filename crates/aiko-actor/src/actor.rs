//! Actor trait and runtime for concurrent element execution.

use aiko_core::error::ActorError;
use aiko_core::message::{ActorId, ActorMessage, ControlMessage, StopReason, SystemMessage};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Handle to an actor for sending messages.
#[derive(Clone)]
pub struct ActorRef {
    pub id: ActorId,
    pub name: String,
    tx: mpsc::Sender<ActorMessage>,
}

impl ActorRef {
    /// Create a new actor reference.
    pub(crate) fn new(id: ActorId, name: String, tx: mpsc::Sender<ActorMessage>) -> Self {
        Self { id, name, tx }
    }

    /// Send a message to the actor, waiting if the mailbox is full.
    pub async fn send(&self, msg: ActorMessage) -> Result<(), ActorError> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| ActorError::MailboxClosed)
    }

    /// Try to send a message without waiting.
    pub fn try_send(&self, msg: ActorMessage) -> Result<(), ActorError> {
        self.tx.try_send(msg).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => ActorError::MailboxFull,
            mpsc::error::TrySendError::Closed(_) => ActorError::MailboxClosed,
        })
    }

    /// Send a control message.
    pub async fn control(&self, msg: ControlMessage) -> Result<(), ActorError> {
        self.send(ActorMessage::Control(msg)).await
    }

    /// Request the actor to stop.
    pub async fn stop(&self) -> Result<(), ActorError> {
        self.control(ControlMessage::Stop).await
    }
}

impl std::fmt::Debug for ActorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRef")
            .field("id", &self.id)
            .field("name", &self.name)
            .finish()
    }
}

/// Context provided to actors during execution.
pub struct ActorContext {
    pub id: ActorId,
    pub name: String,
    pub parent: Option<ActorRef>,
    self_ref: Option<ActorRef>,
    children: Vec<ActorRef>,
}

impl ActorContext {
    pub(crate) fn new(id: ActorId, name: String, parent: Option<ActorRef>) -> Self {
        Self {
            id,
            name,
            parent,
            self_ref: None,
            children: Vec::new(),
        }
    }

    pub(crate) fn set_self_ref(&mut self, actor_ref: ActorRef) {
        self.self_ref = Some(actor_ref);
    }

    /// Get a reference to this actor.
    pub fn self_ref(&self) -> Option<&ActorRef> {
        self.self_ref.as_ref()
    }

    /// Get references to child actors.
    pub fn children(&self) -> &[ActorRef] {
        &self.children
    }

    /// Register a child actor.
    pub fn add_child(&mut self, child: ActorRef) {
        self.children.push(child);
    }

    /// Remove a child actor by ID.
    pub fn remove_child(&mut self, id: &ActorId) {
        self.children.retain(|c| c.id != *id);
    }
}

/// Trait for actor configuration types.
pub trait ActorConfig: Send + Sync + Clone + Default + 'static {}

impl ActorConfig for () {}

/// The core Actor trait.
///
/// Actors are the unit of concurrency in the framework. Each actor has:
/// - Its own mailbox for receiving messages
/// - Internal state that is managed across messages
/// - Lifecycle hooks for initialization and cleanup
#[async_trait]
pub trait Actor: Send + Sync + 'static {
    /// Configuration type for this actor.
    type Config: ActorConfig;

    /// State type managed by the actor.
    type State: Send + Sync + 'static;

    /// Called when the actor starts, returns initial state.
    async fn pre_start(
        &mut self,
        config: Self::Config,
        ctx: &mut ActorContext,
    ) -> Result<Self::State, ActorError>;

    /// Handle an incoming message.
    async fn handle(
        &mut self,
        msg: ActorMessage,
        state: &mut Self::State,
        ctx: &mut ActorContext,
    ) -> Result<(), ActorError>;

    /// Called when the actor is stopping.
    async fn post_stop(
        &mut self,
        _state: &mut Self::State,
        _ctx: &mut ActorContext,
    ) -> Result<(), ActorError> {
        Ok(())
    }

    /// Handle supervision events from children.
    fn on_child_stopped(
        &mut self,
        _child_id: ActorId,
        _reason: StopReason,
        _state: &mut Self::State,
        _ctx: &mut ActorContext,
    ) -> SupervisionAction {
        SupervisionAction::Escalate
    }
}

/// Actions a supervisor can take when a child fails.
#[derive(Debug, Clone)]
pub enum SupervisionAction {
    /// Restart the failed child.
    Restart,
    /// Stop the failed child permanently.
    Stop,
    /// Escalate to parent supervisor.
    Escalate,
    /// Restart all children.
    RestartAll,
    /// Stop all children and self.
    StopAll,
}

/// Spawn an actor and return a reference to it.
pub async fn spawn<A: Actor>(
    name: impl Into<String>,
    mut actor: A,
    config: A::Config,
    parent: Option<ActorRef>,
) -> Result<ActorRef, ActorError> {
    let name = name.into();
    let id = ActorId::new();
    let (tx, mut rx) = mpsc::channel::<ActorMessage>(1024);

    let actor_ref = ActorRef::new(id, name.clone(), tx);
    let mut ctx = ActorContext::new(id, name.clone(), parent);
    ctx.set_self_ref(actor_ref.clone());

    // Spawn the actor task
    tokio::spawn(async move {
        // Initialize
        let mut state = match actor.pre_start(config, &mut ctx).await {
            Ok(s) => {
                info!(actor = %ctx.name, id = %ctx.id, "Actor started");
                s
            }
            Err(e) => {
                error!(actor = %ctx.name, error = %e, "Actor failed to start");
                return;
            }
        };

        // Message loop
        loop {
            match rx.recv().await {
                Some(ActorMessage::Control(ControlMessage::Stop)) => {
                    debug!(actor = %ctx.name, "Actor received stop signal");
                    break;
                }
                Some(msg) => {
                    if let Err(e) = actor.handle(msg, &mut state, &mut ctx).await {
                        error!(actor = %ctx.name, error = %e, "Actor error");
                        // Could implement restart logic here
                    }
                }
                None => {
                    // Channel closed
                    warn!(actor = %ctx.name, "Actor mailbox closed");
                    break;
                }
            }
        }

        // Cleanup
        if let Err(e) = actor.post_stop(&mut state, &mut ctx).await {
            error!(actor = %ctx.name, error = %e, "Actor post_stop error");
        }
        info!(actor = %ctx.name, "Actor stopped");
    });

    Ok(actor_ref)
}

/// A simple actor that processes frames by applying a function.
pub struct FunctionActor<F, S>
where
    F: FnMut(&mut S, ActorMessage) -> Result<(), ActorError> + Send + Sync,
    S: Default + Send + Sync + 'static,
{
    name: String,
    handler: F,
    _phantom: std::marker::PhantomData<S>,
}

impl<F, S> FunctionActor<F, S>
where
    F: FnMut(&mut S, ActorMessage) -> Result<(), ActorError> + Send + Sync,
    S: Default + Send + Sync + 'static,
{
    pub fn new(name: impl Into<String>, handler: F) -> Self {
        Self {
            name: name.into(),
            handler,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, S> Actor for FunctionActor<F, S>
where
    F: FnMut(&mut S, ActorMessage) -> Result<(), ActorError> + Send + Sync + 'static,
    S: Default + Send + Sync + 'static,
{
    type Config = ();
    type State = S;

    async fn pre_start(
        &mut self,
        _config: Self::Config,
        _ctx: &mut ActorContext,
    ) -> Result<Self::State, ActorError> {
        Ok(S::default())
    }

    async fn handle(
        &mut self,
        msg: ActorMessage,
        state: &mut Self::State,
        _ctx: &mut ActorContext,
    ) -> Result<(), ActorError> {
        (self.handler)(state, msg)
    }
}
