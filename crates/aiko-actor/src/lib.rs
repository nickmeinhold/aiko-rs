//! Aiko Actor - Actor system for the Aiko distributed pipeline framework.
//!
//! This crate provides the actor runtime:
//!
//! - [`Actor`](actor::Actor) - Trait for defining actors
//! - [`ActorRef`](actor::ActorRef) - Handle for sending messages to actors
//! - [`ActorContext`](actor::ActorContext) - Context available during actor execution
//! - [`ActorRegistry`](registry::ActorRegistry) - Registry for looking up actors
//!
//! # Example
//!
//! ```rust,ignore
//! use aiko_actor::prelude::*;
//! use aiko_core::message::ActorMessage;
//!
//! struct MyActor;
//!
//! #[async_trait]
//! impl Actor for MyActor {
//!     type Config = ();
//!     type State = i32;
//!
//!     async fn pre_start(&mut self, _: (), _: &mut ActorContext) -> Result<i32, ActorError> {
//!         Ok(0)
//!     }
//!
//!     async fn handle(&mut self, msg: ActorMessage, state: &mut i32, _: &mut ActorContext) -> Result<(), ActorError> {
//!         *state += 1;
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let actor_ref = spawn("my_actor", MyActor, (), None).await.unwrap();
//!     actor_ref.stop().await.unwrap();
//! }
//! ```

pub mod actor;
pub mod registry;

/// Convenient re-exports of commonly used types.
pub mod prelude {
    pub use crate::actor::{
        spawn, Actor, ActorConfig, ActorContext, ActorRef, FunctionActor, SupervisionAction,
    };
    pub use crate::registry::ActorRegistry;
    pub use aiko_core::error::ActorError;
    pub use async_trait::async_trait;
}
