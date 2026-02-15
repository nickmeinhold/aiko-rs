//! Actor registry for looking up actors by name or ID.

use crate::actor::ActorRef;
use aiko_core::message::ActorId;
use dashmap::DashMap;
use std::sync::Arc;

/// Thread-safe registry for looking up actors by name or ID.
#[derive(Clone, Default)]
pub struct ActorRegistry {
    by_id: Arc<DashMap<ActorId, ActorRef>>,
    by_name: Arc<DashMap<String, ActorRef>>,
}

impl ActorRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an actor in the registry.
    pub fn register(&self, actor_ref: ActorRef) {
        self.by_id.insert(actor_ref.id, actor_ref.clone());
        self.by_name.insert(actor_ref.name.clone(), actor_ref);
    }

    /// Unregister an actor by ID.
    pub fn unregister(&self, id: &ActorId) {
        if let Some((_, actor_ref)) = self.by_id.remove(id) {
            self.by_name.remove(&actor_ref.name);
        }
    }

    /// Unregister an actor by name.
    pub fn unregister_by_name(&self, name: &str) {
        if let Some((_, actor_ref)) = self.by_name.remove(name) {
            self.by_id.remove(&actor_ref.id);
        }
    }

    /// Get an actor reference by ID.
    pub fn get_by_id(&self, id: &ActorId) -> Option<ActorRef> {
        self.by_id.get(id).map(|r| r.clone())
    }

    /// Get an actor reference by name.
    pub fn get_by_name(&self, name: &str) -> Option<ActorRef> {
        self.by_name.get(name).map(|r| r.clone())
    }

    /// Get all registered actors.
    pub fn all(&self) -> Vec<ActorRef> {
        self.by_id.iter().map(|r| r.value().clone()).collect()
    }

    /// Get the number of registered actors.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Check if an actor with the given name exists.
    pub fn contains_name(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// Check if an actor with the given ID exists.
    pub fn contains_id(&self, id: &ActorId) -> bool {
        self.by_id.contains_key(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn make_actor_ref(name: &str) -> ActorRef {
        let (tx, _rx) = mpsc::channel(1);
        ActorRef::new(ActorId::new(), name.to_string(), tx)
    }

    #[test]
    fn test_register_and_lookup() {
        let registry = ActorRegistry::new();
        let actor = make_actor_ref("test_actor");
        let id = actor.id;

        registry.register(actor);

        assert!(registry.contains_name("test_actor"));
        assert!(registry.contains_id(&id));
        assert_eq!(registry.len(), 1);

        let found = registry.get_by_name("test_actor").unwrap();
        assert_eq!(found.id, id);
    }

    #[test]
    fn test_unregister() {
        let registry = ActorRegistry::new();
        let actor = make_actor_ref("test_actor");
        let id = actor.id;

        registry.register(actor);
        registry.unregister(&id);

        assert!(!registry.contains_name("test_actor"));
        assert!(!registry.contains_id(&id));
        assert!(registry.is_empty());
    }
}
