//! MQTT topic structure for the distributed pipeline system.
//!
//! Topic hierarchy:
//! - `aiko/{namespace}/services/{service_id}/status` - Service heartbeat/status
//! - `aiko/{namespace}/services/{service_id}/control` - Control messages
//! - `aiko/{namespace}/pipelines/{pipeline_id}/frames` - Frame data
//! - `aiko/{namespace}/pipelines/{pipeline_id}/events` - Pipeline events
//! - `aiko/{namespace}/actors/{actor_id}/inbox` - Actor messages
//! - `aiko/{namespace}/discovery` - Service discovery

use aiko_core::message::ActorId;

/// Builder for constructing MQTT topics.
#[derive(Debug, Clone)]
pub struct TopicBuilder {
    namespace: String,
}

impl TopicBuilder {
    /// Create a new topic builder for the given namespace.
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Service status topic (for publishing heartbeats).
    pub fn service_status(&self, service_id: &str) -> String {
        format!("aiko/{}/services/{}/status", self.namespace, service_id)
    }

    /// Service control topic (for receiving commands).
    pub fn service_control(&self, service_id: &str) -> String {
        format!("aiko/{}/services/{}/control", self.namespace, service_id)
    }

    /// Pipeline frames topic.
    pub fn pipeline_frames(&self, pipeline_id: &str) -> String {
        format!("aiko/{}/pipelines/{}/frames", self.namespace, pipeline_id)
    }

    /// Pipeline events topic.
    pub fn pipeline_events(&self, pipeline_id: &str) -> String {
        format!("aiko/{}/pipelines/{}/events", self.namespace, pipeline_id)
    }

    /// Actor inbox topic.
    pub fn actor_inbox(&self, actor_id: &ActorId) -> String {
        format!("aiko/{}/actors/{}/inbox", self.namespace, actor_id.0)
    }

    /// Discovery topic.
    pub fn discovery(&self) -> String {
        format!("aiko/{}/discovery", self.namespace)
    }

    /// Wildcard subscription for all service status.
    pub fn all_service_status(&self) -> String {
        format!("aiko/{}/services/+/status", self.namespace)
    }

    /// Wildcard for all pipeline frames.
    pub fn all_pipeline_frames(&self) -> String {
        format!("aiko/{}/pipelines/+/frames", self.namespace)
    }

    /// Wildcard for all pipeline events.
    pub fn all_pipeline_events(&self) -> String {
        format!("aiko/{}/pipelines/+/events", self.namespace)
    }
}

/// Parsed topic information.
#[derive(Debug, Clone)]
pub struct ParsedTopic {
    pub kind: TopicKind,
    pub id: String,
}

/// Types of topics in the system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicKind {
    ServiceStatus,
    ServiceControl,
    PipelineFrames,
    PipelineEvents,
    ActorInbox,
    Discovery,
    Unknown,
}

/// Topic pattern matcher.
pub struct TopicMatcher {
    namespace: String,
}

impl TopicMatcher {
    /// Create a new topic matcher for the given namespace.
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }

    /// Parse a topic and extract its components.
    pub fn parse(&self, topic: &str) -> Option<ParsedTopic> {
        let parts: Vec<&str> = topic.split('/').collect();

        if parts.len() < 3 || parts[0] != "aiko" || parts[1] != self.namespace {
            return None;
        }

        match parts.get(2).copied() {
            Some("services") if parts.len() >= 5 => {
                let service_id = parts[3].to_string();
                let kind = match parts.get(4).copied() {
                    Some("status") => TopicKind::ServiceStatus,
                    Some("control") => TopicKind::ServiceControl,
                    _ => TopicKind::Unknown,
                };
                Some(ParsedTopic {
                    kind,
                    id: service_id,
                })
            }
            Some("pipelines") if parts.len() >= 5 => {
                let pipeline_id = parts[3].to_string();
                let kind = match parts.get(4).copied() {
                    Some("frames") => TopicKind::PipelineFrames,
                    Some("events") => TopicKind::PipelineEvents,
                    _ => TopicKind::Unknown,
                };
                Some(ParsedTopic {
                    kind,
                    id: pipeline_id,
                })
            }
            Some("actors") if parts.len() >= 5 => Some(ParsedTopic {
                kind: TopicKind::ActorInbox,
                id: parts[3].to_string(),
            }),
            Some("discovery") => Some(ParsedTopic {
                kind: TopicKind::Discovery,
                id: String::new(),
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_builder() {
        let topics = TopicBuilder::new("test");

        assert_eq!(
            topics.service_status("node1"),
            "aiko/test/services/node1/status"
        );
        assert_eq!(
            topics.pipeline_frames("pipeline1"),
            "aiko/test/pipelines/pipeline1/frames"
        );
        assert_eq!(topics.discovery(), "aiko/test/discovery");
    }

    #[test]
    fn test_topic_matcher() {
        let matcher = TopicMatcher::new("test");

        let parsed = matcher
            .parse("aiko/test/services/node1/status")
            .unwrap();
        assert_eq!(parsed.kind, TopicKind::ServiceStatus);
        assert_eq!(parsed.id, "node1");

        let parsed = matcher
            .parse("aiko/test/pipelines/p1/frames")
            .unwrap();
        assert_eq!(parsed.kind, TopicKind::PipelineFrames);
        assert_eq!(parsed.id, "p1");

        assert!(matcher.parse("invalid/topic").is_none());
        assert!(matcher.parse("aiko/other/discovery").is_none());
    }
}
