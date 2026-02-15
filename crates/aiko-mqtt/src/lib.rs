//! Aiko MQTT - MQTT transport layer for the Aiko distributed pipeline framework.
//!
//! This crate provides MQTT-based communication:
//!
//! - [`MqttTransport`](client::MqttTransport) - MQTT client wrapper
//! - [`TopicBuilder`](topics::TopicBuilder) - Structured topic construction
//! - [`FrameEnvelope`](codec::FrameEnvelope) - Frame serialization for network transport
//! - ML-specific data types (ImageData, Detections)
//!
//! # Example
//!
//! ```rust,ignore
//! use aiko_mqtt::prelude::*;
//! use rumqttc::QoS;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = MqttConfig::new("localhost", 1883)
//!         .with_namespace("my_app");
//!
//!     let (mqtt, event_loop) = MqttTransport::connect(config).await?;
//!
//!     // Run event loop in background
//!     tokio::spawn(event_loop.run());
//!
//!     // Subscribe to pipeline frames
//!     mqtt.subscribe(mqtt.topics().all_pipeline_frames(), QoS::AtLeastOnce).await?;
//!
//!     // Publish a message
//!     mqtt.publish(
//!         &mqtt.topics().pipeline_frames("my_pipeline"),
//!         &"hello",
//!         QoS::AtLeastOnce,
//!         false,
//!     ).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod codec;
pub mod topics;

/// Convenient re-exports of commonly used types.
pub mod prelude {
    pub use crate::client::{IncomingMessage, MqttConfig, MqttError, MqttEventLoop, MqttTransport};
    pub use crate::codec::{
        BoundingBox, CodecError, Detections, FrameEnvelope, ImageData, ImageFormat,
        NetworkSerializable,
    };
    pub use crate::topics::{ParsedTopic, TopicBuilder, TopicKind, TopicMatcher};
    pub use rumqttc::QoS;
}
