//! MQTT client wrapper for the pipeline system.

use crate::topics::TopicBuilder;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

/// Errors that can occur with MQTT operations.
#[derive(Error, Debug)]
pub enum MqttError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Subscribe error: {0}")]
    Subscribe(#[from] rumqttc::ClientError),

    #[error("Publish error: {0}")]
    Publish(rumqttc::ClientError),

    #[error("Serialization error: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("Event loop error: {0}")]
    EventLoop(String),
}

/// Configuration for MQTT connection.
#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub client_id: String,
    pub broker_host: String,
    pub broker_port: u16,
    pub namespace: String,
    pub keep_alive_secs: u64,
    pub clean_session: bool,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            client_id: format!("aiko-{}", uuid::Uuid::new_v4()),
            broker_host: "localhost".to_string(),
            broker_port: 1883,
            namespace: "default".to_string(),
            keep_alive_secs: 30,
            clean_session: true,
            username: None,
            password: None,
        }
    }
}

impl MqttConfig {
    /// Create a new config with the given broker address.
    pub fn new(broker_host: impl Into<String>, broker_port: u16) -> Self {
        Self {
            broker_host: broker_host.into(),
            broker_port,
            ..Default::default()
        }
    }

    /// Set the client ID.
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    /// Set the namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set credentials.
    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }
}

/// An incoming MQTT message.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
}

/// MQTT client wrapper for the pipeline system.
pub struct MqttTransport {
    client: AsyncClient,
    config: MqttConfig,
    topics: TopicBuilder,
    message_tx: broadcast::Sender<IncomingMessage>,
    connected: Arc<RwLock<bool>>,
}

impl MqttTransport {
    /// Create a new MQTT transport and start the event loop.
    pub async fn connect(config: MqttConfig) -> Result<(Self, MqttEventLoop), MqttError> {
        let mut options = MqttOptions::new(&config.client_id, &config.broker_host, config.broker_port);
        options.set_keep_alive(Duration::from_secs(config.keep_alive_secs));
        options.set_clean_session(config.clean_session);

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            options.set_credentials(user, pass);
        }

        let (client, eventloop) = AsyncClient::new(options, 256);
        let (message_tx, _) = broadcast::channel(1024);
        let topics = TopicBuilder::new(&config.namespace);
        let connected = Arc::new(RwLock::new(false));

        let transport = Self {
            client,
            config,
            topics,
            message_tx: message_tx.clone(),
            connected: connected.clone(),
        };

        let event_loop = MqttEventLoop {
            eventloop,
            message_tx,
            connected,
        };

        Ok((transport, event_loop))
    }

    /// Subscribe to a topic.
    pub async fn subscribe(&self, topic: &str, qos: QoS) -> Result<(), MqttError> {
        self.client.subscribe(topic, qos).await?;
        info!(topic = topic, "Subscribed to topic");
        Ok(())
    }

    /// Unsubscribe from a topic.
    pub async fn unsubscribe(&self, topic: &str) -> Result<(), MqttError> {
        self.client.unsubscribe(topic).await?;
        info!(topic = topic, "Unsubscribed from topic");
        Ok(())
    }

    /// Publish a serializable message.
    pub async fn publish<T: Serialize>(
        &self,
        topic: &str,
        payload: &T,
        qos: QoS,
        retain: bool,
    ) -> Result<(), MqttError> {
        let bytes = bincode::serialize(payload)?;
        self.publish_raw(topic, bytes, qos, retain).await
    }

    /// Publish raw bytes.
    pub async fn publish_raw(
        &self,
        topic: &str,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
    ) -> Result<(), MqttError> {
        self.client
            .publish(topic, qos, retain, payload)
            .await
            .map_err(MqttError::Publish)?;
        debug!(topic = topic, "Published message");
        Ok(())
    }

    /// Subscribe to incoming messages.
    pub fn subscribe_messages(&self) -> broadcast::Receiver<IncomingMessage> {
        self.message_tx.subscribe()
    }

    /// Get the topic builder for this namespace.
    pub fn topics(&self) -> &TopicBuilder {
        &self.topics
    }

    /// Get the configuration.
    pub fn config(&self) -> &MqttConfig {
        &self.config
    }

    /// Check if connected.
    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }

    /// Disconnect from the broker.
    pub async fn disconnect(&self) -> Result<(), MqttError> {
        self.client.disconnect().await.map_err(MqttError::Publish)?;
        Ok(())
    }
}

/// MQTT event loop handler - run this in a separate task.
pub struct MqttEventLoop {
    eventloop: EventLoop,
    message_tx: broadcast::Sender<IncomingMessage>,
    connected: Arc<RwLock<bool>>,
}

impl MqttEventLoop {
    /// Run the event loop. This should be spawned as a task.
    pub async fn run(mut self) {
        loop {
            match self.eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let msg = IncomingMessage {
                        topic: publish.topic.to_string(),
                        payload: publish.payload.to_vec(),
                        qos: publish.qos,
                    };
                    // Ignore send errors (no receivers)
                    let _ = self.message_tx.send(msg);
                }
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    *self.connected.write().await = true;
                    info!("MQTT connected");
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    *self.connected.write().await = false;
                    warn!("MQTT disconnected");
                }
                Ok(_) => {}
                Err(e) => {
                    *self.connected.write().await = false;
                    error!(error = %e, "MQTT error");
                    // Small delay before reconnect attempt
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Run the event loop with a shutdown signal.
    pub async fn run_with_shutdown(mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            tokio::select! {
                event = self.eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            let msg = IncomingMessage {
                                topic: publish.topic.to_string(),
                                payload: publish.payload.to_vec(),
                                qos: publish.qos,
                            };
                            let _ = self.message_tx.send(msg);
                        }
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            *self.connected.write().await = true;
                            info!("MQTT connected");
                        }
                        Ok(Event::Incoming(Packet::Disconnect)) => {
                            *self.connected.write().await = false;
                            warn!("MQTT disconnected");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            *self.connected.write().await = false;
                            error!(error = %e, "MQTT error");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("MQTT event loop shutting down");
                        break;
                    }
                }
            }
        }
    }
}
