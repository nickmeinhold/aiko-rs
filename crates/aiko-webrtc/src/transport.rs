//! WebRTC transport — the main API surface.
//!
//! Mirrors the `MqttTransport` / `MqttEventLoop` split from `aiko-mqtt`:
//!
//! | MQTT | WebRTC |
//! |------|--------|
//! | `MqttTransport::connect(config)` | `WebRtcTransport::connect(config, signaling)` |
//! | `transport.subscribe(topic, qos)` | `transport.open_channel(label)` |
//! | `transport.publish(topic, payload, qos, retain)` | `transport.send(channel, payload)` |
//! | `transport.subscribe_messages()` | `transport.subscribe_messages()` |
//! | `event_loop.run()` | `event_loop.run()` |

use crate::config::{PeerRole, WebRtcConfig};
use crate::data_channel::IncomingMessage;
use crate::error::{Result, WebRtcError};
use crate::media::{LocalTrack, LocalTrackConfig, RemoteTrack};
use crate::peer::{ManagedPeer, PeerEvent, PeerState};
use crate::signaling::{SignalingClient, SignalingMessage};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::track::track_local::TrackLocal;

/// WebRTC transport client.
pub struct WebRtcTransport {
    peer: Arc<ManagedPeer>,
    channels: Arc<DashMap<String, Arc<RTCDataChannel>>>,
    message_tx: broadcast::Sender<IncomingMessage>,
    event_tx: broadcast::Sender<PeerEvent>,
    config: WebRtcConfig,
}

impl WebRtcTransport {
    /// Create a new WebRTC transport and prepare the event loop.
    ///
    /// The returned `WebRtcEventLoop` must be spawned to handle signaling
    /// and maintain the connection.
    pub async fn connect(
        config: WebRtcConfig,
        signaling: Box<dyn SignalingClient>,
    ) -> Result<(Self, WebRtcEventLoop)> {
        // Build WebRTC API
        let mut m = MediaEngine::default();
        m.register_default_codecs()?;

        let api = APIBuilder::new().with_media_engine(m).build();

        // Convert ICE servers
        let ice_servers: Vec<RTCIceServer> = config
            .ice_servers
            .iter()
            .map(|s| RTCIceServer {
                urls: s.urls.clone(),
                username: s.username.clone().unwrap_or_default(),
                credential: s.credential.clone().unwrap_or_default(),
                ..Default::default()
            })
            .collect();

        let rtc_config = RTCConfiguration {
            ice_servers,
            ..Default::default()
        };

        let pc = api.new_peer_connection(rtc_config).await?;
        let peer = Arc::new(ManagedPeer::new(pc));

        let (message_tx, _) = broadcast::channel(1024);
        let (event_tx, _) = broadcast::channel(256);
        let (ice_tx, ice_rx) = mpsc::unbounded_channel::<RTCIceCandidateInit>();
        let channels: Arc<DashMap<String, Arc<RTCDataChannel>>> = Arc::new(DashMap::new());

        // --- Callbacks ---

        // Forward local ICE candidates to the signaling pump
        {
            let ice_tx = ice_tx.clone();
            peer.connection()
                .on_ice_candidate(Box::new(move |candidate| {
                    let ice_tx = ice_tx.clone();
                    Box::pin(async move {
                        if let Some(c) = candidate {
                            if let Ok(init) = c.to_json() {
                                let _ = ice_tx.send(init);
                            }
                        }
                    })
                }));
        }

        // Track peer connection state changes
        {
            let event_tx = event_tx.clone();
            let state_handle = peer.state_handle();
            peer.connection()
                .on_peer_connection_state_change(Box::new(move |state| {
                    let event_tx = event_tx.clone();
                    let state_handle = state_handle.clone();
                    Box::pin(async move {
                        let peer_state: PeerState = state.into();
                        *state_handle.write().await = peer_state;
                        let _ = event_tx.send(PeerEvent::StateChanged(peer_state));
                    })
                }));
        }

        // Handle incoming data channels (remote-created)
        {
            let channels = channels.clone();
            let message_tx = message_tx.clone();
            let event_tx = event_tx.clone();
            peer.connection()
                .on_data_channel(Box::new(move |dc| {
                    let channels = channels.clone();
                    let message_tx = message_tx.clone();
                    let event_tx = event_tx.clone();
                    Box::pin(async move {
                        let label = dc.label().to_string();
                        let dc_ref = dc.clone();
                        let label_for_handler = label.clone();
                        let message_tx_for_handler = message_tx.clone();

                        dc.on_message(Box::new(move |msg| {
                            let label = label_for_handler.clone();
                            let message_tx = message_tx_for_handler.clone();
                            Box::pin(async move {
                                let incoming = IncomingMessage {
                                    channel: label,
                                    payload: msg.data.to_vec(),
                                };
                                let _ = message_tx.send(incoming);
                            })
                        }));

                        let _ = event_tx.send(PeerEvent::DataChannelOpened(label.clone()));
                        channels.insert(label, dc_ref);
                    })
                }));
        }

        // Handle incoming remote tracks
        {
            let event_tx = event_tx.clone();
            peer.connection()
                .on_track(Box::new(move |track, _receiver, _transceiver| {
                    let event_tx = event_tx.clone();
                    Box::pin(async move {
                        let _ = event_tx.send(PeerEvent::TrackAdded(RemoteTrack { track }));
                    })
                }));
        }

        let transport = Self {
            peer: peer.clone(),
            channels: channels.clone(),
            message_tx: message_tx.clone(),
            event_tx: event_tx.clone(),
            config: config.clone(),
        };

        let event_loop = WebRtcEventLoop {
            peer,
            signaling,
            role: config.role,
            ice_rx,
        };

        Ok((transport, event_loop))
    }

    /// Open a named data channel (offerer-side).
    ///
    /// Must be called before the event loop sends the offer so the channel
    /// is included in the SDP negotiation.
    pub async fn open_channel(&self, label: &str) -> Result<()> {
        let dc = self.peer.create_data_channel(label, None).await?;
        let label_str = label.to_string();
        let message_tx = self.message_tx.clone();
        let label_for_handler = label_str.clone();

        dc.on_message(Box::new(move |msg| {
            let label = label_for_handler.clone();
            let message_tx = message_tx.clone();
            Box::pin(async move {
                let incoming = IncomingMessage {
                    channel: label,
                    payload: msg.data.to_vec(),
                };
                let _ = message_tx.send(incoming);
            })
        }));

        self.channels.insert(label_str, dc);
        Ok(())
    }

    /// Send data on a named data channel.
    pub async fn send(&self, channel: &str, payload: &[u8]) -> Result<()> {
        let dc = self
            .channels
            .get(channel)
            .ok_or_else(|| WebRtcError::DataChannel(format!("channel '{}' not found", channel)))?;
        dc.send(&Bytes::copy_from_slice(payload))
            .await
            .map_err(|e| WebRtcError::DataChannel(e.to_string()))?;
        debug!(channel = channel, "Sent message on data channel");
        Ok(())
    }

    /// Send raw bytes on a named data channel.
    pub async fn send_raw(&self, channel: &str, data: Vec<u8>) -> Result<()> {
        self.send(channel, &data).await
    }

    /// Subscribe to incoming data channel messages.
    pub fn subscribe_messages(&self) -> broadcast::Receiver<IncomingMessage> {
        self.message_tx.subscribe()
    }

    /// Subscribe to peer connection events.
    pub fn subscribe_events(&self) -> broadcast::Receiver<PeerEvent> {
        self.event_tx.subscribe()
    }

    /// Add a local media track to the peer connection.
    pub async fn add_local_track(&self, config: LocalTrackConfig) -> Result<LocalTrack> {
        let codec = config.codec_capability();
        let track = Arc::new(TrackLocalStaticSample::new(
            codec,
            config.id.clone(),
            config.stream_id.clone(),
        ));

        self.peer
            .connection()
            .add_track(Arc::clone(&track) as Arc<dyn TrackLocal + Send + Sync>)
            .await?;

        Ok(LocalTrack { track, config })
    }

    /// Get the configuration.
    pub fn config(&self) -> &WebRtcConfig {
        &self.config
    }

    /// Close the peer connection.
    pub async fn close(&self) -> Result<()> {
        self.peer.close().await
    }
}

/// WebRTC event loop — handles signaling and ICE candidate exchange.
///
/// Must be spawned as a task after creating the transport.
pub struct WebRtcEventLoop {
    peer: Arc<ManagedPeer>,
    signaling: Box<dyn SignalingClient>,
    role: PeerRole,
    ice_rx: mpsc::UnboundedReceiver<RTCIceCandidateInit>,
}

impl WebRtcEventLoop {
    /// Run the event loop until the signaling channel closes.
    pub async fn run(mut self) -> Result<()> {
        // If offerer, create and send offer
        if self.role == PeerRole::Offerer {
            let offer = self.peer.create_offer().await?;
            self.peer.set_local_description(offer.clone()).await?;

            self.signaling
                .send(SignalingMessage::Offer { sdp: offer.sdp })
                .await?;
            info!("Sent SDP offer");
        }

        // Main signaling pump
        loop {
            tokio::select! {
                Some(candidate) = self.ice_rx.recv() => {
                    self.signaling
                        .send(SignalingMessage::IceCandidate {
                            candidate: candidate.candidate,
                            sdp_mid: candidate.sdp_mid,
                            sdp_mline_index: candidate.sdp_mline_index,
                        })
                        .await?;
                    debug!("Sent ICE candidate via signaling");
                }

                msg = self.signaling.recv() => {
                    match msg {
                        Ok(SignalingMessage::Offer { sdp }) => {
                            info!("Received SDP offer");
                            let offer = RTCSessionDescription::offer(sdp)
                                .map_err(|e| WebRtcError::Signaling(e.to_string()))?;
                            self.peer.set_remote_description(offer).await?;

                            let answer = self.peer.create_answer().await?;
                            self.peer.set_local_description(answer.clone()).await?;

                            self.signaling
                                .send(SignalingMessage::Answer { sdp: answer.sdp })
                                .await?;
                            info!("Sent SDP answer");
                        }
                        Ok(SignalingMessage::Answer { sdp }) => {
                            info!("Received SDP answer");
                            let answer = RTCSessionDescription::answer(sdp)
                                .map_err(|e| WebRtcError::Signaling(e.to_string()))?;
                            self.peer.set_remote_description(answer).await?;
                        }
                        Ok(SignalingMessage::IceCandidate {
                            candidate,
                            sdp_mid,
                            sdp_mline_index,
                        }) => {
                            debug!("Received ICE candidate");
                            let init = RTCIceCandidateInit {
                                candidate,
                                sdp_mid,
                                sdp_mline_index,
                                username_fragment: None,
                            };
                            self.peer.add_ice_candidate(init).await?;
                        }
                        Err(WebRtcError::ChannelClosed) => {
                            info!("Signaling channel closed");
                            break;
                        }
                        Err(e) => {
                            error!(error = %e, "Signaling error");
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Run the event loop with a shutdown signal.
    pub async fn run_with_shutdown(
        mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        if self.role == PeerRole::Offerer {
            let offer = self.peer.create_offer().await?;
            self.peer.set_local_description(offer.clone()).await?;

            self.signaling
                .send(SignalingMessage::Offer { sdp: offer.sdp })
                .await?;
            info!("Sent SDP offer");
        }

        loop {
            tokio::select! {
                Some(candidate) = self.ice_rx.recv() => {
                    self.signaling
                        .send(SignalingMessage::IceCandidate {
                            candidate: candidate.candidate,
                            sdp_mid: candidate.sdp_mid,
                            sdp_mline_index: candidate.sdp_mline_index,
                        })
                        .await?;
                }

                msg = self.signaling.recv() => {
                    match msg {
                        Ok(SignalingMessage::Offer { sdp }) => {
                            let offer = RTCSessionDescription::offer(sdp)
                                .map_err(|e| WebRtcError::Signaling(e.to_string()))?;
                            self.peer.set_remote_description(offer).await?;

                            let answer = self.peer.create_answer().await?;
                            self.peer.set_local_description(answer.clone()).await?;

                            self.signaling
                                .send(SignalingMessage::Answer { sdp: answer.sdp })
                                .await?;
                        }
                        Ok(SignalingMessage::Answer { sdp }) => {
                            let answer = RTCSessionDescription::answer(sdp)
                                .map_err(|e| WebRtcError::Signaling(e.to_string()))?;
                            self.peer.set_remote_description(answer).await?;
                        }
                        Ok(SignalingMessage::IceCandidate {
                            candidate,
                            sdp_mid,
                            sdp_mline_index,
                        }) => {
                            let init = RTCIceCandidateInit {
                                candidate,
                                sdp_mid,
                                sdp_mline_index,
                                username_fragment: None,
                            };
                            self.peer.add_ice_candidate(init).await?;
                        }
                        Err(WebRtcError::ChannelClosed) => break,
                        Err(e) => return Err(e),
                    }
                }

                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("WebRTC event loop shutting down");
                        break;
                    }
                }
            }
        }

        self.peer.close().await?;
        Ok(())
    }
}
