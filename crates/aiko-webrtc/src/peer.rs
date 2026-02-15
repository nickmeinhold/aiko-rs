//! Managed WebRTC peer connection.

use crate::error::Result;
use crate::media::RemoteTrack;
use std::sync::Arc;
use tokio::sync::RwLock;
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

/// State of the WebRTC peer connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    New,
    Connecting,
    Connected,
    Disconnected,
    Failed,
    Closed,
}

impl From<RTCPeerConnectionState> for PeerState {
    fn from(state: RTCPeerConnectionState) -> Self {
        match state {
            RTCPeerConnectionState::New | RTCPeerConnectionState::Unspecified => PeerState::New,
            RTCPeerConnectionState::Connecting => PeerState::Connecting,
            RTCPeerConnectionState::Connected => PeerState::Connected,
            RTCPeerConnectionState::Disconnected => PeerState::Disconnected,
            RTCPeerConnectionState::Failed => PeerState::Failed,
            RTCPeerConnectionState::Closed => PeerState::Closed,
        }
    }
}

/// Events emitted by the peer connection.
#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// Peer connection state changed.
    StateChanged(PeerState),
    /// A data channel was opened (by the remote peer).
    DataChannelOpened(String),
    /// A remote media track was added.
    TrackAdded(RemoteTrack),
    /// An ICE candidate error occurred.
    IceCandidateError(String),
}

/// Wrapper around `RTCPeerConnection` with state tracking.
pub struct ManagedPeer {
    connection: RTCPeerConnection,
    state: Arc<RwLock<PeerState>>,
}

impl ManagedPeer {
    pub(crate) fn new(connection: RTCPeerConnection) -> Self {
        Self {
            connection,
            state: Arc::new(RwLock::new(PeerState::New)),
        }
    }

    /// Get a reference to the underlying peer connection.
    pub fn connection(&self) -> &RTCPeerConnection {
        &self.connection
    }

    /// Get a handle to the state for use in callbacks.
    pub(crate) fn state_handle(&self) -> Arc<RwLock<PeerState>> {
        self.state.clone()
    }

    /// Create an SDP offer.
    pub async fn create_offer(&self) -> Result<RTCSessionDescription> {
        self.connection.create_offer(None).await.map_err(Into::into)
    }

    /// Create an SDP answer.
    pub async fn create_answer(&self) -> Result<RTCSessionDescription> {
        self.connection
            .create_answer(None)
            .await
            .map_err(Into::into)
    }

    /// Set the local SDP description.
    pub async fn set_local_description(&self, desc: RTCSessionDescription) -> Result<()> {
        self.connection
            .set_local_description(desc)
            .await
            .map_err(Into::into)
    }

    /// Set the remote SDP description.
    pub async fn set_remote_description(&self, desc: RTCSessionDescription) -> Result<()> {
        self.connection
            .set_remote_description(desc)
            .await
            .map_err(Into::into)
    }

    /// Add a remote ICE candidate.
    pub async fn add_ice_candidate(&self, candidate: RTCIceCandidateInit) -> Result<()> {
        self.connection
            .add_ice_candidate(candidate)
            .await
            .map_err(Into::into)
    }

    /// Create a new data channel.
    pub async fn create_data_channel(
        &self,
        label: &str,
        options: Option<RTCDataChannelInit>,
    ) -> Result<Arc<RTCDataChannel>> {
        self.connection
            .create_data_channel(label, options)
            .await
            .map_err(Into::into)
    }

    /// Get the current peer state.
    pub async fn state(&self) -> PeerState {
        *self.state.read().await
    }

    /// Close the peer connection.
    pub async fn close(&self) -> Result<()> {
        self.connection.close().await.map_err(Into::into)
    }
}
