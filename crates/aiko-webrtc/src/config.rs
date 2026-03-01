//! Configuration types for WebRTC transport.

/// ICE server configuration.
#[derive(Debug, Clone)]
pub struct IceServer {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}

impl IceServer {
    /// Create an ICE server with the given URLs.
    pub fn new(urls: Vec<String>) -> Self {
        Self {
            urls,
            username: None,
            credential: None,
        }
    }

    /// Create a STUN server entry.
    pub fn stun(url: impl Into<String>) -> Self {
        Self::new(vec![url.into()])
    }

    /// Set credentials for TURN servers.
    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        credential: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.credential = Some(credential.into());
        self
    }
}

/// Role of this peer in the WebRTC connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRole {
    /// This peer creates the offer.
    Offerer,
    /// This peer receives the offer and creates an answer.
    Answerer,
}

/// Configuration for a WebRTC connection.
#[derive(Debug, Clone)]
pub struct WebRtcConfig {
    pub role: PeerRole,
    pub ice_servers: Vec<IceServer>,
    pub ordered_channels: bool,
}

impl Default for WebRtcConfig {
    fn default() -> Self {
        Self {
            role: PeerRole::Offerer,
            ice_servers: vec![IceServer::stun("stun:stun.l.google.com:19302")],
            ordered_channels: true,
        }
    }
}

impl WebRtcConfig {
    /// Create a new config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the peer role.
    pub fn with_role(mut self, role: PeerRole) -> Self {
        self.role = role;
        self
    }

    /// Add an ICE server.
    pub fn with_ice_server(mut self, server: IceServer) -> Self {
        self.ice_servers.push(server);
        self
    }

    /// Replace all ICE servers.
    pub fn with_ice_servers(mut self, servers: Vec<IceServer>) -> Self {
        self.ice_servers = servers;
        self
    }

    /// Set whether data channels should be ordered.
    pub fn with_ordered_channels(mut self, ordered: bool) -> Self {
        self.ordered_channels = ordered;
        self
    }

    /// Add a STUN server.
    ///
    /// STUN servers help peers discover their public IP addresses for NAT traversal.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use aiko_webrtc::config::WebRtcConfig;
    /// let config = WebRtcConfig::new()
    ///     .with_stun("stun:stun.l.google.com:19302");
    /// ```
    pub fn with_stun(self, url: impl Into<String>) -> Self {
        self.with_ice_server(IceServer::stun(url))
    }

    /// Add a TURN server with credentials.
    ///
    /// TURN servers relay traffic when direct peer-to-peer is not possible
    /// (e.g., behind symmetric NATs or restrictive firewalls).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use aiko_webrtc::config::WebRtcConfig;
    /// let config = WebRtcConfig::new()
    ///     .with_turn("turn:turn.example.com:3478", "user", "pass");
    /// ```
    pub fn with_turn(
        self,
        url: impl Into<String>,
        username: impl Into<String>,
        credential: impl Into<String>,
    ) -> Self {
        self.with_ice_server(
            IceServer::new(vec![url.into()]).with_credentials(username, credential),
        )
    }
}
