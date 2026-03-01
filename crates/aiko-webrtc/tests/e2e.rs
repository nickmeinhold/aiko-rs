//! End-to-end test for WebRTC data channels.
//!
//! Uses in-process channel-based signaling (no WebSocket server needed).

use aiko_webrtc::error::WebRtcError;
use aiko_webrtc::prelude::*;
use aiko_webrtc::signaling::SignalingMessage;
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

// --- In-process signaling via crosswired mpsc channels ---

struct ChannelSignalingClient {
    tx: mpsc::UnboundedSender<SignalingMessage>,
    rx: Mutex<mpsc::UnboundedReceiver<SignalingMessage>>,
}

/// Create a crosswired pair: what A sends, B receives, and vice versa.
fn signaling_pair() -> (ChannelSignalingClient, ChannelSignalingClient) {
    let (a_to_b_tx, a_to_b_rx) = mpsc::unbounded_channel();
    let (b_to_a_tx, b_to_a_rx) = mpsc::unbounded_channel();

    (
        ChannelSignalingClient {
            tx: a_to_b_tx,
            rx: Mutex::new(b_to_a_rx),
        },
        ChannelSignalingClient {
            tx: b_to_a_tx,
            rx: Mutex::new(a_to_b_rx),
        },
    )
}

#[async_trait]
impl SignalingClient for ChannelSignalingClient {
    async fn send(&self, msg: SignalingMessage) -> aiko_webrtc::error::Result<()> {
        self.tx.send(msg).map_err(|_| WebRtcError::ChannelClosed)
    }

    async fn recv(&self) -> aiko_webrtc::error::Result<SignalingMessage> {
        self.rx
            .lock()
            .await
            .recv()
            .await
            .ok_or(WebRtcError::ChannelClosed)
    }
}

// --- Tests ---

#[tokio::test]
async fn test_data_channel_roundtrip() {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (sig_a, sig_b) = signaling_pair();

        let (offerer, ev_a) = WebRtcTransport::connect(
            WebRtcConfig::new().with_role(PeerRole::Offerer),
            Box::new(sig_a),
        )
        .await
        .unwrap();

        let (answerer, ev_b) = WebRtcTransport::connect(
            WebRtcConfig::new().with_role(PeerRole::Answerer),
            Box::new(sig_b),
        )
        .await
        .unwrap();

        // Create channel before negotiation so it's in the SDP offer
        offerer.open_channel("test").await.unwrap();

        let mut answerer_events = answerer.subscribe_events();
        let mut answerer_msgs = answerer.subscribe_messages();

        // Spawn event loops (handle signaling exchange)
        tokio::spawn(ev_a.run());
        tokio::spawn(ev_b.run());

        // Wait for the answerer to receive the data channel
        loop {
            match answerer_events.recv().await.unwrap() {
                PeerEvent::DataChannelOpened(label) if label == "test" => break,
                _ => continue,
            }
        }

        // Allow SCTP data channel to fully open on both sides
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Send from offerer
        offerer.send("test", b"hello e2e").await.unwrap();

        // Receive on answerer
        let msg = answerer_msgs.recv().await.unwrap();
        assert_eq!(msg.channel, "test");
        assert_eq!(msg.payload, b"hello e2e");

        // Clean up
        offerer.close().await.unwrap();
        answerer.close().await.unwrap();
    })
    .await;

    result.expect("Test timed out after 30 seconds");
}
