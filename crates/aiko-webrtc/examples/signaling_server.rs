//! Minimal WebSocket signaling relay for two WebRTC peers.
//!
//! Accepts exactly two WebSocket connections on port 9001 and relays
//! every text/binary message from one peer to the other.
//!
//! # Usage
//!
//! ```sh
//! cargo run -p aiko-webrtc --example signaling_server
//! ```

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:9001")
        .await
        .expect("failed to bind port 9001");
    println!("Signaling server listening on ws://localhost:9001");

    loop {
        // Wait for two peers to connect
        let (s1, a1) = listener.accept().await.expect("accept peer 1");
        let ws1 = accept_async(s1).await.expect("ws handshake peer 1");
        println!("Peer 1 connected from {a1}");

        let (s2, a2) = listener.accept().await.expect("accept peer 2");
        let ws2 = accept_async(s2).await.expect("ws handshake peer 2");
        println!("Peer 2 connected from {a2}");

        let (mut w1, mut r1) = ws1.split();
        let (mut w2, mut r2) = ws2.split();

        let relay_1_to_2 = tokio::spawn(async move {
            while let Some(Ok(msg)) = r1.next().await {
                if msg.is_text() || msg.is_binary() {
                    if w2.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        });

        let relay_2_to_1 = tokio::spawn(async move {
            while let Some(Ok(msg)) = r2.next().await {
                if msg.is_text() || msg.is_binary() {
                    if w1.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        });

        tokio::select! {
            _ = relay_1_to_2 => {}
            _ = relay_2_to_1 => {}
        }

        println!("Session ended, waiting for next pair...\n");
    }
}
