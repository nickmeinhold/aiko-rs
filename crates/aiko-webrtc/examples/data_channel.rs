//! Two-peer data channel demo.
//!
//! Requires a WebSocket signaling server that relays JSON messages
//! between two connected clients.
//!
//! # Usage
//!
//! ```sh
//! # Terminal 1 — offerer
//! cargo run -p aiko-webrtc --example data_channel -- --role offerer
//!
//! # Terminal 2 — answerer
//! cargo run -p aiko-webrtc --example data_channel -- --role answerer
//! ```

use aiko_webrtc::prelude::*;
use aiko_webrtc::signaling::WsSignalingClient;
use std::time::Duration;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let role = match args
        .iter()
        .position(|a| a == "--role")
        .and_then(|i| args.get(i + 1))
    {
        Some(r) if r == "offerer" => PeerRole::Offerer,
        Some(r) if r == "answerer" => PeerRole::Answerer,
        _ => {
            eprintln!("Usage: data_channel --role <offerer|answerer> [--signal <ws://url>]");
            std::process::exit(1);
        }
    };

    let signal_url = args
        .iter()
        .position(|a| a == "--signal")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("ws://localhost:8080");

    println!("Starting as {:?}, signaling: {}", role, signal_url);

    let config = WebRtcConfig::new().with_role(role);
    let signaling = Box::new(WsSignalingClient::connect(signal_url).await?);

    let (transport, event_loop) = WebRtcTransport::connect(config, signaling).await?;

    // If offerer, create data channel before negotiation
    if role == PeerRole::Offerer {
        transport.open_channel("chat").await?;
        println!("Created 'chat' data channel");
    }

    // Listen for events
    let mut events = transport.subscribe_events();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            println!("Event: {:?}", event);
        }
    });

    // Listen for messages
    let mut messages = transport.subscribe_messages();
    tokio::spawn(async move {
        while let Ok(msg) = messages.recv().await {
            println!(
                "[{}] {}",
                msg.channel,
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    // Run event loop in background
    tokio::spawn(async move {
        if let Err(e) = event_loop.run().await {
            eprintln!("Event loop error: {}", e);
        }
    });

    // Offerer sends messages after connection is established
    if role == PeerRole::Offerer {
        // Wait for connection
        tokio::time::sleep(Duration::from_secs(3)).await;

        for i in 0..5 {
            let msg = format!("hello #{}", i);
            match transport.send("chat", msg.as_bytes()).await {
                Ok(()) => println!("Sent: {}", msg),
                Err(e) => eprintln!("Send error: {}", e),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // Wait for Ctrl+C
    println!("Press Ctrl+C to exit");
    tokio::signal::ctrl_c().await?;
    transport.close().await?;

    Ok(())
}
