//! WebRTC video demo — Rust peer sends H264 SMPTE color bars.
//!
//! Connects as the Offerer, adds an H264 video track, and continuously
//! streams animated SMPTE color bars at 640×480 / 30 fps.
//!
//! # Prerequisites
//!
//! 1. Start the signaling relay:
//!    `cargo run -p aiko-webrtc --example signaling_server`
//! 2. Start this peer:
//!    `cargo run -p aiko-webrtc --example video_demo --features video-demo`
//! 3. Open `crates/aiko-webrtc/examples/video_demo.html` in a browser.

use aiko_webrtc::media::{LocalTrackConfig, MediaKind};
use aiko_webrtc::prelude::*;
use aiko_webrtc::signaling::WsSignalingClient;
use openh264::encoder::Encoder;
use openh264::formats::YUVSlices;
use std::time::Duration;

const WIDTH: usize = 640;
const HEIGHT: usize = 480;
const FPS: u64 = 30;

/// SMPTE color bars in (R, G, B).
const BARS: [(u8, u8, u8); 8] = [
    (255, 255, 255), // white
    (255, 255, 0),   // yellow
    (0, 255, 255),   // cyan
    (0, 255, 0),     // green
    (255, 0, 255),   // magenta
    (255, 0, 0),     // red
    (0, 0, 255),     // blue
    (0, 0, 0),       // black
];

/// Pre-computed YUV values for each SMPTE bar.
fn bar_yuv() -> [(u8, u8, u8); 8] {
    let mut out = [(0u8, 128u8, 128u8); 8];
    for (i, &(r, g, b)) in BARS.iter().enumerate() {
        let r = r as f64;
        let g = g as f64;
        let b = b as f64;
        let y = (0.257 * r + 0.504 * g + 0.098 * b + 16.0).clamp(0.0, 255.0) as u8;
        let u = (-0.148 * r - 0.291 * g + 0.439 * b + 128.0).clamp(0.0, 255.0) as u8;
        let v = (0.439 * r - 0.368 * g - 0.071 * b + 128.0).clamp(0.0, 255.0) as u8;
        out[i] = (y, u, v);
    }
    out
}

/// Generate YUV420 planes for one frame of scrolling SMPTE bars.
fn generate_yuv420(frame: u64) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let bar_width = WIDTH / 8;
    let offset = (frame as usize * 2) % WIDTH;

    let yuv = bar_yuv();

    let mut y_plane = vec![0u8; WIDTH * HEIGHT];
    let mut u_plane = vec![128u8; (WIDTH / 2) * (HEIGHT / 2)];
    let mut v_plane = vec![128u8; (WIDTH / 2) * (HEIGHT / 2)];

    for row in 0..HEIGHT {
        for col in 0..WIDTH {
            let shifted = (col + offset) % WIDTH;
            let bar = (shifted / bar_width).min(7);
            let (yv, uv, vv) = yuv[bar];

            y_plane[row * WIDTH + col] = yv;

            if row % 2 == 0 && col % 2 == 0 {
                let ci = (row / 2) * (WIDTH / 2) + (col / 2);
                u_plane[ci] = uv;
                v_plane[ci] = vv;
            }
        }
    }

    (y_plane, u_plane, v_plane)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let signal_url = std::env::args()
        .position(|a| a == "--signal")
        .and_then(|i| std::env::args().nth(i + 1))
        .unwrap_or_else(|| "ws://localhost:9001".to_string());

    println!("Connecting to signaling server at {signal_url} ...");

    let config = WebRtcConfig::new().with_role(PeerRole::Offerer);
    let signaling = Box::new(WsSignalingClient::connect(&signal_url).await?);
    let (transport, event_loop) = WebRtcTransport::connect(config, signaling).await?;

    // Add H264 video track before negotiation
    let track_config = LocalTrackConfig {
        kind: MediaKind::Video,
        codec_mime_type: "video/H264".to_string(),
        id: "video0".to_string(),
        stream_id: "aiko-video".to_string(),
    };
    let local_track = transport.add_local_track(track_config).await?;
    println!("Added H264 video track");

    // Subscribe to events: one for logging, one for waiting on connection
    let mut log_events = transport.subscribe_events();
    let mut conn_events = transport.subscribe_events();

    tokio::spawn(async move {
        while let Ok(event) = log_events.recv().await {
            println!("Event: {event:?}");
        }
    });

    // Run signaling event loop
    tokio::spawn(async move {
        if let Err(e) = event_loop.run().await {
            eprintln!("Event loop error: {e}");
        }
    });

    // Wait for actual WebRTC connection before encoding
    println!("Waiting for browser peer to connect...");
    loop {
        match conn_events.recv().await {
            Ok(PeerEvent::StateChanged(PeerState::Connected)) => {
                println!("Peer connected!");
                break;
            }
            Ok(_) => continue,
            Err(e) => {
                eprintln!("Event channel error: {e}");
                return Err(e.into());
            }
        }
    }

    // Small delay so the media pipeline is fully ready
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Setup H264 encoder (detects dimensions from first YUVSource)
    let mut encoder = Encoder::new()?;

    let frame_duration = Duration::from_millis(1000 / FPS);
    let mut frame_num: u64 = 0;
    let keyframe_interval = FPS * 2; // Force keyframe every 2 seconds

    println!("Streaming SMPTE bars at {WIDTH}x{HEIGHT} @ {FPS}fps");

    loop {
        // Force periodic keyframes so the decoder can (re)sync
        if frame_num > 0 && frame_num % keyframe_interval == 0 {
            encoder.force_intra_frame();
        }

        let (y, u, v) = generate_yuv420(frame_num);
        let yuv = YUVSlices::new(
            (&y, &u, &v),
            (WIDTH, HEIGHT),
            (WIDTH, WIDTH / 2, WIDTH / 2),
        );

        let bitstream = encoder.encode(&yuv)?;
        let h264_data = bitstream.to_vec();

        if !h264_data.is_empty() {
            if let Err(e) = local_track.write_sample(&h264_data, frame_duration).await {
                eprintln!("write_sample error: {e}");
            }
        }

        frame_num += 1;
        if frame_num % (FPS * 5) == 0 {
            println!("Sent {frame_num} frames");
        }

        tokio::time::sleep(frame_duration).await;
    }
}
