//! WebRTC video demo — Rust peer sends H264 SMPTE color bars via pipeline.
//!
//! Demonstrates the pipeline ↔ WebRTC integration:
//! - `SmpteSource` generates I420 `VideoFrame`s with scrolling color bars
//! - `WebRtcVideoSink` encodes them to H264 and sends via WebRTC
//!
//! # Prerequisites
//!
//! 1. Start the signaling relay:
//!    `cargo run -p aiko-webrtc --example signaling_server`
//! 2. Start this peer:
//!    `cargo run -p aiko-webrtc --example video_demo --features video-demo`
//! 3. Open `crates/aiko-webrtc/examples/video_demo.html` in a browser.

use aiko_core::element::{ElementContext, SinkElement, SourceElement};
use aiko_core::error::ElementError;
use aiko_core::frame::{Frame, FrameId, StreamId};
use aiko_core::media::{PixelFormat, VideoFrame};
use aiko_webrtc::media::LocalTrackConfig;
use aiko_webrtc::pipeline::{WebRtcVideoSink, WebRtcVideoSinkConfig};
use aiko_webrtc::prelude::*;
use aiko_webrtc::signaling::WsSignalingClient;
use async_trait::async_trait;
use std::time::Duration;

const WIDTH: u32 = 640;
const HEIGHT: u32 = 480;
const FPS: u32 = 30;

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

/// Pipeline source element that generates scrolling SMPTE color bars as I420 `VideoFrame`s.
struct SmpteSource {
    stream_id: StreamId,
    frame_num: u64,
    width: u32,
    height: u32,
    fps: u32,
}

impl SmpteSource {
    fn new(width: u32, height: u32, fps: u32) -> Self {
        Self {
            stream_id: StreamId::new(),
            frame_num: 0,
            width,
            height,
            fps,
        }
    }

    /// Generate an I420 VideoFrame with scrolling SMPTE bars.
    fn generate_frame(&self) -> VideoFrame {
        let w = self.width as usize;
        let h = self.height as usize;
        let bar_width = w / 8;
        let offset = (self.frame_num as usize * 2) % w;
        let yuv = bar_yuv();

        let mut data = vec![0u8; PixelFormat::I420.frame_size(self.width, self.height)];
        let y_size = w * h;

        for row in 0..h {
            for col in 0..w {
                let shifted = (col + offset) % w;
                let bar = (shifted / bar_width).min(7);
                let (yv, uv, vv) = yuv[bar];

                data[row * w + col] = yv;

                if row % 2 == 0 && col % 2 == 0 {
                    let ci = (row / 2) * (w / 2) + (col / 2);
                    data[y_size + ci] = uv;
                    data[y_size + y_size / 4 + ci] = vv;
                }
            }
        }

        VideoFrame::from_raw(self.width, self.height, PixelFormat::I420, data)
    }
}

#[async_trait]
impl SourceElement for SmpteSource {
    type Output = VideoFrame;
    type Config = ();

    fn name(&self) -> &str {
        "SmpteSource"
    }

    async fn next(
        &mut self,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<VideoFrame>>, ElementError> {
        let video_frame = self.generate_frame();
        let frame = Frame::new(self.stream_id, FrameId(self.frame_num), video_frame);

        self.frame_num += 1;
        if self.frame_num % (self.fps as u64 * 5) == 0 {
            println!("SmpteSource: generated {} frames", self.frame_num);
        }

        // Pace to target fps
        tokio::time::sleep(Duration::from_millis(1000 / self.fps as u64)).await;
        Ok(Some(frame))
    }
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
    let local_track = transport
        .add_local_track(LocalTrackConfig::h264_video("video0", "aiko-video"))
        .await?;
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

    // Build the pipeline: SmpteSource -> WebRtcVideoSink
    let source = SmpteSource::new(WIDTH, HEIGHT, FPS);
    let mut sink = WebRtcVideoSink::new(local_track);
    sink.init(WebRtcVideoSinkConfig {
        fps: FPS,
        keyframe_interval_secs: 2,
    })
    .await?;

    let mut source_impl = source;
    let mut ctx = ElementContext::new("video_pipeline", "video_demo");

    println!("Streaming SMPTE bars at {WIDTH}x{HEIGHT} @ {FPS}fps (using pipeline)");

    loop {
        match source_impl.next(&mut ctx).await? {
            Some(frame) => {
                sink.consume(frame, &mut ctx).await?;
            }
            None => break,
        }
    }

    Ok(())
}
