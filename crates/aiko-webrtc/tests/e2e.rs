//! End-to-end tests for WebRTC data channels and media pipelines.
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

/// End-to-end audio pipeline test: PCM I16Le → Opus encode → WebRTC → Opus decode → PCM I16Le.
///
/// Validates the full audio media pipeline by creating two in-process peers, sending silent
/// 20ms stereo frames through `WebRtcAudioSink` (Opus encoder), and receiving decoded frames
/// from `WebRtcAudioSource` (Opus decoder). Asserts that sample rate, channels, and format
/// survive the roundtrip, and that silence stays near-silent after lossy Opus encoding.
#[cfg(feature = "audio")]
#[tokio::test]
async fn test_audio_pipeline_roundtrip() {
    use aiko_core::element::{ElementContext, SinkElement, SourceElement};
    use aiko_core::frame::{Frame, FrameId, StreamId};
    use aiko_core::media::{AudioFrame, SampleFormat};
    use aiko_webrtc::pipeline::{WebRtcAudioSink, WebRtcAudioSinkConfig, WebRtcAudioSource};

    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (sig_a, sig_b) = signaling_pair();

        // --- Offerer: add Opus audio track before negotiation ---
        let (offerer, ev_a) = WebRtcTransport::connect(
            WebRtcConfig::new().with_role(PeerRole::Offerer),
            Box::new(sig_a),
        )
        .await
        .unwrap();

        let local_track = offerer
            .add_local_track(LocalTrackConfig::audio("a0", "stream0"))
            .await
            .unwrap();

        // --- Answerer ---
        let (answerer, ev_b) = WebRtcTransport::connect(
            WebRtcConfig::new().with_role(PeerRole::Answerer),
            Box::new(sig_b),
        )
        .await
        .unwrap();

        let mut offerer_events = offerer.subscribe_events();
        let mut answerer_events = answerer.subscribe_events();

        // Spawn signaling event loops
        tokio::spawn(ev_a.run());
        tokio::spawn(ev_b.run());

        // Channel for delivering the remote track from the event handler to the source
        let (track_tx, track_rx) = tokio::sync::mpsc::channel::<RemoteTrack>(1);

        // Answerer task: wait for TrackAdded and forward to the audio source
        let answerer_handle = tokio::spawn(async move {
            loop {
                match answerer_events.recv().await.unwrap() {
                    PeerEvent::TrackAdded(remote_track) => {
                        track_tx.send(remote_track).await.unwrap();
                        break;
                    }
                    _ => continue,
                }
            }
        });

        // Wait for offerer to reach Connected state
        loop {
            match offerer_events.recv().await.unwrap() {
                PeerEvent::StateChanged(PeerState::Connected) => break,
                _ => continue,
            }
        }

        // Allow the media pipeline to fully establish
        tokio::time::sleep(Duration::from_millis(500)).await;

        // --- Set up audio source (answerer / decoder side) ---
        // Spawn concurrently so it's ready when on_track fires.
        let source_handle = tokio::spawn(async move {
            let mut source = WebRtcAudioSource::new(track_rx, 48000, 2);
            let mut source_ctx = ElementContext::new("WebRtcAudioSource", "test");
            let decoded = source.next(&mut source_ctx).await;
            source.shutdown().await.ok();
            decoded
        });

        // --- Set up audio sink (offerer / encoder side) ---
        let mut sink = WebRtcAudioSink::new(local_track);
        sink.init(WebRtcAudioSinkConfig::default())
            .await
            .unwrap();
        let mut sink_ctx = ElementContext::new("WebRtcAudioSink", "test");

        // Generate silent 20ms stereo frames at 48kHz:
        // 960 samples/channel * 2 channels * 2 bytes = 3840 bytes
        let samples_per_channel: usize = 960;
        let channels: usize = 2;
        let silence_bytes = vec![0u8; samples_per_channel * channels * 2];
        let audio_frame = AudioFrame::new(48000, 2, SampleFormat::I16Le, silence_bytes);

        // Send frames continuously until the source produces a decoded frame.
        let stream_id = StreamId::new();
        let mut frame_idx = 0u64;
        let decoded = loop {
            let frame = Frame::new(stream_id, FrameId(frame_idx), audio_frame.clone());
            sink.consume(frame, &mut sink_ctx).await.unwrap();
            frame_idx += 1;

            // Pace at ~20ms (Opus frame duration)
            tokio::time::sleep(Duration::from_millis(20)).await;

            // Check if the source has produced a frame (non-blocking)
            if source_handle.is_finished() {
                break source_handle
                    .await
                    .expect("Source task panicked")
                    .expect("Source returned error")
                    .expect("Source returned None");
            }

            assert!(frame_idx < 500, "Sent 500 frames without decoded output");
        };

        let decoded_audio = &decoded.payload;

        // Verify audio properties survived the roundtrip
        assert_eq!(decoded_audio.sample_rate, 48000);
        assert_eq!(decoded_audio.channels, 2);
        assert_eq!(decoded_audio.format, SampleFormat::I16Le);
        assert_eq!(decoded_audio.samples_per_channel(), samples_per_channel);
        assert!(!decoded_audio.data.is_empty(), "Decoded audio data is empty");

        // After lossy Opus encode/decode of silence, the RMS should be near zero.
        // Interpret decoded bytes as i16 samples and check RMS < 100.
        let samples: Vec<i16> = decoded_audio
            .data
            .chunks_exact(2)
            .map(|c| i16::from_le_bytes([c[0], c[1]]))
            .collect();
        let rms = (samples.iter().map(|&s| (s as f64).powi(2)).sum::<f64>()
            / samples.len() as f64)
            .sqrt();
        assert!(
            rms < 100.0,
            "Expected near-silent output after Opus roundtrip, got RMS={rms}"
        );

        // Clean up
        answerer_handle.await.unwrap();
        sink.shutdown().await.unwrap();
        offerer.close().await.unwrap();
        answerer.close().await.unwrap();
    })
    .await;

    result.expect("Test timed out after 30 seconds");
}

/// End-to-end video pipeline test: I420 → H264 encode → WebRTC → H264 decode → I420.
///
/// Validates the full media pipeline by creating two in-process peers, sending a solid-color
/// I420 frame through `WebRtcVideoSink` (encoder), and receiving the decoded frame from
/// `WebRtcVideoSource` (decoder). Asserts that dimensions are preserved and pixel data
/// survives the lossy H264 roundtrip.
#[cfg(feature = "video")]
#[tokio::test]
async fn test_video_pipeline_roundtrip() {
    use aiko_core::element::{ElementContext, SinkElement, SourceElement};
    use aiko_core::frame::{Frame, FrameId, StreamId};
    use aiko_core::media::{PixelFormat, VideoFrame};
    use aiko_webrtc::pipeline::{WebRtcVideoSink, WebRtcVideoSinkConfig, WebRtcVideoSource};

    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (sig_a, sig_b) = signaling_pair();

        // --- Offerer: add H264 track before negotiation ---
        let (offerer, ev_a) = WebRtcTransport::connect(
            WebRtcConfig::new().with_role(PeerRole::Offerer),
            Box::new(sig_a),
        )
        .await
        .unwrap();

        let local_track = offerer
            .add_local_track(LocalTrackConfig::h264_video("v0", "stream0"))
            .await
            .unwrap();

        // --- Answerer ---
        let (answerer, ev_b) = WebRtcTransport::connect(
            WebRtcConfig::new().with_role(PeerRole::Answerer),
            Box::new(sig_b),
        )
        .await
        .unwrap();

        let mut offerer_events = offerer.subscribe_events();
        let mut answerer_events = answerer.subscribe_events();

        // Spawn signaling event loops
        tokio::spawn(ev_a.run());
        tokio::spawn(ev_b.run());

        // Channel for delivering the remote track from the event handler to the source
        let (track_tx, track_rx) = tokio::sync::mpsc::channel::<RemoteTrack>(1);

        // Answerer task: wait for TrackAdded and forward to the video source
        let answerer_handle = tokio::spawn(async move {
            loop {
                match answerer_events.recv().await.unwrap() {
                    PeerEvent::TrackAdded(remote_track) => {
                        track_tx.send(remote_track).await.unwrap();
                        break;
                    }
                    _ => continue,
                }
            }
        });

        // Wait for offerer to reach Connected state
        loop {
            match offerer_events.recv().await.unwrap() {
                PeerEvent::StateChanged(PeerState::Connected) => break,
                _ => continue,
            }
        }

        // Allow the media pipeline to fully establish
        tokio::time::sleep(Duration::from_millis(500)).await;

        // --- Set up video source (answerer / decoder side) ---
        // Spawn concurrently so it's ready when on_track fires.
        // In webrtc-rs, on_track fires only after the first RTP packet arrives,
        // so the source blocks on track_rx until the sink starts sending.
        let source_handle = tokio::spawn(async move {
            let mut source = WebRtcVideoSource::new(track_rx);
            let mut source_ctx = ElementContext::new("WebRtcVideoSource", "test");
            let decoded = source.next(&mut source_ctx).await;
            source.shutdown().await.ok();
            decoded
        });

        // --- Set up video sink (offerer / encoder side) ---
        let mut sink = WebRtcVideoSink::new(local_track);
        sink.init(WebRtcVideoSinkConfig {
            fps: 30,
            keyframe_interval_secs: 1,
        })
        .await
        .unwrap();
        let mut sink_ctx = ElementContext::new("WebRtcVideoSink", "test");

        // Generate a solid mid-gray I420 frame (Y=128, U=128, V=128)
        let width: u32 = 64;
        let height: u32 = 48;
        let y_size = (width * height) as usize;
        let uv_size = y_size / 4;
        let mut data = vec![0u8; y_size + 2 * uv_size];
        data[..y_size].fill(128); // Y = 128 (mid-gray)
        data[y_size..y_size + uv_size].fill(128); // U = 128
        data[y_size + uv_size..].fill(128); // V = 128
        let video_frame = VideoFrame::from_raw(width, height, PixelFormat::I420, data);

        // Send frames continuously until the source produces a decoded frame.
        // The first RTP triggers on_track, then the source reads RTP and decodes.
        let stream_id = StreamId::new();
        let mut frame_idx = 0u64;
        let decoded = loop {
            let frame = Frame::new(stream_id, FrameId(frame_idx), video_frame.clone());
            sink.consume(frame, &mut sink_ctx).await.unwrap();
            frame_idx += 1;

            // Check if the source has produced a frame (non-blocking)
            tokio::time::sleep(Duration::from_millis(33)).await;
            if source_handle.is_finished() {
                break source_handle
                    .await
                    .expect("Source task panicked")
                    .expect("Source returned error")
                    .expect("Source returned None");
            }

            assert!(frame_idx < 300, "Sent 300 frames without decoded output");
        };

        let decoded_video = &decoded.payload;

        // Verify dimensions survived the roundtrip
        assert_eq!(decoded_video.width, width);
        assert_eq!(decoded_video.height, height);

        // After lossy H264 encode/decode, the Y plane should be approximately 128 (not zero).
        // We allow a generous tolerance since H264 is lossy.
        let decoded_y_size = (decoded_video.width * decoded_video.height) as usize;
        let y_avg: f64 = decoded_video.data[..decoded_y_size]
            .iter()
            .map(|&b| b as f64)
            .sum::<f64>()
            / decoded_y_size as f64;
        assert!(
            y_avg > 100.0 && y_avg < 156.0,
            "Expected Y plane average ≈128 after H264 roundtrip, got {y_avg}"
        );

        // Clean up
        answerer_handle.await.unwrap();
        sink.shutdown().await.unwrap();
        offerer.close().await.unwrap();
        answerer.close().await.unwrap();
    })
    .await;

    result.expect("Test timed out after 30 seconds");
}
