//! WebRTC video sink — encodes I420 `VideoFrame`s to H264 and writes to a local track.

use aiko_core::element::{ElementConfig, ElementContext, SinkElement};
use aiko_core::error::ElementError;
use aiko_core::frame::Frame;
use aiko_core::media::VideoFrame;
use async_trait::async_trait;
use openh264::encoder::Encoder;
use openh264::formats::YUVSlices;
use std::time::Duration;
use tracing::{debug, trace};

use crate::media::LocalTrack;

/// Configuration for [`WebRtcVideoSink`].
#[derive(Debug, Clone)]
pub struct WebRtcVideoSinkConfig {
    /// Target frames per second. Used to compute per-frame duration for RTP.
    pub fps: u32,
    /// Interval in seconds between forced keyframes.
    pub keyframe_interval_secs: u32,
}

impl Default for WebRtcVideoSinkConfig {
    fn default() -> Self {
        Self {
            fps: 30,
            keyframe_interval_secs: 2,
        }
    }
}

impl ElementConfig for WebRtcVideoSinkConfig {}

/// A pipeline sink that encodes `VideoFrame` (I420) → H264 and writes to a WebRTC track.
///
/// The caller creates a `LocalTrack` via [`WebRtcTransport::add_local_track()`] and passes
/// it in at construction. The sink owns the H264 encoder and handles periodic keyframe
/// insertion.
///
/// # Example
///
/// ```rust,ignore
/// use aiko_webrtc::pipeline::WebRtcVideoSink;
///
/// let local_track = transport.add_local_track(LocalTrackConfig::h264_video("v0", "stream0")).await?;
/// let sink = WebRtcVideoSink::new(local_track);
///
/// Pipeline::new("video")
///     .source(my_video_source)
///     .sink(sink);
/// ```
pub struct WebRtcVideoSink {
    track: LocalTrack,
    encoder: Option<Encoder>,
    frame_count: u64,
    fps: u32,
    keyframe_interval_frames: u64,
}

impl WebRtcVideoSink {
    /// Create a new video sink that writes H264 to the given local track.
    pub fn new(track: LocalTrack) -> Self {
        Self {
            track,
            encoder: None,
            frame_count: 0,
            fps: 30,
            keyframe_interval_frames: 60, // 2 seconds at 30fps
        }
    }
}

#[async_trait]
impl SinkElement for WebRtcVideoSink {
    type Input = VideoFrame;
    type Config = WebRtcVideoSinkConfig;

    fn name(&self) -> &str {
        "WebRtcVideoSink"
    }

    async fn init(&mut self, config: Self::Config) -> Result<(), ElementError> {
        self.fps = config.fps;
        self.keyframe_interval_frames = (config.fps * config.keyframe_interval_secs) as u64;
        debug!(
            "WebRtcVideoSink initialized: {}fps, keyframe every {}s ({} frames)",
            self.fps, config.keyframe_interval_secs, self.keyframe_interval_frames
        );
        Ok(())
    }

    async fn consume(
        &mut self,
        frame: Frame<VideoFrame>,
        _ctx: &mut ElementContext,
    ) -> Result<(), ElementError> {
        let video = &frame.payload;

        // Lazy-init encoder on first frame (openh264 detects dimensions from YUVSource)
        if self.encoder.is_none() {
            self.encoder = Some(
                Encoder::new().map_err(|e| ElementError::Processing(format!("H264 encoder init: {e}")))?,
            );
            debug!(
                "H264 encoder initialized for {}x{} {:?}",
                video.width, video.height, video.format
            );
        }

        let encoder = self.encoder.as_mut().unwrap();

        // Force periodic keyframes so the browser decoder can (re)sync
        if self.frame_count > 0 && self.frame_count % self.keyframe_interval_frames == 0 {
            encoder.force_intra_frame();
            trace!("Forced keyframe at frame {}", self.frame_count);
        }

        let (y, u, v) = video.i420_planes();
        let w = video.width as usize;
        let h = video.height as usize;

        let yuv = YUVSlices::new(
            (y, u, v),
            (w, h),
            (w, w / 2, w / 2), // strides: Y=width, U=width/2, V=width/2
        );

        // Encode and extract bytes in a block so EncodedBitStream (which is !Send)
        // is dropped before the .await point below.
        let h264_data = {
            let bitstream = encoder
                .encode(&yuv)
                .map_err(|e| ElementError::Processing(format!("H264 encode: {e}")))?;
            bitstream.to_vec()
        };

        if !h264_data.is_empty() {
            let frame_duration = Duration::from_millis(1000 / self.fps as u64);
            self.track
                .write_sample(&h264_data, frame_duration)
                .await
                .map_err(|e| ElementError::Processing(format!("write_sample: {e}")))?;
        }

        self.frame_count += 1;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ElementError> {
        debug!("WebRtcVideoSink shutting down after {} frames", self.frame_count);
        self.encoder = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aiko_core::frame::{FrameId, StreamId};
    use aiko_core::media::PixelFormat;

    #[test]
    fn test_config_default() {
        let config = WebRtcVideoSinkConfig::default();
        assert_eq!(config.fps, 30);
        assert_eq!(config.keyframe_interval_secs, 2);
    }

    // Integration test with actual encoding requires a LocalTrack (WebRTC peer),
    // tested via the video_demo example and e2e tests.

    #[test]
    fn test_i420_frame_for_encoding() {
        // Verify that a VideoFrame can be constructed and its planes extracted
        // in the format expected by the encoder
        let vf = VideoFrame::new(640, 480, PixelFormat::I420);
        let (y, u, v) = vf.i420_planes();
        assert_eq!(y.len(), 640 * 480);
        assert_eq!(u.len(), 320 * 240);
        assert_eq!(v.len(), 320 * 240);

        // Verify YUVSlices can be constructed (this is what the encoder expects)
        let _yuv = YUVSlices::new(
            (y, u, v),
            (640, 480),
            (640, 320, 320),
        );
    }
}
