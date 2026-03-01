//! WebRTC video source — reads H264 RTP from a remote track and decodes to `VideoFrame`.

use aiko_core::element::{ElementContext, SourceElement};
use aiko_core::error::ElementError;
use aiko_core::frame::{Frame, FrameId, StreamId};
use aiko_core::media::{PixelFormat, VideoFrame};
use async_trait::async_trait;
use openh264::decoder::Decoder;
use openh264::formats::YUVSource;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::media::RemoteTrack;
use crate::pipeline::h264::H264Depacketizer;

/// A pipeline source that reads H264 RTP from a WebRTC remote track and decodes to `VideoFrame`.
///
/// The caller listens for `PeerEvent::TrackAdded` and sends the `RemoteTrack` through
/// the provided `mpsc::Sender`. The source waits for the track on first call to `next()`,
/// then continuously reads RTP packets, depacketizes H264 NALUs, and decodes to I420.
///
/// # Example
///
/// ```rust,ignore
/// let (track_tx, track_rx) = tokio::sync::mpsc::channel(1);
/// let source = WebRtcVideoSource::new(track_rx);
///
/// // In event handler:
/// if let PeerEvent::TrackAdded(remote_track) = event {
///     track_tx.send(remote_track).await.ok();
/// }
///
/// Pipeline::new("ingest")
///     .source(source)
///     .then(MyTransform)
///     .sink(MySink);
/// ```
pub struct WebRtcVideoSource {
    track_rx: Option<mpsc::Receiver<RemoteTrack>>,
    track: Option<RemoteTrack>,
    decoder: Option<Decoder>,
    depacketizer: H264Depacketizer,
    stream_id: StreamId,
    frame_count: u64,
}

impl WebRtcVideoSource {
    /// Create a new video source that waits for a remote track on the given channel.
    pub fn new(track_rx: mpsc::Receiver<RemoteTrack>) -> Self {
        Self {
            track_rx: Some(track_rx),
            track: None,
            decoder: None,
            depacketizer: H264Depacketizer::new(),
            stream_id: StreamId::new(),
            frame_count: 0,
        }
    }
}

#[async_trait]
impl SourceElement for WebRtcVideoSource {
    type Output = VideoFrame;
    type Config = ();

    fn name(&self) -> &str {
        "WebRtcVideoSource"
    }

    async fn next(
        &mut self,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<VideoFrame>>, ElementError> {
        // Wait for remote track on first call
        if self.track.is_none() {
            if let Some(rx) = self.track_rx.as_mut() {
                match rx.recv().await {
                    Some(track) => {
                        debug!("WebRtcVideoSource received remote track");
                        self.track = Some(track);
                    }
                    None => {
                        debug!("WebRtcVideoSource track channel closed — shutting down");
                        return Ok(None);
                    }
                }
            } else {
                return Ok(None);
            }
        }

        // Lazy-init decoder
        if self.decoder.is_none() {
            self.decoder = Some(
                Decoder::new()
                    .map_err(|e| ElementError::Processing(format!("H264 decoder init: {e}")))?,
            );
            debug!("H264 decoder initialized");
        }

        let track = self.track.as_ref().unwrap().track().clone();
        let decoder = self.decoder.as_mut().unwrap();

        // Read RTP packets until we get a complete frame
        loop {
            let (rtp_packet, _attributes) = track
                .read_rtp()
                .await
                .map_err(|e| ElementError::Processing(format!("read_rtp: {e}")))?;

            // Feed to depacketizer; returns Some(nalu_bytes) when a complete NAL unit is ready
            if let Some(nalu_data) = self.depacketizer.process_rtp(&rtp_packet) {
                // Decode the NAL unit
                match decoder.decode(&nalu_data) {
                    Ok(Some(decoded)) => {
                        let (w, h) = decoded.dimensions();
                        let w = w as u32;
                        let h = h as u32;

                        // Extract I420 planes from the decoded frame
                        let y_size = (w * h) as usize;
                        let uv_size = y_size / 4;
                        let mut data = vec![0u8; y_size + 2 * uv_size];

                        // Copy Y plane
                        let y_stride = decoded.strides().0;
                        for row in 0..h as usize {
                            let src_start = row * y_stride;
                            let dst_start = row * w as usize;
                            data[dst_start..dst_start + w as usize]
                                .copy_from_slice(&decoded.y()[src_start..src_start + w as usize]);
                        }

                        // Copy U plane
                        let u_stride = decoded.strides().1;
                        let half_w = (w / 2) as usize;
                        let half_h = (h / 2) as usize;
                        for row in 0..half_h {
                            let src_start = row * u_stride;
                            let dst_start = y_size + row * half_w;
                            data[dst_start..dst_start + half_w]
                                .copy_from_slice(&decoded.u()[src_start..src_start + half_w]);
                        }

                        // Copy V plane
                        let v_stride = decoded.strides().2;
                        for row in 0..half_h {
                            let src_start = row * v_stride;
                            let dst_start = y_size + uv_size + row * half_w;
                            data[dst_start..dst_start + half_w]
                                .copy_from_slice(&decoded.v()[src_start..src_start + half_w]);
                        }

                        let video_frame = VideoFrame::from_raw(w, h, PixelFormat::I420, data);
                        let frame_id = FrameId(self.frame_count);
                        self.frame_count += 1;

                        return Ok(Some(Frame::new(self.stream_id, frame_id, video_frame)));
                    }
                    Ok(None) => {
                        // Decoder buffering — needs more data
                        continue;
                    }
                    Err(e) => {
                        warn!("H264 decode error: {e}");
                        continue;
                    }
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), ElementError> {
        debug!(
            "WebRtcVideoSource shutting down after {} frames",
            self.frame_count
        );
        self.decoder = None;
        self.track = None;
        self.track_rx = None;
        Ok(())
    }
}
