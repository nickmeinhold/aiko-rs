//! Media track types for WebRTC audio/video transport.
//!
//! These are thin wrappers around the `webrtc` crate's track types,
//! providing no codec encoding/decoding — just transport plumbing
//! for pre-encoded samples.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::track::track_remote::TrackRemote;

/// The kind of media track.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaKind {
    Audio,
    Video,
}

/// Configuration for creating a local media track.
#[derive(Debug, Clone)]
pub struct LocalTrackConfig {
    pub kind: MediaKind,
    pub codec_mime_type: String,
    pub id: String,
    pub stream_id: String,
}

impl LocalTrackConfig {
    /// Create an Opus audio track config.
    pub fn audio(id: impl Into<String>, stream_id: impl Into<String>) -> Self {
        Self {
            kind: MediaKind::Audio,
            codec_mime_type: "audio/opus".to_string(),
            id: id.into(),
            stream_id: stream_id.into(),
        }
    }

    /// Create a VP8 video track config.
    pub fn video(id: impl Into<String>, stream_id: impl Into<String>) -> Self {
        Self {
            kind: MediaKind::Video,
            codec_mime_type: "video/VP8".to_string(),
            id: id.into(),
            stream_id: stream_id.into(),
        }
    }

    /// Create an H264 video track config (used by `WebRtcVideoSink` / `WebRtcVideoSource`).
    pub fn h264_video(id: impl Into<String>, stream_id: impl Into<String>) -> Self {
        Self {
            kind: MediaKind::Video,
            codec_mime_type: "video/H264".to_string(),
            id: id.into(),
            stream_id: stream_id.into(),
        }
    }

    /// Build the RTP codec capability for this track config.
    pub(crate) fn codec_capability(&self) -> RTCRtpCodecCapability {
        let (clock_rate, channels) = match self.kind {
            MediaKind::Audio => (48_000, 2),
            MediaKind::Video => (90_000, 0),
        };
        RTCRtpCodecCapability {
            mime_type: self.codec_mime_type.clone(),
            clock_rate,
            channels,
            ..Default::default()
        }
    }
}

/// A local media track that can send pre-encoded samples.
pub struct LocalTrack {
    pub(crate) track: Arc<TrackLocalStaticSample>,
    pub config: LocalTrackConfig,
}

impl LocalTrack {
    /// Write a pre-encoded media sample to the track.
    pub async fn write_sample(
        &self,
        data: &[u8],
        duration: Duration,
    ) -> crate::error::Result<()> {
        use webrtc::media::Sample;

        self.track
            .write_sample(&Sample {
                data: bytes::Bytes::copy_from_slice(data),
                duration,
                ..Default::default()
            })
            .await
            .map_err(|e| crate::error::WebRtcError::DataChannel(e.to_string()))?;
        Ok(())
    }
}

/// A remote media track received from the peer.
#[derive(Clone)]
pub struct RemoteTrack {
    pub(crate) track: Arc<TrackRemote>,
}

impl RemoteTrack {
    /// Get a reference to the underlying WebRTC track.
    pub fn track(&self) -> &Arc<TrackRemote> {
        &self.track
    }
}

impl std::fmt::Debug for RemoteTrack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTrack").finish()
    }
}
