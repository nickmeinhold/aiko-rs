//! WebRTC audio source — reads Opus RTP from a remote track and decodes to `AudioFrame`.

use aiko_core::element::{ElementContext, SourceElement};
use aiko_core::error::ElementError;
use aiko_core::frame::{Frame, FrameId, StreamId};
use aiko_core::media::{AudioFrame, SampleFormat};
use async_trait::async_trait;
use audiopus::coder::Decoder;
use audiopus::{Channels, SampleRate};
use std::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::media::RemoteTrack;

/// A pipeline source that reads Opus RTP from a WebRTC remote track and decodes to `AudioFrame`.
///
/// The caller listens for `PeerEvent::TrackAdded` and sends the `RemoteTrack` through
/// the provided `mpsc::Sender`. The source waits for the track on first call to `next()`,
/// then continuously reads RTP packets and decodes Opus to PCM I16Le.
///
/// # Example
///
/// ```rust,ignore
/// let (track_tx, track_rx) = tokio::sync::mpsc::channel(1);
/// let source = WebRtcAudioSource::new(track_rx, 48000, 2);
///
/// // In event handler:
/// if let PeerEvent::TrackAdded(remote_track) = event {
///     track_tx.send(remote_track).await.ok();
/// }
///
/// Pipeline::new("audio_ingest")
///     .source(source)
///     .then(MyAudioTransform)
///     .sink(MySink);
/// ```
/// Uses `Mutex<Decoder>` because `audiopus::Decoder` is `!Sync` (raw pointer inside).
/// Since `SourceElement::next()` takes `&mut self`, we always have exclusive access,
/// so the Mutex is uncontended.
pub struct WebRtcAudioSource {
    track_rx: Option<mpsc::Receiver<RemoteTrack>>,
    track: Option<RemoteTrack>,
    decoder: Option<Mutex<Decoder>>,
    stream_id: StreamId,
    frame_count: u64,
    sample_rate: u32,
    channels: u16,
    /// PCM output buffer: sized for 120ms at max sample rate stereo (max Opus frame).
    pcm_buffer: Vec<i16>,
}

impl WebRtcAudioSource {
    /// Create a new audio source that waits for a remote track on the given channel.
    ///
    /// `sample_rate` and `channels` must match the sender's Opus configuration.
    pub fn new(track_rx: mpsc::Receiver<RemoteTrack>, sample_rate: u32, channels: u16) -> Self {
        // Max Opus frame: 120ms at 48kHz = 5760 samples per channel
        let max_samples = (sample_rate as usize * 120 / 1000) * channels as usize;
        Self {
            track_rx: Some(track_rx),
            track: None,
            decoder: None,
            stream_id: StreamId::new(),
            frame_count: 0,
            sample_rate,
            channels,
            pcm_buffer: vec![0i16; max_samples],
        }
    }
}

#[async_trait]
impl SourceElement for WebRtcAudioSource {
    type Output = AudioFrame;
    type Config = ();

    fn name(&self) -> &str {
        "WebRtcAudioSource"
    }

    async fn next(
        &mut self,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<AudioFrame>>, ElementError> {
        // Wait for remote track on first call
        if self.track.is_none() {
            if let Some(rx) = self.track_rx.as_mut() {
                match rx.recv().await {
                    Some(track) => {
                        debug!("WebRtcAudioSource received remote track");
                        self.track = Some(track);
                    }
                    None => {
                        debug!("WebRtcAudioSource track channel closed — shutting down");
                        return Ok(None);
                    }
                }
            } else {
                return Ok(None);
            }
        }

        // Lazy-init decoder
        if self.decoder.is_none() {
            let sr = match self.sample_rate {
                8000 => SampleRate::Hz8000,
                12000 => SampleRate::Hz12000,
                16000 => SampleRate::Hz16000,
                24000 => SampleRate::Hz24000,
                48000 => SampleRate::Hz48000,
                rate => {
                    return Err(ElementError::Processing(format!(
                        "Unsupported Opus sample rate: {rate}"
                    )));
                }
            };
            let ch = match self.channels {
                1 => Channels::Mono,
                2 => Channels::Stereo,
                n => {
                    return Err(ElementError::Processing(format!(
                        "Unsupported channel count: {n}"
                    )));
                }
            };

            self.decoder =
                Some(Mutex::new(Decoder::new(sr, ch).map_err(|e| {
                    ElementError::Processing(format!("Opus decoder init: {e}"))
                })?));
            debug!(
                "Opus decoder initialized: {}Hz, {} channels",
                self.sample_rate, self.channels
            );
        }

        let track = self.track.as_ref().unwrap().track().clone();

        // Read RTP packets until we get a decodable frame
        loop {
            let (rtp_packet, _attributes) = track
                .read_rtp()
                .await
                .map_err(|e| ElementError::Processing(format!("read_rtp: {e}")))?;

            let opus_data = &rtp_packet.payload;
            if opus_data.is_empty() {
                continue;
            }

            // Decode Opus to PCM I16 (inside a block so MutexGuard is dropped)
            let decode_result = {
                let mut decoder = self.decoder.as_ref().unwrap().lock().unwrap();
                decoder.decode(Some(opus_data.as_ref()), &mut self.pcm_buffer, false)
            };

            match decode_result {
                Ok(decoded_samples) => {
                    // decoded_samples is per channel; total samples = decoded_samples * channels
                    let total_samples = decoded_samples * self.channels as usize;
                    let pcm_i16 = &self.pcm_buffer[..total_samples];

                    // Convert i16 samples to bytes (little-endian)
                    let mut data = Vec::with_capacity(total_samples * 2);
                    for &sample in pcm_i16 {
                        data.extend_from_slice(&sample.to_le_bytes());
                    }

                    let audio_frame =
                        AudioFrame::new(self.sample_rate, self.channels, SampleFormat::I16Le, data);
                    let frame_id = FrameId(self.frame_count);
                    self.frame_count += 1;

                    return Ok(Some(Frame::new(self.stream_id, frame_id, audio_frame)));
                }
                Err(e) => {
                    warn!("Opus decode error: {e}");
                    continue;
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), ElementError> {
        debug!(
            "WebRtcAudioSource shutting down after {} frames",
            self.frame_count
        );
        self.decoder = None;
        self.track = None;
        self.track_rx = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use audiopus::{coder::Encoder, Application, Channels, SampleRate};

    #[test]
    fn test_opus_encode_decode_roundtrip() {
        let encoder =
            Encoder::new(SampleRate::Hz48000, Channels::Stereo, Application::Voip).unwrap();
        let mut decoder =
            audiopus::coder::Decoder::new(SampleRate::Hz48000, Channels::Stereo).unwrap();

        // 20ms at 48kHz stereo = 960 samples per channel * 2 channels = 1920 i16 samples
        let silence = vec![0i16; 960 * 2];
        let mut opus_buf = vec![0u8; 4000];
        let encoded_len = encoder.encode(&silence, &mut opus_buf).unwrap();
        assert!(encoded_len > 0);

        let mut pcm_out = vec![0i16; 960 * 2];
        let decoded_samples = decoder
            .decode(Some(&opus_buf[..encoded_len]), &mut pcm_out, false)
            .unwrap();

        // Should get 960 samples per channel
        assert_eq!(decoded_samples, 960);
    }
}
