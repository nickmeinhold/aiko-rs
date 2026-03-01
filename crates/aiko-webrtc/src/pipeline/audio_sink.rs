//! WebRTC audio sink — encodes PCM `AudioFrame`s to Opus and writes to a local track.

use aiko_core::element::{ElementConfig, ElementContext, SinkElement};
use aiko_core::error::ElementError;
use aiko_core::frame::Frame;
use aiko_core::media::{AudioFrame, SampleFormat};
use async_trait::async_trait;
use audiopus::coder::Encoder;
use audiopus::{Application, Channels, SampleRate};
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, trace};

use crate::media::LocalTrack;

/// Configuration for [`WebRtcAudioSink`].
#[derive(Debug, Clone)]
pub struct WebRtcAudioSinkConfig {
    /// Target sample rate. Must be one of: 8000, 12000, 16000, 24000, 48000.
    pub sample_rate: u32,
    /// Number of channels (1 = mono, 2 = stereo).
    pub channels: u16,
    /// Opus application mode.
    pub application: OpusApplication,
}

/// Opus application mode selection.
#[derive(Debug, Clone, Copy)]
pub enum OpusApplication {
    /// Best for most VoIP/videoconference applications.
    Voip,
    /// Best for broadcast/high-fidelity applications.
    Audio,
    /// Only use when lowest-achievable latency is critical.
    LowDelay,
}

impl Default for WebRtcAudioSinkConfig {
    fn default() -> Self {
        Self {
            sample_rate: 48000,
            channels: 2,
            application: OpusApplication::Voip,
        }
    }
}

impl ElementConfig for WebRtcAudioSinkConfig {}

fn to_audiopus_sample_rate(rate: u32) -> Result<SampleRate, ElementError> {
    match rate {
        8000 => Ok(SampleRate::Hz8000),
        12000 => Ok(SampleRate::Hz12000),
        16000 => Ok(SampleRate::Hz16000),
        24000 => Ok(SampleRate::Hz24000),
        48000 => Ok(SampleRate::Hz48000),
        _ => Err(ElementError::Configuration(format!(
            "Unsupported Opus sample rate: {rate}. Must be 8000/12000/16000/24000/48000."
        ))),
    }
}

fn to_audiopus_channels(ch: u16) -> Result<Channels, ElementError> {
    match ch {
        1 => Ok(Channels::Mono),
        2 => Ok(Channels::Stereo),
        _ => Err(ElementError::Configuration(format!(
            "Unsupported channel count: {ch}. Must be 1 (mono) or 2 (stereo)."
        ))),
    }
}

/// A pipeline sink that encodes `AudioFrame` (PCM I16Le) → Opus and writes to a WebRTC track.
///
/// The caller creates a `LocalTrack` via [`WebRtcTransport::add_local_track()`] with
/// `LocalTrackConfig::audio()` and passes it in at construction.
///
/// # Frame size requirements
///
/// Opus requires frames of exactly 2.5, 5, 10, 20, 40, or 60ms duration.
/// At 48kHz, 20ms = 960 samples per channel (the most common choice).
/// A pipeline sink that encodes `AudioFrame` (PCM I16Le) → Opus and writes to a WebRTC track.
///
/// Uses `Mutex<Encoder>` because `audiopus::Encoder` is `!Sync` (raw pointer inside).
/// Since `SinkElement::consume()` takes `&mut self`, we always have exclusive access,
/// so the Mutex is uncontended.
pub struct WebRtcAudioSink {
    track: LocalTrack,
    encoder: Option<Mutex<Encoder>>,
    opus_output_buf: Vec<u8>,
    frame_count: u64,
    sample_rate: u32,
    channels: u16,
    application: OpusApplication,
}

impl WebRtcAudioSink {
    /// Create a new audio sink that writes Opus to the given local track.
    pub fn new(track: LocalTrack) -> Self {
        Self {
            track,
            encoder: None,
            opus_output_buf: vec![0u8; 4000], // Max Opus frame is ~4000 bytes
            frame_count: 0,
            sample_rate: 48000,
            channels: 2,
            application: OpusApplication::Voip,
        }
    }
}

#[async_trait]
impl SinkElement for WebRtcAudioSink {
    type Input = AudioFrame;
    type Config = WebRtcAudioSinkConfig;

    fn name(&self) -> &str {
        "WebRtcAudioSink"
    }

    async fn init(&mut self, config: Self::Config) -> Result<(), ElementError> {
        self.sample_rate = config.sample_rate;
        self.channels = config.channels;
        self.application = config.application;

        let sr = to_audiopus_sample_rate(config.sample_rate)?;
        let ch = to_audiopus_channels(config.channels)?;
        let app = match config.application {
            OpusApplication::Voip => Application::Voip,
            OpusApplication::Audio => Application::Audio,
            OpusApplication::LowDelay => Application::LowDelay,
        };

        self.encoder = Some(Mutex::new(
            Encoder::new(sr, ch, app)
                .map_err(|e| ElementError::Processing(format!("Opus encoder init: {e}")))?,
        ));

        debug!(
            "WebRtcAudioSink initialized: {}Hz, {} channels, {:?}",
            config.sample_rate, config.channels, config.application
        );
        Ok(())
    }

    async fn consume(
        &mut self,
        frame: Frame<AudioFrame>,
        _ctx: &mut ElementContext,
    ) -> Result<(), ElementError> {
        let audio = &frame.payload;

        // Lazy-init encoder if init() wasn't called
        if self.encoder.is_none() {
            let sr = to_audiopus_sample_rate(audio.sample_rate)?;
            let ch = to_audiopus_channels(audio.channels)?;
            self.encoder = Some(Mutex::new(
                Encoder::new(sr, ch, Application::Voip)
                    .map_err(|e| ElementError::Processing(format!("Opus encoder init: {e}")))?,
            ));
            self.sample_rate = audio.sample_rate;
            self.channels = audio.channels;
            debug!(
                "Opus encoder lazy-initialized: {}Hz, {} channels",
                audio.sample_rate, audio.channels
            );
        }

        if audio.format != SampleFormat::I16Le {
            return Err(ElementError::Processing(
                "WebRtcAudioSink requires I16Le audio frames".to_string(),
            ));
        }

        // Interpret raw bytes as i16 samples
        let samples: &[i16] = bytemuck_cast_i16(&audio.data);

        // Encode inside a block so the MutexGuard is dropped before .await
        let encoded_len = {
            let encoder = self.encoder.as_ref().unwrap().lock().unwrap();
            encoder
                .encode(samples, &mut self.opus_output_buf)
                .map_err(|e| ElementError::Processing(format!("Opus encode: {e}")))?
        };

        if encoded_len > 0 {
            let duration = Duration::from_millis(
                (audio.samples_per_channel() as u64 * 1000) / audio.sample_rate as u64,
            );
            self.track
                .write_sample(&self.opus_output_buf[..encoded_len], duration)
                .await
                .map_err(|e| ElementError::Processing(format!("write_sample: {e}")))?;
            trace!("Encoded {} bytes Opus at frame {}", encoded_len, self.frame_count);
        }

        self.frame_count += 1;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ElementError> {
        debug!("WebRtcAudioSink shutting down after {} frames", self.frame_count);
        self.encoder = None;
        Ok(())
    }
}

/// Safe cast from &[u8] to &[i16] assuming little-endian I16Le format.
fn bytemuck_cast_i16(data: &[u8]) -> &[i16] {
    assert!(data.len() % 2 == 0, "I16Le data must have even byte count");
    // SAFETY: i16 has alignment 2. We ensure the data is properly aligned by
    // using from_raw_parts. For unaligned data we fall back to a copy.
    // In practice, Vec<u8> allocates with sufficient alignment.
    let ptr = data.as_ptr();
    if ptr.align_offset(std::mem::align_of::<i16>()) == 0 {
        unsafe { std::slice::from_raw_parts(ptr as *const i16, data.len() / 2) }
    } else {
        // This branch shouldn't happen with normal Vec allocations,
        // but is here for safety. We'd need a copy in this case,
        // but since this is a hot path, we panic instead.
        panic!("Audio data is not aligned for i16 access");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = WebRtcAudioSinkConfig::default();
        assert_eq!(config.sample_rate, 48000);
        assert_eq!(config.channels, 2);
    }

    #[test]
    fn test_sample_rate_conversion() {
        assert!(to_audiopus_sample_rate(48000).is_ok());
        assert!(to_audiopus_sample_rate(44100).is_err());
    }

    #[test]
    fn test_channel_conversion() {
        assert!(to_audiopus_channels(1).is_ok());
        assert!(to_audiopus_channels(2).is_ok());
        assert!(to_audiopus_channels(5).is_err());
    }

    #[test]
    fn test_opus_encode_silence() {
        // Verify Opus can encode a silent frame
        let encoder = Encoder::new(SampleRate::Hz48000, Channels::Stereo, Application::Voip)
            .expect("encoder init");

        // 20ms at 48kHz stereo = 960 samples per channel * 2 channels = 1920 samples
        let silence = vec![0i16; 960 * 2];
        let mut output = vec![0u8; 4000];
        let len = encoder.encode(&silence, &mut output).expect("encode");
        assert!(len > 0, "Opus should produce output for silence");
    }

    #[test]
    fn test_bytemuck_cast() {
        let bytes: Vec<u8> = vec![0x01, 0x00, 0xFF, 0x7F]; // 1, 32767 in little-endian
        let samples = bytemuck_cast_i16(&bytes);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0], 1);
        assert_eq!(samples[1], 32767);
    }
}
