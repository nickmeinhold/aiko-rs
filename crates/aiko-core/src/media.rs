//! Media types for video and audio pipelines.
//!
//! These types serve as the canonical data representations flowing through
//! pipeline elements. [`VideoFrame`] uses I420 by default because it is the
//! native format for H264 encode/decode, avoiding unnecessary color-space
//! conversions in the common path.

use crate::codec::NetworkSerializable;
use serde::{Deserialize, Serialize};

/// Pixel format for video frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PixelFormat {
    /// YUV 4:2:0 planar — native H264 encode/decode format.
    /// Plane layout: Y (width*height) + U (width/2 * height/2) + V (width/2 * height/2).
    /// Total size: width * height * 3 / 2.
    I420,
    /// 8-bit RGB interleaved. Size: width * height * 3.
    Rgb8,
    /// 8-bit RGBA interleaved. Size: width * height * 4.
    Rgba8,
    /// 8-bit grayscale. Size: width * height.
    Gray8,
}

impl PixelFormat {
    /// Returns the number of bytes per frame for the given dimensions.
    pub fn frame_size(&self, width: u32, height: u32) -> usize {
        let pixels = (width * height) as usize;
        match self {
            PixelFormat::I420 => pixels * 3 / 2,
            PixelFormat::Rgb8 => pixels * 3,
            PixelFormat::Rgba8 => pixels * 4,
            PixelFormat::Gray8 => pixels,
        }
    }
}

/// A single video frame with raw pixel data.
///
/// The default pipeline format is [`PixelFormat::I420`] because H264 encodes
/// and decodes in YUV420P natively. Using I420 throughout the pipeline avoids
/// double color-space conversion.
///
/// # Example
///
/// ```rust
/// use aiko_core::media::{VideoFrame, PixelFormat};
///
/// // Create a black 640x480 I420 frame
/// let frame = VideoFrame::new(640, 480, PixelFormat::I420);
/// assert_eq!(frame.data.len(), 640 * 480 * 3 / 2);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoFrame {
    pub width: u32,
    pub height: u32,
    pub format: PixelFormat,
    pub data: Vec<u8>,
}

impl VideoFrame {
    /// Create a new zeroed video frame with the given dimensions and format.
    pub fn new(width: u32, height: u32, format: PixelFormat) -> Self {
        let size = format.frame_size(width, height);
        Self {
            width,
            height,
            format,
            data: vec![0u8; size],
        }
    }

    /// Create a video frame from existing pixel data.
    ///
    /// # Panics
    ///
    /// Panics if `data.len()` does not match the expected size for the format.
    pub fn from_raw(width: u32, height: u32, format: PixelFormat, data: Vec<u8>) -> Self {
        let expected = format.frame_size(width, height);
        assert_eq!(
            data.len(),
            expected,
            "VideoFrame::from_raw: data length {} does not match expected {} for {}x{} {:?}",
            data.len(),
            expected,
            width,
            height,
            format,
        );
        Self {
            width,
            height,
            format,
            data,
        }
    }

    /// Returns the Y, U, V plane slices for an I420 frame.
    ///
    /// # Panics
    ///
    /// Panics if the format is not `PixelFormat::I420`.
    pub fn i420_planes(&self) -> (&[u8], &[u8], &[u8]) {
        assert_eq!(
            self.format,
            PixelFormat::I420,
            "i420_planes called on non-I420 frame"
        );
        let y_size = (self.width * self.height) as usize;
        let uv_size = y_size / 4;
        let y = &self.data[..y_size];
        let u = &self.data[y_size..y_size + uv_size];
        let v = &self.data[y_size + uv_size..y_size + 2 * uv_size];
        (y, u, v)
    }
}

impl NetworkSerializable for VideoFrame {
    fn type_name() -> &'static str {
        "video_frame"
    }
}

/// Sample format for audio frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SampleFormat {
    /// 16-bit signed integer, little-endian. 2 bytes per sample.
    I16Le,
    /// 32-bit float, little-endian. 4 bytes per sample.
    F32Le,
}

impl SampleFormat {
    /// Returns the number of bytes per sample.
    pub fn bytes_per_sample(&self) -> usize {
        match self {
            SampleFormat::I16Le => 2,
            SampleFormat::F32Le => 4,
        }
    }
}

/// A single audio frame with raw PCM sample data.
///
/// # Example
///
/// ```rust
/// use aiko_core::media::{AudioFrame, SampleFormat};
///
/// // 20ms of 48kHz stereo silence (960 samples * 2 channels * 2 bytes)
/// let frame = AudioFrame::silence(48000, 2, SampleFormat::I16Le, 960);
/// assert_eq!(frame.data.len(), 960 * 2 * 2);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioFrame {
    pub sample_rate: u32,
    pub channels: u16,
    pub format: SampleFormat,
    pub data: Vec<u8>,
}

impl AudioFrame {
    /// Create an audio frame from raw PCM data.
    pub fn new(sample_rate: u32, channels: u16, format: SampleFormat, data: Vec<u8>) -> Self {
        Self {
            sample_rate,
            channels,
            format,
            data,
        }
    }

    /// Create a silent audio frame with the given number of samples per channel.
    pub fn silence(
        sample_rate: u32,
        channels: u16,
        format: SampleFormat,
        samples_per_channel: usize,
    ) -> Self {
        let size = samples_per_channel * channels as usize * format.bytes_per_sample();
        Self {
            sample_rate,
            channels,
            format,
            data: vec![0u8; size],
        }
    }

    /// Returns the number of samples per channel in this frame.
    pub fn samples_per_channel(&self) -> usize {
        if self.channels == 0 {
            return 0;
        }
        self.data.len() / (self.channels as usize * self.format.bytes_per_sample())
    }

    /// Returns the duration of this frame in seconds.
    pub fn duration_secs(&self) -> f64 {
        if self.sample_rate == 0 {
            return 0.0;
        }
        self.samples_per_channel() as f64 / self.sample_rate as f64
    }
}

impl NetworkSerializable for AudioFrame {
    fn type_name() -> &'static str {
        "audio_frame"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::FrameEnvelope;
    use crate::frame::{Frame, FrameId, StreamId};

    #[test]
    fn test_pixel_format_frame_size() {
        assert_eq!(PixelFormat::I420.frame_size(640, 480), 640 * 480 * 3 / 2);
        assert_eq!(PixelFormat::Rgb8.frame_size(640, 480), 640 * 480 * 3);
        assert_eq!(PixelFormat::Rgba8.frame_size(640, 480), 640 * 480 * 4);
        assert_eq!(PixelFormat::Gray8.frame_size(640, 480), 640 * 480);
    }

    #[test]
    fn test_video_frame_new() {
        let frame = VideoFrame::new(640, 480, PixelFormat::I420);
        assert_eq!(frame.width, 640);
        assert_eq!(frame.height, 480);
        assert_eq!(frame.format, PixelFormat::I420);
        assert_eq!(frame.data.len(), 640 * 480 * 3 / 2);
    }

    #[test]
    fn test_video_frame_i420_planes() {
        let mut frame = VideoFrame::new(4, 2, PixelFormat::I420);
        // Fill with identifiable data: Y=1, U=2, V=3
        let y_size = 4 * 2;
        let uv_size = 2 * 1;
        frame.data[..y_size].fill(1);
        frame.data[y_size..y_size + uv_size].fill(2);
        frame.data[y_size + uv_size..].fill(3);

        let (y, u, v) = frame.i420_planes();
        assert_eq!(y.len(), 8);
        assert_eq!(u.len(), 2);
        assert_eq!(v.len(), 2);
        assert!(y.iter().all(|&b| b == 1));
        assert!(u.iter().all(|&b| b == 2));
        assert!(v.iter().all(|&b| b == 3));
    }

    #[test]
    fn test_video_frame_serialization() {
        let vf = VideoFrame::new(320, 240, PixelFormat::I420);
        let frame = Frame::new(StreamId::new(), FrameId(0), vf);
        let envelope = FrameEnvelope::from_frame(frame).unwrap();
        assert_eq!(envelope.payload_type, "video_frame");

        let recovered: Frame<VideoFrame> = envelope.into_frame().unwrap();
        assert_eq!(recovered.payload.width, 320);
        assert_eq!(recovered.payload.height, 240);
        assert_eq!(recovered.payload.format, PixelFormat::I420);
    }

    #[test]
    fn test_audio_frame_silence() {
        let frame = AudioFrame::silence(48000, 2, SampleFormat::I16Le, 960);
        assert_eq!(frame.sample_rate, 48000);
        assert_eq!(frame.channels, 2);
        assert_eq!(frame.data.len(), 960 * 2 * 2); // 960 samples * 2 channels * 2 bytes
        assert_eq!(frame.samples_per_channel(), 960);
    }

    #[test]
    fn test_audio_frame_duration() {
        let frame = AudioFrame::silence(48000, 2, SampleFormat::I16Le, 48000);
        let duration = frame.duration_secs();
        assert!((duration - 1.0).abs() < 1e-10); // 48000 samples at 48kHz = 1 second
    }

    #[test]
    fn test_audio_frame_serialization() {
        let af = AudioFrame::silence(48000, 2, SampleFormat::I16Le, 960);
        let frame = Frame::new(StreamId::new(), FrameId(0), af);
        let envelope = FrameEnvelope::from_frame(frame).unwrap();
        assert_eq!(envelope.payload_type, "audio_frame");

        let recovered: Frame<AudioFrame> = envelope.into_frame().unwrap();
        assert_eq!(recovered.payload.sample_rate, 48000);
        assert_eq!(recovered.payload.channels, 2);
    }

    #[test]
    fn test_sample_format_bytes() {
        assert_eq!(SampleFormat::I16Le.bytes_per_sample(), 2);
        assert_eq!(SampleFormat::F32Le.bytes_per_sample(), 4);
    }

    #[test]
    #[should_panic(expected = "does not match expected")]
    fn test_video_frame_from_raw_wrong_size() {
        VideoFrame::from_raw(640, 480, PixelFormat::I420, vec![0u8; 100]);
    }
}
