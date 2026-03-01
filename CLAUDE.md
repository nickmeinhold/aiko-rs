# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/claude-code) when working with this codebase.

## Project Overview

Aiko-Strong is a strongly-typed distributed pipeline framework in Rust, inspired by the Python-based [Aiko Services](https://github.com/geekscape/aiko_services). It provides compile-time type safety for data pipelines using Rust's type system.

## Architecture

### Crate Structure

```
crates/
├── aiko-core/      # Core traits and types (no dependencies on other aiko crates)
├── aiko-actor/     # Actor system (depends on aiko-core)
├── aiko-pipeline/  # Pipeline builder (depends on aiko-core, aiko-actor)
├── aiko-mqtt/      # MQTT transport (depends on aiko-core)
└── aiko-webrtc/    # WebRTC transport (depends on aiko-core)
```

### Key Types

- `Frame<T>` - Data unit with metadata (stream_id, frame_id, timestamp) and typed payload
- `Element` trait - Processing unit with `Input` and `Output` associated types
- `SourceElement` trait - Produces frames (no input)
- `SinkElement` trait - Consumes frames (no output)
- `Pipeline` builder - Type-state pattern for compile-time validation
- `ActorRef` - Handle for sending messages to actors

### Type Safety Pattern

The pipeline builder uses type-state to track the current output type:

```rust
// SourcedPipelineBuilder<CurrentOutput> only accepts elements where Input == CurrentOutput
.then<E: Element<Input = CurrentOutput>>(element) -> SourcedPipelineBuilder<E::Output>
```

This ensures type mismatches are caught at compile time, not runtime.

## Common Commands

```bash
# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Run the example
cargo run -p aiko-pipeline --example simple_pipeline

# Check for issues
cargo check --workspace

# Format code
cargo fmt --all
```

## Development Guidelines

### Adding a New Element

1. Implement the `Element` trait with appropriate `Input` and `Output` types
2. The `Config` type should implement `ElementConfig` (use `()` if no config needed)
3. Mark stateless elements with `fn is_stateless(&self) -> bool { true }`

### Adding New Data Types for MQTT / WebRTC

1. Define the type in `aiko-core/src/codec.rs` (shared types) or the transport's `codec.rs` (transport-specific)
2. Implement `NetworkSerializable` trait with a unique type name
3. Derive `Serialize` and `Deserialize`

> **Note:** `NetworkSerializable` and `FrameEnvelope` live in `aiko-core/src/codec.rs` and are re-exported by both `aiko-mqtt` and `aiko-webrtc`. ML-specific types (ImageData, Detections) remain in `aiko-mqtt/src/codec.rs`. Media types (`VideoFrame`, `AudioFrame`) live in `aiko-core/src/media.rs`.

### Pipeline Patterns

```rust
// Source-based pipeline (self-driven)
Pipeline::new("name")
    .source(MySource)
    .then(Transform)
    .sink(MySink);

// Input-based pipeline (externally fed)
Pipeline::new("name")
    .input::<InputType>()
    .then(Transform)
    .sink(MySink);

// Open pipeline (returns output)
Pipeline::new("name")
    .input::<InputType>()
    .then(Transform)
    .build();  // Returns OpenPipeline<OutputType>
```

## Current Status & Roadmap

### What's Working

- **Core framework**: Frame types, Element/Source/Sink traits, type-state pipeline builder, actor system — all functional with compile-time type safety
- **Shared codec layer**: `NetworkSerializable`, `FrameEnvelope`, and `CodecError` live in `aiko-core` and are re-exported by both transport crates
- **Media types**: `VideoFrame` (I420/RGB/RGBA/Gray) and `AudioFrame` (I16Le/F32Le) in `aiko-core::media` with `NetworkSerializable` impls
- **MQTT transport**: Publish/subscribe with typed frame serialization over MQTT
- **WebRTC data channels**: Full peer-to-peer data channel communication with pluggable signaling (WebSocket impl provided), e2e tested
- **WebRTC media tracks**: H264 video streaming from Rust to browser, verified with a live demo (SMPTE color bars at 640x480/30fps encoded with `openh264`)
- **Pipeline ↔ WebRTC integration**: `WebRtcVideoSink` and `WebRtcVideoSource` bridge pipelines to WebRTC tracks (behind `video` feature)
- **Audio pipeline elements**: `WebRtcAudioSink` and `WebRtcAudioSource` with Opus encoding/decoding (behind `audio` feature)
- **Reconnection**: `ReconnectStrategy` with exponential backoff support
- **STUN/TURN helpers**: `WebRtcConfig::with_stun()` and `with_turn()` builder methods
- **Video demo**: Uses pipeline pattern — `SmpteSource` → `WebRtcVideoSink`

### What's Next

1. **End-to-end video pipeline test** — Wire `WebRtcVideoSink` and `WebRtcVideoSource` together in a two-peer test that encodes → transmits → receives → decodes a known frame.
2. **End-to-end audio pipeline test** — Same for `WebRtcAudioSink` → `WebRtcAudioSource` with Opus.
3. **Integrate reconnection into transport** — Currently `ReconnectStrategy` exists as a standalone module; integrate it into `WebRtcEventLoop` to auto-reconnect on `Disconnected`/`Failed`.
4. **Bidirectional video demo** — The browser sends camera frames to Rust but they're currently ignored. Feed inbound H264 through `WebRtcVideoSource` into a processing pipeline.

### Running the Video Demo

```bash
# Terminal 1: signaling relay
cargo run -p aiko-webrtc --example signaling_server

# Terminal 2: Rust video peer (H264 SMPTE bars)
cargo run -p aiko-webrtc --example video_demo --features video-demo

# Browser: open crates/aiko-webrtc/examples/video_demo.html
# → left panel: camera, right panel: Rust-generated color bars
```

## File Locations

- Core traits: `crates/aiko-core/src/element.rs`
- Frame types: `crates/aiko-core/src/frame.rs`
- Codec types: `crates/aiko-core/src/codec.rs`
- Media types: `crates/aiko-core/src/media.rs`
- Actor system: `crates/aiko-actor/src/actor.rs`
- Pipeline builder: `crates/aiko-pipeline/src/builder.rs`
- MQTT client: `crates/aiko-mqtt/src/client.rs`
- WebRTC transport: `crates/aiko-webrtc/src/transport.rs`
- WebRTC signaling: `crates/aiko-webrtc/src/signaling.rs`
- WebRTC peer management: `crates/aiko-webrtc/src/peer.rs`
- WebRTC pipeline elements: `crates/aiko-webrtc/src/pipeline/`
  - Video sink: `pipeline/video_sink.rs`
  - Video source: `pipeline/video_source.rs`
  - H264 depacketizer: `pipeline/h264.rs`
  - Audio sink: `pipeline/audio_sink.rs`
  - Audio source: `pipeline/audio_source.rs`
- Reconnection: `crates/aiko-webrtc/src/reconnect.rs`
- Pipeline example: `crates/aiko-pipeline/examples/simple_pipeline.rs`
- WebRTC example: `crates/aiko-webrtc/examples/data_channel.rs`
- Video demo: `crates/aiko-webrtc/examples/video_demo.rs`
- Signaling server: `crates/aiko-webrtc/examples/signaling_server.rs`
- Browser page: `crates/aiko-webrtc/examples/video_demo.html`

## Testing

Each crate has unit tests. Run all with: `cargo test --workspace --features "video,audio"`

Key test files:
- `aiko-core/src/frame.rs` - Frame creation and type erasure
- `aiko-core/src/codec.rs` - FrameEnvelope roundtrips, type mismatch errors
- `aiko-core/src/media.rs` - VideoFrame/AudioFrame construction, I420 planes, serialization
- `aiko-actor/src/registry.rs` - Actor registration
- `aiko-mqtt/src/codec.rs` - ML types, serialization roundtrips
- `aiko-pipeline/src/builder.rs` - Pipeline construction
- `aiko-webrtc/src/codec.rs` - Frame envelope roundtrips, audio sample
- `aiko-webrtc/src/pipeline/h264.rs` - H264 depacketization (Single NAL, FU-A, STAP-A)
- `aiko-webrtc/src/pipeline/video_sink.rs` - Config, I420 frame encoding prep
- `aiko-webrtc/src/pipeline/audio_sink.rs` - Opus encode, config validation, byte casting
- `aiko-webrtc/src/pipeline/audio_source.rs` - Opus encode/decode roundtrip
- `aiko-webrtc/src/reconnect.rs` - Backoff strategy, attempt tracking, reset
- `aiko-webrtc/tests/e2e.rs` - Full data channel roundtrip (in-process signaling)

## WebRTC Media Gotchas

These are easy to hit when working on media tracks:

- **`codec_capability()` must set `clock_rate`** — Video needs 90000, audio needs 48000. Defaulting to 0 silently breaks codec negotiation.
- **Don't encode before connection** — Wait for `PeerEvent::StateChanged(PeerState::Connected)` before sending frames. The browser's H264 decoder needs the initial keyframe (IDR + SPS/PPS); if you start encoding early, the keyframe is sent into the void and the browser shows black.
- **Force periodic keyframes** — Call `encoder.force_intra_frame()` every ~2 seconds so the decoder can (re)sync if it misses a frame. Without this, a single dropped packet means permanent black until restart.
- **Connection sequence**: Connecting → Connected → TrackAdded(RemoteTrack) — only start encoding after Connected.
- **`openh264::Encoder::new()`** detects dimensions from the first `YUVSource` — no need to pass width/height to the config.

## Notes

- Uses Tokio for async runtime
- Actor mailboxes use `tokio::sync::mpsc` channels
- MQTT uses `rumqttc` with optional TLS
- WebRTC uses the pure-Rust `webrtc` crate (v0.14)
- WebRTC signaling is pluggable via the `SignalingClient` trait (built-in WebSocket impl provided)
- WebRTC data channels map to MQTT topics conceptually — named channels replace named topics
- H264 encoding/decoding uses `openh264` (v0.9), which bundles its own codec — no system dependency
- Opus encoding/decoding uses `audiopus` (v0.2), which requires `libopus` (`brew install opus` on macOS)
- `audiopus::Encoder` and `Decoder` are `!Sync` (raw C pointers), so pipeline elements wrap them in `Mutex` to satisfy the `SinkElement: Sync` bound. This is uncontended because elements always have exclusive `&mut self` access.
- All elements run as separate actors for concurrency
- Frame metadata includes nanosecond timestamps
