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

1. Define the type in the transport's `codec.rs` (`aiko-mqtt/src/codec.rs` or `aiko-webrtc/src/codec.rs`)
2. Implement `NetworkSerializable` trait with a unique type name
3. Derive `Serialize` and `Deserialize`

> **Note:** `NetworkSerializable` and `FrameEnvelope` are currently duplicated between `aiko-mqtt` and `aiko-webrtc` to avoid cross-transport dependency. A future refactor could move these to `aiko-core`.

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
- **MQTT transport**: Publish/subscribe with typed frame serialization over MQTT
- **WebRTC data channels**: Full peer-to-peer data channel communication with pluggable signaling (WebSocket impl provided), e2e tested
- **WebRTC media tracks**: H264 video streaming from Rust to browser, verified with a live demo (SMPTE color bars at 640x480/30fps encoded with `openh264`)
- **Video demo**: Three-component demo (signaling server + Rust peer + browser page) that shows browser camera alongside Rust-generated video

### What's Next

1. **Pipeline ↔ WebRTC integration** — The pipeline system and WebRTC transport are currently separate. Create `WebRtcVideoSource` / `WebRtcVideoSink` elements so WebRTC streams can be pipeline stages:
   ```rust
   Pipeline::new("video")
       .source(WebRtcVideoSource::new(config))
       .then(MyVideoTransform)
       .sink(WebRtcVideoSink::new(config));
   ```
   This is the most important architectural gap — connecting the two halves of the system.

2. **Incoming video processing** — The browser sends camera frames to Rust but they're currently ignored. Decode them (H264 via `openh264::Decoder`) and feed them into a pipeline for processing.

3. **Audio support** — The media track plumbing already handles `MediaKind::Audio` with clock_rate 48000. Add Opus encoding/decoding to enable audio pipelines.

4. **Move `NetworkSerializable` to `aiko-core`** — Currently duplicated between `aiko-mqtt` and `aiko-webrtc`. Moving to core would let both transports share the codec abstraction.

5. **Robustness** — Reconnection logic, graceful shutdown, error handling for codec negotiation failures, proper STUN/TURN configuration.

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
- Actor system: `crates/aiko-actor/src/actor.rs`
- Pipeline builder: `crates/aiko-pipeline/src/builder.rs`
- MQTT client: `crates/aiko-mqtt/src/client.rs`
- WebRTC transport: `crates/aiko-webrtc/src/transport.rs`
- WebRTC signaling: `crates/aiko-webrtc/src/signaling.rs`
- WebRTC peer management: `crates/aiko-webrtc/src/peer.rs`
- Pipeline example: `crates/aiko-pipeline/examples/simple_pipeline.rs`
- WebRTC example: `crates/aiko-webrtc/examples/data_channel.rs`
- Video demo: `crates/aiko-webrtc/examples/video_demo.rs`
- Signaling server: `crates/aiko-webrtc/examples/signaling_server.rs`
- Browser page: `crates/aiko-webrtc/examples/video_demo.html`

## Testing

Each crate has unit tests. Key test files:
- `aiko-core/src/frame.rs` - Frame creation and type erasure
- `aiko-actor/src/registry.rs` - Actor registration
- `aiko-mqtt/src/codec.rs` - Serialization roundtrips
- `aiko-pipeline/src/builder.rs` - Pipeline construction
- `aiko-webrtc/src/codec.rs` - Frame envelope roundtrips, audio sample
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
- All elements run as separate actors for concurrency
- Frame metadata includes nanosecond timestamps
