# Aiko-Strong

A strongly-typed distributed pipeline framework in Rust, inspired by [Aiko Services](https://github.com/geekscape/aiko_services).

## Features

- **Compile-time Type Safety**: Pipeline element connections are validated at compile time
- **Actor-based Execution**: Each element runs as an actor with its own mailbox
- **MQTT Integration**: Ready for distributed deployment across multiple nodes
- **WebRTC Transport**: Peer-to-peer data channels and media streaming (H.264 video, Opus audio)
- **ML-ready**: Built-in types for images, detections, and bounding boxes

## Quick Start

```rust
use aiko_pipeline::prelude::*;
use aiko_core::element::MapElement;

// Build a type-safe pipeline
let pipeline = Pipeline::new("my_pipeline")
    .source(MySource::new())              // () -> i32
    .then(MapElement::new("double", |x| x * 2))  // i32 -> i32
    .then(MapElement::new("stringify", |x: i32| x.to_string()))  // i32 -> String
    .sink(MySink::new());                 // String -> ()

// Type mismatches are caught at compile time!
// .then(WrongElement)  // ERROR: expected String, found i32
```

## Crates

| Crate | Description |
|-------|-------------|
| `aiko-core` | Core traits and types: `Frame<T>`, `Element`, `SourceElement`, `SinkElement` |
| `aiko-actor` | Actor system: `Actor` trait, `ActorRef`, `ActorRegistry` |
| `aiko-pipeline` | Type-safe pipeline builder and executor |
| `aiko-mqtt` | MQTT transport layer for distributed deployment |
| `aiko-webrtc` | WebRTC transport layer for peer-to-peer data channels and media streaming (H.264 video, Opus audio) |

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │────▶│  Element A  │────▶│  Element B  │────▶│    Sink     │
│  () -> T1   │     │  T1 -> T2   │     │  T2 -> T3   │     │  T3 -> ()   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   │                   │
      └───────────────────┴───────────────────┴───────────────────┘
                                    │
                          ┌─────────▼─────────┐
                          │   Actor System    │
                          │  (Tokio + mpsc)   │
                          └─────────┬─────────┘
                            ┌───────┴───────┐
                  ┌─────────▼──────┐ ┌──────▼─────────┐
                  │ MQTT Transport │ │ WebRTC Transport│
                  │ (Distributed)  │ │  (Peer-to-Peer) │
                  └────────────────┘ └────────────────┘
```

## Core Concepts

### Frame\<T\>

A unit of data flowing through the pipeline:

```rust
pub struct Frame<T> {
    pub metadata: FrameMetadata,  // stream_id, frame_id, timestamp
    pub payload: T,
}
```

### Element Trait

Processing units with typed inputs and outputs:

```rust
#[async_trait]
pub trait Element: Send + Sync + 'static {
    type Input: PipelineData;
    type Output: PipelineData;
    type Config: ElementConfig;

    fn name(&self) -> &str;

    async fn process(
        &mut self,
        frame: Frame<Self::Input>,
        ctx: &mut ElementContext,
    ) -> Result<Option<Frame<Self::Output>>, ElementError>;
}
```

### Pipeline Builder

Type-state pattern ensures compile-time validation:

```rust
// The generic parameter tracks the current output type
pub struct SourcedPipelineBuilder<CurrentOutput: PipelineData> { ... }

impl<CurrentOutput: PipelineData> SourcedPipelineBuilder<CurrentOutput> {
    // Only accepts elements where E::Input == CurrentOutput
    pub fn then<E>(self, element: E) -> SourcedPipelineBuilder<E::Output>
    where
        E: Element<Input = CurrentOutput>,
    { ... }
}
```

## MQTT Topics

For distributed deployment:

```
aiko/{namespace}/services/{service_id}/status   # Heartbeats
aiko/{namespace}/services/{service_id}/control  # Commands
aiko/{namespace}/pipelines/{pipeline_id}/frames # Frame data
aiko/{namespace}/pipelines/{pipeline_id}/events # Events
aiko/{namespace}/actors/{actor_id}/inbox        # Actor messages
aiko/{namespace}/discovery                      # Service discovery
```

## WebRTC & Media

The `aiko-webrtc` crate provides peer-to-peer communication as an alternative to MQTT:

- **Data channels** for bidirectional messaging (named channels replace named MQTT topics)
- **H.264 video streaming** in both directions (Rust to browser, browser to Rust) via `openh264`
- **Opus audio** encoding/decoding via `audiopus`
- **Pluggable signaling** via the `SignalingClient` trait (WebSocket implementation provided)
- **Auto-reconnection** with exponential backoff and ICE restart on connection failure

Pipeline integration elements bridge the type-safe pipeline to WebRTC tracks:

| Element | Direction | Description |
|---------|-----------|-------------|
| `WebRtcVideoSink` | Pipeline → WebRTC | Encodes `VideoFrame` to H.264 and sends over media track |
| `WebRtcVideoSource` | WebRTC → Pipeline | Receives H.264 track, decodes to `VideoFrame` |
| `WebRtcAudioSink` | Pipeline → WebRTC | Encodes `AudioFrame` to Opus and sends over media track |
| `WebRtcAudioSource` | WebRTC → Pipeline | Receives Opus track, decodes to `AudioFrame` |

### Feature Flags

| Feature | Enables | System dependency |
|---------|---------|-------------------|
| `video` | H.264 encode/decode via `openh264` | None (bundles its own codec) |
| `audio` | Opus encode/decode via `audiopus` | `libopus` (`brew install opus` on macOS) |
| `video-demo` | Video demo example binary | None |

## Video Demo

Bidirectional video: Rust sends SMPTE color bars, browser sends camera feed.

```bash
# Terminal 1: signaling relay
cargo run -p aiko-webrtc --example signaling_server

# Terminal 2: Rust video peer
cargo run -p aiko-webrtc --example video_demo --features video-demo

# Browser: open crates/aiko-webrtc/examples/video_demo.html
```

## Examples

Run the pipeline demo:

```bash
cargo run -p aiko-pipeline --example simple_pipeline
```

## Dependencies

- **tokio**: Async runtime
- **async-trait**: Async trait support
- **serde/bincode**: Serialization
- **rumqttc**: MQTT client
- **webrtc**: Pure-Rust WebRTC stack
- **tokio-tungstenite**: WebSocket signaling
- **openh264**: H.264 codec (optional, `video` feature)
- **audiopus**: Opus codec (optional, `audio` feature)
- **tracing**: Logging

## Status

Core framework, MQTT transport, WebRTC data channels, and media tracks are all working. 54 tests pass. ~7,000 lines of Rust across 5 crates.

The key innovation is the type-state pipeline builder -- it is impossible to wire together pipeline elements with incompatible types. The compiler catches it.

## License

MIT OR Apache-2.0
