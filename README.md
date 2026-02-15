# Aiko-Strong

A strongly-typed distributed pipeline framework in Rust, inspired by [Aiko Services](https://github.com/geekscape/aiko_services).

## Features

- **Compile-time Type Safety**: Pipeline element connections are validated at compile time
- **Actor-based Execution**: Each element runs as an actor with its own mailbox
- **MQTT Integration**: Ready for distributed deployment across multiple nodes
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
                                    │
                          ┌─────────▼─────────┐
                          │   MQTT Transport  │
                          │   (Distributed)   │
                          └───────────────────┘
```

## Core Concepts

### Frame<T>

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

## Examples

Run the demo:

```bash
cargo run -p aiko-pipeline --example simple_pipeline
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

## Dependencies

- **tokio**: Async runtime
- **async-trait**: Async trait support
- **serde/bincode**: Serialization
- **rumqttc**: MQTT client
- **tracing**: Logging

## License

MIT OR Apache-2.0
