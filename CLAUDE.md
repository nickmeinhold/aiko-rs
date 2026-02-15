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
└── aiko-mqtt/      # MQTT transport (depends on aiko-core)
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

### Adding New Data Types for MQTT

1. Define the type in `aiko-mqtt/src/codec.rs`
2. Implement `NetworkSerializable` trait with a unique type name
3. Derive `Serialize` and `Deserialize`

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

## File Locations

- Core traits: `crates/aiko-core/src/element.rs`
- Frame types: `crates/aiko-core/src/frame.rs`
- Actor system: `crates/aiko-actor/src/actor.rs`
- Pipeline builder: `crates/aiko-pipeline/src/builder.rs`
- MQTT client: `crates/aiko-mqtt/src/client.rs`
- Example: `crates/aiko-pipeline/examples/simple_pipeline.rs`

## Testing

Each crate has unit tests. Key test files:
- `aiko-core/src/frame.rs` - Frame creation and type erasure
- `aiko-actor/src/registry.rs` - Actor registration
- `aiko-mqtt/src/codec.rs` - Serialization roundtrips
- `aiko-pipeline/src/builder.rs` - Pipeline construction

## Notes

- Uses Tokio for async runtime
- Actor mailboxes use `tokio::sync::mpsc` channels
- MQTT uses `rumqttc` with optional TLS
- All elements run as separate actors for concurrency
- Frame metadata includes nanosecond timestamps
