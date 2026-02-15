//! Simple pipeline example demonstrating type-safe element chaining.
//!
//! This example shows:
//! 1. Creating custom elements with typed inputs/outputs
//! 2. Building pipelines with compile-time type checking
//! 3. Running elements as actors
//!
//! Run with: cargo run --example simple_pipeline

use aiko_actor::prelude::*;
use aiko_core::element::{Element, ElementConfig, ElementContext, SinkElement, SourceElement};
use aiko_core::error::ElementError;
use aiko_core::frame::{Frame, FrameId, StreamId};
use aiko_pipeline::prelude::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============ Custom Elements ============

/// A source that generates incrementing numbers.
struct CounterSource {
    stream_id: StreamId,
    count: u64,
    max: u64,
}

impl CounterSource {
    fn new(max: u64) -> Self {
        Self {
            stream_id: StreamId::new(),
            count: 0,
            max,
        }
    }
}

#[async_trait]
impl SourceElement for CounterSource {
    type Output = i32;
    type Config = ();

    fn name(&self) -> &str {
        "counter_source"
    }

    async fn next(
        &mut self,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<i32>>, ElementError> {
        if self.count >= self.max {
            return Ok(None);
        }

        let value = self.count as i32;
        let frame = Frame::new(self.stream_id, FrameId(self.count), value);
        self.count += 1;

        // Small delay to simulate real-world source
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        Ok(Some(frame))
    }
}

/// An element that doubles its input.
struct DoubleElement;

#[async_trait]
impl Element for DoubleElement {
    type Input = i32;
    type Output = i32;
    type Config = ();

    fn name(&self) -> &str {
        "double"
    }

    async fn process(
        &mut self,
        frame: Frame<i32>,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<i32>>, ElementError> {
        Ok(Some(frame.map(|x| x * 2)))
    }

    fn is_stateless(&self) -> bool {
        true
    }
}

/// An element that converts i32 to String.
struct ToStringElement;

#[async_trait]
impl Element for ToStringElement {
    type Input = i32;
    type Output = String;
    type Config = ();

    fn name(&self) -> &str {
        "to_string"
    }

    async fn process(
        &mut self,
        frame: Frame<i32>,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<String>>, ElementError> {
        Ok(Some(frame.map(|x| format!("Value: {}", x))))
    }

    fn is_stateless(&self) -> bool {
        true
    }
}

/// An element that filters even numbers.
struct EvenFilterElement;

#[async_trait]
impl Element for EvenFilterElement {
    type Input = i32;
    type Output = i32;
    type Config = ();

    fn name(&self) -> &str {
        "even_filter"
    }

    async fn process(
        &mut self,
        frame: Frame<i32>,
        _ctx: &mut ElementContext,
    ) -> Result<Option<Frame<i32>>, ElementError> {
        if frame.payload % 2 == 0 {
            Ok(Some(frame))
        } else {
            Ok(None) // Filter out odd numbers
        }
    }

    fn is_stateless(&self) -> bool {
        true
    }
}

/// A sink that collects strings.
struct CollectorSink {
    collected: Arc<tokio::sync::Mutex<Vec<String>>>,
}

impl CollectorSink {
    fn new() -> (Self, Arc<tokio::sync::Mutex<Vec<String>>>) {
        let collected = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        (
            Self {
                collected: collected.clone(),
            },
            collected,
        )
    }
}

#[async_trait]
impl SinkElement for CollectorSink {
    type Input = String;
    type Config = ();

    fn name(&self) -> &str {
        "collector"
    }

    async fn consume(
        &mut self,
        frame: Frame<String>,
        _ctx: &mut ElementContext,
    ) -> Result<(), ElementError> {
        self.collected.lock().await.push(frame.payload);
        Ok(())
    }
}

/// A sink that prints to stdout.
struct PrintSink {
    prefix: String,
}

impl PrintSink {
    fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

#[async_trait]
impl SinkElement for PrintSink {
    type Input = String;
    type Config = ();

    fn name(&self) -> &str {
        "print_sink"
    }

    async fn consume(
        &mut self,
        frame: Frame<String>,
        _ctx: &mut ElementContext,
    ) -> Result<(), ElementError> {
        println!(
            "{} [{}:{}] {}",
            self.prefix,
            frame.stream_id(),
            frame.frame_id(),
            frame.payload
        );
        Ok(())
    }
}

// ============ Main ============

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("aiko=debug".parse()?),
        )
        .init();

    println!("=== Aiko Pipeline Example ===\n");

    // Example 1: Type-safe pipeline building
    println!("1. Building type-safe pipeline...");

    let (collector_sink, collected) = CollectorSink::new();

    // This pipeline is checked at compile time!
    // CounterSource -> i32
    // DoubleElement: i32 -> i32
    // EvenFilterElement: i32 -> i32
    // ToStringElement: i32 -> String
    // CollectorSink: String -> ()
    let pipeline = Pipeline::new("demo_pipeline")
        .source(CounterSource::new(10))
        .then(DoubleElement)
        .then(EvenFilterElement) // Filter even numbers (all are even after doubling)
        .then(ToStringElement)
        .sink(collector_sink);

    println!("   Pipeline name: {}", pipeline.name());
    println!("   Elements: {:?}", pipeline.element_names());
    println!();

    // Example 2: Using the MapElement helper
    println!("2. Using closure-based elements...");

    use aiko_core::element::MapElement;

    let _transform_pipeline = Pipeline::new("transform")
        .input::<i32>()
        .then(MapElement::new("triple", |x: i32| x * 3))
        .then(MapElement::new("add_ten", |x: i32| x + 10))
        .then(MapElement::new("format", |x: i32| format!("Result: {}", x)))
        .sink(PrintSink::new(">>"));

    println!("   Built transform pipeline with closures");
    println!();

    // Example 3: Demonstrating type safety
    println!("3. Type safety demonstration:");
    println!("   The following would NOT compile:");
    println!("   ");
    println!("   // Pipeline::new(\"broken\")");
    println!("   //     .source(CounterSource::new(5))  // Output: i32");
    println!("   //     .then(ToStringElement)          // Input: i32, Output: String");
    println!("   //     .then(DoubleElement);           // ERROR: expected String, got i32");
    println!();

    // Example 4: Manual execution test
    println!("4. Manual element execution test...");

    let mut source = CounterSource::new(5);
    let mut double = DoubleElement;
    let mut to_string = ToStringElement;
    let mut ctx = ElementContext::new("test", "test_pipeline");

    println!("   Processing frames manually:");
    while let Some(frame) = source.next(&mut ctx).await? {
        let doubled = double.process(frame, &mut ctx).await?.unwrap();
        let stringified = to_string.process(doubled, &mut ctx).await?.unwrap();
        println!("   -> {}", stringified.payload);
    }
    println!();

    // Example 5: Actor-based execution
    println!("5. Actor-based execution...");

    let (print_sink, _) = CollectorSink::new();

    // Create actor wrappers
    let sink_actor = spawn(
        "sink",
        SinkActor::new(PrintSink::new("  [Actor]")),
        (),
        None,
    )
    .await?;

    let to_string_actor = spawn(
        "to_string",
        ElementActor::new(ToStringElement, Some(sink_actor.clone())),
        (),
        None,
    )
    .await?;

    let double_actor = spawn(
        "double",
        ElementActor::new(DoubleElement, Some(to_string_actor.clone())),
        (),
        None,
    )
    .await?;

    // Send frames through the actor chain
    use aiko_core::frame::AnyFrame;
    use aiko_core::message::ActorMessage;

    for i in 0..5 {
        let frame = Frame::new(StreamId::new(), FrameId(i), i as i32);
        double_actor
            .send(ActorMessage::ProcessFrame(AnyFrame::new(frame)))
            .await?;
    }

    // Give actors time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Stop actors
    use aiko_core::message::ControlMessage;
    double_actor.control(ControlMessage::Stop).await?;
    to_string_actor.control(ControlMessage::Stop).await?;
    sink_actor.control(ControlMessage::Stop).await?;

    println!("\n=== Example Complete ===");

    Ok(())
}

// Compile-time type safety test - uncomment to see the error:
/*
fn _this_would_not_compile() {
    // This creates a type error because DoubleElement expects i32
    // but ToStringElement outputs String
    let _broken = Pipeline::new("broken")
        .source(CounterSource::new(5))
        .then(ToStringElement)
        .then(DoubleElement);  // Error: expected String, found i32
}
*/
