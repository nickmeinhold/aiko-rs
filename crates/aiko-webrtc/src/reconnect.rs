//! Reconnection strategies for WebRTC transport.
//!
//! When a peer connection transitions to `Disconnected` or `Failed` state,
//! a reconnection strategy determines how and when to re-establish the connection.

use std::time::Duration;
use tracing::{debug, info};

/// Strategy for reconnecting after a connection failure.
#[derive(Debug, Clone)]
pub enum ReconnectStrategy {
    /// Do not attempt to reconnect.
    None,
    /// Exponential backoff with configurable parameters.
    ExponentialBackoff {
        /// Initial delay before the first retry.
        initial_delay: Duration,
        /// Maximum delay between retries.
        max_delay: Duration,
        /// Multiplier applied to the delay after each retry (typically 2.0).
        multiplier: f64,
        /// Maximum number of reconnection attempts (0 = unlimited).
        max_attempts: u32,
    },
}

impl ReconnectStrategy {
    /// Create an exponential backoff strategy with sensible defaults.
    ///
    /// Defaults: 1s initial delay, 30s max delay, 2x multiplier, 10 attempts.
    pub fn exponential_backoff() -> Self {
        Self::ExponentialBackoff {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            max_attempts: 10,
        }
    }
}

impl Default for ReconnectStrategy {
    fn default() -> Self {
        Self::None
    }
}

/// Stateful tracker for reconnection attempts.
pub struct ReconnectState {
    strategy: ReconnectStrategy,
    attempt: u32,
    current_delay: Duration,
}

impl ReconnectState {
    /// Create a new reconnection state from a strategy.
    pub fn new(strategy: ReconnectStrategy) -> Self {
        let initial_delay = match &strategy {
            ReconnectStrategy::None => Duration::ZERO,
            ReconnectStrategy::ExponentialBackoff { initial_delay, .. } => *initial_delay,
        };
        Self {
            strategy,
            attempt: 0,
            current_delay: initial_delay,
        }
    }

    /// Check if another reconnection attempt should be made.
    pub fn should_retry(&self) -> bool {
        match &self.strategy {
            ReconnectStrategy::None => false,
            ReconnectStrategy::ExponentialBackoff { max_attempts, .. } => {
                *max_attempts == 0 || self.attempt < *max_attempts
            }
        }
    }

    /// Wait for the appropriate backoff period and advance the attempt counter.
    ///
    /// Returns `true` if the wait completed (i.e., should proceed with retry),
    /// or `false` if no more retries should be attempted.
    pub async fn wait_and_advance(&mut self) -> bool {
        if !self.should_retry() {
            return false;
        }

        let delay = self.current_delay;
        self.attempt += 1;

        info!(
            attempt = self.attempt,
            delay_ms = delay.as_millis(),
            "Reconnecting after backoff"
        );

        tokio::time::sleep(delay).await;

        // Advance delay for next attempt
        if let ReconnectStrategy::ExponentialBackoff {
            max_delay,
            multiplier,
            ..
        } = &self.strategy
        {
            self.current_delay = Duration::from_secs_f64(
                (delay.as_secs_f64() * multiplier).min(max_delay.as_secs_f64()),
            );
        }

        true
    }

    /// Reset the reconnection state (e.g., after a successful connection).
    pub fn reset(&mut self) {
        self.attempt = 0;
        if let ReconnectStrategy::ExponentialBackoff { initial_delay, .. } = &self.strategy {
            self.current_delay = *initial_delay;
        }
        debug!("Reconnection state reset");
    }

    /// Get the current attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_strategy_never_retries() {
        let state = ReconnectState::new(ReconnectStrategy::None);
        assert!(!state.should_retry());
    }

    #[test]
    fn test_exponential_backoff_retries() {
        let state = ReconnectState::new(ReconnectStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            max_attempts: 3,
        });
        assert!(state.should_retry());
        assert_eq!(state.attempt(), 0);
    }

    #[tokio::test]
    async fn test_exponential_backoff_advances() {
        let mut state = ReconnectState::new(ReconnectStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(1), // Very short for testing
            max_delay: Duration::from_millis(100),
            multiplier: 2.0,
            max_attempts: 3,
        });

        assert!(state.wait_and_advance().await);
        assert_eq!(state.attempt(), 1);

        assert!(state.wait_and_advance().await);
        assert_eq!(state.attempt(), 2);

        assert!(state.wait_and_advance().await);
        assert_eq!(state.attempt(), 3);

        // Exhausted
        assert!(!state.wait_and_advance().await);
        assert_eq!(state.attempt(), 3);
    }

    #[tokio::test]
    async fn test_reset_clears_attempts() {
        let mut state = ReconnectState::new(ReconnectStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(100),
            multiplier: 2.0,
            max_attempts: 2,
        });

        assert!(state.wait_and_advance().await);
        assert!(state.wait_and_advance().await);
        assert!(!state.should_retry());

        state.reset();
        assert_eq!(state.attempt(), 0);
        assert!(state.should_retry());
    }

    #[test]
    fn test_unlimited_attempts() {
        let state = ReconnectState::new(ReconnectStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            max_attempts: 0, // unlimited
        });
        assert!(state.should_retry());
    }
}
