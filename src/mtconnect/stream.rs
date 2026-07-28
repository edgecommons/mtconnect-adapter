//! # Streaming acquisition — the heartbeat supervision half (LLD §5, ladder 1)
//!
//! The streaming path reads one long multipart response ([`super::multipart`]), classifies each
//! part, and drives the recovery ladders in [`super::sequence`]. What lives here today is the piece
//! every ladder depends on and that can be decided without a socket: **liveness**.
//!
//! An MTConnect agent that has nothing to say still sends an *empty* Streams document every
//! `heartbeat` milliseconds. Silence past that window means the connection is dead even though TCP
//! has not noticed — and a TCP-only view of liveness is exactly how a stalled stream goes unnoticed
//! for hours. [`HeartbeatWatch`] carries the window (2× the agent's heartbeat, LLD ladder 1) and the
//! last time anything arrived; the state machine drops and re-establishes the stream from
//! `nextSequence` when it expires.
//!
//! The part classifier and the reconnect loop that consume [`PartOutcome`] land with the streaming
//! milestone; the vocabulary is fixed here so the poll path, the ladders, and the metrics families
//! already agree on what a part *is*.

use std::time::{Duration, Instant};

/// What one part of a multipart stream turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartOutcome {
    /// A Streams document carrying observations.
    Observations { count: usize, next_sequence: Option<u64> },
    /// An empty Streams document: liveness, no data.
    Heartbeat,
    /// The agent's buffer ran past our position (recovery ladder 2).
    OutOfRange { first_sequence: u64 },
    /// Any other `MTConnectError` document.
    AgentError { code: String },
    /// A part that could not be parsed. Counted; repeated failures drop the stream.
    Undecodable,
}

impl PartOutcome {
    /// Whether this part proves the agent is alive — every part does, including a heartbeat and
    /// even an error document.
    #[must_use]
    pub fn is_liveness(&self) -> bool {
        !matches!(self, Self::Undecodable)
    }
}

/// Liveness supervision for one stream.
#[derive(Debug, Clone)]
pub struct HeartbeatWatch {
    window: Duration,
    last_seen: Instant,
}

impl HeartbeatWatch {
    /// A watch over an agent's `heartbeatMs`. The tolerated silence is **twice** the heartbeat
    /// (LLD ladder 1): one missed heartbeat is a hiccup, two is a dead stream.
    #[must_use]
    pub fn new(heartbeat_ms: u32, now: Instant) -> Self {
        Self {
            window: Duration::from_millis(u64::from(heartbeat_ms).saturating_mul(2)),
            last_seen: now,
        }
    }

    /// The tolerated silence.
    #[must_use]
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Record that something arrived from the agent.
    pub fn touch(&mut self, now: Instant) {
        self.last_seen = now;
    }

    /// Whether the stream has been silent past its window.
    #[must_use]
    pub fn is_expired(&self, now: Instant) -> bool {
        now.duration_since(self.last_seen) >= self.window
    }

    /// How long until the window expires — what a `select!` waits on.
    #[must_use]
    pub fn remaining(&self, now: Instant) -> Duration {
        self.window.saturating_sub(now.duration_since(self.last_seen))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_liveness_window_is_two_heartbeats() {
        let t0 = Instant::now();
        let w = HeartbeatWatch::new(10_000, t0);
        assert_eq!(w.window(), Duration::from_secs(20));
        assert!(!w.is_expired(t0 + Duration::from_secs(19)), "one missed heartbeat is a hiccup");
        assert!(w.is_expired(t0 + Duration::from_secs(20)), "two is a dead stream");
        assert_eq!(w.remaining(t0 + Duration::from_secs(5)), Duration::from_secs(15));
        assert_eq!(w.remaining(t0 + Duration::from_secs(30)), Duration::ZERO, "never negative");
    }

    #[test]
    fn anything_arriving_refreshes_the_deadline() {
        let t0 = Instant::now();
        let mut w = HeartbeatWatch::new(1_000, t0);
        assert!(w.is_expired(t0 + Duration::from_secs(3)));
        w.touch(t0 + Duration::from_secs(3));
        assert!(!w.is_expired(t0 + Duration::from_secs(4)));
        assert!(w.is_expired(t0 + Duration::from_secs(5)));
    }

    #[test]
    fn every_readable_part_proves_liveness_including_a_heartbeat_and_an_error() {
        assert!(PartOutcome::Heartbeat.is_liveness());
        assert!(PartOutcome::Observations { count: 3, next_sequence: Some(42) }.is_liveness());
        assert!(PartOutcome::OutOfRange { first_sequence: 153 }.is_liveness());
        assert!(PartOutcome::AgentError { code: "UNAUTHORIZED".into() }.is_liveness());
        // A part that cannot be read proves nothing about the agent.
        assert!(!PartOutcome::Undecodable.is_liveness());
    }
}
