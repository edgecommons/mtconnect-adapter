//! # Streaming acquisition — supervision and classification (LLD §5/§6)
//!
//! The streaming path reads one long multipart response ([`super::multipart`]), classifies each
//! part, and drives the recovery ladders in [`super::sequence`]. This module owns the pieces that
//! can be decided without an `AgentRuntime`:
//!
//! * **Liveness** ([`HeartbeatWatch`]): an MTConnect agent that has nothing to say still sends an
//!   *empty* Streams document every `heartbeat` milliseconds. Silence past that window means the
//!   connection is dead even though TCP has not noticed — and a TCP-only view of liveness is
//!   exactly how a stalled stream goes unnoticed for hours. The watch carries the window (2× the
//!   agent's heartbeat, ladder 1) and the last time anything arrived; the state machine drops and
//!   re-establishes the stream from `nextSequence` when it expires.
//! * **Classification** ([`classify_part`]): one part is a Streams document, an Errors document, or
//!   garbage — and the reader must not guess. The parsed document rides along so it is tokenized
//!   exactly once.
//! * **The transport seam** ([`ChunkSource`]): the state machine consumes chunks through this
//!   trait, which is what lets the virtual-clock sequence tests drive every ladder with a scripted
//!   source instead of a socket.
//! * **The exit vocabulary** ([`StreamExit`]): every way a live stream ends, each mapping to one
//!   rung of the LLD §5 recovery ladder.

use std::time::{Duration, Instant};

use super::error::MtcError;
use super::multipart::Part;
use super::xml::{self, DocKind, ErrorsDoc, StreamsDoc};

/// What one part of a multipart stream turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartOutcome {
    /// A Streams document carrying observations.
    Observations {
        count: usize,
        next_sequence: Option<u64>,
    },
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

/// A source of multipart body chunks — the transport seam the streaming state machine reads
/// through. [`super::client::StreamResponse`] is the live implementation; the virtual-clock
/// sequence tests implement it with a script.
pub trait ChunkSource: Send {
    /// The next chunk of the body, or `None` at end of stream.
    fn next_chunk(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, MtcError>> + Send;
}

/// One classified — and, when readable, parsed — multipart part.
#[derive(Debug)]
pub enum PartDoc {
    /// A Streams document: observations, or the agent's empty heartbeat.
    Streams(StreamsDoc),
    /// An `MTConnectError(s)` document.
    Errors(ErrorsDoc),
    /// A well-formed document of a kind that has no business in a `/sample` stream (a Devices
    /// document, an HTML error page). Named for the log; counted as undecodable.
    Unexpected(String),
    /// A part that could not be read at all.
    Unreadable(MtcError),
}

/// Classify one part by its root element, parsing it exactly once.
#[must_use]
pub fn classify_part(part: &Part) -> PartDoc {
    let text = match part.text() {
        Ok(t) => t,
        Err(e) => return PartDoc::Unreadable(e),
    };
    let doc = match xml::parse_document(text) {
        Ok(doc) => doc,
        Err(e) => return PartDoc::Unreadable(e),
    };
    match xml::document_kind(&doc.root.name) {
        DocKind::Streams => match xml::streams_from_doc(doc) {
            Ok(d) => PartDoc::Streams(d),
            Err(e) => PartDoc::Unreadable(e),
        },
        DocKind::Errors => match xml::errors_from_doc(doc) {
            Ok(d) => PartDoc::Errors(d),
            Err(e) => PartDoc::Unreadable(e),
        },
        DocKind::Devices => PartDoc::Unexpected("MTConnectDevices".into()),
        DocKind::Unknown(name) => PartDoc::Unexpected(name),
    }
}

/// Every way a live stream ends. Each variant maps onto one rung of the LLD §5 recovery ladder —
/// the state machine in [`super::AgentRuntime`] matches on this and nothing else.
#[derive(Debug)]
pub enum StreamExit {
    /// Nothing arrived within 2× the heartbeat: the stream is dead even if TCP disagrees.
    /// Re-establish from the same `nextSequence` (ladder 1).
    HeartbeatMissed,
    /// The transport failed mid-stream. Re-establish from the same `nextSequence` (ladder 1).
    TransportLost(MtcError),
    /// The multipart framing broke, a part was oversize, or parts stopped being decodable.
    /// Re-establish from the same `nextSequence` (ladder 1).
    Malformed(MtcError),
    /// The agent closed the response body. Re-establish (ladder 1).
    EndOfStream,
    /// The agent's buffer ran past our position: `DataLoss`, snapshot republish, resume from the
    /// snapshot's `nextSequence` (ladder 2).
    OutOfRange { first_sequence: u64 },
    /// The agent restarted (`instanceId` changed): full resync (ladder 3).
    InstanceChanged,
    /// A `reconnect` control request asked for a full drop + re-probe.
    CtlReconnect,
    /// Shutdown was requested; the acquisition task returns.
    Shutdown,
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
        self.window
            .saturating_sub(now.duration_since(self.last_seen))
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
        assert!(
            !w.is_expired(t0 + Duration::from_secs(19)),
            "one missed heartbeat is a hiccup"
        );
        assert!(
            w.is_expired(t0 + Duration::from_secs(20)),
            "two is a dead stream"
        );
        assert_eq!(
            w.remaining(t0 + Duration::from_secs(5)),
            Duration::from_secs(15)
        );
        assert_eq!(
            w.remaining(t0 + Duration::from_secs(30)),
            Duration::ZERO,
            "never negative"
        );
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
        assert!(
            PartOutcome::Observations {
                count: 3,
                next_sequence: Some(42)
            }
            .is_liveness()
        );
        assert!(
            PartOutcome::OutOfRange {
                first_sequence: 153
            }
            .is_liveness()
        );
        assert!(
            PartOutcome::AgentError {
                code: "UNAUTHORIZED".into()
            }
            .is_liveness()
        );
        // A part that cannot be read proves nothing about the agent.
        assert!(!PartOutcome::Undecodable.is_liveness());
    }
}
