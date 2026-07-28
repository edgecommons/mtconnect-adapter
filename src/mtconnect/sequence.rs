//! # Sequence integrity: the acquisition state and the dedupe floor (LLD §5)
//!
//! MTConnect's guarantee is a *sequence*: every observation the agent buffers has a monotonic
//! number, and a client that tracks `nextSequence` can prove it lost nothing. This module owns that
//! bookkeeping — the state the acquisition task is in ([`AcqState`]) and the numbers it carries
//! ([`SequenceState`]).
//!
//! Two rules are subtle enough to state outright:
//!
//! * **Dedupe is per data item, not global.** One stream serves many devices, and a `/current`
//!   snapshot overlaps the stream window it interrupts. Publishing "everything with a sequence
//!   above the last one I saw" would drop a *different* data item's older-but-unseen observation.
//!   [`SequenceState::should_publish`] therefore keeps a floor per `dataItemId`.
//! * **An `instanceId` change voids everything.** The agent restarted: its sequence numbering
//!   starts over, so every floor and the `next` cursor must be reset before anything is published,
//!   or a fresh observation would be discarded as "older" than a number from a dead incarnation.
//!
//! The polling path and the streaming ladders (heartbeat expiry, `OUT_OF_RANGE` recovery,
//! `instanceId` resync) both drive this same state — the state machine lives in
//! [`super::AgentRuntime`], the vocabulary in [`super::stream`].

use std::collections::HashMap;

use super::xml::MtcHeader;

/// Where the per-agent acquisition task is in its lifecycle (LLD §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcqState {
    /// Probing / establishing; nothing has been delivered yet.
    Connecting,
    /// A multipart `/sample` stream is live, and `next` is where it resumes.
    Streaming { next: u64 },
    /// The buffer overran our position (`OUT_OF_RANGE`); recovering via a `/current` snapshot.
    Recovering,
    /// The agent restarted (`instanceId` changed); re-probing and re-verifying the model.
    Resyncing,
    /// `/current` on a timer — the configured `poll-only` mode, or the fallback after repeated
    /// stream-establish failures.
    Polling,
    /// Waiting out a reconnect window after a failure.
    Backoff,
}

impl AcqState {
    /// The `mode` string `sb/status` publishes: `stream` while streaming, else `poll`.
    #[must_use]
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Streaming { .. } => "stream",
            _ => "poll",
        }
    }

    /// Whether observations are currently being delivered (as opposed to connecting/recovering).
    #[must_use]
    pub fn is_delivering(&self) -> bool {
        matches!(self, Self::Streaming { .. } | Self::Polling)
    }
}

/// What a freshly received document's header means for the acquisition state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderOutcome {
    /// The first header seen from this agent.
    First,
    /// Same agent incarnation as before — carry on.
    Same,
    /// The agent restarted: every sequence number from before is void (recovery ladder 3).
    InstanceChanged { old: u64, new: u64 },
}

/// The sequence bookkeeping for one agent.
#[derive(Debug, Clone, Default)]
pub struct SequenceState {
    /// The agent incarnation these numbers belong to.
    pub instance_id: Option<u64>,
    /// Where the next `/sample` request resumes from.
    pub next: u64,
    /// The dedupe floor: the highest sequence already published, per `dataItemId`.
    pub last_published: HashMap<String, u64>,
}

impl SequenceState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold in a document header. On an `instanceId` change the state is reset **before** the
    /// caller publishes anything from that document.
    pub fn observe_header(&mut self, header: &MtcHeader) -> HeaderOutcome {
        let outcome = match self.instance_id {
            None => HeaderOutcome::First,
            Some(old) if old == header.instance_id => HeaderOutcome::Same,
            Some(old) => HeaderOutcome::InstanceChanged { old, new: header.instance_id },
        };
        if matches!(outcome, HeaderOutcome::InstanceChanged { .. }) {
            self.reset_for_new_instance();
        }
        self.instance_id = Some(header.instance_id);
        if let Some(next) = header.next_sequence {
            self.next = next;
        }
        outcome
    }

    /// Whether this observation is new for its data item — and record it if so. Idempotent: asking
    /// twice about the same observation answers `true` once.
    pub fn should_publish(&mut self, data_item_id: &str, sequence: u64) -> bool {
        match self.last_published.get(data_item_id) {
            Some(&seen) if sequence <= seen => false,
            _ => {
                self.last_published.insert(data_item_id.to_string(), sequence);
                true
            }
        }
    }

    /// The recorded floor for one data item, for diagnosis.
    #[must_use]
    pub fn floor(&self, data_item_id: &str) -> Option<u64> {
        self.last_published.get(data_item_id).copied()
    }

    /// Forget every floor (used when a snapshot must republish everything as fresh — after an
    /// `OUT_OF_RANGE` recovery, a resume from pause, or a forced `repoll`).
    pub fn reset_dedupe(&mut self) {
        self.last_published.clear();
    }

    /// The agent restarted: the incarnation, the cursor and every floor are void.
    pub fn reset_for_new_instance(&mut self) {
        self.instance_id = None;
        self.next = 0;
        self.last_published.clear();
    }

    /// How far the agent's buffer has run past our position — the observations we provably missed
    /// (recovery ladder 2's `MtconnectDataLossEvent` count).
    #[must_use]
    pub fn skipped_before(&self, first_sequence: u64) -> u64 {
        first_sequence.saturating_sub(self.next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(instance_id: u64, next: Option<u64>) -> MtcHeader {
        MtcHeader { instance_id, next_sequence: next, ..MtcHeader::default() }
    }

    #[test]
    fn the_dedupe_floor_is_per_data_item_not_global() {
        let mut s = SequenceState::new();
        assert!(s.should_publish("Xabs", 37));
        assert!(!s.should_publish("Xabs", 37), "the same observation twice is one publish");
        assert!(!s.should_publish("Xabs", 12), "an older sequence is not news");
        assert!(s.should_publish("Xabs", 38));

        // A DIFFERENT data item with a lower sequence is still new — this is exactly the case a
        // global floor gets wrong when a /current snapshot overlaps the stream window.
        assert!(s.should_publish("Xload", 12));
        assert_eq!(s.floor("Xabs"), Some(38));
        assert_eq!(s.floor("Xload"), Some(12));
        assert_eq!(s.floor("nope"), None);
    }

    #[test]
    fn a_header_advances_the_resume_cursor() {
        let mut s = SequenceState::new();
        assert_eq!(s.observe_header(&header(7, Some(42))), HeaderOutcome::First);
        assert_eq!(s.instance_id, Some(7));
        assert_eq!(s.next, 42);
        assert_eq!(s.observe_header(&header(7, Some(43))), HeaderOutcome::Same);
        assert_eq!(s.next, 43);
        // A header without nextSequence (an Errors document) leaves the cursor alone.
        assert_eq!(s.observe_header(&header(7, None)), HeaderOutcome::Same);
        assert_eq!(s.next, 43);
    }

    #[test]
    fn an_instance_change_voids_every_number_from_the_dead_incarnation() {
        let mut s = SequenceState::new();
        s.observe_header(&header(7, Some(500)));
        assert!(s.should_publish("Xabs", 480));

        // The agent restarted: its sequences start over at 1, and a floor of 480 would silently
        // swallow every fresh observation.
        assert_eq!(
            s.observe_header(&header(8, Some(2))),
            HeaderOutcome::InstanceChanged { old: 7, new: 8 }
        );
        assert_eq!(s.instance_id, Some(8));
        assert_eq!(s.next, 2);
        assert!(s.should_publish("Xabs", 1), "a fresh sequence is published, not discarded");
    }

    #[test]
    fn resetting_the_dedupe_republishes_a_snapshot_as_fresh() {
        let mut s = SequenceState::new();
        s.should_publish("Xabs", 37);
        s.reset_dedupe();
        assert!(s.should_publish("Xabs", 37), "the same value, deliberately republished");
        assert_eq!(s.instance_id, None, "resetting the floors is not a resync");
    }

    #[test]
    fn skipped_counts_what_the_buffer_ran_past() {
        let mut s = SequenceState::new();
        s.observe_header(&header(1, Some(100)));
        assert_eq!(s.skipped_before(153), 53, "the agent's floor minus our cursor");
        assert_eq!(s.skipped_before(100), 0);
        assert_eq!(s.skipped_before(7), 0, "never negative");
    }

    #[test]
    fn the_acquisition_state_reports_the_mode_status_publishes() {
        assert_eq!(AcqState::Streaming { next: 3 }.mode(), "stream");
        assert_eq!(AcqState::Polling.mode(), "poll");
        assert_eq!(AcqState::Connecting.mode(), "poll");
        assert_eq!(AcqState::Recovering.mode(), "poll");
        assert_eq!(AcqState::Resyncing.mode(), "poll");
        assert_eq!(AcqState::Backoff.mode(), "poll");

        assert!(AcqState::Streaming { next: 1 }.is_delivering());
        assert!(AcqState::Polling.is_delivering());
        for s in [AcqState::Connecting, AcqState::Recovering, AcqState::Resyncing, AcqState::Backoff] {
            assert!(!s.is_delivering(), "{s:?} delivers nothing");
        }
    }
}
