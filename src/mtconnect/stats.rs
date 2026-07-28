//! # Acquisition counters — what the `MtconnectStream` / `MtconnectProbe` families report (HLD §9)
//!
//! The runtime accumulates **monotonic totals** here; `src/metrics.rs` turns them into the
//! `(Total, Interval)` measure pairs the fleet dashboard reads by diffing successive
//! [`AgentStatsSnapshot`]s. Splitting it that way keeps the emitter's arithmetic in one tested
//! place and keeps this module — like everything under `src/mtconnect/` — free of any `edgecommons`
//! import.
//!
//! Every counter here is an **agent-scoped** number: one agent serves many devices from one
//! document stream, so documents, observations, heartbeats and gaps belong to the agent, not to a
//! device (which is why the family is dimensioned `agentId`, never `deviceUuid` — an unbounded
//! dimension would shred a fleet dashboard).

use std::sync::atomic::{AtomicU64, Ordering};

/// A point-in-time read of every acquisition counter. All values are monotonic since process
/// start; the interval measures are the difference between two snapshots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentStatsSnapshot {
    /// Streams/Errors documents that were decoded (`/current` cycles and streamed parts alike).
    pub documents: u64,
    /// Documents (or multipart parts) that could not be decoded.
    pub documents_failed: u64,
    /// Observations decoded across every attached device.
    pub observations: u64,
    /// Documents that carried no observation — the agent's liveness beat.
    pub heartbeats: u64,
    /// Times a multipart stream had to be **re**-established (the first establishment of a
    /// process's stream is not a reconnect).
    pub reconnects: u64,
    /// Observations provably lost because the agent's buffer overran our position (ladder 2).
    pub gaps: u64,
    /// `OUT_OF_RANGE` error documents — the recovery events those gaps were discovered through.
    pub out_of_range: u64,
    /// Accumulated latency of acquisition requests that succeeded.
    pub latency_ms: u64,
    /// Accumulated latency of acquisition requests that failed.
    pub error_latency_ms: u64,
    /// `/probe` requests that answered.
    pub probes: u64,
    /// `/probe` requests that failed.
    pub probes_failed: u64,
    /// Probes whose device model digest differed from the cached one (D-MTC-5 drift).
    pub model_changes: u64,
    /// Accumulated latency of probes that succeeded.
    pub probe_latency_ms: u64,
    /// Accumulated latency of probes that failed.
    pub probe_error_latency_ms: u64,
}

/// The live, lock-free counter bag one [`crate::mtconnect::AgentRuntime`] owns.
#[derive(Debug, Default)]
pub struct AgentStats {
    documents: AtomicU64,
    documents_failed: AtomicU64,
    observations: AtomicU64,
    heartbeats: AtomicU64,
    streams_opened: AtomicU64,
    reconnects: AtomicU64,
    gaps: AtomicU64,
    out_of_range: AtomicU64,
    latency_ms: AtomicU64,
    error_latency_ms: AtomicU64,
    probes: AtomicU64,
    probes_failed: AtomicU64,
    model_changes: AtomicU64,
    probe_latency_ms: AtomicU64,
    probe_error_latency_ms: AtomicU64,
}

fn bump(counter: &AtomicU64, by: u64) {
    counter.fetch_add(by, Ordering::Relaxed);
}

impl AgentStats {
    /// One decoded document: its observations, and — when it carried none — one heartbeat. An
    /// empty Streams document IS the agent's liveness beat (HLD §5.2), so the two are recorded
    /// from the same call and can never disagree.
    pub fn record_document(&self, observations: u64) {
        bump(&self.documents, 1);
        bump(&self.observations, observations);
        if observations == 0 {
            bump(&self.heartbeats, 1);
        }
    }

    /// One document (or multipart part) that could not be decoded.
    pub fn record_document_failed(&self) {
        bump(&self.documents_failed, 1);
    }

    /// One established multipart stream. The **first** one is the initial connect; every one after
    /// it got there by recovering from a lost stream, so only those count as reconnects — the same
    /// rule the shared `MtconnectAdapterConnection` family applies to a device link.
    pub fn record_stream_opened(&self) {
        if self.streams_opened.fetch_add(1, Ordering::Relaxed) > 0 {
            bump(&self.reconnects, 1);
        }
    }

    /// One `OUT_OF_RANGE` recovery, and how many observations it proved lost (ladder 2).
    pub fn record_gap(&self, skipped: u64) {
        bump(&self.out_of_range, 1);
        bump(&self.gaps, skipped);
    }

    /// One acquisition request's latency, split by whether it answered.
    pub fn record_latency(&self, ms: u64, ok: bool) {
        bump(if ok { &self.latency_ms } else { &self.error_latency_ms }, ms);
    }

    /// One `/probe` outcome and its latency.
    pub fn record_probe(&self, ms: u64, ok: bool) {
        if ok {
            bump(&self.probes, 1);
            bump(&self.probe_latency_ms, ms);
        } else {
            bump(&self.probes_failed, 1);
            bump(&self.probe_error_latency_ms, ms);
        }
    }

    /// One probe whose digest differed from the cached model's.
    pub fn record_model_change(&self) {
        bump(&self.model_changes, 1);
    }

    /// Read every counter.
    #[must_use]
    pub fn snapshot(&self) -> AgentStatsSnapshot {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        AgentStatsSnapshot {
            documents: g(&self.documents),
            documents_failed: g(&self.documents_failed),
            observations: g(&self.observations),
            heartbeats: g(&self.heartbeats),
            reconnects: g(&self.reconnects),
            gaps: g(&self.gaps),
            out_of_range: g(&self.out_of_range),
            latency_ms: g(&self.latency_ms),
            error_latency_ms: g(&self.error_latency_ms),
            probes: g(&self.probes),
            probes_failed: g(&self.probes_failed),
            model_changes: g(&self.model_changes),
            probe_latency_ms: g(&self.probe_latency_ms),
            probe_error_latency_ms: g(&self.probe_error_latency_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_with_observations_is_not_a_heartbeat_and_an_empty_one_is() {
        let s = AgentStats::default();
        s.record_document(3);
        s.record_document(0);
        let snap = s.snapshot();
        assert_eq!(snap.documents, 2, "both documents count");
        assert_eq!(snap.observations, 3);
        assert_eq!(snap.heartbeats, 1, "only the empty document is the liveness beat");
    }

    #[test]
    fn a_gap_records_both_the_recovery_and_how_much_was_lost() {
        let s = AgentStats::default();
        s.record_gap(17);
        s.record_gap(3);
        let snap = s.snapshot();
        // Two distinct measures on purpose: `outOfRange` counts the recoveries an operator sees,
        // `gaps` counts the observations that were provably never delivered.
        assert_eq!(snap.out_of_range, 2);
        assert_eq!(snap.gaps, 20);
    }

    #[test]
    fn latency_is_kept_apart_by_outcome_so_a_failing_agent_cannot_flatter_the_average() {
        let s = AgentStats::default();
        s.record_latency(40, true);
        s.record_latency(9_000, false);
        s.record_probe(12, true);
        s.record_probe(500, false);
        let snap = s.snapshot();
        assert_eq!((snap.latency_ms, snap.error_latency_ms), (40, 9_000));
        assert_eq!((snap.probes, snap.probes_failed), (1, 1));
        assert_eq!((snap.probe_latency_ms, snap.probe_error_latency_ms), (12, 500));
    }

    #[test]
    fn failures_reconnects_and_model_changes_each_have_their_own_counter() {
        let s = AgentStats::default();
        s.record_document_failed();
        s.record_stream_opened(); // the initial connect
        s.record_stream_opened();
        s.record_stream_opened();
        s.record_model_change();
        let snap = s.snapshot();
        assert_eq!(snap.documents_failed, 1);
        assert_eq!(snap.reconnects, 2, "the first established stream is not a reconnect");
        assert_eq!(snap.model_changes, 1);
        assert_eq!(snap.documents, 0, "an undecodable document is not a decoded one");
    }
}
