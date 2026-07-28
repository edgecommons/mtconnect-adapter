//! # Publish shaping — per-signal coalescing and deadband, ABOVE the device session
//!
//! The engine that makes a signal's `publish` policy (`mode`, `batchMs`, `deadband`) govern what
//! actually reaches the wire. It sits between the session's readings and the `data()` facade
//! (ADP-5: shaping lives above the protocol session, so the real MTConnect backend and the
//! simulator are shaped identically), and it is deliberately **pure**: every decision takes an
//! explicit `Instant`, so the whole engine is unit-tested on a virtual clock and the supervisor
//! only has to drive it.
//!
//! ## The rules (D-MtconnectAdapter-L11)
//!
//! * **Unshaped is untouched.** A signal with no policy — or `batchMs: 0`, the default — publishes
//!   each reading immediately, exactly as before the engine existed.
//! * **`batchMs > 0` opens a window per signal.** GOOD readings buffer; when the window expires the
//!   buffer flushes as ONE `SouthboundSignalUpdate` whose `samples[]` carries every buffered
//!   reading in arrival order — each keeping its own timestamps and extras. Under
//!   `mode: "interval"` the window keeps only the **latest** reading instead.
//! * **Quality never sits in a window.** A BAD or UNCERTAIN reading flushes its signal's buffer
//!   immediately, itself included.
//! * **Deadband is applied on ENTRY.** A numeric value differing from the last *accepted* value by
//!   less than `deadband` is dropped. Non-numeric/array values always pass, a quality or
//!   `qualityRaw` change always passes, and the first reading after connect / resync / resume
//!   always passes ([`Shaper::reset_deadband`]).
//! * **One timer per instance task.** [`Shaper::next_deadline`] is the earliest open window; the
//!   supervisor sleeps until it and calls [`Shaper::due`] — no per-signal timers.
//! * **Lifecycle:** pause **clears** the buffers (the resume-time snapshot republishes the current
//!   truth, so flushing pre-pause readings after it would publish stale data out of order);
//!   shutdown, link loss and reconnect **flush** them ([`Shaper::flush_all`] — no data loss on
//!   SIGTERM); a reload swaps the policy table atomically with the signal-set swap, and the buffers
//!   of removed/changed signals flush with their old policy ([`Shaper::set_policies`]).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::device::{Quality, Reading};
use crate::mtconnect::config::{PublishMode, SignalConfig};

/// One flushed publication: every reading becomes one sample of ONE `SouthboundSignalUpdate`, in
/// arrival order. Never empty.
pub type Update = Vec<Reading>;

/// One signal's effective shaping policy — the engine's own vocabulary, compiled from
/// [`PublishCfg`](crate::mtconnect::config::PublishCfg) by whoever knows the signal (the MTConnect
/// session compiles the served set against the probe model; a backend with no compile step uses
/// [`policies_from_signals`]).
#[derive(Debug, Clone, PartialEq)]
pub struct PublishPolicy {
    /// The coalescing window, in milliseconds. `0` publishes immediately.
    pub batch_ms: u32,
    /// `mode: "interval"` — the window keeps only the latest accepted reading, publishing one
    /// sample per window instead of every reading.
    pub latest_only: bool,
    /// Absolute deadband, applied on entry. Set only where it applies (numeric SAMPLE-category
    /// data items for MTConnect); `None` means every reading enters.
    pub deadband: Option<f64>,
}

impl PublishPolicy {
    /// Whether this policy shapes anything at all. A trivial policy is not worth a table entry:
    /// the unshaped fast path is identical.
    #[must_use]
    pub fn is_trivial(&self) -> bool {
        self.batch_ms == 0 && self.deadband.is_none()
    }
}

/// Compile a static signal list into the engine's policy table — the fallback for a backend that
/// does not compile its signals against a device model (the simulator). Only non-trivial policies
/// are kept: an absent entry IS the immediate-publish default.
///
/// Without a model there is no category to gate on, so a configured `deadband` applies to any
/// numeric value the signal produces.
#[must_use]
pub fn policies_from_signals(signals: &[SignalConfig]) -> HashMap<String, PublishPolicy> {
    let mut out = HashMap::new();
    for s in signals {
        let p = s.publish_policy();
        let policy = PublishPolicy {
            batch_ms: p.batch_ms,
            latest_only: p.mode == PublishMode::Interval,
            deadband: p.deadband,
        };
        if !policy.is_trivial() {
            out.insert(s.id.clone(), policy);
        }
    }
    out
}

/// The engine's counters since the last [`Shaper::take_counters`] — the `MtconnectAdapterShaping`
/// metric family's feed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShapingCounters {
    /// Updates the engine released to the wire (immediate publishes and window flushes alike).
    pub published: u64,
    /// Readings deferred into a batch window instead of publishing immediately.
    pub coalesced: u64,
    /// Readings suppressed by a deadband on entry.
    pub deadband_dropped: u64,
}

impl ShapingCounters {
    fn is_zero(self) -> bool {
        self == Self::default()
    }
}

/// One signal's open window.
#[derive(Debug)]
struct Buffer {
    readings: Vec<Reading>,
    deadline: Instant,
}

/// One signal's deadband entry state: the last **accepted** numeric value, and the quality key a
/// change of which must always pass.
#[derive(Debug)]
struct EntryState {
    last_value: Option<f64>,
    last_quality: (Quality, Option<String>),
}

/// The per-instance shaping engine. One per device task, rebuilt with each session — which is what
/// makes the first reading after a (re)connect pass the deadband.
#[derive(Debug, Default)]
pub struct Shaper {
    policies: HashMap<String, PublishPolicy>,
    buffers: HashMap<String, Buffer>,
    entries: HashMap<String, EntryState>,
    counters: ShapingCounters,
}

impl Shaper {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a new policy table — the reload path, riding the same atomic signal-set swap the
    /// session already performs. The open buffers of signals whose policy changed or disappeared
    /// flush **now**, with the readings their old policy collected; unchanged signals keep their
    /// windows running.
    pub fn set_policies(&mut self, policies: HashMap<String, PublishPolicy>) -> Vec<Update> {
        let stale: Vec<String> = self
            .buffers
            .keys()
            .filter(|id| self.policies.get(*id) != policies.get(*id))
            .cloned()
            .collect();
        let mut flushed = Vec::new();
        for id in stale {
            if let Some(update) = self.flush_signal(&id) {
                flushed.push(update);
            }
        }
        self.policies = policies;
        flushed
    }

    /// Offer one reading to the engine. Returns the updates to publish **now**: the reading alone
    /// (unshaped / `batchMs: 0`), the signal's flushed window (a BAD/UNCERTAIN reading), or nothing
    /// (buffered, or deadband-dropped).
    pub fn offer(&mut self, reading: Reading, now: Instant) -> Vec<Update> {
        let Some(policy) = self.policies.get(&reading.signal_id).cloned() else {
            // No policy = today's behavior, byte for byte: publish immediately.
            self.counters.published += 1;
            return vec![vec![reading]];
        };

        // Deadband gates ENTRY — a dropped reading never reaches a buffer.
        if let Some(deadband) = policy.deadband {
            if self.deadband_suppresses(&reading, deadband) {
                self.counters.deadband_dropped += 1;
                return Vec::new();
            }
        }
        self.note_entry(&reading);

        if policy.batch_ms == 0 {
            self.counters.published += 1;
            return vec![vec![reading]];
        }

        // A quality problem must not sit in a window: flush the buffer, this reading included.
        if reading.quality != Quality::Good {
            let mut readings = self
                .buffers
                .remove(&reading.signal_id)
                .map(|b| b.readings)
                .unwrap_or_default();
            readings.push(reading);
            self.counters.published += 1;
            return vec![readings];
        }

        let buffer = self.buffers.entry(reading.signal_id.clone()).or_insert_with(|| Buffer {
            readings: Vec::new(),
            deadline: now + Duration::from_millis(u64::from(policy.batch_ms)),
        });
        if policy.latest_only {
            buffer.readings.clear();
        }
        buffer.readings.push(reading);
        self.counters.coalesced += 1;
        Vec::new()
    }

    /// The earliest open window's expiry — the one deadline the instance task sleeps on. `None`
    /// while nothing is buffered.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        self.buffers.values().map(|b| b.deadline).min()
    }

    /// Flush every window whose deadline has passed. Deterministic order: deadline, then signal id.
    pub fn due(&mut self, now: Instant) -> Vec<Update> {
        let mut expired: Vec<(Instant, String)> = self
            .buffers
            .iter()
            .filter(|(_, b)| b.deadline <= now)
            .map(|(id, b)| (b.deadline, id.clone()))
            .collect();
        expired.sort();
        expired.into_iter().filter_map(|(_, id)| self.flush_signal(&id)).collect()
    }

    /// Flush every open window — shutdown, link loss, reconnect: buffered readings are data, and
    /// they must not be lost with the task. Deterministic order: deadline, then signal id.
    pub fn flush_all(&mut self) -> Vec<Update> {
        let mut all: Vec<(Instant, String)> = self
            .buffers
            .iter()
            .map(|(id, b)| (b.deadline, id.clone()))
            .collect();
        all.sort();
        all.into_iter().filter_map(|(_, id)| self.flush_signal(&id)).collect()
    }

    /// Discard every open window — the pause discipline: nothing reaches the wire while paused,
    /// and the resume-time snapshot republishes the current truth, so flushing pre-pause readings
    /// after it would publish stale data out of order. Returns how many readings were discarded.
    pub fn clear_buffers(&mut self) -> usize {
        self.buffers.drain().map(|(_, b)| b.readings.len()).sum()
    }

    /// Forget the deadband entry state, so the **next** reading of every signal passes — called on
    /// resume and on an acquisition resync (a fresh snapshot re-baselines the fleet's view, and
    /// suppressing its successor against a pre-gap value would be wrong).
    pub fn reset_deadband(&mut self) {
        self.entries.clear();
    }

    /// Drain the counters accumulated since the last call. `None` when nothing moved.
    pub fn take_counters(&mut self) -> Option<ShapingCounters> {
        let taken = std::mem::take(&mut self.counters);
        (!taken.is_zero()).then_some(taken)
    }

    fn flush_signal(&mut self, id: &str) -> Option<Update> {
        let buffer = self.buffers.remove(id)?;
        if buffer.readings.is_empty() {
            return None;
        }
        self.counters.published += 1;
        Some(buffer.readings)
    }

    /// Whether the deadband suppresses this reading. It never suppresses: the first reading since
    /// the entry state was (re)built, any quality or `qualityRaw` change, or a non-numeric value.
    fn deadband_suppresses(&self, reading: &Reading, deadband: f64) -> bool {
        let Some(state) = self.entries.get(&reading.signal_id) else {
            return false; // first after connect/resync/resume always passes
        };
        if (reading.quality, reading.quality_raw.clone()) != state.last_quality {
            return false; // a quality transition must never be suppressed
        }
        let (Some(value), Some(last)) =
            (reading.value.as_ref().and_then(serde_json::Value::as_f64), state.last_value)
        else {
            return false; // non-numeric/array values (and a missing baseline) always pass
        };
        (value - last).abs() < deadband
    }

    /// Record an accepted reading as the new deadband baseline. The numeric anchor moves only on a
    /// numeric value — deadband compares against the last **accepted** value, not the last seen.
    fn note_entry(&mut self, reading: &Reading) {
        let numeric = reading.value.as_ref().and_then(serde_json::Value::as_f64);
        let state = self
            .entries
            .entry(reading.signal_id.clone())
            .or_insert_with(|| EntryState { last_value: None, last_quality: (reading.quality, reading.quality_raw.clone()) });
        state.last_quality = (reading.quality, reading.quality_raw.clone());
        if let Some(v) = numeric {
            state.last_value = Some(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy(batch_ms: u32) -> PublishPolicy {
        PublishPolicy { batch_ms, latest_only: false, deadband: None }
    }

    fn table(entries: &[(&str, PublishPolicy)]) -> HashMap<String, PublishPolicy> {
        entries.iter().map(|(id, p)| ((*id).to_string(), p.clone())).collect()
    }

    fn good(id: &str, value: f64) -> Reading {
        Reading::good(id, json!(value))
    }

    fn t0() -> Instant {
        Instant::now()
    }

    fn ms(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    // --- the unshaped fast path -----------------------------------------------------------------

    #[test]
    fn a_signal_without_a_policy_publishes_each_reading_immediately() {
        // batchMs=0 / no policy is today's behavior, byte for byte: one reading, one update.
        let mut s = Shaper::new();
        let out = s.offer(good("a", 1.0), t0());
        assert_eq!(out, vec![vec![good("a", 1.0)]]);
        assert_eq!(s.next_deadline(), None, "nothing buffered, nothing to sleep on");
        assert_eq!(
            s.take_counters(),
            Some(ShapingCounters { published: 1, ..Default::default() })
        );
    }

    #[test]
    fn a_zero_window_policy_publishes_immediately_after_the_deadband() {
        let mut s = Shaper::new();
        s.set_policies(table(&[(
            "a",
            PublishPolicy { batch_ms: 0, latest_only: false, deadband: Some(0.5) },
        )]));
        assert_eq!(s.offer(good("a", 1.0), t0()).len(), 1, "the first reading always passes");
        assert!(s.offer(good("a", 1.2), t0()).is_empty(), "sub-deadband change dropped");
        assert_eq!(s.offer(good("a", 2.0), t0()).len(), 1, "a real change publishes immediately");
    }

    #[test]
    fn a_trivial_policy_is_recognized_so_the_fast_path_stays_fast() {
        assert!(policy(0).is_trivial());
        assert!(!policy(100).is_trivial());
        assert!(
            !PublishPolicy { batch_ms: 0, latest_only: false, deadband: Some(1.0) }.is_trivial()
        );
        // latest_only with a zero window degenerates to immediate — trivial.
        assert!(PublishPolicy { batch_ms: 0, latest_only: true, deadband: None }.is_trivial());
    }

    // --- batching -------------------------------------------------------------------------------

    #[test]
    fn a_window_buffers_and_flushes_every_reading_in_arrival_order() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(250))]));
        let start = t0();

        assert!(s.offer(good("a", 1.0), start).is_empty(), "buffered, not published");
        assert!(s.offer(good("a", 2.0), ms(start, 100)).is_empty());
        assert!(s.offer(good("a", 3.0), ms(start, 200)).is_empty());

        // The window opened at the FIRST buffered reading.
        assert_eq!(s.next_deadline(), Some(ms(start, 250)));
        assert!(s.due(ms(start, 249)).is_empty(), "not yet");

        let flushed = s.due(ms(start, 250));
        assert_eq!(flushed.len(), 1, "ONE update for the whole window");
        let values: Vec<_> = flushed[0].iter().map(|r| r.value.clone().unwrap()).collect();
        assert_eq!(values, vec![json!(1.0), json!(2.0), json!(3.0)], "arrival order");
        assert_eq!(s.next_deadline(), None, "the window closed");
        assert_eq!(
            s.take_counters(),
            Some(ShapingCounters { published: 1, coalesced: 3, ..Default::default() })
        );
    }

    #[test]
    fn each_buffered_reading_keeps_its_own_timestamps_and_extras() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(100))]));
        let start = t0();
        let first = Reading {
            capture_ts: Some("2026-07-27T10:00:00Z".into()),
            ..good("a", 1.0).with_extra("sequence", json!(37))
        };
        let second = Reading {
            capture_ts: Some("2026-07-27T10:00:00.050Z".into()),
            ..good("a", 2.0).with_extra("sequence", json!(38))
        };
        s.offer(first.clone(), start);
        s.offer(second.clone(), ms(start, 50));

        let flushed = s.due(ms(start, 100));
        assert_eq!(flushed[0], vec![first, second], "readings ride the flush untouched");
    }

    #[test]
    fn windows_of_different_signals_are_independent() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(100)), ("b", policy(300))]));
        let start = t0();
        s.offer(good("a", 1.0), start);
        s.offer(good("b", 9.0), ms(start, 10));
        s.offer(good("a", 2.0), ms(start, 20));

        assert_eq!(s.next_deadline(), Some(ms(start, 100)), "the EARLIEST window");
        let flushed = s.due(ms(start, 100));
        assert_eq!(flushed.len(), 1, "only a's window expired");
        assert_eq!(flushed[0].len(), 2);
        assert_eq!(flushed[0][0].signal_id, "a");

        assert_eq!(s.next_deadline(), Some(ms(start, 310)), "b's window is still open");
        let flushed = s.due(ms(start, 310));
        assert_eq!(flushed[0][0].signal_id, "b");
    }

    #[test]
    fn a_new_reading_after_a_flush_opens_a_fresh_window() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(100))]));
        let start = t0();
        s.offer(good("a", 1.0), start);
        s.due(ms(start, 100));

        s.offer(good("a", 2.0), ms(start, 500));
        assert_eq!(s.next_deadline(), Some(ms(start, 600)), "the window opens at arrival");
    }

    #[test]
    fn interval_mode_keeps_only_the_latest_reading_per_window() {
        let mut s = Shaper::new();
        s.set_policies(table(&[(
            "a",
            PublishPolicy { batch_ms: 200, latest_only: true, deadband: None },
        )]));
        let start = t0();
        s.offer(good("a", 1.0), start);
        s.offer(good("a", 2.0), ms(start, 50));
        s.offer(good("a", 3.0), ms(start, 100));

        let flushed = s.due(ms(start, 200));
        assert_eq!(flushed[0].len(), 1, "one sample per window");
        assert_eq!(flushed[0][0].value, Some(json!(3.0)), "the latest");
        // An empty window publishes nothing — there is nothing to say.
        assert!(s.due(ms(start, 500)).is_empty());
    }

    #[test]
    fn multiple_expired_windows_flush_in_deadline_then_id_order() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("b", policy(50)), ("a", policy(100)), ("c", policy(50))]));
        let start = t0();
        s.offer(good("c", 3.0), start);
        s.offer(good("b", 2.0), start);
        s.offer(good("a", 1.0), start);

        let flushed = s.due(ms(start, 100));
        let order: Vec<_> = flushed.iter().map(|u| u[0].signal_id.clone()).collect();
        assert_eq!(order, vec!["b", "c", "a"], "deadline first, id as the tiebreak");
    }

    // --- quality flushes ------------------------------------------------------------------------

    #[test]
    fn a_bad_reading_flushes_the_window_immediately_with_itself_last() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(60_000))]));
        let start = t0();
        s.offer(good("a", 1.0), start);
        s.offer(good("a", 2.0), ms(start, 10));

        let flushed = s.offer(Reading::bad("a", "UNAVAILABLE"), ms(start, 20));
        assert_eq!(flushed.len(), 1, "a quality problem must not sit in a window");
        let ids: Vec<_> = flushed[0].iter().map(|r| r.value.clone()).collect();
        assert_eq!(ids, vec![Some(json!(1.0)), Some(json!(2.0)), None], "buffered first, BAD last");
        assert_eq!(s.next_deadline(), None, "the window closed with the flush");
    }

    #[test]
    fn an_uncertain_reading_flushes_too_and_an_empty_buffer_flushes_it_alone() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(60_000))]));
        let uncertain = Reading {
            quality: Quality::Uncertain,
            quality_raw: Some("MTC_CONDITION:WARNING:ALM-7".into()),
            ..good("a", 5.0)
        };
        let flushed = s.offer(uncertain.clone(), t0());
        assert_eq!(flushed, vec![vec![uncertain]], "no buffer to drain — it goes out alone, now");
    }

    // --- deadband -------------------------------------------------------------------------------

    #[test]
    fn deadband_drops_a_subthreshold_change_and_anchors_on_the_last_accepted_value() {
        let mut s = Shaper::new();
        s.set_policies(table(&[(
            "a",
            PublishPolicy { batch_ms: 0, latest_only: false, deadband: Some(1.0) },
        )]));
        assert_eq!(s.offer(good("a", 10.0), t0()).len(), 1, "first always passes");
        assert!(s.offer(good("a", 10.5), t0()).is_empty(), "|10.5-10.0| < 1.0");
        assert!(s.offer(good("a", 10.9), t0()).is_empty(), "still against 10.0 — the last ACCEPTED");
        assert_eq!(s.offer(good("a", 11.0), t0()).len(), 1, "|11.0-10.0| >= 1.0 passes");
        assert!(s.offer(good("a", 11.5), t0()).is_empty(), "the anchor moved to 11.0");
        assert_eq!(
            s.take_counters().unwrap().deadband_dropped,
            3,
            "every suppressed entry is counted"
        );
    }

    #[test]
    fn a_quality_transition_is_never_deadband_suppressed() {
        let mut s = Shaper::new();
        s.set_policies(table(&[(
            "a",
            PublishPolicy { batch_ms: 0, latest_only: false, deadband: Some(100.0) },
        )]));
        s.offer(good("a", 10.0), t0());

        // GOOD -> UNCERTAIN with the same value: the quality is the news.
        let warned = Reading {
            quality: Quality::Uncertain,
            quality_raw: Some("MTC_CONDITION:WARNING:ALM-7".into()),
            ..good("a", 10.0)
        };
        assert_eq!(s.offer(warned, t0()).len(), 1, "GOOD->UNCERTAIN must pass");

        // A qualityRaw change alone (a different alarm) is a transition too.
        let other_alarm = Reading {
            quality: Quality::Uncertain,
            quality_raw: Some("MTC_CONDITION:WARNING:ALM-9".into()),
            ..good("a", 10.0)
        };
        assert_eq!(s.offer(other_alarm, t0()).len(), 1, "a qualityRaw change must pass");

        // Recovery is a transition as well.
        let recovered = good("a", 10.0);
        assert_eq!(s.offer(recovered, t0()).len(), 1, "UNCERTAIN->GOOD must pass");

        // And only now does the deadband suppress a same-quality subthreshold change.
        assert!(s.offer(good("a", 10.1), t0()).is_empty());
    }

    #[test]
    fn non_numeric_and_array_values_always_pass_the_deadband() {
        let mut s = Shaper::new();
        s.set_policies(table(&[(
            "a",
            PublishPolicy { batch_ms: 0, latest_only: false, deadband: Some(100.0) },
        )]));
        s.offer(good("a", 1.0), t0());
        let array = Reading::good("a", json!([1.0, 2.0]));
        assert_eq!(s.offer(array, t0()).len(), 1, "a TIME_SERIES array is never suppressed");
        let text = Reading::good("a", json!("ACTIVE"));
        assert_eq!(s.offer(text, t0()).len(), 1, "a string is never suppressed");
        // After non-numeric values the anchor is still the last accepted NUMERIC value.
        assert!(s.offer(good("a", 1.5), t0()).is_empty(), "anchored on 1.0, not the array");
    }

    #[test]
    fn the_first_reading_after_a_resync_or_resume_always_passes() {
        let mut s = Shaper::new();
        s.set_policies(table(&[(
            "a",
            PublishPolicy { batch_ms: 0, latest_only: false, deadband: Some(1.0) },
        )]));
        s.offer(good("a", 10.0), t0());
        assert!(s.offer(good("a", 10.1), t0()).is_empty(), "suppressed before the resync");

        s.reset_deadband();
        assert_eq!(s.offer(good("a", 10.1), t0()).len(), 1, "the first after a resync passes");
        assert!(s.offer(good("a", 10.2), t0()).is_empty(), "and the gate is armed again");
    }

    #[test]
    fn deadband_gates_entry_to_a_window_not_just_immediate_publishes() {
        let mut s = Shaper::new();
        s.set_policies(table(&[(
            "a",
            PublishPolicy { batch_ms: 100, latest_only: false, deadband: Some(1.0) },
        )]));
        let start = t0();
        s.offer(good("a", 10.0), start);
        s.offer(good("a", 10.2), ms(start, 10)); // dropped on entry
        s.offer(good("a", 12.0), ms(start, 20)); // buffered

        let flushed = s.due(ms(start, 100));
        let values: Vec<_> = flushed[0].iter().map(|r| r.value.clone().unwrap()).collect();
        assert_eq!(values, vec![json!(10.0), json!(12.0)], "the dropped reading never buffered");
    }

    // --- lifecycle ------------------------------------------------------------------------------

    #[test]
    fn flush_all_drains_every_window_so_shutdown_loses_nothing() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(60_000)), ("b", policy(60_000))]));
        s.offer(good("a", 1.0), t0());
        s.offer(good("b", 2.0), t0());

        let flushed = s.flush_all();
        assert_eq!(flushed.len(), 2, "every buffered signal flushes");
        assert_eq!(s.next_deadline(), None);
        assert!(s.flush_all().is_empty(), "idempotent");
    }

    #[test]
    fn pause_clears_the_buffers_without_publishing() {
        // The resume-time snapshot republishes the current truth; flushing pre-pause readings
        // after it would publish stale data out of order — so pause DISCARDS, deliberately.
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(60_000))]));
        s.offer(good("a", 1.0), t0());
        s.offer(good("a", 2.0), t0());

        assert_eq!(s.clear_buffers(), 2, "the discarded readings are counted for the log line");
        assert_eq!(s.next_deadline(), None);
        assert!(s.flush_all().is_empty(), "nothing left to flush");
        let counters = s.take_counters().unwrap();
        assert_eq!(counters.published, 0, "nothing reached the wire");
    }

    #[test]
    fn a_policy_swap_flushes_the_windows_of_changed_and_removed_signals_only() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(60_000)), ("b", policy(60_000)), ("c", policy(60_000))]));
        let start = t0();
        s.offer(good("a", 1.0), start);
        s.offer(good("b", 2.0), start);
        s.offer(good("c", 3.0), start);

        // a: window changed; b: policy removed; c: unchanged.
        let flushed = s.set_policies(table(&[("a", policy(100)), ("c", policy(60_000))]));
        let mut ids: Vec<_> = flushed.iter().map(|u| u[0].signal_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"], "changed + removed flush with their old buffers");
        assert_eq!(s.next_deadline(), Some(ms(start, 60_000)), "c's window keeps running");

        // b is unshaped now: its next reading publishes immediately.
        assert_eq!(s.offer(good("b", 9.0), ms(start, 10)).len(), 1);
    }

    #[test]
    fn an_identical_policy_table_flushes_nothing() {
        let mut s = Shaper::new();
        s.set_policies(table(&[("a", policy(60_000))]));
        s.offer(good("a", 1.0), t0());
        assert!(
            s.set_policies(table(&[("a", policy(60_000))])).is_empty(),
            "an unchanged policy keeps its window mid-flight"
        );
        assert!(s.next_deadline().is_some());
    }

    // --- counters + the static fallback ---------------------------------------------------------

    #[test]
    fn counters_drain_and_return_none_when_nothing_moved() {
        let mut s = Shaper::new();
        assert_eq!(s.take_counters(), None);
        s.offer(good("a", 1.0), t0());
        assert!(s.take_counters().is_some());
        assert_eq!(s.take_counters(), None, "drained");
    }

    #[test]
    fn the_static_fallback_compiles_only_non_trivial_policies() {
        let signals: Vec<SignalConfig> = serde_json::from_value(json!([
            { "id": "plain", "dataItemId": "d1" },
            { "id": "batched", "dataItemId": "d2", "publish": { "batchMs": 250 } },
            { "id": "banded", "dataItemId": "d3", "publish": { "deadband": 0.5 } },
            { "id": "latest", "dataItemId": "d4",
              "publish": { "mode": "interval", "batchMs": 100 } }
        ]))
        .unwrap();
        let policies = policies_from_signals(&signals);
        assert!(!policies.contains_key("plain"), "the default is not worth a table entry");
        assert_eq!(policies["batched"], policy(250));
        assert_eq!(
            policies["banded"],
            PublishPolicy { batch_ms: 0, latest_only: false, deadband: Some(0.5) }
        );
        assert_eq!(
            policies["latest"],
            PublishPolicy { batch_ms: 100, latest_only: true, deadband: None }
        );
    }
}
