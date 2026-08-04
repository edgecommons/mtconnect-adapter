//! # Passive quality — the link facts a held reading is judged against (HLD §6)
//!
//! MTConnect is an **on-change** protocol: an unchanged value is not a silent one, so a signal's own
//! age says nothing about whether its value is still true. What does say so is the **liveness
//! clock** — the moment the agent last vouched for currency, either by delivering a Streams document
//! (data or heartbeat) or by answering a `/current` cycle. This module owns that vocabulary, apart
//! from the device seam, so it is decided on an explicit clock and unit-tested without a runtime.
//!
//! [`PassiveLink`] is what a session reports through
//! [`DeviceSession::passive_input`](crate::device::DeviceSession::passive_input): the connectivity
//! verdict of the one authority that owns it, plus the liveness age and the window that age is
//! measured against. A backend whose read IS its liveness reports `None` and is never judged
//! passively at all.
//!
//! [`QualityWatchdog`] is what turns those facts into wire motion. It remembers the last reading of
//! every signal that reached the wire and, when the link crosses a threshold, republishes that
//! **held value** with a degraded quality — `UNCERTAIN`/`MTC_STALE:<ageMs>` past one missed
//! heartbeat, `BAD` past `staleSignalSecs`, `BAD`/`MTC_AGENT_UNREACHABLE` when the connectivity
//! authority says the agent is gone — and restores the held verdict verbatim when liveness comes
//! back. Without it a consumer would keep a `GOOD` value forever against a silent agent, which is
//! precisely the failure HLD §6 rows 2–3 exist to prevent.
//!
//! Two properties are deliberate:
//!
//! * **Transitions only.** A steady state emits nothing — quality motion is news, not a heartbeat.
//! * **The clock is the link's, not the signal's** (D-R12). Every signal of an instance crosses at
//!   the same moment, carrying the same `ageMs`, because the fact being reported is about the agent.
//!   The per-signal instants in [`crate::metrics`] keep driving the `staleSignals` *metric*, whose
//!   meaning — genuine value silence — is a different question and is left alone (D-R13).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::device::{Quality, Reading};

/// `qualityRaw` prefix for a value held beyond the liveness window: `MTC_STALE:<ageMs>`.
pub const QUALITY_STALE_PREFIX: &str = "MTC_STALE";
/// `qualityRaw` for readings synthetically degraded because the agent is unreachable.
pub const QUALITY_AGENT_UNREACHABLE: &str = "MTC_AGENT_UNREACHABLE";
/// The update-shape marker extra on every synthetic reading (D-R14): `stale` | `expired` |
/// `unreachable` | `recovered`. A consumer reads it to tell synthetic quality motion from a sample
/// the agent actually delivered.
pub const PASSIVE_EXTRA_KEY: &str = "passive";

/// The link facts a passive-quality evaluation reads.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PassiveLink {
    /// The connectivity authority says the agent is not delivering.
    pub unreachable: bool,
    /// Time since the agent last vouched for data currency. `None` before first contact.
    pub liveness_age: Option<Duration>,
    /// "One missed heartbeat/poll" for this acquisition mode — the threshold `liveness_age` is
    /// judged against.
    pub liveness_window: Duration,
}

/// The link's verdict on everything an instance is holding.
///
/// The ladder is per **instance**, not per signal: the trigger is the link, and the per-signal
/// records only carry what is held (D-R12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PassivePhase {
    /// The agent vouched for currency inside the liveness window — held values are current.
    #[default]
    Fresh,
    /// One missed heartbeat/poll: held past the window, but not yet past `staleSignalSecs`.
    Stale,
    /// Held past `staleSignalSecs` — the configured limit of how long a value may stand in.
    Expired,
    /// The connectivity authority says the agent is not delivering at all.
    Unreachable,
}

impl PassivePhase {
    /// The [`PASSIVE_EXTRA_KEY`] value a transition INTO this phase publishes (D-R14).
    #[must_use]
    pub fn marker(self) -> &'static str {
        match self {
            Self::Fresh => "recovered",
            Self::Stale => "stale",
            Self::Expired => "expired",
            Self::Unreachable => "unreachable",
        }
    }

    /// Whether readings degraded in this phase are the watchdog's own doing (and therefore owed a
    /// restoration when liveness returns).
    #[must_use]
    fn is_degraded(self) -> bool {
        self != Self::Fresh
    }

    /// Classify the link.
    ///
    /// Unreachability outranks every clock — an agent that is not delivering cannot be vouching for
    /// anything. Expiry is checked before staleness so a `staleSignalSecs` shorter than the liveness
    /// window lands straight on [`PassivePhase::Expired`] rather than pausing at `Stale`. The
    /// thresholds are strict (`>`): an age exactly equal to the window has not missed anything yet.
    fn of(link: PassiveLink, stale_after: Duration) -> Self {
        if link.unreachable {
            return Self::Unreachable;
        }
        match link.liveness_age {
            // Nothing has been vouched for yet and the authority still calls the agent reachable:
            // there is no age to judge, and nothing has been published to hold either.
            None => Self::Fresh,
            Some(age) if age > stale_after => Self::Expired,
            Some(age) if age > link.liveness_window => Self::Stale,
            Some(_) => Self::Fresh,
        }
    }
}

/// What one signal is holding on the wire.
#[derive(Debug)]
struct SignalRecord {
    /// The last reading of this signal that reached the wire — the value, timestamps, routing and
    /// extras (including its `sequence`) a synthetic transition republishes.
    held: Reading,
    /// When it reached the wire. The passive clock is the link's, so this is a diagnostic — how
    /// long the fleet has been looking at this value — not a threshold.
    published_at: Instant,
    /// This signal currently carries a synthetic quality, so recovery owes it a restoration.
    degraded: bool,
}

/// The per-instance passive-quality state machine (HLD §6 rows 2–3).
///
/// Fed every reading that reaches the wire ([`Self::on_published`]) and evaluated against the link
/// on every poll tick ([`Self::evaluate`]), it publishes quality transitions for values it is
/// holding. It is pure: the only clock it sees is the [`Instant`] and the [`PassiveLink`] handed to
/// it, so the whole ladder is exercised on virtual instants in the tests below.
#[derive(Debug, Default)]
pub struct QualityWatchdog {
    /// Keyed by `signal_id` — several signals may share one data item, and each is held (and
    /// degraded) in its own right.
    signals: HashMap<String, SignalRecord>,
    phase: PassivePhase,
}

impl QualityWatchdog {
    /// The phase the last [`Self::evaluate`] left the instance in.
    #[must_use]
    pub fn phase(&self) -> PassivePhase {
        self.phase
    }

    /// Record a reading that reached the wire, and clear any synthetic degradation on that signal —
    /// the agent has spoken for it again, so the watchdog has nothing left to say.
    ///
    /// Called from the publish path beside `DeviceMetrics::on_signal_update`. The watchdog's own
    /// output is ignored here: re-recording a synthetic reading would make a held value look freshly
    /// delivered and would overwrite the very verdict recovery has to restore.
    pub fn on_published(&mut self, reading: &Reading, now: Instant) {
        if is_synthetic(reading) {
            return;
        }
        self.signals.insert(
            reading.signal_id.clone(),
            SignalRecord {
                held: reading.clone(),
                published_at: now,
                degraded: false,
            },
        );
    }

    /// Evaluate the link and return the synthetic readings to publish NOW.
    ///
    /// Only **transitions** emit: a steady state — however long it lasts — returns an empty vector,
    /// so a quiet agent costs one comparison per tick and nothing on the wire. `stale_after` is
    /// `healthThresholds.staleSignalSecs`.
    ///
    /// The emitted readings hold their value, timestamps, routing and extras (D-R14) and differ from
    /// what was published only in `quality`/`quality_raw` plus the [`PASSIVE_EXTRA_KEY`] marker.
    /// They are returned in `signal_id` order so one transition reaches the wire the same way twice.
    pub fn evaluate(
        &mut self,
        link: PassiveLink,
        stale_after: Duration,
        now: Instant,
    ) -> Vec<Reading> {
        let next = PassivePhase::of(link, stale_after);
        if next == self.phase {
            return Vec::new();
        }
        let previous = std::mem::replace(&mut self.phase, next);
        // The age every signal of this instance reports — the link's, not each signal's (D-R12).
        let age_ms =
            u64::try_from(link.liveness_age.unwrap_or_default().as_millis()).unwrap_or(u64::MAX);

        let mut out = Vec::new();
        for record in self.signals.values_mut() {
            let Some((quality, quality_raw)) = transition_quality(next, record, age_ms) else {
                continue;
            };
            record.degraded = next.is_degraded();
            out.push(synthetic(&record.held, quality, quality_raw, next.marker()));
        }
        out.sort_by(|a, b| a.signal_id.cmp(&b.signal_id));

        let oldest_hold_ms = self.oldest_hold_ms(now);
        tracing::debug!(
            from = ?previous, to = ?next, signals = out.len(), oldest_hold_ms,
            "passive quality transition"
        );
        out
    }

    /// A re-baseline is coming (a resync or resume snapshot): forget what is held so the snapshot's
    /// own publishes rebuild it, and start the ladder over — the snapshot is the current truth, and
    /// nothing about the previous incarnation's values is worth restoring. Emits nothing: the real
    /// data is already on its way.
    pub fn on_rebaseline(&mut self) {
        self.signals.clear();
        self.phase = PassivePhase::Fresh;
    }

    /// How long the longest-held reading has been standing in, in milliseconds — the diagnostic
    /// behind a transition. Zero when nothing is held.
    fn oldest_hold_ms(&self, now: Instant) -> u64 {
        self.signals
            .values()
            .map(|r| now.saturating_duration_since(r.published_at))
            .max()
            .map_or(0, |held| {
                u64::try_from(held.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

/// Whether a reading is the watchdog's own output rather than something the agent delivered.
///
/// The publish path uses it to keep synthetic readings out of both trackers (D-R13): a quality
/// transition is not a value update, so it must neither reset the `staleSignals` metric's age nor be
/// recorded as a fresh hold.
#[must_use]
pub fn is_synthetic(reading: &Reading) -> bool {
    reading
        .extra
        .as_ref()
        .is_some_and(|extra| extra.contains_key(PASSIVE_EXTRA_KEY))
}

/// The quality and `qualityRaw` a transition into `phase` publishes for one held signal, or `None`
/// when that signal does not transition at all.
fn transition_quality(
    phase: PassivePhase,
    record: &SignalRecord,
    age_ms: u64,
) -> Option<(Quality, Option<String>)> {
    let held = &record.held;
    match phase {
        // Recovery restores only what the watchdog itself degraded, and restores the held verdict
        // verbatim: a value that was UNCERTAIN because of a condition Warning comes back UNCERTAIN,
        // not GOOD. Ladder-1 re-establishment does not snapshot, so without this a held signal would
        // stay degraded until its value next changed.
        PassivePhase::Fresh => record
            .degraded
            .then(|| (held.quality, held.quality_raw.clone())),
        // One missed heartbeat/poll: a delivered value is no longer vouched for, so GOOD becomes
        // UNCERTAIN with the age that made it doubtful. An already-UNCERTAIN reading keeps ITS
        // reason — never overwrite a condition Warning with a stale marker — and a BAD one has
        // nowhere left to fall.
        PassivePhase::Stale => match held.quality {
            Quality::Good => Some((Quality::Uncertain, Some(stale_raw(age_ms)))),
            Quality::Uncertain => Some((Quality::Uncertain, held.quality_raw.clone())),
            Quality::Bad => None,
        },
        // Past `staleSignalSecs` the value is no longer allowed to stand in at all: BAD, naming the
        // age (HLD §6 row 3, "staleness expiry").
        PassivePhase::Expired => {
            (held.quality != Quality::Bad).then(|| (Quality::Bad, Some(stale_raw(age_ms))))
        }
        // The authority lost the agent: BAD, naming the link rather than a clock.
        PassivePhase::Unreachable => (held.quality != Quality::Bad)
            .then(|| (Quality::Bad, Some(QUALITY_AGENT_UNREACHABLE.to_string()))),
    }
}

/// `MTC_STALE:<ageMs>` — the liveness age in whole milliseconds, unpadded.
fn stale_raw(age_ms: u64) -> String {
    format!("{QUALITY_STALE_PREFIX}:{age_ms}")
}

/// The synthetic reading one held signal publishes on a transition (D-R14): the held value with the
/// held stamps, routing and extras — its `sequence` included, so the sample still names the
/// observation it describes — carrying a new verdict and the `passive` marker. `received_ts` is
/// cleared for the worker to stamp: the value's capture moment is history, the emission is now.
fn synthetic(
    held: &Reading,
    quality: Quality,
    quality_raw: Option<String>,
    marker: &str,
) -> Reading {
    let mut reading = held.clone();
    reading.quality = quality;
    reading.quality_raw = quality_raw;
    reading.received_ts = None;
    reading
        .extra
        .get_or_insert_with(serde_json::Map::new)
        .insert(
            PASSIVE_EXTRA_KEY.to_string(),
            serde_json::Value::String(marker.to_string()),
        );
    reading
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// One missed poll of a 5 s poller, and the shipped 30 s expiry.
    const WINDOW: Duration = Duration::from_secs(10);
    const STALE_AFTER: Duration = Duration::from_secs(30);

    fn link(unreachable: bool, age_ms: u64) -> PassiveLink {
        PassiveLink {
            unreachable,
            liveness_age: Some(Duration::from_millis(age_ms)),
            liveness_window: WINDOW,
        }
    }

    /// A delivered reading in the shape acquisition publishes: agent capture stamp, the sequence
    /// riding as an extra, the worker's receive moment already stamped.
    fn delivered(id: &str, value: f64) -> Reading {
        Reading {
            capture_ts: Some("2026-07-27T10:00:04.250000Z".into()),
            received_ts: Some("2026-07-27T10:00:05.000000Z".into()),
            component_path: Some("Axes/Linear[X]".into()),
            ..Reading::good(id, json!(value)).with_extra("sequence", json!(37))
        }
    }

    /// The watchdog holding one GOOD reading, at `t0`.
    fn holding_one(t0: Instant) -> QualityWatchdog {
        let mut watchdog = QualityWatchdog::default();
        watchdog.on_published(&delivered("x-position", 123.456), t0);
        watchdog
    }

    fn only(readings: &[Reading]) -> &Reading {
        assert_eq!(readings.len(), 1, "one held signal, one transition");
        &readings[0]
    }

    fn marker_of(reading: &Reading) -> &str {
        reading.extra.as_ref().expect("extras")[PASSIVE_EXTRA_KEY]
            .as_str()
            .expect("the passive marker is a string")
    }

    #[test]
    fn the_link_facts_are_a_plain_value_a_test_can_pin() {
        let fresh = PassiveLink {
            unreachable: false,
            liveness_age: Some(Duration::from_millis(250)),
            liveness_window: Duration::from_secs(10),
        };
        assert_eq!(fresh, fresh);
        assert!(
            fresh
                .liveness_age
                .is_some_and(|age| age < fresh.liveness_window)
        );

        // Before first contact there is no age to judge — and that is not the same as "old".
        let cold = PassiveLink {
            liveness_age: None,
            ..fresh
        };
        assert_ne!(cold, fresh);
        assert!(
            !cold.unreachable,
            "never having answered is not the same as having gone away"
        );
    }

    #[test]
    fn a_value_still_inside_the_liveness_window_is_current_however_old_it_is() {
        // The on-change point (D-R12): the agent vouched 10 s ago and the window is 10 s, so a value
        // delivered hours earlier is still the truth. Nothing moves.
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);

        assert!(
            watchdog
                .evaluate(link(false, 0), STALE_AFTER, t0)
                .is_empty()
        );
        assert!(
            watchdog
                .evaluate(link(false, 10_000), STALE_AFTER, t0 + WINDOW)
                .is_empty(),
            "an age exactly equal to the window has not missed anything yet"
        );
        assert_eq!(watchdog.phase(), PassivePhase::Fresh);
    }

    #[test]
    fn one_missed_heartbeat_holds_the_value_and_downgrades_it_to_uncertain() {
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);

        let emitted = watchdog.evaluate(
            link(false, 10_001),
            STALE_AFTER,
            t0 + Duration::from_millis(10_001),
        );
        let stale = only(&emitted);
        assert_eq!(watchdog.phase(), PassivePhase::Stale);

        // The value is HELD — this says "I no longer vouch for it", not "it changed".
        assert_eq!(stale.signal_id, "x-position");
        assert_eq!(stale.value, Some(json!(123.456)));
        assert_eq!(stale.quality, Quality::Uncertain);
        assert_eq!(stale.quality_raw.as_deref(), Some("MTC_STALE:10001"));
        assert_eq!(marker_of(stale), "stale");
        // Everything that identifies the observation rides along (D-R14, D-MTC-6).
        assert_eq!(stale.extra.as_ref().unwrap()["sequence"], json!(37));
        assert_eq!(
            stale.capture_ts.as_deref(),
            Some("2026-07-27T10:00:04.250000Z"),
            "the value's own truth-time is history and stays as it was"
        );
        assert_eq!(
            stale.received_ts, None,
            "the emission moment belongs to the emission — the worker stamps it"
        );
        assert_eq!(stale.component_path.as_deref(), Some("Axes/Linear[X]"));

        // A transition is news; the state that follows it is not.
        assert!(
            watchdog
                .evaluate(
                    link(false, 25_000),
                    STALE_AFTER,
                    t0 + Duration::from_secs(25)
                )
                .is_empty(),
            "steady state republishes nothing"
        );
    }

    #[test]
    fn the_stale_marker_names_the_liveness_age_in_whole_milliseconds() {
        let t0 = Instant::now();
        let mut watchdog = QualityWatchdog::default();
        watchdog.on_published(&delivered("x-position", 1.0), t0);
        let emitted = watchdog.evaluate(
            PassiveLink {
                unreachable: false,
                liveness_age: Some(Duration::from_millis(1_500)),
                liveness_window: Duration::from_millis(10),
            },
            STALE_AFTER,
            t0,
        );
        assert_eq!(
            only(&emitted).quality_raw.as_deref(),
            Some("MTC_STALE:1500"),
            "plain milliseconds: no padding, no unit suffix, no float"
        );
    }

    #[test]
    fn past_the_expiry_the_held_value_is_no_longer_allowed_to_stand_in() {
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);
        watchdog.evaluate(link(false, 10_001), STALE_AFTER, t0);

        let emitted = watchdog.evaluate(link(false, 30_001), STALE_AFTER, t0);
        let expired = only(&emitted);
        assert_eq!(watchdog.phase(), PassivePhase::Expired);
        assert_eq!(expired.value, Some(json!(123.456)), "still held");
        assert_eq!(expired.quality, Quality::Bad);
        assert_eq!(expired.quality_raw.as_deref(), Some("MTC_STALE:30001"));
        assert_eq!(marker_of(expired), "expired");
    }

    #[test]
    fn an_expiry_shorter_than_the_liveness_window_goes_straight_to_bad() {
        // A three-second `staleSignalSecs` on a ten-second window: there is no room for an UNCERTAIN
        // step, and the ladder must not invent one.
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);

        let emitted = watchdog.evaluate(link(false, 4_000), Duration::from_secs(3), t0);
        assert_eq!(watchdog.phase(), PassivePhase::Expired);
        assert_eq!(only(&emitted).quality, Quality::Bad);
        assert_eq!(
            only(&emitted).quality_raw.as_deref(),
            Some("MTC_STALE:4000")
        );
    }

    #[test]
    fn an_unreachable_agent_degrades_the_held_value_from_any_phase() {
        let t0 = Instant::now();
        for (name, warm_up) in [
            ("fresh", None),
            ("stale", Some(10_001)),
            ("expired", Some(30_001)),
        ] {
            let mut watchdog = holding_one(t0);
            if let Some(age) = warm_up {
                assert!(
                    !watchdog
                        .evaluate(link(false, age), STALE_AFTER, t0)
                        .is_empty(),
                    "{name}: the warm-up transition itself emits"
                );
            }

            let emitted = watchdog.evaluate(
                PassiveLink {
                    unreachable: true,
                    ..link(false, 30_001)
                },
                STALE_AFTER,
                t0,
            );
            let lost = only(&emitted);
            assert_eq!(watchdog.phase(), PassivePhase::Unreachable, "{name}");
            assert_eq!(lost.quality, Quality::Bad, "{name}");
            assert_eq!(
                lost.quality_raw.as_deref(),
                Some(QUALITY_AGENT_UNREACHABLE),
                "{name}: the link is named, not a clock"
            );
            assert_eq!(marker_of(lost), "unreachable", "{name}");
            assert_eq!(lost.value, Some(json!(123.456)), "{name}: still held");
        }
    }

    #[test]
    fn an_unreachable_agent_outranks_a_fresh_clock() {
        // The authority wins: a runtime that just marked down cannot be vouching for currency,
        // whatever its last-liveness stamp says.
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);
        let emitted = watchdog.evaluate(link(true, 0), STALE_AFTER, t0);
        assert_eq!(only(&emitted).quality, Quality::Bad);
        assert_eq!(watchdog.phase(), PassivePhase::Unreachable);
    }

    #[test]
    fn recovery_restores_the_held_verdict_verbatim() {
        let t0 = Instant::now();
        let mut watchdog = QualityWatchdog::default();
        // Two signals: one plain GOOD, one UNCERTAIN because a bound condition is in Warning.
        watchdog.on_published(&delivered("x-position", 123.456), t0);
        watchdog.on_published(
            &Reading {
                quality: Quality::Uncertain,
                quality_raw: Some("MTC_CONDITION:WARNING:ALM-9".into()),
                ..delivered("spindle-load", 42.0)
            },
            t0,
        );

        let stale = watchdog.evaluate(link(false, 10_001), STALE_AFTER, t0);
        assert_eq!(stale.len(), 2, "both held signals cross together");
        // Sorted by signal id, so the wire order is stable.
        assert_eq!(stale[0].signal_id, "spindle-load");
        assert_eq!(
            stale[0].quality_raw.as_deref(),
            Some("MTC_CONDITION:WARNING:ALM-9"),
            "a condition Warning keeps ITS reason — the stale marker never overwrites it"
        );
        assert_eq!(stale[0].quality, Quality::Uncertain);
        assert_eq!(marker_of(&stale[0]), "stale");
        assert_eq!(stale[1].quality_raw.as_deref(), Some("MTC_STALE:10001"));

        // Liveness comes back with no new data (a re-established stream does not snapshot): the
        // watchdog is the only thing that can un-degrade what it degraded.
        let recovered = watchdog.evaluate(link(false, 250), STALE_AFTER, t0);
        assert_eq!(watchdog.phase(), PassivePhase::Fresh);
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].signal_id, "spindle-load");
        assert_eq!(recovered[0].quality, Quality::Uncertain);
        assert_eq!(
            recovered[0].quality_raw.as_deref(),
            Some("MTC_CONDITION:WARNING:ALM-9"),
            "restored verbatim, not promoted to GOOD"
        );
        assert_eq!(recovered[1].signal_id, "x-position");
        assert_eq!(recovered[1].quality, Quality::Good);
        assert_eq!(
            recovered[1].quality_raw, None,
            "the held reading carried no raw, so neither does its restoration"
        );
        assert_eq!(marker_of(&recovered[1]), "recovered");

        // ...and having restored them, it owes them nothing on the next round trip.
        watchdog.evaluate(link(false, 10_001), STALE_AFTER, t0);
        let again = watchdog.evaluate(link(false, 0), STALE_AFTER, t0);
        assert_eq!(again.len(), 2, "degraded again, restored again");
    }

    #[test]
    fn a_signal_the_agent_republished_is_not_restored_a_second_time() {
        // A real publish clears the degradation: the agent has spoken for the signal, so recovery
        // has nothing to say about it.
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);
        assert_eq!(
            watchdog
                .evaluate(link(false, 10_001), STALE_AFTER, t0)
                .len(),
            1
        );

        watchdog.on_published(&delivered("x-position", 200.0), t0);
        assert!(
            watchdog
                .evaluate(link(false, 0), STALE_AFTER, t0)
                .is_empty(),
            "the fresh value already told the fleet everything"
        );
        assert_eq!(watchdog.phase(), PassivePhase::Fresh);
    }

    #[test]
    fn a_value_the_agent_already_called_bad_never_transitions_passively() {
        // An UNAVAILABLE data item is BAD with the agent's own reason. Staleness cannot make it
        // worse, and recovery must not invent a restoration for it.
        let t0 = Instant::now();
        let mut watchdog = QualityWatchdog::default();
        watchdog.on_published(&Reading::bad("x-load", "UNAVAILABLE"), t0);

        for age in [10_001, 30_001] {
            assert!(
                watchdog
                    .evaluate(link(false, age), STALE_AFTER, t0)
                    .is_empty(),
                "nowhere left to fall"
            );
        }
        assert!(watchdog.evaluate(link(true, 0), STALE_AFTER, t0).is_empty());
        assert!(
            watchdog
                .evaluate(link(false, 0), STALE_AFTER, t0)
                .is_empty()
        );
        assert_eq!(
            watchdog.phase(),
            PassivePhase::Fresh,
            "the ladder still ran"
        );
    }

    #[test]
    fn a_rebaseline_forgets_what_was_held_and_starts_the_ladder_over() {
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);
        assert_eq!(
            watchdog
                .evaluate(link(false, 10_001), STALE_AFTER, t0)
                .len(),
            1
        );

        // The resync/resume snapshot is about to republish the current truth.
        watchdog.on_rebaseline();
        assert_eq!(watchdog.phase(), PassivePhase::Fresh);
        assert!(
            watchdog
                .evaluate(link(false, 0), STALE_AFTER, t0)
                .is_empty(),
            "the snapshot's own publishes are the recovery — no synthetic one is owed"
        );

        // The snapshot lands; the rebuilt records degrade on their own merits from here.
        watchdog.on_published(&delivered("x-position", 200.0), t0);
        let emitted = watchdog.evaluate(link(false, 10_001), STALE_AFTER, t0);
        assert_eq!(
            only(&emitted).value,
            Some(json!(200.0)),
            "the new held value"
        );
    }

    #[test]
    fn an_instance_holding_nothing_still_walks_the_ladder() {
        // Before the first publish there is nothing to degrade — but the phase must still track the
        // link, or the first transition after data arrives would be missed.
        let t0 = Instant::now();
        let mut watchdog = QualityWatchdog::default();
        assert!(
            watchdog
                .evaluate(link(false, 10_001), STALE_AFTER, t0)
                .is_empty()
        );
        assert_eq!(watchdog.phase(), PassivePhase::Stale);

        watchdog.on_published(&delivered("x-position", 1.0), t0);
        let emitted = watchdog.evaluate(link(false, 30_001), STALE_AFTER, t0);
        assert_eq!(only(&emitted).quality, Quality::Bad);
    }

    #[test]
    fn a_link_that_never_answered_is_judged_by_the_authority_not_the_clock() {
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);
        // `liveness_age: None` with a reachable agent is not an age at all — it cannot expire.
        let cold = PassiveLink {
            unreachable: false,
            liveness_age: None,
            liveness_window: WINDOW,
        };
        assert!(watchdog.evaluate(cold, STALE_AFTER, t0).is_empty());
        assert_eq!(watchdog.phase(), PassivePhase::Fresh);
    }

    #[test]
    fn the_watchdogs_own_output_is_never_mistaken_for_delivered_data() {
        let t0 = Instant::now();
        let mut watchdog = holding_one(t0);
        let emitted = watchdog.evaluate(link(false, 10_001), STALE_AFTER, t0);
        let stale = only(&emitted).clone();
        assert!(is_synthetic(&stale));
        assert!(!is_synthetic(&delivered("x-position", 1.0)));

        // Feeding it back (the publish path guards against this too) must not re-baseline the hold:
        // the held GOOD reading is what recovery restores.
        watchdog.on_published(&stale, t0);
        let recovered = watchdog.evaluate(link(false, 0), STALE_AFTER, t0);
        assert_eq!(only(&recovered).quality, Quality::Good);
        assert_eq!(
            only(&recovered).quality_raw,
            None,
            "restored to the delivered verdict, not to the synthetic one"
        );
    }

    #[test]
    fn every_phase_names_itself_on_the_wire() {
        assert_eq!(PassivePhase::default(), PassivePhase::Fresh);
        assert_eq!(PassivePhase::Fresh.marker(), "recovered");
        assert_eq!(PassivePhase::Stale.marker(), "stale");
        assert_eq!(PassivePhase::Expired.marker(), "expired");
        assert_eq!(PassivePhase::Unreachable.marker(), "unreachable");
        assert!(!PassivePhase::Fresh.is_degraded());
        assert!(PassivePhase::Stale.is_degraded());
    }
}
