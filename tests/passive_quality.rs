//! # Passive quality, on the wire (HLD §6 rows 2-3, P1-5)
//!
//! The ladder itself is unit-tested on virtual instants in `src/staleness.rs`, and the link facts it
//! reads are proven against the real runtime in `src/device.rs`. What this suite proves is the part
//! neither can: that a synthetic transition **survives the library's real protobuf codec** as the
//! shape the fleet is promised — the held value with a new verdict, the `MTC_STALE:<ageMs>` /
//! `MTC_AGENT_UNREACHABLE` token in `qualityRaw`, the `passive` marker, and the held `sequence`
//! still naming the observation the sample describes.
//!
//! None of this needs a core change: `quality` is a wire token from the library's own closed set,
//! `qualityRaw` is free-form, and `passive`/`sequence` ride the sample's additive `extra` map — the
//! same `map<string, EcValue>` `componentPath` already travels in at the update level.

use std::time::{Duration, Instant};

use edgecommons::messaging::{Message, MessageBuilder};
use edgecommons::prelude::Sample;
use mtconnect_adapter::app::{
    build_sample, stamp_component_path, stamp_received, COMPONENT_PATH_KEY,
};
use mtconnect_adapter::device::{Quality, Reading};
use mtconnect_adapter::staleness::{
    PassiveLink, QualityWatchdog, PASSIVE_EXTRA_KEY, QUALITY_AGENT_UNREACHABLE,
};
use serde_json::{json, Value};

/// The wire name/version the `data()` facade publishes a signal update under.
const MESSAGE_NAME: &str = "SouthboundSignalUpdate";
const MESSAGE_VERSION: &str = "1.0";

/// One missed poll of a 5 s poller, and the shipped 30 s expiry.
const WINDOW: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(30);

/// The reading acquisition published — the agent's capture stamp, the sequence riding as an extra,
/// the canonical component path.
fn delivered() -> Reading {
    Reading {
        capture_ts: Some("2026-07-27T10:00:04.250000Z".into()),
        received_ts: Some("2026-07-27T10:00:05.000000Z".into()),
        component_path: Some("Axes/Linear[X]".into()),
        ..Reading::good("x-position", json!(123.456)).with_extra("sequence", json!(37))
    }
}

fn link(unreachable: bool, age: Duration) -> PassiveLink {
    PassiveLink {
        unreachable,
        liveness_age: Some(age),
        liveness_window: WINDOW,
    }
}

/// The watchdog's output for one transition, stamped by the worker exactly as the device task does
/// before publishing it.
fn transition(link: PassiveLink) -> Reading {
    let now = Instant::now();
    let mut watchdog = QualityWatchdog::default();
    watchdog.on_published(&delivered(), now);
    let mut emitted = watchdog.evaluate(link, STALE_AFTER, now);
    assert_eq!(emitted.len(), 1, "one held signal, one transition");
    stamp_received(&mut emitted, "2026-07-27T10:07:00.000000Z");
    emitted.remove(0)
}

/// One sample rendered the way `DataFacade::build_body` renders it: the canonical members, then the
/// additive extras.
fn sample_json(sample: &Sample) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "value".to_string(),
        sample.value.clone().unwrap_or(Value::Null),
    );
    obj.insert(
        "quality".to_string(),
        json!(sample.quality.expect("this adapter always sets one").wire()),
    );
    if let Some(raw) = &sample.quality_raw {
        obj.insert("qualityRaw".to_string(), json!(raw));
    }
    if let Some(ts) = &sample.server_ts {
        obj.insert("serverTs".to_string(), json!(ts));
    }
    if let Some(extra) = &sample.extra {
        for (key, value) in extra {
            obj.insert(key.clone(), value.clone());
        }
    }
    Value::Object(obj)
}

/// Publish one reading through the adapter's own two-step (facade body ▸ `stamp_component_path`)
/// and read it back through the library's envelope, exactly as a subscriber would.
fn published(reading: &Reading) -> Value {
    let mut body = json!({
        "device": { "adapter": "mtconnect", "instance": "cnc-1", "endpoint": "http://agent:5000" },
        "signal": { "id": reading.signal_id },
        "samples": [sample_json(&build_sample(reading))],
    });
    stamp_component_path(&mut body, reading.component_path.as_deref());

    let msg = MessageBuilder::new(MESSAGE_NAME, MESSAGE_VERSION)
        .southbound_signal_update(body)
        .build();
    let bytes = msg.to_vec().expect("the envelope serializes");
    Message::from_slice(&bytes)
        .expect("the envelope deserializes")
        .body
}

#[test]
fn a_stale_transition_publishes_the_held_value_under_an_uncertain_verdict() {
    let stale = transition(link(false, Duration::from_millis(12_000)));
    assert_eq!(stale.quality, Quality::Uncertain);

    let body = published(&stale);
    let sample = &body["samples"][0];

    // The value is the one the agent delivered — this update says "I no longer vouch for it".
    assert_eq!(sample["value"], json!(123.456));
    assert_eq!(sample["quality"], json!("UNCERTAIN"));
    assert_eq!(
        sample["qualityRaw"],
        json!("MTC_STALE:12000"),
        "the liveness age in whole milliseconds, the operator's own diagnosis"
    );
    assert_eq!(
        sample[PASSIVE_EXTRA_KEY],
        json!("stale"),
        "the marker that tells synthetic quality motion from delivered data"
    );
    assert_eq!(
        sample["sequence"],
        json!(37),
        "the held sequence still names the observation the sample describes (D-MTC-6)"
    );

    // The value's own truth-time is history and stays as it was; the emission moment rides beside
    // it as the adapter's receive stamp.
    assert_eq!(sample["serverTs"], json!("2026-07-27T10:00:04.250000Z"));
    assert_eq!(sample["receivedTs"], json!("2026-07-27T10:07:00.000000Z"));
    // And the update-level contract is untouched by any of it.
    assert_eq!(body[COMPONENT_PATH_KEY], json!("Axes/Linear[X]"));
    assert_eq!(body["signal"]["id"], json!("x-position"));
}

#[test]
fn expiry_and_an_unreachable_agent_both_publish_bad_with_their_own_reason() {
    let expired = published(&transition(link(false, Duration::from_millis(45_500))));
    let sample = &expired["samples"][0];
    assert_eq!(sample["value"], json!(123.456), "still the held value");
    assert_eq!(sample["quality"], json!("BAD"));
    assert_eq!(sample["qualityRaw"], json!("MTC_STALE:45500"));
    assert_eq!(sample[PASSIVE_EXTRA_KEY], json!("expired"));

    let lost = published(&transition(link(true, Duration::from_millis(200))));
    let sample = &lost["samples"][0];
    assert_eq!(sample["value"], json!(123.456));
    assert_eq!(sample["quality"], json!("BAD"));
    assert_eq!(
        sample["qualityRaw"],
        json!(QUALITY_AGENT_UNREACHABLE),
        "the link is named, not a clock: the agent stopped answering at all"
    );
    assert_eq!(sample[PASSIVE_EXTRA_KEY], json!("unreachable"));
    assert_eq!(sample["sequence"], json!(37));
}

#[test]
fn recovery_republishes_the_delivered_verdict_verbatim() {
    // Ladder-1 re-establishment does not snapshot, so the watchdog is the only thing that can undo
    // what it did — and it must put back exactly what the agent last said, marked as its own doing.
    let now = Instant::now();
    let mut watchdog = QualityWatchdog::default();
    watchdog.on_published(
        &Reading {
            quality_raw: Some("MTC_OK".into()),
            ..delivered()
        },
        now,
    );
    assert_eq!(
        watchdog
            .evaluate(link(false, Duration::from_secs(12)), STALE_AFTER, now)
            .len(),
        1
    );
    let mut recovered = watchdog.evaluate(link(false, Duration::from_millis(50)), STALE_AFTER, now);
    stamp_received(&mut recovered, "2026-07-27T10:07:30.000000Z");

    let sample = published(&recovered[0])["samples"][0].clone();
    assert_eq!(sample["quality"], json!("GOOD"));
    assert_eq!(sample["qualityRaw"], json!("MTC_OK"));
    assert_eq!(sample[PASSIVE_EXTRA_KEY], json!("recovered"));
    assert_eq!(sample["value"], json!(123.456));
    assert_eq!(sample["sequence"], json!(37));
}
