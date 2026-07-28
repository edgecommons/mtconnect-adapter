//! # The canonical `componentPath`, on the wire (D-MtconnectAdapter-L13)
//!
//! Every published `SouthboundSignalUpdate` carries the signal's full MTConnect component path as
//! an **update-level** extra under the key `componentPath`. What this suite proves is the part the
//! unit tests cannot: that the key survives the library's real protobuf codec at that level, and
//! that it does so once per update however the shaping engine batched the readings.
//!
//! The library's `SouthboundSignalUpdate` message reserves exactly two update-level members —
//! `signal` and `samples` — and round-trips every other body key through the protobuf `extra` map
//! (`map<string, EcValue> extra = 100`). `componentPath` is such a key, so the assertions below
//! encode/decode with `Message::to_vec`/`Message::from_slice` rather than trusting the JSON.

use std::time::{Duration, Instant};

use edgecommons::messaging::{Message, MessageBuilder};
use mtconnect_adapter::app::{build_sample, stamp_component_path, COMPONENT_PATH_KEY};
use mtconnect_adapter::device::Reading;
use mtconnect_adapter::mtconnect::config::SignalConfig;
use mtconnect_adapter::shaping::{policies_from_signals, Shaper};
use serde_json::{json, Value};

/// The wire name/version the `data()` facade publishes a signal update under.
const MESSAGE_NAME: &str = "SouthboundSignalUpdate";
const MESSAGE_VERSION: &str = "1.0";

/// A body in the shape `DataFacade::build_body` produces, with this adapter's stamp applied — the
/// exact two-step the supervisor performs (facade body ▸ `stamp_component_path`).
fn stamped_body(signal_id: &str, samples: Vec<Value>, component_path: Option<&str>) -> Value {
    let mut body = json!({
        "device": { "adapter": "mtconnect", "instance": "cnc-1", "endpoint": "http://agent:5000" },
        "signal": { "id": signal_id },
        "samples": samples,
    });
    stamp_component_path(&mut body, component_path);
    body
}

/// Serialize through the library's protobuf envelope and read it back, exactly as a subscriber
/// receiving the publish would.
fn round_trip(body: Value) -> Value {
    let msg = MessageBuilder::new(MESSAGE_NAME, MESSAGE_VERSION)
        .southbound_signal_update(body)
        .build();
    let bytes = msg.to_vec().expect("the envelope serializes");
    let decoded = Message::from_slice(&bytes).expect("the envelope deserializes");
    decoded.body
}

fn samples_of(body: &Value) -> &Vec<Value> {
    body["samples"].as_array().expect("samples[]")
}

#[test]
fn the_component_path_survives_the_protobuf_codec_at_the_update_level() {
    let decoded = round_trip(stamped_body(
        "x-position",
        vec![json!({ "value": 123.456, "quality": "GOOD", "serverTs": "2026-07-27T10:00:00Z" })],
        Some("Axes/Linear[X]"),
    ));

    assert_eq!(
        decoded[COMPONENT_PATH_KEY],
        json!("Axes/Linear[X]"),
        "the update-level extra round-trips through `map<string, EcValue> extra = 100`"
    );
    // It landed beside the canonical members, not inside one of them, and cost nothing.
    assert_eq!(decoded["signal"]["id"], json!("x-position"));
    assert_eq!(samples_of(&decoded).len(), 1);
    assert_eq!(samples_of(&decoded)[0]["value"], json!(123.456));
    assert!(samples_of(&decoded)[0].get(COMPONENT_PATH_KEY).is_none());
    assert!(decoded["signal"].get(COMPONENT_PATH_KEY).is_none());
    // The other update-level extra the facade already relies on is undisturbed.
    assert_eq!(decoded["device"]["adapter"], json!("mtconnect"));
}

#[test]
fn a_deep_path_rides_untruncated_however_short_the_channel_had_to_be() {
    // The demo Mazak's `stock`: four component tokens where an instance-scoped topic has room for
    // three, so L12 shortens the *channel*. The body still carries the whole path.
    let deep = "Resources[resources]/Materials[materials]/Stock[stock]";
    let decoded = round_trip(stamped_body(
        "stock",
        vec![json!({ "value": "42", "quality": "GOOD" })],
        Some(deep),
    ));
    assert_eq!(decoded[COMPONENT_PATH_KEY], json!(deep));
}

#[test]
fn an_empty_path_and_a_null_path_both_survive_as_themselves() {
    // A device-level data item is `""`; a signal no model describes is `null` — both are what
    // `sb/signals` serves, and the codec must not collapse either into the other or drop the key.
    let empty = round_trip(stamped_body(
        "availability",
        vec![json!({ "value": "AVAILABLE", "quality": "GOOD" })],
        Some(""),
    ));
    assert_eq!(empty[COMPONENT_PATH_KEY], json!(""));
    assert!(empty.as_object().expect("body").contains_key(COMPONENT_PATH_KEY));

    let null = round_trip(stamped_body(
        "ghost",
        vec![json!({ "value": null, "quality": "BAD", "qualityRaw": "MTC_NO_SUCH_DATAITEM" })],
        None,
    ));
    assert_eq!(
        null[COMPONENT_PATH_KEY],
        json!(null),
        "EcValue carries a null, so the key is present even here"
    );
    assert!(
        null.as_object().expect("body").contains_key(COMPONENT_PATH_KEY),
        "presence is unconditional: a consumer never branches on a missing key"
    );
}

#[test]
fn a_batched_window_carries_exactly_one_component_path_for_all_its_samples() {
    // The shaping engine coalesces one signal's readings into ONE update; the path is
    // per-signal-static, so it is stamped once on the update and never repeated per sample.
    let signals: Vec<SignalConfig> = serde_json::from_value(json!([
        { "id": "x-position", "dataItemId": "Xabs", "publish": { "batchMs": 250 } }
    ]))
    .unwrap();
    let mut shaper = Shaper::new();
    let _ = shaper.set_policies(policies_from_signals(&signals));

    let start = Instant::now();
    for (i, value) in [1.0_f64, 2.0, 3.0].into_iter().enumerate() {
        let reading = Reading {
            component_path: Some("Axes/Linear[X]".into()),
            capture_ts: Some(format!("2026-07-27T10:00:0{i}Z")),
            ..Reading::good("x-position", json!(value)).with_extra("sequence", json!(37 + i as u64))
        };
        assert!(
            shaper.offer(reading, start + Duration::from_millis(i as u64 * 80)).is_empty(),
            "buffered, not published"
        );
    }

    let flushed = shaper.due(start + Duration::from_millis(250));
    assert_eq!(flushed.len(), 1, "ONE update for the whole window");
    let readings = &flushed[0];
    assert_eq!(readings.len(), 3);

    // Each reading maps onto one sample through the same `build_sample` the supervisor uses —
    // proving the batch reaches the body intact — and the update-level stamp comes from the FIRST
    // reading's path (they are one signal's readings, so they all agree).
    for (i, r) in readings.iter().enumerate() {
        let sample = build_sample(r);
        assert_eq!(sample.value, Some(json!((i + 1) as f64)));
        assert!(
            sample.extra.as_ref().expect("extras").get(COMPONENT_PATH_KEY).is_none(),
            "`build_sample` never puts the path on a sample"
        );
    }
    let decoded = round_trip(stamped_body(
        &readings[0].signal_id,
        readings
            .iter()
            .enumerate()
            .map(|(i, r)| {
                json!({
                    "value": r.value,
                    "quality": "GOOD",
                    "serverTs": r.capture_ts,
                    "sequence": 37 + i as u64,
                })
            })
            .collect(),
        readings[0].component_path.as_deref(),
    ));

    assert_eq!(decoded[COMPONENT_PATH_KEY], json!("Axes/Linear[X]"));
    assert_eq!(samples_of(&decoded).len(), 3, "every buffered reading, arrival order");
    for (i, sample) in samples_of(&decoded).iter().enumerate() {
        assert_eq!(sample["sequence"], json!(37 + i as u64), "per-sample extras still ride");
        assert!(
            sample.get(COMPONENT_PATH_KEY).is_none(),
            "one path per update, not one per sample"
        );
    }
    assert!(readings.iter().all(|r| r.component_path.as_deref() == Some("Axes/Linear[X]")));
}

#[test]
fn the_immediate_and_flush_paths_stamp_the_same_path_as_a_batched_one() {
    // Three release paths reach the wire: an unshaped immediate publish, a due-window flush, and
    // `flush_all` (the reload/shutdown drain). None of them may lose the path.
    let signals: Vec<SignalConfig> = serde_json::from_value(json!([
        { "id": "x-position", "dataItemId": "Xabs", "publish": { "batchMs": 250 } }
    ]))
    .unwrap();
    let mut shaper = Shaper::new();
    let _ = shaper.set_policies(policies_from_signals(&signals));

    let path = Some("Axes/Linear[X]");
    let reading = |id: &str, value: f64| Reading {
        component_path: path.map(str::to_string),
        ..Reading::good(id, json!(value))
    };

    // Unshaped: no policy for `x-load`, so the reading is released immediately.
    let immediate = shaper.offer(reading("x-load", 1.0), Instant::now());
    assert_eq!(immediate.len(), 1);
    assert_eq!(immediate[0][0].component_path.as_deref(), path);

    // The window drain: `flush_all` releases what is buffered without a due deadline.
    let start = Instant::now();
    assert!(shaper.offer(reading("x-position", 2.0), start).is_empty());
    let drained = shaper.flush_all();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0][0].component_path.as_deref(), path);

    for update in [&immediate[0], &drained[0]] {
        let decoded = round_trip(stamped_body(
            &update[0].signal_id,
            vec![json!({ "value": update[0].value, "quality": "GOOD" })],
            update[0].component_path.as_deref(),
        ));
        assert_eq!(decoded[COMPONENT_PATH_KEY], json!("Axes/Linear[X]"));
    }
}
