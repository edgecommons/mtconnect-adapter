//! # Publish shaping — the wire shape, and backend parity
//!
//! The engine itself is exhaustively unit-tested in `src/shaping.rs` (window math, deadband entry
//! rules, quality flushes, lifecycle) on a virtual clock. What this suite proves is the two seams
//! around it:
//!
//! * **The wire shape.** A flushed window becomes ONE `SouthboundSignalUpdate` whose `samples[]`
//!   carries every buffered reading in arrival order — each sample keeping its own `serverTs` and
//!   extras (`sequence`, `receivedTs`, …). The `samples[]` array exists for exactly this
//!   (docs/SOUTHBOUND.md §2 batching).
//! * **Backend parity (ADP-5).** The engine sits above the device session and never learns which
//!   backend produced a reading: the same policies shape the simulator's readings and an MTConnect
//!   session's readings identically.

use std::time::{Duration, Instant};

use edgecommons::prelude::SignalUpdate;
use mtconnect_adapter::app::{build_sample, stamp_received};
use mtconnect_adapter::device::{ConnectionConfig, DeviceBackend, Reading, SimBackend};
use mtconnect_adapter::mtconnect::config::SignalConfig;
use mtconnect_adapter::shaping::{Shaper, policies_from_signals};
use serde_json::json;

fn signals(raw: serde_json::Value) -> Vec<SignalConfig> {
    serde_json::from_value(raw).unwrap()
}

#[test]
fn a_flushed_window_becomes_one_update_whose_samples_ride_in_arrival_order() {
    let mut shaper = Shaper::new();
    let _ = shaper.set_policies(policies_from_signals(&signals(json!([
        { "id": "x-position", "dataItemId": "Xabs", "publish": { "batchMs": 250 } }
    ]))));

    // Three readings across one window, the way an MTConnect stream delivers them: the agent's
    // capture stamp on each, the sequence riding as an extra, the receive moment stamped by the
    // worker at read completion.
    let start = Instant::now();
    let mut batch = vec![
        Reading {
            capture_ts: Some("2026-07-27T10:00:04.250000Z".into()),
            ..Reading::good("x-position", json!(1.0)).with_extra("sequence", json!(37))
        },
        Reading {
            capture_ts: Some("2026-07-27T10:00:04.500000Z".into()),
            ..Reading::good("x-position", json!(2.0)).with_extra("sequence", json!(38))
        },
        Reading {
            capture_ts: Some("2026-07-27T10:00:04.750000Z".into()),
            ..Reading::good("x-position", json!(3.0)).with_extra("sequence", json!(39))
        },
    ];
    stamp_received(&mut batch, "2026-07-27T10:00:05.000000Z");

    for (i, reading) in batch.clone().into_iter().enumerate() {
        let released = shaper.offer(reading, start + Duration::from_millis(i as u64 * 80));
        assert!(released.is_empty(), "buffered, not published");
    }

    let flushed = shaper.due(start + Duration::from_millis(250));
    assert_eq!(flushed.len(), 1, "ONE update for the whole window");

    // The supervisor's publish path in miniature: every reading of the flush becomes one sample
    // of ONE SouthboundSignalUpdate, via the same unit-tested `build_sample` mapping.
    let update = SignalUpdate::builder()
        .signal_id(&flushed[0][0].signal_id)
        .samples(flushed[0].iter().map(build_sample))
        .build();

    assert_eq!(update.signal_id.as_deref(), Some("x-position"));
    assert_eq!(
        update.samples.len(),
        3,
        "samples[] carries every buffered reading"
    );
    for (i, sample) in update.samples.iter().enumerate() {
        // Arrival order, each sample keeping its OWN capture stamp and extras.
        assert_eq!(sample.value, Some(json!((i + 1) as f64)));
        let extra = sample
            .extra
            .as_ref()
            .expect("per-sample extras survive batching");
        assert_eq!(
            extra["sequence"],
            json!(37 + i as u64),
            "exact once-only ordering per sample"
        );
        assert_eq!(
            extra["receivedTs"],
            json!("2026-07-27T10:00:05.000000Z"),
            "the worker's receive moment rides each sample"
        );
    }
    assert_eq!(
        update.samples[0].server_ts.as_deref(),
        Some("2026-07-27T10:00:04.250000Z"),
        "the agent's capture stamp is each sample's serverTs"
    );
    assert_eq!(
        update.samples[2].server_ts.as_deref(),
        Some("2026-07-27T10:00:04.750000Z")
    );
}

#[tokio::test]
async fn the_simulators_readings_are_shaped_by_the_very_same_engine() {
    // ADP-5: shaping lives ABOVE the protocol session. The simulator's session knows nothing about
    // publish policies — the same static-config compilation and the same engine shape its flow.
    let conn: ConnectionConfig =
        serde_json::from_value(json!({ "endpoint": "sim://plc-1" })).unwrap();
    let mut session = SimBackend.connect(&conn).await.unwrap();

    let mut shaper = Shaper::new();
    let _ = shaper.set_policies(policies_from_signals(&signals(json!([
        { "id": "temperature-1", "dataItemId": "sim", "publish": { "batchMs": 60000 } }
    ]))));

    let start = Instant::now();
    let mut published = Vec::new();
    for i in 0..3u64 {
        let readings = session.read_signals().await.unwrap();
        for reading in readings {
            published.extend(shaper.offer(reading, start + Duration::from_millis(i * 10)));
        }
    }

    // pressure-1 carries no policy: its BAD readings published immediately, one per read —
    // exactly today's unshaped behavior.
    assert_eq!(published.len(), 3);
    assert!(published.iter().all(|u| u[0].signal_id == "pressure-1"));

    // temperature-1 sat in its window; the flush is one update with the three readings in order.
    let flushed = shaper.flush_all();
    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].len(), 3, "every buffered reading, arrival order");
    assert!(flushed[0].iter().all(|r| r.signal_id == "temperature-1"));
}

#[test]
fn deadband_shapes_a_static_config_exactly_as_a_compiled_one() {
    // The deadband rules are the engine's, not the backend's: entry gating, quality-transition
    // passthrough and the first-after-reset rule behave identically however the table was built.
    let mut shaper = Shaper::new();
    let _ = shaper.set_policies(policies_from_signals(&signals(json!([
        { "id": "temperature-1", "dataItemId": "sim", "publish": { "deadband": 0.5 } }
    ]))));

    let now = Instant::now();
    assert_eq!(
        shaper
            .offer(Reading::good("temperature-1", json!(20.0)), now)
            .len(),
        1
    );
    assert!(
        shaper
            .offer(Reading::good("temperature-1", json!(20.2)), now)
            .is_empty()
    );
    let bad = Reading::bad("temperature-1", "SENSOR_FAULT");
    assert_eq!(
        shaper.offer(bad, now).len(),
        1,
        "a quality transition is never suppressed"
    );
    assert_eq!(
        shaper
            .offer(Reading::good("temperature-1", json!(20.2)), now)
            .len(),
        1,
        "recovery is a transition too"
    );
}
