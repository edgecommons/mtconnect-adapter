//! # Live simulator/device integration test — self-skipping
//!
//! Gated on `EC_LIVE_SIM=<endpoint>`, matching the `ethernet-ip-adapter`/`file-replicator` live-test
//! idiom: a fast, explicit env-var check up front, an `eprintln!` explaining how to opt in, and an
//! early return when it is unset — so this suite is **skipped** (not failed) in a normal `cargo
//! test` and in the scaffold-build CI gate, and only runs against a real endpoint when a developer
//! (or a lab CI leg) explicitly asks for it.
//!
//! `ethernet-ip-adapter` points this at a real PLC simulator (cpppo/OpENer); `modbus-adapter` has a
//! permanent Modbus sim container on the lab host. This scaffold ships only the in-process
//! [`SimBackend`](mtconnect_adapter::device::SimBackend), which needs no real endpoint at all — so today
//! this suite mostly proves the *harness* is wired correctly. Once you replace `SimBackend` with a
//! real protocol backend (see `docs/how-to-guides.md`), point `EC_LIVE_SIM` at your real
//! simulator/device and this becomes the live E2E gate for it — connect the same way your
//! `DeviceBackend::connect` does, using the endpoint below instead of a hardcoded one.

use mtconnect_adapter::device::{ConnectionConfig, DeviceBackend, Quality, SimBackend};

#[tokio::test]
async fn connects_polls_once_and_asserts_readings_and_quality() {
    let Ok(endpoint) = std::env::var("EC_LIVE_SIM") else {
        eprintln!("skipped: set EC_LIVE_SIM=<endpoint> to run against a real simulator/device");
        return;
    };

    // --- connect --------------------------------------------------------------------------
    let backend = SimBackend;
    let cfg = ConnectionConfig { endpoint, extra: serde_json::Map::new() };
    let mut session = backend
        .connect(&cfg)
        .await
        .expect("connect to the live endpoint");

    // --- one poll cycle ---------------------------------------------------------------------
    let readings = session
        .read_signals()
        .await
        .expect("one read cycle against the live endpoint");
    assert!(!readings.is_empty(), "a live poll must return at least one reading");

    // --- assert readings + quality ------------------------------------------------------------
    // Every reading carries an explicit quality — GOOD or BAD, never omitted — so a consumer can
    // always tell a real value from a failed one (see docs/explanation.md's "Quality is structural"
    // section). This scaffold's sim always reports `temperature-1` GOOD and `pressure-1` BAD; a
    // real backend's mix will differ, but every entry must still carry a quality either way.
    for r in &readings {
        assert!(
            matches!(r.quality, Quality::Good | Quality::Bad | Quality::Uncertain),
            "signal `{}` must carry a normalized quality",
            r.signal_id
        );
        if r.quality == Quality::Good {
            assert!(!r.value.is_null(), "a GOOD reading must carry a value");
        }
    }

    session.close().await;
}
