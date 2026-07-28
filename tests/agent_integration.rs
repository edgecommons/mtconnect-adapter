//! # Live integration against the pinned MTConnect reference agent (LLD §12 row 6)
//!
//! Env-gated: set `EC_MTC_AGENT` to the main agent's base URL (and optionally
//! `EC_MTC_AGENT_TINY` for the tiny-buffer agent, default `http://localhost:5011`) after starting
//! the compose harness:
//!
//! ```text
//! docker compose -f tests/compose.mtconnect-agent.yaml up -d
//! EC_MTC_AGENT=http://localhost:5010 cargo test --test agent_integration
//! ```
//!
//! Without the variable every test self-skips, so the ordinary `cargo test` gate stays green on a
//! machine with no Docker. The SHDR feed is served by this process (the cppagent adapter
//! protocol) on fixed host ports 7401/7402/7403 — the containers dial back via
//! `host.docker.internal`. The restart and buffer-wrap tests drive the containers through the
//! `docker` CLI, so the suite needs it on PATH.
//!
//! The tests serialize on one lock: they share the SHDR ports and one of them restarts the agent.

use std::time::Duration;

use mtconnect_adapter::mtconnect::config::{parse_agents, AgentCredentials};
use mtconnect_adapter::mtconnect::{AgentRuntime, InstanceEvent, MtcClient, ObsValue};
use serde_json::json;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DEV_ONE: &str = "MTC-E2E-001";
const DEV_TWO: &str = "MTC-E2E-002";
const DEV_TINY: &str = "MTC-E2E-TINY";

fn main_agent_url() -> Option<String> {
    match std::env::var("EC_MTC_AGENT") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("EC_MTC_AGENT not set - skipping the live agent integration test");
            None
        }
    }
}

fn tiny_agent_url() -> String {
    std::env::var("EC_MTC_AGENT_TINY").unwrap_or_else(|_| "http://localhost:5011".to_string())
}

// =================================================================================================
// The in-process SHDR feed (the cppagent adapter protocol)
// =================================================================================================

/// One SHDR adapter port: the agent dials in, we feed `|key|value` lines and answer `* PING`.
/// Lines sent while no agent is connected are queued and flushed on (re)connect — which is what
/// makes the restart test work without racing the reconnect.
struct ShdrFeed {
    tx: tokio::sync::mpsc::UnboundedSender<String>,
    task: tokio::task::JoinHandle<()>,
}

impl ShdrFeed {
    async fn start(port: u16) -> Self {
        // The previous test's feed frees its socket asynchronously (task abort): retry briefly.
        let listener = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match TcpListener::bind(("0.0.0.0", port)).await {
                    Ok(l) => return l,
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("bind SHDR port {port}"));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                let mut buf = [0u8; 256];
                loop {
                    tokio::select! {
                        line = rx.recv() => match line {
                            None => return,
                            Some(l) => {
                                if sock.write_all(format!("{l}\n").as_bytes()).await.is_err() {
                                    break;
                                }
                                let _ = sock.flush().await;
                            }
                        },
                        read = sock.read(&mut buf) => match read {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if buf[..n].windows(6).any(|w| w == b"* PING")
                                    && sock.write_all(b"* PONG 60000\n").await.is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        Self { tx, task }
    }

    fn send(&self, line: &str) {
        let _ = self.tx.send(line.to_string());
    }
}

impl Drop for ShdrFeed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn docker(args: &[&str]) {
    let args: Vec<String> = args.iter().map(ToString::to_string).collect();
    let printable = args.join(" ");
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker").args(&args).status()
    })
    .await
    .unwrap()
    .unwrap_or_else(|e| panic!("docker {printable}: {e}"));
    assert!(status.success(), "docker {printable} failed: {status}");
}

fn runtime(url: &str, extra: serde_json::Value) -> Arc<AgentRuntime> {
    let mut entry = json!({
        "id": "live-agent",
        "url": url,
        "heartbeatMs": 2_000,
        "requestTimeoutMs": 5_000,
        "reconnect": { "initialMs": 200, "maxMs": 1_000 },
        "pollIntervalMs": 250
    });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        entry[k] = v;
    }
    let cfg = parse_agents(&json!({ "agents": [entry] })).unwrap().remove(0);
    AgentRuntime::new(cfg, &AgentCredentials::default()).unwrap()
}

/// Wait (bounded) for the next event matching the predicate, discarding others.
async fn wait_for(
    rx: &mut tokio::sync::mpsc::Receiver<InstanceEvent>,
    secs: u64,
    what: &str,
    mut pred: impl FnMut(&InstanceEvent) -> bool,
) -> InstanceEvent {
    tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            let event = rx.recv().await.expect("event channel open");
            if pred(&event) {
                return event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

fn scalar_eq(obs_value: &ObsValue, want: f64) -> bool {
    matches!(obs_value, ObsValue::Scalar(v) if v.as_f64() == Some(want))
}

// =================================================================================================
// Probe → snapshot → stream E2E
// =================================================================================================

#[tokio::test]
async fn observations_flow_probe_snapshot_then_stream() {
    let Some(url) = main_agent_url() else { return };
    let _serial = SERIAL.lock().await;
    let feed = ShdrFeed::start(7401).await;
    feed.send("|avail|AVAILABLE");
    feed.send("|Xabs|10.5");
    feed.send("|exec|ACTIVE");
    // Let the agent ingest before the snapshot, so the snapshot has real values.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let rt = runtime(&url, json!({}));
    let mut handle = rt.attach(DEV_ONE);
    rt.spawn().unwrap();

    // The /current snapshot carries the fed values.
    wait_for(&mut handle.rx, 20, "the snapshot with the fed value", |e| {
        matches!(e, InstanceEvent::Snapshot(obs)
            if obs.iter().any(|o| o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 10.5)))
    })
    .await;

    // Live streaming: new values arrive through the multipart stream.
    for v in ["11.5", "12.5"] {
        feed.send(&format!("|Xabs|{v}"));
    }
    wait_for(&mut handle.rx, 20, "streamed observations", |e| {
        matches!(e, InstanceEvent::Snapshot(obs)
            if obs.iter().any(|o| o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 12.5)))
    })
    .await;

    let info = rt.info();
    assert_eq!(info.mode, "stream", "live acquisition is streaming, not polling");
    assert!(info.connected);
    assert!(info.instance_id.is_some());
    assert!(
        info.agent_version.as_deref().unwrap_or_default().starts_with("2.7.0"),
        "the pinned agent: {info:?}"
    );
    println!(
        "EVIDENCE probe/stream: mode={} instance={:?} agentVersion={:?} standard={:?} buffer={:?}",
        info.mode, info.instance_id, info.agent_version, info.standard_version, info.buffer_size
    );

    rt.shutdown().await;
}

// =================================================================================================
// Agent restart → instanceId change → full resync (ladder 3)
// =================================================================================================

#[tokio::test]
async fn an_agent_restart_changes_the_instance_and_the_machine_resyncs() {
    let Some(url) = main_agent_url() else { return };
    let _serial = SERIAL.lock().await;
    let feed = ShdrFeed::start(7401).await;
    feed.send("|avail|AVAILABLE");
    feed.send("|Xabs|20.5");

    let rt = runtime(&url, json!({}));
    let mut handle = rt.attach(DEV_ONE);
    rt.spawn().unwrap();
    wait_for(&mut handle.rx, 20, "pre-restart data", |e| {
        matches!(e, InstanceEvent::Snapshot(obs)
            if obs.iter().any(|o| o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 20.5)))
    })
    .await;
    let old_instance = rt.info().instance_id.expect("an instance id");

    // Restart the real agent: a new incarnation, sequence numbering restarted.
    docker(&["restart", "mtc-e2e-agent"]).await;
    // The agent reconnects to the (queued) feed; give it a fresh value to prove the pipeline.
    feed.send("|avail|AVAILABLE");
    feed.send("|Xabs|21.5");

    wait_for(&mut handle.rx, 60, "post-restart data", |e| {
        matches!(e, InstanceEvent::Snapshot(obs)
            if obs.iter().any(|o| o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 21.5)))
    })
    .await;
    let new_instance = rt.info().instance_id.expect("an instance id");
    assert_ne!(old_instance, new_instance, "a restart is a new incarnation");
    println!("EVIDENCE restart: instanceId {old_instance} -> {new_instance}, data resumed");

    rt.shutdown().await;
}

// =================================================================================================
// Buffer wrap on the tiny agent (BufferSize = 2^7 = 128) → overrun recovery (ladder 2)
// =================================================================================================

#[tokio::test]
async fn a_buffer_wrap_is_recovered_without_wedging_the_machine() {
    let Some(_url) = main_agent_url() else { return };
    let _serial = SERIAL.lock().await;
    let url = tiny_agent_url();
    let feed = ShdrFeed::start(7403).await;
    feed.send("|tavail|AVAILABLE");
    feed.send("|Tpos|1.0");

    // First, the deterministic protocol-level evidence: wrap the 128-slot buffer, then ask for a
    // provably-expired position and record how cppagent 2.7 answers it.
    let cfg = parse_agents(&json!({ "agents": [{
        "id": "tiny-probe", "url": url, "requestTimeoutMs": 5_000
    }] }))
    .unwrap()
    .remove(0);
    let client = MtcClient::new(&cfg, &AgentCredentials::default()).unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if client.current(None).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("tiny agent reachable");
    for i in 0..200 {
        feed.send(&format!("|Tpos|{}", 2.0 + f64::from(i)));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let overrun = client.sample(Some(2), Some(10), None).await;
    let overrun_evidence = match &overrun {
        Ok(text) => match mtconnect_adapter::mtconnect::xml::parse_errors(text) {
            Ok(doc) => format!("HTTP 200 error document, OUT_OF_RANGE={:?}", doc.out_of_range()),
            Err(_) => match mtconnect_adapter::mtconnect::xml::parse_streams(text) {
                // cppagent >= 2.x clamps an expired `from` to its buffer floor instead of
                // answering OUT_OF_RANGE — worth recording either way.
                Ok(doc) => format!(
                    "HTTP 200 Streams document (agent CLAMPED the stale from): first={:?} next={:?}",
                    doc.header.first_sequence, doc.header.next_sequence
                ),
                Err(_) => "HTTP 200 unrecognized document".to_string(),
            },
        },
        Err(e) => format!("HTTP-level error: {e}"),
    };
    println!("EVIDENCE overrun surface (sample?from=2 after wrap): {overrun_evidence}");

    // Now the machine: stream live, freeze the agent past the heartbeat window (the stream dies
    // silently), wrap the buffer while the machine is trying to recover, and require that it ends
    // up streaming fresh data again — with the loss surfaced, not papered over.
    let rt = runtime(&url, json!({ "heartbeatMs": 1_000, "requestTimeoutMs": 15_000 }));
    let mut handle = rt.attach(DEV_TINY);
    rt.spawn().unwrap();
    wait_for(&mut handle.rx, 20, "initial tiny-agent data", |e| {
        matches!(e, InstanceEvent::Snapshot(_))
    })
    .await;

    docker(&["pause", "mtc-e2e-agent-tiny"]).await;
    tokio::time::sleep(Duration::from_millis(3_000)).await; // > 2 x heartbeat: ladder 1 fires
    docker(&["unpause", "mtc-e2e-agent-tiny"]).await;
    for i in 0..200 {
        feed.send(&format!("|Tpos|{}", 500.0 + f64::from(i)));
    }

    // The machine must recover to fresh data on its own; record which rung it took.
    let mut saw_data_loss = false;
    let mut saw_degraded = false;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match handle.rx.recv().await.expect("event channel open") {
                InstanceEvent::DataLoss { skipped } => {
                    saw_data_loss = true;
                    println!("EVIDENCE ladder 2: DataLoss skipped={skipped}");
                }
                InstanceEvent::StreamDegraded { failures } => {
                    saw_degraded = true;
                    println!("EVIDENCE degradation: after {failures} establish failures");
                }
                InstanceEvent::Snapshot(obs)
                    if obs.iter().any(|o| {
                        o.data_item_id == "t1-Tpos"
                            && matches!(&o.value, ObsValue::Scalar(v)
                                if v.as_f64().is_some_and(|f| f >= 690.0))
                    }) =>
                {
                    return;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("post-wrap data must flow again");
    println!(
        "EVIDENCE wrap recovery: data resumed; dataLoss={saw_data_loss} degraded={saw_degraded}"
    );
    assert!(
        saw_data_loss || saw_degraded,
        "an overrun this size must surface as DataLoss (ladder 2) or degradation, never silently"
    );

    // And the machine settles back into streaming.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if rt.info().mode == "stream" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the machine returns to streaming");

    rt.shutdown().await;
}

// =================================================================================================
// Multi-device demultiplexing on one shared agent runtime
// =================================================================================================

#[tokio::test]
async fn one_agent_runtime_demultiplexes_two_live_devices() {
    let Some(url) = main_agent_url() else { return };
    let _serial = SERIAL.lock().await;
    let one = ShdrFeed::start(7401).await;
    let two = ShdrFeed::start(7402).await;
    one.send("|avail|AVAILABLE");
    two.send("|avail2|AVAILABLE");

    let rt = runtime(&url, json!({}));
    let mut h1 = rt.attach(DEV_ONE);
    let mut h2 = rt.attach(DEV_TWO);
    rt.spawn().unwrap();

    one.send("|Xabs|77.7");
    two.send("|Ypos|88.8");

    let e1 = wait_for(&mut h1.rx, 20, "device one's value", |e| {
        matches!(e, InstanceEvent::Snapshot(obs)
            if obs.iter().any(|o| o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 77.7)))
    })
    .await;
    if let InstanceEvent::Snapshot(obs) = &e1 {
        assert!(
            obs.iter().all(|o| o.data_item_id.starts_with("d1-")),
            "no cross-device leakage into device one: {obs:?}"
        );
    }
    let e2 = wait_for(&mut h2.rx, 20, "device two's value", |e| {
        matches!(e, InstanceEvent::Snapshot(obs)
            if obs.iter().any(|o| o.data_item_id == "d2-Ypos" && scalar_eq(&o.value, 88.8)))
    })
    .await;
    if let InstanceEvent::Snapshot(obs) = &e2 {
        assert!(
            obs.iter().all(|o| o.data_item_id.starts_with("d2-")),
            "no cross-device leakage into device two: {obs:?}"
        );
    }
    println!("EVIDENCE multi-device: one stream, two devices, no leakage");

    rt.shutdown().await;
}

// =================================================================================================
// Probe-derived selection (R1.1), live: mode "all" on the tiny fixture device
// =================================================================================================

#[tokio::test]
async fn selection_mode_all_derives_the_tiny_devices_signals_live() {
    let Some(_url) = main_agent_url() else { return };
    let _serial = SERIAL.lock().await;
    let url = tiny_agent_url();
    let feed = ShdrFeed::start(7403).await;
    feed.send("|tavail|AVAILABLE");
    feed.send("|Tpos|3.25");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    use mtconnect_adapter::device::{ConnectionConfig, DeviceBackend, MtcBackend};
    use mtconnect_adapter::mtconnect::config::DeviceConfig;
    use std::collections::HashMap;

    let rt = runtime(&url, json!({}));
    let device = DeviceConfig {
        id: "tiny".into(),
        agent_id: "live-agent".into(),
        device_uuid: DEV_TINY.into(),
        signals: Vec::new(),
        selection: Some(serde_json::from_value(json!({ "mode": "all" })).unwrap()),
    };
    let backend = MtcBackend::new(
        HashMap::from([("live-agent".to_string(), Arc::clone(&rt))]),
        vec![device],
        mtconnect_adapter::app::ChannelBudgets::default(),
    );
    let conn: ConnectionConfig = serde_json::from_value(json!({
        "agentId": "live-agent", "deviceUuid": DEV_TINY
    }))
    .unwrap();

    // Connect (probing the real agent) — retried briefly while the container warms up.
    let mut session = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match backend.connect(&conn).await {
                Ok(session) => return session,
                Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    })
    .await
    .expect("tiny agent reachable");

    // Not one signal was configured, yet the whole device serves: the derived set came from the
    // live probe.
    let served = session.served_signals().expect("an MTConnect session reports served signals");
    assert!(served >= 2, "the tiny device has at least tavail + Tpos: {served}");

    // The agent reconnects to this test's fresh SHDR feed on its own cadence (an earlier test in
    // the serialized suite owned the same port), so wait for the fed value rather than assuming it.
    let readings = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            feed.send("|Tpos|3.25");
            let readings = session.snapshot_now().await.expect("scoped /current");
            if readings
                .iter()
                .any(|r| r.signal_id == "t1-tpos" && r.value == Some(json!(3.25)))
            {
                return readings;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the fed value arrives under its derived identity");
    let tpos = readings
        .iter()
        .find(|r| r.signal_id == "t1-tpos")
        .expect("the derived lower-kebab id of dataItemId `t1-Tpos`");
    assert_eq!(tpos.value, Some(json!(3.25)), "the live fed value, under a derived identity");
    assert!(tpos.channel.is_some(), "a derived channel from the live component path");
    // Every reading off a live agent carries the canonical component path (L13) — the untruncated
    // string the derived channel above was shaped from.
    for r in &readings {
        assert!(r.component_path.is_some(), "{} carries no componentPath", r.signal_id);
    }
    println!(
        "EVIDENCE selection: served={served} derived ids={:?} t1-tpos value={:?} channel={:?} \
         componentPath={:?}",
        readings.iter().map(|r| r.signal_id.as_str()).collect::<Vec<_>>(),
        tpos.value,
        tpos.channel,
        tpos.component_path
    );
    session.close().await;
}

// =================================================================================================
// Publish shaping, live: a batched signal coalesces real streamed readings (the tiny device)
// =================================================================================================

#[tokio::test]
async fn a_batched_signal_coalesces_live_streamed_readings_into_one_update() {
    let Some(_url) = main_agent_url() else { return };
    let _serial = SERIAL.lock().await;
    let url = tiny_agent_url();
    let feed = ShdrFeed::start(7403).await;
    feed.send("|tavail|AVAILABLE");
    feed.send("|Tpos|40.0");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    use mtconnect_adapter::device::{ConnectionConfig, DeviceBackend, MtcBackend};
    use mtconnect_adapter::mtconnect::config::DeviceConfig;
    use mtconnect_adapter::shaping::Shaper;
    use std::collections::HashMap;
    use std::time::Instant;

    let rt = runtime(&url, json!({}));
    rt.spawn().unwrap(); // streaming acquisition: read_signals drains what the task delivers
    let device = DeviceConfig {
        id: "tiny".into(),
        agent_id: "live-agent".into(),
        device_uuid: DEV_TINY.into(),
        signals: vec![serde_json::from_value(json!({
            "id": "t-pos", "dataItemId": "t1-Tpos", "publish": { "batchMs": 60000 }
        }))
        .unwrap()],
        selection: None,
    };
    let backend = MtcBackend::new(
        HashMap::from([("live-agent".to_string(), Arc::clone(&rt))]),
        vec![device],
        mtconnect_adapter::app::ChannelBudgets::default(),
    );
    let conn: ConnectionConfig = serde_json::from_value(json!({
        "agentId": "live-agent", "deviceUuid": DEV_TINY
    }))
    .unwrap();
    let mut session = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match backend.connect(&conn).await {
                Ok(session) => return session,
                Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    })
    .await
    .expect("tiny agent reachable");

    // The session compiled the policy table from the live probe: the SAMPLE keeps its window.
    let mut shaper = Shaper::new();
    let policies = session.shaping_policies();
    assert_eq!(policies["t-pos"].batch_ms, 60_000, "the live-compiled policy");
    let _ = shaper.set_policies(policies);

    // Feed three values and pump real streamed readings through the engine: every t-pos reading
    // must BUFFER (none released), and the flush must carry them in arrival order.
    let wanted = [41.25, 42.25, 43.25];
    let mut fed = 0usize;
    let mut buffered: Vec<f64> = Vec::new();
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if fed < wanted.len() {
                feed.send(&format!("|Tpos|{}", wanted[fed]));
                fed += 1;
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
            let readings = session.read_signals().await.expect("live drain");
            for reading in readings {
                if reading.signal_id != "t-pos" {
                    continue;
                }
                let value = reading.value.clone().and_then(|v| v.as_f64());
                let good = reading.quality == mtconnect_adapter::device::Quality::Good;
                let released = shaper.offer(reading, Instant::now());
                if good {
                    assert!(
                        released.is_empty(),
                        "a GOOD reading of a batched signal must buffer, not publish: {released:?}"
                    );
                }
                // A BAD reading (the pre-feed UNAVAILABLE) flushing immediately is the engine's
                // quality rule working as designed.
                if let Some(v) = value {
                    if wanted.contains(&v) && buffered.last() != Some(&v) {
                        buffered.push(v);
                    }
                }
            }
            if buffered == wanted {
                return;
            }
        }
    })
    .await
    .expect("the three fed values buffer through the live stream");

    let flushed = shaper.flush_all();
    assert_eq!(flushed.len(), 1, "ONE update for the whole window");
    let values: Vec<f64> = flushed[0]
        .iter()
        .filter_map(|r| r.value.clone().and_then(|v| v.as_f64()))
        .filter(|v| wanted.contains(v))
        .collect();
    assert_eq!(values, wanted, "arrival order, every reading its own sample");
    assert!(
        flushed[0].iter().all(|r| r.extra.as_ref().is_some_and(|e| e.contains_key("sequence"))),
        "each buffered sample keeps its own sequence extra"
    );
    println!(
        "EVIDENCE live shaping: buffered={:?} flushed_samples={} (one update)",
        buffered,
        flushed[0].len()
    );
    session.close().await;
}
