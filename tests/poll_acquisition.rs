//! # Poll-mode acquisition, end to end against a fake agent
//!
//! A real HTTP server serving canned `/probe` and `/current` documents, the real
//! [`AgentRuntime`](mtconnect_adapter::mtconnect::AgentRuntime), the real
//! [`MtcBackend`](mtconnect_adapter::device::MtcBackend), and the real publish mapping — so what
//! this proves is the whole path an operator cares about: an XML document on a socket becomes
//! published `Reading`s with the right values, qualities, timestamps and extras.
//!
//! Everything is deterministic: the documents are fixtures, and the agent answers exactly what the
//! test tells it to.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mtconnect_adapter::app::build_sample;
use mtconnect_adapter::device::{
    ConnectionConfig, DeviceBackend, MtcBackend, Quality, Reading,
};
use mtconnect_adapter::mtconnect::config::{parse_agents, AgentCredentials, DeviceConfig, SignalConfig};
use mtconnect_adapter::mtconnect::AgentRuntime;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const PROBE: &str = include_str!("fixtures/devices_2.7.xml");
const CURRENT: &str = include_str!("fixtures/current_2.7.xml");
/// A device whose component paths run deeper than a UNS topic can carry (the demo Mazak shape).
const PROBE_DEEP: &str = include_str!("fixtures/devices_deep_2.7.xml");
const CURRENT_DEEP: &str = include_str!("fixtures/current_deep_2.7.xml");

/// A fake MTConnect agent: it answers `/probe` and `/current` with whatever documents the test has
/// installed, and counts the requests it served.
struct FakeAgent {
    addr: SocketAddr,
    documents: Arc<Mutex<HashMap<&'static str, String>>>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl FakeAgent {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let documents = Arc::new(Mutex::new(HashMap::from([
            ("probe", PROBE.to_string()),
            ("current", CURRENT.to_string()),
        ])));
        let requests = Arc::new(Mutex::new(Vec::new()));

        let docs = Arc::clone(&documents);
        let seen = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                let docs = Arc::clone(&docs);
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match sock.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => head.push(byte[0]),
                        }
                    }
                    let head = String::from_utf8_lossy(&head).into_owned();
                    let path = head.split_whitespace().nth(1).unwrap_or("/").to_string();
                    seen.lock().unwrap().push(path.clone());

                    let endpoint = path.trim_start_matches('/').split('?').next().unwrap_or("");
                    let body = docs.lock().unwrap().get(endpoint).cloned();
                    let response = match body {
                        Some(body) => format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        ),
                        None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
                    };
                    let _ = sock.write_all(response.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });

        Self { addr, documents, requests }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn set(&self, endpoint: &'static str, document: String) {
        self.documents.lock().unwrap().insert(endpoint, document);
    }

    fn request_count(&self, endpoint: &str) -> usize {
        self.requests.lock().unwrap().iter().filter(|p| p.starts_with(endpoint)).count()
    }
}

fn signal(id: &str, data_item_id: &str) -> SignalConfig {
    serde_json::from_value(json!({ "id": id, "dataItemId": data_item_id })).unwrap()
}

fn device(signals: Vec<SignalConfig>) -> DeviceConfig {
    DeviceConfig {
        id: "cnc-1".into(),
        agent_id: "line-a-agent".into(),
        device_uuid: "OKUMA.123456".into(),
        signals,
        selection: None,
    }
}

/// A device whose signal set is (partly) derived from the probe by a `selection` block (R1.1).
fn selecting(signals: Vec<SignalConfig>, selection: serde_json::Value) -> DeviceConfig {
    DeviceConfig {
        selection: Some(serde_json::from_value(selection).unwrap()),
        ..device(signals)
    }
}

fn connection() -> ConnectionConfig {
    serde_json::from_value(json!({
        "agentId": "line-a-agent",
        "deviceUuid": "OKUMA.123456"
    }))
    .unwrap()
}

/// A runtime pointed at the fake agent, with the configured devices bound to it.
fn wire(agent: &FakeAgent, devices: Vec<DeviceConfig>) -> (Arc<AgentRuntime>, MtcBackend) {
    wire_with(agent, devices, mtconnect_adapter::app::ChannelBudgets::default())
}

/// [`wire`] with explicit per-instance UNS channel budgets — what shapes a deep path's channel.
fn wire_with(
    agent: &FakeAgent,
    devices: Vec<DeviceConfig>,
    budgets: mtconnect_adapter::app::ChannelBudgets,
) -> (Arc<AgentRuntime>, MtcBackend) {
    let cfg = parse_agents(&json!({ "agents": [{
        "id": "line-a-agent",
        "url": agent.url(),
        "streaming": "poll-only",
        "pollIntervalMs": 50,
        "requestTimeoutMs": 2000
    }] }))
    .unwrap()
    .remove(0);
    let runtime = AgentRuntime::new(cfg, &AgentCredentials::default()).unwrap();
    let backend = MtcBackend::new(
        HashMap::from([("line-a-agent".to_string(), Arc::clone(&runtime))]),
        devices,
        budgets,
    );
    (runtime, backend)
}

fn reading<'a>(readings: &'a [Reading], id: &str) -> &'a Reading {
    readings.iter().find(|r| r.signal_id == id).unwrap_or_else(|| panic!("no reading `{id}`"))
}

#[tokio::test]
async fn a_configured_device_probes_polls_and_publishes_its_signals() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(
        &agent,
        vec![device(vec![
            signal("x-position", "Xabs"),
            signal("x-load", "Xload"),
            signal("spindle-speed", "Sspeed"),
            signal("execution", "execution"),
            signal("x-travel-condition", "Xtravel"),
            signal("path-position", "Ppos"),
            signal("tool-offsets", "tool-offsets"),
        ])],
    );

    // Connecting verifies the device is really in the agent's probe.
    let mut session = backend.connect(&connection()).await.expect("connect");
    assert_eq!(agent.request_count("/probe"), 1, "the model is fetched once and cached");

    // One acquisition cycle, then the instance drains what arrived.
    runtime.poll_once().await.expect("poll");
    let readings = session.read_signals().await.expect("read");
    assert_eq!(readings.len(), 7, "every configured signal that the agent reported");

    // --- a Sample: value, quality, the agent's capture stamp, and the sequence extra ---
    let x = reading(&readings, "x-position");
    assert_eq!(x.value, Some(json!(123.456)));
    assert_eq!(x.quality, Quality::Good);
    assert_eq!(x.quality_raw.as_deref(), Some("MTC_OK"));
    assert_eq!(x.capture_ts.as_deref(), Some("2026-07-27T10:00:04.250000Z"));
    assert!(x.source_ts.is_none(), "MTConnect has no device-authored time");
    assert_eq!(x.extra.as_ref().unwrap()["sequence"], json!(37));

    // ... and the published sample maps capture -> serverTs, with the extras riding along.
    let sample = build_sample(x);
    assert_eq!(sample.server_ts.as_deref(), Some("2026-07-27T10:00:04.250000Z"));
    assert_eq!(sample.value, Some(json!(123.456)));
    let extra = sample.extra.expect("extras");
    assert_eq!(extra["sequence"], json!(37), "exact once-only ordering, on every sample");

    // --- UNAVAILABLE: an explicit null with BAD quality, never a zero ---
    let load = reading(&readings, "x-load");
    assert_eq!(load.value, None);
    assert_eq!(load.quality, Quality::Bad);
    assert_eq!(load.quality_raw.as_deref(), Some("UNAVAILABLE"));
    let sample = build_sample(load);
    assert!(sample.explicit_null);
    assert_eq!(sample.quality, Some(edgecommons::facades::Quality::Bad));

    // --- an Event stays verbatim; a vector sample becomes an array; a data set an object ---
    assert_eq!(reading(&readings, "execution").value, Some(json!("ACTIVE")));
    assert_eq!(reading(&readings, "path-position").value, Some(json!([10.5, 20.25, 30])));
    assert_eq!(
        reading(&readings, "tool-offsets").value,
        Some(json!({ "T1": 12.5, "T2": 7.25 }))
    );

    // --- a Condition publishes its state, with the alarm code in quality and extras ---
    let cond = reading(&readings, "x-travel-condition");
    assert_eq!(cond.value, Some(json!("FAULT")));
    assert_eq!(cond.quality, Quality::Bad);
    assert_eq!(cond.quality_raw.as_deref(), Some("MTC_CONDITION:FAULT:ALM-1041"));

    // --- per-sample extras the agent sent ---
    let spindle = reading(&readings, "spindle-speed");
    let extra = spindle.extra.as_ref().unwrap();
    assert_eq!(extra["resetTriggered"], json!("MANUAL"));
    assert_eq!(extra["duration"], json!(1.5));

    session.close().await;
}

#[tokio::test]
async fn a_bound_condition_degrades_the_value_it_guards() {
    let agent = FakeAgent::start().await;
    let mut guarded = signal("x-position", "Xabs");
    guarded.condition_binding = Some(vec!["Xtravel".into()]);
    let (runtime, backend) = wire(&agent, vec![device(vec![guarded])]);

    let mut session = backend.connect(&connection()).await.expect("connect");
    runtime.poll_once().await.expect("poll");
    let readings = session.read_signals().await.expect("read");

    // The fixture's X axis is in Fault, so the position it guards is BAD — with the alarm named.
    let x = reading(&readings, "x-position");
    assert_eq!(x.value, Some(json!(123.456)), "the value is still published");
    assert_eq!(x.quality, Quality::Bad);
    assert_eq!(x.quality_raw.as_deref(), Some("MTC_CONDITION:FAULT:ALM-1041"));
}

#[tokio::test]
async fn only_changed_observations_are_published_and_a_new_one_is() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    runtime.poll_once().await.unwrap();
    assert_eq!(session.read_signals().await.unwrap().len(), 1);

    // The agent still reports the same observation: nothing new to say.
    runtime.poll_once().await.unwrap();
    assert!(session.read_signals().await.unwrap().is_empty());

    // The machine moved: a new sequence, a new value, and it is published.
    agent.set(
        "current",
        CURRENT
            .replace(r#"sequence="37" timestamp="2026-07-27T10:00:04.250000Z">123.456"#,
                     r#"sequence="45" timestamp="2026-07-27T10:00:09.750000Z">200.5"#),
    );
    runtime.poll_once().await.unwrap();
    let readings = session.read_signals().await.unwrap();
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].value, Some(json!(200.5)));
    assert_eq!(readings[0].capture_ts.as_deref(), Some("2026-07-27T10:00:09.750000Z"));
    assert_eq!(readings[0].extra.as_ref().unwrap()["sequence"], json!(45));
}

#[tokio::test]
async fn the_acquisition_task_polls_on_its_own_cadence() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    // The shared task drives acquisition; the instance only drains its queue.
    runtime.spawn().expect("the acquisition task starts once");
    assert!(runtime.spawn().is_none(), "a second start is a no-op");

    let mut readings = Vec::new();
    for _ in 0..50 {
        readings = session.read_signals().await.expect("read");
        if !readings.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(readings.len(), 1, "the task polled and delivered without being asked");
    assert_eq!(readings[0].value, Some(json!(123.456)));
    assert!(agent.request_count("/current") >= 1);

    // The published state is what `sb/status` will report.
    let info = runtime.info();
    assert!(info.connected);
    assert_eq!(info.mode, "poll");
    assert_eq!(info.instance_id, Some(1_749_000_000));
    assert_eq!(info.next_sequence, Some(42));
    assert_eq!(info.agent_version.as_deref(), Some("2.7.0.12"));
    assert_eq!(info.standard_version.as_deref(), Some("2.7"));
    assert_eq!(info.probe_digests.len(), 1, "the device's probe digest is published");

    runtime.shutdown().await;
}

#[tokio::test]
async fn a_read_is_always_live_and_never_deduplicated() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![
        signal("x-position", "Xabs"),
        signal("x-load", "Xload"),
    ])]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    // Drain the poll path first, so a deduplicating read would answer with nothing.
    runtime.poll_once().await.unwrap();
    session.read_signals().await.unwrap();

    let readings = session.read_named(&["x-position".to_string()]).await.expect("read");
    assert_eq!(readings.len(), 1, "only what was asked for");
    assert_eq!(readings[0].signal_id, "x-position");
    assert_eq!(readings[0].value, Some(json!(123.456)), "a read answers with what the agent has");
}

#[tokio::test]
async fn a_device_that_is_not_in_the_probe_is_a_permanent_failure() {
    let agent = FakeAgent::start().await;
    let (_runtime, backend) = wire(
        &agent,
        vec![DeviceConfig { device_uuid: "GHOST.1".into(), ..device(vec![]) }],
    );
    let cfg: ConnectionConfig = serde_json::from_value(json!({
        "agentId": "line-a-agent", "deviceUuid": "GHOST.1"
    }))
    .unwrap();

    let Err(e) = backend.connect(&cfg).await else { panic!("a device that is not there must fail") };
    assert!(!e.is_transient(), "a uuid the agent does not serve will not appear by retrying");
    assert!(e.to_string().contains("GHOST.1"));
}

#[tokio::test]
async fn an_agent_that_stops_answering_surfaces_as_a_transient_failure() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");
    runtime.poll_once().await.unwrap();
    session.read_signals().await.unwrap();

    // The agent starts refusing: `/current` 404s.
    agent.documents.lock().unwrap().remove("current");
    assert!(runtime.poll_once().await.is_err());

    let Err(e) = session.read_signals().await else { panic!("the lost agent must surface") };
    assert!(e.is_transient(), "the supervisor reconnects rather than giving up");
    assert!(!runtime.info().connected);
}

#[tokio::test]
async fn a_probe_that_changes_under_us_is_surfaced_as_drift_not_silently_remapped() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");
    let before = runtime.model("OKUMA.123456").unwrap().digest_hex();

    // The machine is reconfigured: the X position data item is gone.
    agent.set(
        "probe",
        PROBE.replace(
            r#"<DataItem category="SAMPLE" id="Xabs" name="Xabs" nativeUnits="MILLIMETER" subType="ACTUAL" type="POSITION" units="MILLIMETER"/>"#,
            "",
        ),
    );
    let (model, changed) = runtime.refresh_model("OKUMA.123456").await.unwrap();
    assert!(changed, "the digest moved");
    assert_ne!(model.digest_hex(), before);

    // The instance recompiles against the new model, and the signal that lost its data item is
    // published BAD rather than disappearing.
    let readings = session.read_signals().await.expect("read");
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].signal_id, "x-position");
    assert_eq!(readings[0].quality, Quality::Bad);
    assert_eq!(readings[0].quality_raw.as_deref(), Some("MTC_NO_SUCH_DATAITEM"));
    assert_eq!(readings[0].value, None);
}

#[tokio::test]
async fn browsing_serves_the_cached_probe_while_the_agent_is_unreachable() {
    let agent = FakeAgent::start().await;
    let (_runtime, backend) = wire(&agent, vec![device(vec![])]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    // The agent goes away entirely; the address space is still browsable.
    agent.documents.lock().unwrap().clear();
    let page = session.browse(None, 100).await.expect("browse from cache");
    assert_eq!(page.entries.len(), 20, "the whole device tree");
    assert_eq!(page.entries[0].id, "mtc:/component/");
    assert!(page.entries.iter().any(|e| e.id == "mtc:/item/Xabs"));
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn writes_are_refused_even_at_the_backend() {
    let agent = FakeAgent::start().await;
    let (_runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");
    let Err(e) = session.write_signal("x-position", &json!(1.0)).await else {
        panic!("MTConnect is read-only")
    };
    assert!(!e.is_transient());
}

#[tokio::test]
async fn a_read_serializes_with_acquisition_through_the_control_channel() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    // With the acquisition task running, a read rides the control channel rather than opening its
    // own request behind the task's back (the single-owner rule).
    runtime.spawn().expect("task");
    let readings = session.read_named(&["x-position".to_string()]).await.expect("read");
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].value, Some(json!(123.456)));

    runtime.shutdown().await;
    // The task is gone; a read still answers, by falling back to a direct request.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let readings = session.read_named(&["x-position".to_string()]).await.expect("read");
    assert_eq!(readings.len(), 1);
}

#[tokio::test]
async fn reconnecting_re_probes_and_republishes_everything_as_fresh() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    runtime.poll_once().await.unwrap();
    assert_eq!(session.read_signals().await.unwrap().len(), 1);
    runtime.poll_once().await.unwrap();
    assert!(session.read_signals().await.unwrap().is_empty(), "nothing changed");

    let probes_before = agent.request_count("/probe");
    runtime.request_reconnect().await.expect("reconnect");
    assert_eq!(agent.request_count("/probe"), probes_before + 1, "the model is verified again");

    // The dedupe floors are gone, so the same observation is deliberately said again.
    runtime.poll_once().await.unwrap();
    let readings = session.read_signals().await.unwrap();
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].value, Some(json!(123.456)));
}

#[tokio::test]
async fn a_reconnect_through_the_running_task_reports_a_probe_failure() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let _session = backend.connect(&connection()).await.expect("connect");
    runtime.spawn().expect("task");

    agent.documents.lock().unwrap().remove("probe");
    let err = runtime.request_reconnect().await.expect_err("the agent cannot be re-probed");
    // The failure is reported through the control channel verbatim — an agent that answers 404 to
    // /probe is not a link problem to retry blindly.
    assert!(err.to_string().contains("404"), "{err:?}");
    assert_eq!(err.code(), "HTTP");
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_repoll_takes_a_fresh_current_and_says_every_configured_signal_again() {
    // The M4 interim behaviour was a drain of whatever had arrived, so a repoll on an idle machine
    // published nothing. LLD §7 makes it a FORCED scoped snapshot: `/current` is fetched again and
    // every configured signal answers, `UNAVAILABLE` and unbound ones included.
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(
        &agent,
        vec![device(vec![
            signal("x-position", "Xabs"),
            signal("x-load", "Xload"),
            signal("ghost", "NOT-IN-MODEL"),
        ])],
    );
    let mut session = backend.connect(&connection()).await.expect("connect");

    // A normal cycle publishes, and a second one publishes nothing: `/current` repeats itself.
    runtime.poll_once().await.unwrap();
    assert_eq!(session.read_signals().await.unwrap().len(), 3);
    runtime.poll_once().await.unwrap();
    assert_eq!(
        session.read_signals().await.unwrap().len(),
        1,
        "only the permanently-BAD unbound signal, which is republished every cycle"
    );

    // The repoll goes to the agent regardless, and answers with the whole configured set.
    let before = agent.request_count("/current");
    let readings = session.snapshot_now().await.expect("repoll");
    assert_eq!(agent.request_count("/current"), before + 1, "a repoll is a fresh /current");
    assert_eq!(readings.len(), 3, "polled counts published results, BAD ones included");

    let x = reading(&readings, "x-position");
    assert_eq!(x.value, Some(json!(123.456)), "said again, though nothing changed");
    assert_eq!(x.quality, Quality::Good);
    let load = reading(&readings, "x-load");
    assert_eq!(load.value, None);
    assert_eq!(load.quality_raw.as_deref(), Some("UNAVAILABLE"));
    let ghost = reading(&readings, "ghost");
    assert_eq!(ghost.quality_raw.as_deref(), Some("MTC_NO_SUCH_DATAITEM"));
}

#[tokio::test]
async fn a_repoll_asks_only_for_this_devices_configured_data_items() {
    let agent = FakeAgent::start().await;
    let (_runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    let readings = session.snapshot_now().await.expect("repoll");
    assert_eq!(readings.len(), 1, "the scope is this instance's signals, not the whole agent");
    assert_eq!(readings[0].signal_id, "x-position");

    // A device with no configured signals has nothing to snapshot, and asks the agent for nothing.
    let (_r2, backend) = wire(&agent, vec![device(vec![])]);
    let mut empty = backend.connect(&connection()).await.expect("connect");
    let before = agent.request_count("/current");
    assert!(empty.snapshot_now().await.expect("repoll").is_empty());
    assert_eq!(agent.request_count("/current"), before, "nothing configured, nothing asked");
}

#[tokio::test]
async fn a_repoll_against_an_unreachable_agent_reports_the_failure() {
    let agent = FakeAgent::start().await;
    let (_runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");
    agent.documents.lock().unwrap().remove("current");

    let err = session.snapshot_now().await.expect_err("the agent has no /current");
    // The failure surfaces verbatim rather than being reported as an empty poll: a repoll that
    // could not reach the agent must not look like a machine with nothing to say.
    assert!(err.to_string().contains("404"), "{err}");
    assert!(!err.is_transient(), "a 404 is a client-side mistake, not a link to retry blindly");
}

#[tokio::test]
async fn a_reloaded_signal_set_reaches_a_live_session_without_a_reconnect() {
    // LLD §8: an instance's signals recompile against the CACHED probe model and swap atomically.
    // The session keeps its socketless attachment; nothing reconnects, nothing re-probes.
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    runtime.poll_once().await.unwrap();
    assert_eq!(session.read_signals().await.unwrap().len(), 1);
    let probes = agent.request_count("/probe");

    // The operator adds a signal and rebinds nothing else.
    let reloaded = json!({
        "component": {
            "global": { "agents": [{ "id": "line-a-agent", "url": agent.url() }] },
            "instances": [{
                "id": "cnc-1",
                "adapter": "mtconnect",
                "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
                "signals": [
                    { "id": "x-position", "dataItemId": "Xabs" },
                    { "id": "spindle-speed", "dataItemId": "Sspeed" }
                ]
            }]
        }
    });
    let changed = backend.signals().apply(&reloaded).expect("the candidate compiles");
    assert_eq!(changed, vec!["cnc-1".to_string()]);

    // The live session publishes the new signal on its next read, from the cached model.
    let readings = session.snapshot_now().await.expect("repoll");
    assert_eq!(readings.len(), 2);
    assert!(readings.iter().any(|r| r.signal_id == "spindle-speed"));
    assert_eq!(agent.request_count("/probe"), probes, "a reload re-probes nothing");

    // And `sb/signals` answers from the same live set.
    let inventory = backend.inventory(&connection());
    assert_eq!(inventory.len(), 2);
    assert!(inventory.iter().any(|s| s.id == "spindle-speed"));

    // A signal removed by a reload stops being published.
    let trimmed = json!({
        "component": {
            "global": { "agents": [{ "id": "line-a-agent", "url": agent.url() }] },
            "instances": [{
                "id": "cnc-1",
                "adapter": "mtconnect",
                "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
                "signals": [{ "id": "spindle-speed", "dataItemId": "Sspeed" }]
            }]
        }
    });
    backend.signals().apply(&trimmed).expect("the candidate compiles");
    let readings = session.snapshot_now().await.expect("repoll");
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].signal_id, "spindle-speed");
}

// =================================================================================================
// Probe-derived signal selection (R1.1) — the fake-agent end-to-end legs
// =================================================================================================

#[tokio::test]
async fn a_minimal_selection_all_instance_publishes_the_whole_device() {
    // The soak-style minimal instance: {id, connection, selection: {mode: "all"}} and not one
    // hand-written signal. Every data item the agent reports publishes with a derived identity.
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![selecting(vec![], json!({ "mode": "all" }))]);

    let mut session = backend.connect(&connection()).await.expect("connect");
    runtime.poll_once().await.expect("poll");
    let readings = session.read_signals().await.expect("read");
    // The fixture's /current carries observations for 12 of the probe's 14 items.
    assert_eq!(readings.len(), 12, "every observed data item published, none configured by hand");

    // Identity derivation: lower-kebab id, probe name, componentPath/id channel.
    let x = reading(&readings, "xabs");
    assert_eq!(x.value, Some(json!(123.456)));
    assert_eq!(x.name.as_deref(), Some("Xabs"));
    assert_eq!(x.channel.as_deref(), Some("axes/linear-x/xabs"));
    // Auto conditionBinding: Xtravel (this component's CONDITION) is in Fault, so the derived
    // x-axis samples are degraded — exactly as a hand-bound signal would be.
    assert_eq!(x.quality, Quality::Bad);
    assert_eq!(x.quality_raw.as_deref(), Some("MTC_CONDITION:FAULT:ALM-1041"));

    // The condition itself publishes its state as a derived signal.
    let travel = reading(&readings, "xtravel");
    assert_eq!(travel.value, Some(json!("FAULT")));

    // A different component is untouched by that condition.
    let spindle = reading(&readings, "sspeed");
    assert_eq!(spindle.quality, Quality::Good);
    assert_eq!(spindle.channel.as_deref(), Some("axes/rotary-c/sspeed"));

    // camelCase ids sanitize to kebab; device-level items publish on their id alone.
    assert_eq!(reading(&readings, "part-count").channel.as_deref(),
        Some("controller/path-p1/part-count"));
    assert_eq!(reading(&readings, "avail").channel.as_deref(), Some("avail"));

    // signalsSubscribed counts the served union (14 derived, none unbound).
    assert_eq!(session.served_signals(), Some(14));

    // sb/signals sees the same union without a device round-trip.
    let inventory = backend.inventory(&connection());
    assert_eq!(inventory.len(), 14);
    assert!(inventory.iter().any(|s| s.id == "xabs"));
    session.close().await;
}

#[tokio::test]
async fn selection_matchers_scope_the_derived_set_and_explicit_entries_override() {
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(
        &agent,
        vec![selecting(
            // The explicit entry claims Xabs with its own identity and NO condition binding.
            vec![serde_json::from_value(json!({
                "id": "x-position", "dataItemId": "Xabs", "conditionBinding": []
            }))
            .unwrap()],
            json!({ "mode": "include",
                    "include": [{ "path": "Axes/**", "category": "SAMPLE" }] }),
        )],
    );

    let mut session = backend.connect(&connection()).await.expect("connect");
    runtime.poll_once().await.expect("poll");
    let readings = session.read_signals().await.expect("read");

    // Axes samples only: Xabs (as the explicit x-position), Xload, Xfreq, Sspeed.
    let mut ids: Vec<&str> = readings.iter().map(|r| r.signal_id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["sspeed", "x-position", "xfreq", "xload"]);

    // The explicit entry overrode the derived one: its id, and its EMPTY conditionBinding beat
    // the auto binding, so the Fault does not degrade it.
    let x = reading(&readings, "x-position");
    assert_eq!(x.quality, Quality::Good, "conditionBinding: [] cleared the auto binding");
    // A derived sibling in the same component keeps the auto binding and IS degraded.
    let load = reading(&readings, "xload");
    assert_eq!(load.quality, Quality::Bad, "UNAVAILABLE stays BAD");
    let freq = reading(&readings, "xfreq");
    assert_eq!(freq.quality, Quality::Bad);
    assert_eq!(freq.quality_raw.as_deref(), Some("MTC_CONDITION:FAULT:ALM-1041"));
    session.close().await;
}

#[tokio::test]
async fn max_signals_truncates_with_a_warning_event_never_silently() {
    let agent = FakeAgent::start().await;
    let (_runtime, backend) = wire(
        &agent,
        vec![selecting(vec![], json!({ "mode": "all", "maxSignals": 3 }))],
    );
    let mut session = backend.connect(&connection()).await.expect("connect");

    let notices = session.take_notices();
    let cut = notices
        .iter()
        .find(|n| n.event_type == "MtconnectSignalSetEvent")
        .expect("the truncation warning event");
    assert_eq!(cut.context["reason"], json!("maxSignals"));
    assert_eq!(cut.context["maxSignals"], json!(3));
    assert_eq!(cut.context["matched"], json!(14));
    assert_eq!(cut.context["truncated"], json!(11));

    // The kept three are the FIRST three in browse-tree order — deterministic, not arbitrary.
    let inventory = backend.inventory(&connection());
    let ids: Vec<&str> = inventory.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, vec!["avail", "asset-changed", "xabs"]);
    assert_eq!(session.served_signals(), Some(3));
    session.close().await;
}

#[tokio::test]
async fn the_derived_set_follows_the_model_through_drift() {
    // D-MTC-5 + R1.1: on model drift the derived set is recomputed inside the same generation
    // bump — new matching items START publishing, removed ones STOP (no lingering BAD), and the
    // change is announced with counts.
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(
        &agent,
        vec![selecting(vec![], json!({ "mode": "include",
            "include": [{ "category": "SAMPLE", "path": "Axes/**" }] }))],
    );
    let mut session = backend.connect(&connection()).await.expect("connect");
    runtime.poll_once().await.expect("poll");
    let before = session.read_signals().await.expect("read");
    assert!(before.iter().any(|r| r.signal_id == "xabs"));
    assert_eq!(session.served_signals(), Some(4), "Xabs, Xload, Xfreq, Sspeed");
    session.take_notices();

    // The machine is reconfigured: Xabs is gone, and a new Y axis appears.
    agent.set(
        "probe",
        PROBE
            .replace(
                r#"<DataItem category="SAMPLE" id="Xabs" name="Xabs" nativeUnits="MILLIMETER" subType="ACTUAL" type="POSITION" units="MILLIMETER"/>"#,
                "",
            )
            .replace(
                "</Linear>",
                r#"</Linear><Linear id="y-axis" name="Y"><DataItems><DataItem category="SAMPLE" id="Yabs" name="Yabs" type="POSITION" units="MILLIMETER"/></DataItems></Linear>"#,
            ),
    );
    let (_model, changed) = runtime.refresh_model("OKUMA.123456").await.unwrap();
    assert!(changed, "the digest moved");

    // The session recompiles on its next read: the removed item is GONE (not BAD — it was
    // discovered, not configured), the new one is served.
    let readings = session.read_signals().await.expect("read");
    assert!(
        readings.iter().all(|r| r.signal_id != "xabs"),
        "a removed derived signal stops publishing rather than lingering BAD: {readings:?}"
    );
    assert_eq!(session.served_signals(), Some(4), "Xload, Xfreq, Sspeed, and now Yabs");

    let notices = session.take_notices();
    let set_change = notices
        .iter()
        .find(|n| n.event_type == "MtconnectSignalSetEvent")
        .expect("the set-change event");
    assert_eq!(set_change.context["added"], json!(1), "Yabs");
    assert_eq!(set_change.context["removed"], json!(1), "Xabs");

    // The new axis publishes as soon as the agent reports it.
    agent.set(
        "current",
        CURRENT.replace(
            r#"<AccelerationTimeSeries dataItemId="Xfreq""#,
            r#"<Position dataItemId="Yabs" name="Yabs" sequence="50" timestamp="2026-07-27T10:00:06.000000Z">7.5</Position>
          <AccelerationTimeSeries dataItemId="Xfreq""#,
        ),
    );
    runtime.poll_once().await.expect("poll");
    let readings = session.read_signals().await.expect("read");
    let y = reading(&readings, "yabs");
    assert_eq!(y.value, Some(json!(7.5)));
    assert_eq!(y.channel.as_deref(), Some("axes/linear-y/yabs"));
    session.close().await;
}

#[tokio::test]
async fn a_reloaded_selection_reaches_a_live_session_atomically() {
    // A selection change is a signals-level change: it rides the same atomic swap as an explicit
    // signals[] edit — no restart, no reconnect, no re-probe.
    let agent = FakeAgent::start().await;
    let (runtime, backend) = wire(&agent, vec![device(vec![signal("x-position", "Xabs")])]);
    let mut session = backend.connect(&connection()).await.expect("connect");
    runtime.poll_once().await.unwrap();
    assert_eq!(session.read_signals().await.unwrap().len(), 1);
    let probes = agent.request_count("/probe");

    // The operator turns on mode "all" without touching the explicit signal.
    let reloaded = json!({
        "component": {
            "global": { "agents": [{ "id": "line-a-agent", "url": agent.url() }] },
            "instances": [{
                "id": "cnc-1",
                "adapter": "mtconnect",
                "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
                "signals": [{ "id": "x-position", "dataItemId": "Xabs" }],
                "selection": { "mode": "all" }
            }]
        }
    });
    let changed = backend.signals().apply(&reloaded).expect("the candidate compiles");
    assert_eq!(changed, vec!["cnc-1".to_string()]);

    // The live session serves the union on its next read, from the cached model.
    let readings = session.snapshot_now().await.expect("repoll");
    assert_eq!(readings.len(), 12, "the whole observed device");
    assert!(readings.iter().any(|r| r.signal_id == "x-position"), "the explicit id survives");
    assert!(readings.iter().any(|r| r.signal_id == "sspeed"), "the derived half arrived");
    assert_eq!(agent.request_count("/probe"), probes, "a reload re-probes nothing");
    assert_eq!(session.served_signals(), Some(14));

    // Dropping the selection again shrinks the served set back to the explicit signal.
    let trimmed = json!({
        "component": {
            "global": { "agents": [{ "id": "line-a-agent", "url": agent.url() }] },
            "instances": [{
                "id": "cnc-1",
                "adapter": "mtconnect",
                "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
                "signals": [{ "id": "x-position", "dataItemId": "Xabs" }]
            }]
        }
    });
    backend.signals().apply(&trimmed).expect("the candidate compiles");
    let readings = session.snapshot_now().await.expect("repoll");
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].signal_id, "x-position");
    assert_eq!(session.served_signals(), Some(1));
    session.close().await;
}

#[tokio::test]
async fn sb_read_resolves_derived_ids_like_configured_ones() {
    let agent = FakeAgent::start().await;
    let (_runtime, backend) = wire(&agent, vec![selecting(vec![], json!({ "mode": "all" }))]);
    let mut session = backend.connect(&connection()).await.expect("connect");

    let readings = session.read_named(&["sspeed".to_string()]).await.expect("read");
    assert_eq!(readings.len(), 1, "a derived signal is readable by its derived id");
    assert_eq!(readings[0].signal_id, "sspeed");
    assert_eq!(readings[0].value, Some(json!(1200)));
    session.close().await;
}

// =================================================================================================
// Depth-aware channel derivation
// =================================================================================================

/// The deep-path device, bound to the same fake agent.
fn deep_device(selection: serde_json::Value) -> DeviceConfig {
    DeviceConfig {
        id: "cnc-1".into(),
        agent_id: "line-a-agent".into(),
        device_uuid: "Mazak".into(),
        signals: Vec::new(),
        selection: Some(serde_json::from_value(selection).unwrap()),
    }
}

fn deep_connection() -> ConnectionConfig {
    serde_json::from_value(json!({ "agentId": "line-a-agent", "deviceUuid": "Mazak" })).unwrap()
}

/// The budget an ordinary instance really has: `ecv1/gw-01/mtconnect-adapter/cnc-1/data/` leaves
/// three tokens and 216 bytes.
fn realistic_budgets() -> mtconnect_adapter::app::ChannelBudgets {
    let mut budgets = mtconnect_adapter::app::ChannelBudgets::default();
    budgets.insert(
        "cnc-1",
        mtconnect_adapter::mtconnect::ChannelBudget { max_tokens: 3, max_bytes: 216 },
    );
    budgets
}

#[tokio::test]
async fn a_deep_component_path_publishes_on_a_channel_that_fits_the_uns_topic() {
    // The reported bug, end to end: under `mode: "all"` the demo Mazak's `stock` sits four
    // channel tokens deep where three fit, so the library refused its topic (DEPTH_EXCEEDED) and
    // it never published. The derived channel now keeps the leaf-most segments instead.
    let agent = FakeAgent::start().await;
    agent.set("probe", PROBE_DEEP.to_string());
    agent.set("current", CURRENT_DEEP.to_string());
    let (runtime, backend) =
        wire_with(&agent, vec![deep_device(json!({ "mode": "all" }))], realistic_budgets());

    let mut session = backend.connect(&deep_connection()).await.expect("connect");
    runtime.poll_once().await.expect("poll");
    let readings = session.read_signals().await.expect("read");

    // `stock` publishes, and on a channel within the budget.
    let stock = reading(&readings, "stock");
    assert_eq!(stock.value, Some(json!("ALUMINUM-6061")));
    assert_eq!(stock.channel.as_deref(), Some("materials-materials/stock-stock/stock"));

    // Every published channel fits — that is the invariant, not just this one signal.
    for r in &readings {
        let channel = r.channel.as_deref().unwrap_or(&r.signal_id);
        assert!(channel.split('/').count() <= 3, "{} -> {channel}", r.signal_id);
        assert!(channel.len() <= 216, "{} -> {channel}", r.signal_id);
    }

    // The five-level signal keeps its two leaf-most segments; the shallow ones are untouched.
    assert_eq!(
        reading(&readings, "ptemp").channel.as_deref(),
        Some("motor-motor/sensor-sensor/ptemp")
    );
    assert_eq!(reading(&readings, "avail").channel.as_deref(), Some("avail"));
    assert_eq!(
        reading(&readings, "estop").channel.as_deref(),
        Some("controller-controller/estop")
    );

    // Ordinary shaping is NOT an event: it is normal derivation on a deep machine.
    assert!(
        session.take_notices().iter().all(|n| n.context["reason"] != json!("channelBudget")),
        "a fitted channel raises no warning"
    );

    // Nothing is lost: the untruncated component path is still what `sb/signals` addresses with.
    let model = runtime.model("Mazak").expect("the cached probe model");
    assert_eq!(
        model.address_of("line-a-agent", "stock").expect("an address")["componentPath"],
        json!("Resources[resources]/Materials[materials]/Stock[stock]")
    );
    session.close().await;
}

#[tokio::test]
async fn a_budget_that_cannot_fit_even_an_id_warns_rather_than_publishing_silence() {
    // The pathological floor: an identity that has consumed the whole topic. There is nothing to
    // derive that would publish, so it is a warning event — the one case that is NOT ordinary.
    let agent = FakeAgent::start().await;
    agent.set("probe", PROBE_DEEP.to_string());
    agent.set("current", CURRENT_DEEP.to_string());
    let mut budgets = mtconnect_adapter::app::ChannelBudgets::default();
    budgets.insert(
        "cnc-1",
        mtconnect_adapter::mtconnect::ChannelBudget { max_tokens: 0, max_bytes: 0 },
    );
    let (_runtime, backend) =
        wire_with(&agent, vec![deep_device(json!({ "mode": "all" }))], budgets);

    let mut session = backend.connect(&deep_connection()).await.expect("connect");
    let notices = session.take_notices();
    let warned = notices
        .iter()
        .find(|n| n.context["reason"] == json!("channelBudget"))
        .expect("the pathological-floor warning event");
    assert_eq!(warned.event_type, "MtconnectSignalSetEvent");
    assert_eq!(warned.context["unfit"], json!(6), "every signal of the device");
    assert_eq!(warned.context["instance"], json!("cnc-1"));

    // The channel is still the bare id: this adapter invents no name, so the library's own topic
    // validation is what refuses it, loudly, on publish.
    let inventory = backend.inventory(&deep_connection());
    let ids: Vec<&str> = inventory.iter().map(|s| s.id.as_str()).collect();
    assert!(ids.contains(&"stock"));

    // A second recompile of the same shape does not repeat the warning.
    session.close().await;
}
