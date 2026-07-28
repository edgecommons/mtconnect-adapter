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
    guarded.condition_binding = vec!["Xtravel".into()];
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
