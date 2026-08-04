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
//! machine with no Docker. A run that is **supposed** to have the harness sets
//! `EC_REQUIRE_LIVE` as well, and then the self-skip becomes a hard failure: a CI or lab leg
//! whose compose harness never came up must report red, not a green suite that exercised nothing.
//! Once the harness is named, nothing in this file skips — every way of failing to reach the peer
//! (connection refused, timeout, an unexpected fixture shape) panics and names the URL.
//!
//! The SHDR feed is served by this process (the cppagent adapter protocol) on fixed host ports
//! 7401/7402/7403 — the containers dial back via `host.docker.internal`. The restart and
//! buffer-wrap tests drive the containers through the `docker` CLI, so the suite needs it on PATH.
//!
//! The tests serialize on one lock: they share the SHDR ports and one of them restarts the agent.
//!
//! ## Which event class each assertion expects
//!
//! Two product rules decide the shape of everything below, and getting them the wrong way round is
//! what this file is here to catch against the canonical peer:
//!
//! * **Ordinary delivery is one [`InstanceEvent::Obs`] per observation.** The cold-start connect
//!   `/current`, every streamed part, and every degraded poll are ordinary flow. A fresh session
//!   has a fresh shaper, so there is no prior deadband state to rebuild (F-N1: a per-batch
//!   `Snapshot` re-baselined the session on *every* cycle and left the deadband inert).
//! * **[`InstanceEvent::Snapshot`] means re-baseline**, and only a genuine one: the snapshot owed
//!   to an instance that attached to an already-running stream, the ladder-2 `OUT_OF_RANGE`
//!   recovery, and the ladder-3 post-restart resync. Attaching *before* the runtime spawns is not
//!   one of them — the connect cycle covers that device and clears the attach debt.
//!
//! ## The connectivity gate (D-R1)
//!
//! `MtcBackend::connect` and `MtcSession::read_signals` mirror `AgentRuntime.info().connected`, and
//! only an ingested Streams document sets it. A cached probe model is not liveness, so a test that
//! opens a device link must bring the shared runtime up first — exactly as production does through
//! the acquisition task's first successful cycle.

use std::time::Duration;

use mtconnect_adapter::mtconnect::config::{AgentCredentials, parse_agents};
use mtconnect_adapter::mtconnect::{
    AgentRuntime, InstanceEvent, InstanceReceiver, MtcClient, ObsValue, Observation,
};
use serde_json::json;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DEV_ONE: &str = "MTC-E2E-001";
const DEV_TWO: &str = "MTC-E2E-002";
const DEV_TINY: &str = "MTC-E2E-TINY";

/// Every data item `dev-one` declares in `tests/fixtures/agent-e2e/devices.xml` — the EVENT and
/// SAMPLE items and the CONDITION one (`d1-travel`, the `Xtravel` position condition the wire gate
/// drives). A re-baseline of this device must carry all of them — that is what makes it a rebuilt
/// *view*, and a condition is exactly the kind of item a rebuild must not quietly omit.
const DEV_ONE_ITEMS: [&str; 4] = ["d1-avail", "d1-Xabs", "d1-exec", "d1-travel"];

/// The switch a CI or lab leg sets to declare "the live harness is supposed to be up". It turns the
/// self-skip below into a hard failure, so a leg whose compose harness never started cannot report
/// a green suite that exercised nothing. Unset (an ordinary developer machine) the skip stands.
const REQUIRE_LIVE: &str = "EC_REQUIRE_LIVE";

/// Whether this run claims to have the live harness.
fn live_required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|v| {
        let v = v.trim();
        !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
    })
}

fn main_agent_url() -> Option<String> {
    match std::env::var("EC_MTC_AGENT") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            assert!(
                !live_required(),
                "{REQUIRE_LIVE} is set, so this run is supposed to exercise the pinned cppagent \
                 harness — but EC_MTC_AGENT is unset or empty. Start it with `docker compose -f \
                 tests/compose.mtconnect-agent.yaml up -d` and export \
                 EC_MTC_AGENT=http://localhost:5010. Refusing to report a pass for a suite that \
                 ran nothing."
            );
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
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
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
    let cfg = parse_agents(&json!({ "agents": [entry] }))
        .unwrap()
        .remove(0);
    AgentRuntime::new(
        cfg,
        &AgentCredentials::default(),
        edgecommons::facades::system_clock(),
    )
    .unwrap()
}

// =================================================================================================
// Reading the event stream
// =================================================================================================

/// The observations an event delivers, whatever shape delivered them — for the assertions that care
/// that a value arrived, separately from the ones that care which class carried it.
fn observed(event: &InstanceEvent) -> &[Observation] {
    match event {
        InstanceEvent::Obs(obs) => std::slice::from_ref(obs.as_ref()),
        InstanceEvent::Snapshot(batch) => batch,
        _ => &[],
    }
}

/// Whether an event delivers a data item at a value, in either shape.
fn carries(event: &InstanceEvent, data_item_id: &str, want: f64) -> bool {
    observed(event)
        .iter()
        .any(|o| o.data_item_id == data_item_id && scalar_eq(&o.value, want))
}

/// One event on one line — what a live test prints as evidence and names when it times out.
fn describe(event: &InstanceEvent) -> String {
    match event {
        InstanceEvent::Obs(o) => {
            format!("Obs[{} seq={} {:?}]", o.data_item_id, o.sequence, o.value)
        }
        InstanceEvent::Snapshot(batch) => format!(
            "Snapshot[{}: {}]",
            batch.len(),
            batch
                .iter()
                .map(|o| o.data_item_id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
        InstanceEvent::AgentUp(info) => {
            format!(
                "AgentUp[mode={} instance={:?}]",
                info.mode, info.instance_id
            )
        }
        InstanceEvent::AgentDown(reason) => format!("AgentDown[{reason}]"),
        InstanceEvent::DataLoss { skipped } => format!("DataLoss[skipped={skipped}]"),
        InstanceEvent::ModelDrift { old, new } => format!("ModelDrift[{old} -> {new}]"),
        InstanceEvent::StreamDegraded { failures } => format!("StreamDegraded[{failures}]"),
    }
}

fn trace_line(trace: &[InstanceEvent]) -> String {
    if trace.is_empty() {
        return "<nothing>".to_string();
    }
    trace.iter().map(describe).collect::<Vec<_>>().join(" | ")
}

/// Accumulate every event that arrives, in order, until one satisfies `pred` — so a test can assert
/// not only that the event came but **what came before it**.
///
/// One caveat the assertions built on this must respect: a single `drain()` empties the
/// loss-intolerant lane before the data lane (LLD §3), so a `Snapshot` and an `Obs` that were
/// queued together always appear snapshot-first. Ordering claims across a multi-second window (a
/// container restart, a recovery ladder) are sound; ordering claims *within one drain* are not.
async fn trace_until(
    rx: &mut InstanceReceiver,
    secs: u64,
    what: &str,
    mut pred: impl FnMut(&InstanceEvent) -> bool,
) -> Vec<InstanceEvent> {
    let mut trace: Vec<InstanceEvent> = Vec::new();
    let outcome = tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            for event in rx.drain() {
                let hit = pred(&event);
                trace.push(event);
                if hit {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "timed out waiting for {what}; what did arrive: {}",
        trace_line(&trace)
    );
    trace
}

/// Wait (bounded) for the next event matching the predicate, discarding others.
async fn wait_for(
    rx: &mut InstanceReceiver,
    secs: u64,
    what: &str,
    pred: impl FnMut(&InstanceEvent) -> bool,
) -> InstanceEvent {
    let mut trace = trace_until(rx, secs, what, pred).await;
    trace.pop().expect("the matched event is the trace's last")
}

/// Bring the shared runtime up before a device link is opened.
///
/// D-R1: `connect` refuses until the agent runtime is **delivering**, because a cached probe model
/// is not liveness. Production reaches that state through the acquisition task's first ingested
/// Streams document, and so does this — the task is already spawned; this only waits for it.
async fn wait_until_delivering(rt: &Arc<AgentRuntime>, secs: u64) {
    let outcome = tokio::time::timeout(Duration::from_secs(secs), async {
        while !rt.info().connected {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "the agent runtime never began delivering: {:?}",
        rt.info()
    );
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
    rt.spawn(CancellationToken::new()).unwrap();

    // Probe → snapshot → stream, in that order: the state machine opens no stream until the connect
    // `/current` cycle has completed, so the first observations this instance is given are that
    // snapshot's. They arrive as **ordinary flow**, one `Obs` per observation — this device attached
    // before the runtime spawned, so the connect cycle covers it and there is no attach debt left to
    // re-baseline. A cold-started session has a fresh shaper; there is no prior view to rebuild.
    let connect_trace = trace_until(&mut handle.rx, 30, "the fed value", |e| {
        matches!(e, InstanceEvent::Obs(o)
            if o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 10.5))
    })
    .await;
    let first_delivery = connect_trace
        .iter()
        .find(|e| !observed(e).is_empty())
        .unwrap_or_else(|| panic!("nothing was delivered: {}", trace_line(&connect_trace)));
    assert!(
        matches!(first_delivery, InstanceEvent::Obs(_)),
        "the connect snapshot is ordinary flow, not a re-baseline: {}",
        trace_line(&connect_trace)
    );
    let snapshot_seq = observed(first_delivery)[0].sequence;
    // ...and it really is a *snapshot*: `/current` answers for the whole device, so every data item
    // the fixture declares is delivered, `UNAVAILABLE` ones included.
    for item in DEV_ONE_ITEMS {
        assert!(
            connect_trace
                .iter()
                .flat_map(observed)
                .any(|o| o.data_item_id == item),
            "the connect snapshot covers the whole device; `{item}` is missing: {}",
            trace_line(&connect_trace)
        );
    }

    // Live streaming: new values arrive through the multipart stream — also ordinary flow.
    for v in ["11.5", "12.5"] {
        feed.send(&format!("|Xabs|{v}"));
    }
    let stream_trace = trace_until(&mut handle.rx, 30, "streamed observations", |e| {
        matches!(e, InstanceEvent::Obs(o)
            if o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 12.5))
    })
    .await;
    let streamed = &observed(stream_trace.last().expect("the matched event"))[0];

    // F-N1, live: nothing in a cold-started, uninterrupted session is a re-baseline. When ordinary
    // delivery said `Snapshot`, every cycle re-armed the session's deadband and it never suppressed
    // anything — which no unit test could see, because only the real agent drives both paths.
    for event in connect_trace.iter().chain(stream_trace.iter()) {
        assert!(
            !matches!(event, InstanceEvent::Snapshot(_)),
            "ordinary flow must never re-baseline the session: {}",
            trace_line(&connect_trace)
        );
    }

    // The stream resumed from the snapshot's position rather than replaying it: a strictly later
    // sequence, off the live agent's own numbering.
    assert!(
        streamed.sequence > snapshot_seq,
        "the streamed observation continues the snapshot's sequence: {} vs {snapshot_seq}",
        streamed.sequence
    );
    // Every live observation carries the agent's own capture stamp and this adapter's arrival
    // stamp — the two timestamps the published sample is built from.
    assert!(
        streamed.timestamp.starts_with("20"),
        "the agent's RFC3339 capture stamp: {:?}",
        streamed.timestamp
    );
    assert!(
        streamed.received.is_some(),
        "the arrival stamp the runtime puts on every ingested document"
    );

    let info = rt.info();
    assert_eq!(
        info.mode, "stream",
        "live acquisition is streaming, not polling"
    );
    assert!(info.connected);
    assert!(info.instance_id.is_some());
    assert!(
        info.agent_version
            .as_deref()
            .unwrap_or_default()
            .starts_with("2.7.0"),
        "the pinned agent: {info:?}"
    );
    // The header facts `sb/status` publishes are real numbers off the live peer, not defaults.
    let (first, next) = (
        info.first_sequence.expect("a firstSequence"),
        info.next_sequence.expect("a nextSequence"),
    );
    assert!(
        next > first,
        "the agent's live buffer window: first={first} next={next}"
    );
    assert!(
        info.buffer_size.is_some_and(|b| b > 0),
        "the agent reports its buffer size: {info:?}"
    );
    println!(
        "EVIDENCE probe/stream: mode={} instance={:?} agentVersion={:?} standard={:?} buffer={:?} \
         window={first}..{next}",
        info.mode, info.instance_id, info.agent_version, info.standard_version, info.buffer_size
    );
    println!("EVIDENCE connect trace: {}", trace_line(&connect_trace));
    println!("EVIDENCE stream trace: {}", trace_line(&stream_trace));

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
    rt.spawn(CancellationToken::new()).unwrap();
    // Pre-restart delivery is ordinary flow: the connect cycle and the stream both say `Obs`.
    wait_for(&mut handle.rx, 30, "pre-restart data", |e| {
        matches!(e, InstanceEvent::Obs(o)
            if o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 20.5))
    })
    .await;
    let old_instance = rt.info().instance_id.expect("an instance id");
    // Everything the live incarnation had to say is now behind us: what the trace below holds is
    // the restart and nothing else.
    handle.rx.drain();

    // Restart the real agent: a new incarnation, sequence numbering restarted.
    docker(&["restart", "mtc-e2e-agent"]).await;
    // The agent reconnects to the (queued) feed; give it a fresh value to prove the pipeline.
    feed.send("|avail|AVAILABLE");
    feed.send("|Xabs|21.5");

    let trace = trace_until(&mut handle.rx, 90, "post-restart data", |e| {
        carries(e, "d1-Xabs", 21.5)
    })
    .await;
    println!("EVIDENCE restart trace: {}", trace_line(&trace));

    let new_instance = rt.info().instance_id.expect("an instance id");
    assert_ne!(old_instance, new_instance, "a restart is a new incarnation");

    // --- Ladder 3, in the order LLD §5 mandates: re-probe → recompile → THEN snapshot ------------
    //
    // The restart document itself must publish NOTHING: it was decoded against a model generation
    // the runtime already knows is void, and dispatching it would mix generations. The observable
    // proof is the shape of the recovery: the first thing this instance hears after the restart is
    // ONE `Snapshot` — a re-baseline, on the loss-intolerant lane — and never ordinary flow.
    let first_data = trace
        .iter()
        .find(|e| !observed(e).is_empty())
        .unwrap_or_else(|| {
            panic!(
                "nothing was delivered after the restart: {}",
                trace_line(&trace)
            )
        });
    let InstanceEvent::Snapshot(resync) = first_data else {
        panic!(
            "the first delivery after a restart is the ladder-3 re-baseline, not ordinary flow: {}",
            trace_line(&trace)
        );
    };
    // ...and it is a rebuilt *view*, not a value. This is the load-bearing half of the proof: had
    // the restart document been published instead of deferred, it would have recorded dedupe floors
    // for the items it carried, and this snapshot would come back missing exactly those.
    for item in DEV_ONE_ITEMS {
        assert!(
            resync.iter().any(|o| o.data_item_id == item),
            "the post-resync snapshot rebuilds the whole device view; `{item}` is missing: {}",
            trace_line(&trace)
        );
    }
    // The re-probe really did run before it: a model the runtime refused to trust is re-verified,
    // and the resync flag is clear again.
    assert!(
        !rt.needs_resync(),
        "the re-probe completed: {:?}",
        rt.info()
    );
    assert!(
        rt.model(DEV_ONE).is_some(),
        "the device model was re-verified against the new incarnation"
    );

    println!(
        "EVIDENCE restart: instanceId {old_instance} -> {new_instance}, resync re-baseline of {} \
         observations, data resumed",
        resync.len()
    );

    rt.shutdown().await;
}

// =================================================================================================
// Buffer wrap on the tiny agent (BufferSize = 2^7 = 128) → overrun recovery
// =================================================================================================
//
// Which recovery rung the machine takes here is the *agent's* choice, not ours, and the pinned peer
// makes it: cppagent 2.7.0.12 answers a `from` its buffer has run past with **HTTP 400**, not with
// an HTTP 200 `OUT_OF_RANGE` error document. A refused request is an establish failure, so the live
// machine recovers along the degradation floor — three failures, `StreamDegraded`, `/current`
// polling, then streaming re-established — rather than along ladder 2. The overrun surface is
// recorded below either way, and the assertions bind the event class to the rung actually taken:
// ladder 2 republishes as a re-baseline `Snapshot`, degraded polling is ordinary `Obs` flow.
// Ladder 2's own dispatch is covered deterministically by `tests/stream_sequence.rs`.

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
    let mut last_error = String::from("never attempted");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match client.current(None).await {
                Ok(_) => return,
                Err(e) => last_error = e.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("the tiny-buffer agent at {url} never answered /current: {last_error}")
    });
    for i in 0..200 {
        feed.send(&format!("|Tpos|{}", 2.0 + f64::from(i)));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let overrun = client.sample(Some(2), Some(10), None).await;
    let overrun_evidence = match &overrun {
        Ok(text) => match mtconnect_adapter::mtconnect::xml::parse_errors(text) {
            Ok(doc) => format!(
                "HTTP 200 error document, OUT_OF_RANGE={:?}",
                doc.out_of_range()
            ),
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
    let rt = runtime(
        &url,
        json!({ "heartbeatMs": 1_000, "requestTimeoutMs": 15_000 }),
    );
    let mut handle = rt.attach(DEV_TINY);
    rt.spawn(CancellationToken::new()).unwrap();
    // Cold start against the tiny agent: ordinary flow, one `Obs` per observation.
    wait_for(
        &mut handle.rx,
        30,
        "initial tiny-agent data",
        |e| matches!(e, InstanceEvent::Obs(o) if o.data_item_id.starts_with("t1-")),
    )
    .await;

    docker(&["pause", "mtc-e2e-agent-tiny"]).await;
    tokio::time::sleep(Duration::from_millis(3_000)).await; // > 2 x heartbeat: ladder 1 fires
    docker(&["unpause", "mtc-e2e-agent-tiny"]).await;
    for i in 0..200 {
        feed.send(&format!("|Tpos|{}", 500.0 + f64::from(i)));
    }

    // The machine must recover to fresh data on its own; record which rung it took.
    let trace = trace_until(&mut handle.rx, 90, "post-wrap data", |e| {
        observed(e).iter().any(|o| {
            o.data_item_id == "t1-Tpos"
                && matches!(&o.value, ObsValue::Scalar(v)
                    if v.as_f64().is_some_and(|f| f >= 690.0))
        })
    })
    .await;
    println!("EVIDENCE wrap trace: {}", trace_line(&trace));

    let loss_at = trace
        .iter()
        .position(|e| matches!(e, InstanceEvent::DataLoss { .. }));
    let saw_degraded = trace
        .iter()
        .any(|e| matches!(e, InstanceEvent::StreamDegraded { .. }));
    assert!(
        loss_at.is_some() || saw_degraded,
        "an overrun this size must surface as DataLoss (ladder 2) or degradation, never silently: \
         {}",
        trace_line(&trace)
    );
    // Ladder 2 IS a re-baseline: the recovery snapshot deliberately says everything again as fresh
    // off cleared dedupe floors, so the session rebuilds its view rather than treating the jump as
    // on-change flow. Where the machine degraded to `/current` polling instead, that polling is
    // ordinary flow and correctly says `Obs` — the rung decides the class, and both are asserted.
    if let Some(at) = loss_at {
        assert!(
            trace[at..]
                .iter()
                .any(|e| matches!(e, InstanceEvent::Snapshot(_))),
            "an OUT_OF_RANGE recovery republishes as ONE re-baseline snapshot: {}",
            trace_line(&trace)
        );
    }
    println!(
        "EVIDENCE wrap recovery: data resumed; dataLoss={} degraded={saw_degraded}",
        loss_at.is_some()
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
    assert!(rt.info().connected, "and it is delivering again");

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
    rt.spawn(CancellationToken::new()).unwrap();

    one.send("|Xabs|77.7");
    two.send("|Ypos|88.8");

    // One `/current` document and one stream carry both devices; each instance sees only its own —
    // as ordinary flow, one `Obs` per observation.
    let t1 = trace_until(&mut h1.rx, 30, "device one's value", |e| {
        matches!(e, InstanceEvent::Obs(o)
            if o.data_item_id == "d1-Xabs" && scalar_eq(&o.value, 77.7))
    })
    .await;
    let t2 = trace_until(&mut h2.rx, 30, "device two's value", |e| {
        matches!(e, InstanceEvent::Obs(o)
            if o.data_item_id == "d2-Ypos" && scalar_eq(&o.value, 88.8))
    })
    .await;

    // No cross-device leakage — across EVERY event either instance was given, not just the one that
    // happened to match. A shared runtime that mixed the two device streams would show up here.
    // The prefix is the device element's id, which every one of its data items carries: the ones
    // the fixture declares (`d1-Xabs`) and the ones cppagent generates for it (`d1_asset_chg`).
    for (label, trace, prefix) in [("one", &t1, "d1"), ("two", &t2, "d2")] {
        for event in trace {
            for obs in observed(event) {
                assert!(
                    obs.data_item_id.starts_with(prefix),
                    "device {label} was given `{}`: {}",
                    obs.data_item_id,
                    trace_line(trace)
                );
            }
        }
    }
    println!(
        "EVIDENCE multi-device: one stream, two devices, no leakage; one={} two={}",
        trace_line(&t1),
        trace_line(&t2)
    );

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

    use mtconnect_adapter::device::{ConnectionConfig, DeviceBackend, DeviceError, MtcBackend};
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

    // D-R1 against the real peer: the agent is up and answering, and the link is STILL refused,
    // because nothing has been ingested yet. No acquisition task is running, so this is a fact
    // about the gate rather than a race — and it is the fix for the sticky-false-ONLINE defect,
    // where a cached probe model let an instance report ONLINE against a dead agent forever.
    let refused = backend
        .connect(&conn)
        .await
        .err()
        .expect("a runtime that has never delivered must not open a device link");
    assert!(
        matches!(refused, DeviceError::Transient(_)),
        "not delivering YET is transient, never permanent: {refused}"
    );
    assert!(
        refused.to_string().contains("not delivering"),
        "the refusal names the reason: {refused}"
    );

    // Now bring acquisition up the way production does, and the same connect succeeds.
    rt.spawn(CancellationToken::new()).unwrap();
    wait_until_delivering(&rt, 60).await;
    let mut last_error = String::from("never attempted");
    let mut session = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match backend.connect(&conn).await {
                Ok(session) => return session,
                Err(e) => {
                    last_error = e.to_string();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no session against the tiny-buffer agent at {url}: {last_error}"));

    // Not one signal was configured, yet the whole device serves: the derived set came from the
    // live probe.
    let served = session
        .served_signals()
        .expect("an MTConnect session reports served signals");
    assert!(
        served >= 2,
        "the tiny device has at least tavail + Tpos: {served}"
    );

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
    assert_eq!(
        tpos.value,
        Some(json!(3.25)),
        "the live fed value, under a derived identity"
    );
    assert!(
        tpos.channel.is_some(),
        "a derived channel from the live component path"
    );
    // Every reading off a live agent carries the canonical component path (L13) — the untruncated
    // string the derived channel above was shaped from.
    for r in &readings {
        assert!(
            r.component_path.is_some(),
            "{} carries no componentPath",
            r.signal_id
        );
    }
    // A served read carries the agent's own sequence on every sample: the once-only ordering key,
    // present whether the reading came off the stream or off a scoped `/current`.
    assert!(
        tpos.extra
            .as_ref()
            .is_some_and(|e| e.contains_key("sequence")),
        "the live sequence extra: {:?}",
        tpos.extra
    );
    println!(
        "EVIDENCE selection: served={served} derived ids={:?} t1-tpos value={:?} channel={:?} \
         componentPath={:?}",
        readings
            .iter()
            .map(|r| r.signal_id.as_str())
            .collect::<Vec<_>>(),
        tpos.value,
        tpos.channel,
        tpos.component_path
    );
    session.close().await;
    rt.shutdown().await;
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
    rt.spawn(CancellationToken::new()).unwrap(); // streaming acquisition: read_signals drains what the task delivers
    let device = DeviceConfig {
        id: "tiny".into(),
        agent_id: "live-agent".into(),
        device_uuid: DEV_TINY.into(),
        signals: vec![
            serde_json::from_value(json!({
                "id": "t-pos", "dataItemId": "t1-Tpos", "publish": { "batchMs": 60000 }
            }))
            .unwrap(),
        ],
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
    // The link opens only once acquisition is really delivering (D-R1).
    wait_until_delivering(&rt, 60).await;
    let mut last_error = String::from("never attempted");
    let mut session = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match backend.connect(&conn).await {
                Ok(session) => return session,
                Err(e) => {
                    last_error = e.to_string();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no session against the tiny-buffer agent at {url}: {last_error}"));

    // The session compiled the policy table from the live probe: the SAMPLE keeps its window.
    let mut shaper = Shaper::new();
    let policies = session.shaping_policies();
    assert_eq!(
        policies["t-pos"].batch_ms, 60_000,
        "the live-compiled policy"
    );
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
    assert_eq!(
        values, wanted,
        "arrival order, every reading its own sample"
    );
    assert!(
        flushed[0]
            .iter()
            .all(|r| r.extra.as_ref().is_some_and(|e| e.contains_key("sequence"))),
        "each buffered sample keeps its own sequence extra"
    );
    println!(
        "EVIDENCE live shaping: buffered={:?} flushed_samples={} (one update)",
        buffered,
        flushed[0].len()
    );
    session.close().await;
    rt.shutdown().await;
}
