//! # The streaming state machine, end to end against a scripted agent (LLD §5)
//!
//! A real HTTP server on a real socket, the real [`AgentRuntime`] task, and the real multipart
//! reader — what these tests prove is the *outer* machine that `tests/stream_sequence.rs` cannot:
//! that each [`StreamExit`] leads to the right next requests on the wire (reconnect from the same
//! `from`, ladder-2 snapshot republish, ladder-3 re-probe with `ModelDrift`), and that repeated
//! establish failures degrade to `/current` polling and recover.
//!
//! The agent is scripted: `/probe` and `/current` answer from swappable document queues, and each
//! `/sample` request pops the next scripted behavior (serve parts then hang, or refuse).

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mtconnect_adapter::mtconnect::config::{parse_agents, AgentCredentials};
use mtconnect_adapter::mtconnect::{AgentRuntime, InstanceEvent};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const PROBE: &str = include_str!("fixtures/devices_2.7.xml");
const CURRENT: &str = include_str!("fixtures/current_2.7.xml");
const HEARTBEAT: &str = include_str!("fixtures/heartbeat_2.7.xml");
const ERRORS_OUT_OF_RANGE: &str = include_str!("fixtures/errors_out_of_range_2.7.xml");

const BOUNDARY: &str = "MTC-E2E-BOUNDARY";
const DEVICE: &str = "OKUMA.123456";

// =================================================================================================
// The scripted agent
// =================================================================================================

#[derive(Clone)]
enum SampleBehavior {
    /// Answer 404 (an agent that does not serve interval streaming).
    Refuse,
    /// Serve these parts as one multipart response, then keep the socket open forever.
    Parts(Vec<String>),
}

struct ScriptedAgent {
    addr: SocketAddr,
    /// `/probe` answers: the front is served and kept until a later doc is pushed behind it.
    probe: Arc<Mutex<VecDeque<String>>>,
    current: Arc<Mutex<VecDeque<String>>>,
    sample: Arc<Mutex<VecDeque<SampleBehavior>>>,
    /// When the scripted queue is empty: serve a hanging heartbeat stream (true) or refuse (false).
    allow_stream: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl ScriptedAgent {
    async fn start(probe: &str, current: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let agent = Self {
            addr,
            probe: Arc::new(Mutex::new(VecDeque::from([probe.to_string()]))),
            current: Arc::new(Mutex::new(VecDeque::from([current.to_string()]))),
            sample: Arc::new(Mutex::new(VecDeque::new())),
            allow_stream: Arc::new(AtomicBool::new(true)),
            requests: Arc::new(Mutex::new(Vec::new())),
        };

        let probe = Arc::clone(&agent.probe);
        let current = Arc::clone(&agent.current);
        let sample = Arc::clone(&agent.sample);
        let allow = Arc::clone(&agent.allow_stream);
        let seen = Arc::clone(&agent.requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let probe = Arc::clone(&probe);
                let current = Arc::clone(&current);
                let sample = Arc::clone(&sample);
                let allow = Arc::clone(&allow);
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

                    /// Serve the front doc, then advance the queue when a newer one waits behind
                    /// (the last doc keeps being served).
                    fn doc_from(q: &Mutex<VecDeque<String>>) -> String {
                        let mut q = q.lock().unwrap();
                        let doc = q.front().cloned().unwrap_or_default();
                        if q.len() > 1 {
                            q.pop_front();
                        }
                        doc
                    }

                    match endpoint {
                        "probe" | "current" => {
                            let body = doc_from(if endpoint == "probe" {
                                &probe
                            } else {
                                &current
                            });
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = sock.write_all(response.as_bytes()).await;
                        }
                        "sample" => {
                            let behavior =
                                sample.lock().unwrap().pop_front().unwrap_or_else(|| {
                                    if allow.load(Ordering::Relaxed) {
                                        SampleBehavior::Parts(vec![HEARTBEAT.to_string()])
                                    } else {
                                        SampleBehavior::Refuse
                                    }
                                });
                            match behavior {
                                SampleBehavior::Refuse => {
                                    let _ = sock
                                        .write_all(
                                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                        )
                                        .await;
                                }
                                SampleBehavior::Parts(parts) => {
                                    let headers = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: multipart/x-mixed-replace;boundary={BOUNDARY}\r\nConnection: close\r\n\r\n"
                                    );
                                    let _ = sock.write_all(headers.as_bytes()).await;
                                    for body in parts {
                                        let part = format!(
                                            "--{BOUNDARY}\r\nContent-type: text/xml\r\nContent-length: {}\r\n\r\n{body}\r\n",
                                            body.len()
                                        );
                                        let _ = sock.write_all(part.as_bytes()).await;
                                        let _ = sock.flush().await;
                                    }
                                    // Keep the stream open: liveness is the heartbeat's job.
                                    tokio::time::sleep(Duration::from_secs(3600)).await;
                                }
                            }
                        }
                        _ => {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                )
                                .await;
                        }
                    }
                });
            }
        });
        agent
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn push_sample(&self, behavior: SampleBehavior) {
        self.sample.lock().unwrap().push_back(behavior);
    }

    fn push_probe(&self, doc: &str) {
        self.probe.lock().unwrap().push_back(doc.to_string());
    }

    fn push_current(&self, doc: &str) {
        self.current.lock().unwrap().push_back(doc.to_string());
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

fn runtime_for(agent: &ScriptedAgent, extra: serde_json::Value) -> Arc<AgentRuntime> {
    let mut entry = json!({
        "id": "e2e-agent",
        "url": agent.url(),
        // Long heartbeat: these tests assert the request/recovery choreography, not expiry
        // (expiry math is pinned under the virtual clock in tests/stream_sequence.rs).
        "heartbeatMs": 60_000,
        "requestTimeoutMs": 2_000,
        "reconnect": { "initialMs": 20, "maxMs": 60 }
    });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        entry[k] = v;
    }
    let cfg = parse_agents(&json!({ "agents": [entry] }))
        .unwrap()
        .remove(0);
    AgentRuntime::new(cfg, &AgentCredentials::default()).unwrap()
}

/// A minimal Streams part for the OKUMA device.
fn streams_part(instance_id: u64, next: u64, samples: &[(u64, f64)]) -> String {
    let obs: String = samples
        .iter()
        .map(|(seq, v)| {
            format!(
                "<Position dataItemId=\"Xabs\" name=\"Xabs\" sequence=\"{seq}\" \
                 timestamp=\"2026-07-27T11:00:00Z\">{v}</Position>"
            )
        })
        .collect();
    format!(
        "<MTConnectStreams xmlns=\"urn:mtconnect.org:MTConnectStreams:2.7\">\
           <Header instanceId=\"{instance_id}\" bufferSize=\"131072\" nextSequence=\"{next}\"/>\
           <Streams><DeviceStream uuid=\"{DEVICE}\" name=\"OKUMA-CNC\">\
             <ComponentStream component=\"Linear\" name=\"X\" componentId=\"x\">\
               <Samples>{obs}</Samples>\
             </ComponentStream>\
           </DeviceStream></Streams>\
         </MTConnectStreams>"
    )
}

/// Wait (bounded) for the next event matching the predicate, discarding others.
async fn wait_for(
    rx: &mut tokio::sync::mpsc::Receiver<InstanceEvent>,
    what: &str,
    mut pred: impl FnMut(&InstanceEvent) -> bool,
) -> InstanceEvent {
    tokio::time::timeout(Duration::from_secs(10), async {
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

/// Poll (bounded) until the runtime's published info satisfies the predicate.
async fn wait_info(
    rt: &Arc<AgentRuntime>,
    what: &str,
    pred: impl Fn(&mtconnect_adapter::mtconnect::AgentInfo) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if pred(&rt.info()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

// =================================================================================================
// The happy path: Connecting → probe → snapshot → Streaming(from = nextSequence)
// =================================================================================================

#[tokio::test]
async fn the_machine_probes_snapshots_then_streams_from_next_sequence() {
    let agent = ScriptedAgent::start(PROBE, CURRENT).await;
    agent.push_sample(SampleBehavior::Parts(vec![
        HEARTBEAT.to_string(),
        streams_part(1_749_000_000, 44, &[(43, 124.5)]),
    ]));

    let rt = runtime_for(&agent, json!({}));
    let mut handle = rt.attach(DEVICE);
    rt.spawn().unwrap();

    // The /current snapshot arrives first, then the agent-up announcement rides behind it.
    wait_for(
        &mut handle.rx,
        "the snapshot",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 37)),
    )
    .await;
    wait_for(&mut handle.rx, "agent up", |e| {
        matches!(e, InstanceEvent::AgentUp(_))
    })
    .await;
    // ...then the stream delivers what is new.
    wait_for(
        &mut handle.rx,
        "the streamed observation",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 43)),
    )
    .await;

    let info = rt.info();
    assert_eq!(
        info.mode, "stream",
        "an established stream is the published mode"
    );
    assert!(info.connected);
    assert_eq!(info.instance_id, Some(1_749_000_000));

    // The choreography on the wire: probe (the attached device's model), snapshot, then the
    // stream resuming exactly at the snapshot's nextSequence with interval+heartbeat.
    let requests = agent.requests();
    assert!(requests[0].starts_with("/probe"), "{requests:?}");
    assert!(
        requests.iter().any(|r| r.starts_with("/current")),
        "{requests:?}"
    );
    let sample = requests
        .iter()
        .find(|r| r.starts_with("/sample"))
        .expect("a stream request");
    assert!(
        sample.contains("from=42"),
        "resumes at the snapshot's nextSequence: {sample}"
    );
    assert!(sample.contains("interval=250"), "{sample}");
    assert!(sample.contains("heartbeat=60000"), "{sample}");

    rt.shutdown().await;
}

// =================================================================================================
// Ladder 2: OUT_OF_RANGE → DataLoss + snapshot republished as fresh + resume
// =================================================================================================

#[tokio::test]
async fn out_of_range_republishes_a_snapshot_and_resumes() {
    let agent = ScriptedAgent::start(PROBE, CURRENT).await;
    // First stream: the agent's buffer overran our position.
    agent.push_sample(SampleBehavior::Parts(vec![ERRORS_OUT_OF_RANGE.to_string()]));
    // Second stream: data flows again.
    agent.push_sample(SampleBehavior::Parts(vec![streams_part(
        1_749_000_000,
        44,
        &[(43, 125.0)],
    )]));

    let rt = runtime_for(&agent, json!({}));
    let mut handle = rt.attach(DEVICE);
    rt.spawn().unwrap();

    // The initial snapshot (seq 37, next 42).
    wait_for(
        &mut handle.rx,
        "the initial snapshot",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 37)),
    )
    .await;

    // Ladder 2: the loss is quantified — the agent's firstSequence (153) minus our cursor (42).
    let loss = wait_for(&mut handle.rx, "the data-loss event", |e| {
        matches!(e, InstanceEvent::DataLoss { .. })
    })
    .await;
    assert!(
        matches!(loss, InstanceEvent::DataLoss { skipped: 111 }),
        "{loss:?}"
    );

    // The recovery snapshot REPUBLISHES the same values as fresh (dedupe floors bypassed): the
    // same /current document, the same sequence 37, deliberately said again.
    wait_for(
        &mut handle.rx,
        "the republished snapshot",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 37)),
    )
    .await;
    // And the stream resumes and delivers.
    wait_for(
        &mut handle.rx,
        "the post-recovery observation",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 43)),
    )
    .await;

    let requests = agent.requests();
    let currents = requests
        .iter()
        .filter(|r| r.starts_with("/current"))
        .count();
    assert!(
        currents >= 2,
        "the recovery fetched a fresh snapshot: {requests:?}"
    );
    let samples: Vec<&String> = requests
        .iter()
        .filter(|r| r.starts_with("/sample"))
        .collect();
    assert!(samples.len() >= 2, "{requests:?}");
    assert!(
        samples.last().unwrap().contains("from=42"),
        "resumes from the recovery snapshot's nextSequence: {samples:?}"
    );

    rt.shutdown().await;
}

// =================================================================================================
// Ladder 3: instanceId change → re-probe (ModelDrift on digest change) → snapshot → resume
// =================================================================================================

#[tokio::test]
async fn an_agent_restart_reprobes_surfaces_drift_and_resumes() {
    let agent = ScriptedAgent::start(PROBE, CURRENT).await;
    const NEW_INSTANCE: u64 = 1_753_000_000;

    // The restarted agent: new instanceId in the stream, a changed device model behind /probe,
    // and a /current from the new incarnation.
    agent.push_sample(SampleBehavior::Parts(vec![streams_part(
        NEW_INSTANCE,
        4,
        &[(3, 9.5)],
    )]));
    agent.push_sample(SampleBehavior::Parts(vec![streams_part(
        NEW_INSTANCE,
        6,
        &[(5, 10.5)],
    )]));
    agent.push_probe(&PROBE.replace("name=\"OKUMA-CNC\"", "name=\"OKUMA-CNC-REFITTED\""));
    agent.push_current(
        &CURRENT
            .replace(
                "instanceId=\"1749000000\"",
                &format!("instanceId=\"{NEW_INSTANCE}\""),
            )
            .replace("nextSequence=\"42\"", "nextSequence=\"4\"")
            // A restarted buffer cannot hold pre-restart sequence numbers: keep the recovery
            // snapshot coherent with the new incarnation.
            .replace("sequence=\"37\"", "sequence=\"3\""),
    );

    let rt = runtime_for(&agent, json!({}));
    let mut handle = rt.attach(DEVICE);
    rt.spawn().unwrap();

    // The restart's own observation is published fresh (sequence numbering restarted at 3).
    wait_for(
        &mut handle.rx,
        "the post-restart observation",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 3)),
    )
    .await;

    // Ladder 3: the model was re-verified and its digest CHANGED — surfaced, never remapped.
    let drift = wait_for(&mut handle.rx, "the model-drift event", |e| {
        matches!(e, InstanceEvent::ModelDrift { .. })
    })
    .await;
    let InstanceEvent::ModelDrift { old, new } = drift else {
        unreachable!()
    };
    assert_ne!(old, new, "the digest change is the drift");

    // The machine resumed streaming against the new incarnation.
    wait_for(
        &mut handle.rx,
        "the resumed stream",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 5)),
    )
    .await;
    wait_info(&rt, "the new instance id", |i| {
        i.instance_id == Some(NEW_INSTANCE)
    })
    .await;

    let requests = agent.requests();
    let probes = requests.iter().filter(|r| r.starts_with("/probe")).count();
    assert!(probes >= 2, "the restart forced a re-probe: {requests:?}");

    rt.shutdown().await;
}

// =================================================================================================
// Degradation: N consecutive establish failures → poll, retry, recover
// =================================================================================================

#[tokio::test]
async fn repeated_establish_failures_degrade_to_polling_and_recover() {
    let agent = ScriptedAgent::start(PROBE, CURRENT).await;
    agent.allow_stream.store(false, Ordering::Relaxed);
    // While degraded, /current advances so polling has something to deliver.
    agent.push_current(
        &CURRENT
            .replace("sequence=\"37\"", "sequence=\"57\"")
            .replace("nextSequence=\"42\"", "nextSequence=\"58\""),
    );

    let rt = runtime_for(&agent, json!({ "pollIntervalMs": 25 }));
    let mut handle = rt.attach(DEVICE);
    rt.spawn().unwrap();

    // The machine says it degraded, after exactly the configured failure budget.
    let degraded = wait_for(&mut handle.rx, "the degradation event", |e| {
        matches!(e, InstanceEvent::StreamDegraded { .. })
    })
    .await;
    assert!(
        matches!(degraded, InstanceEvent::StreamDegraded { failures: 3 }),
        "{degraded:?}"
    );

    // Degraded is not down: /current polling keeps data flowing between stream retries.
    wait_for(
        &mut handle.rx,
        "poll-delivered data while degraded",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 57)),
    )
    .await;
    assert_eq!(
        rt.info().mode,
        "poll",
        "degraded mode is honest in sb/status"
    );

    // The agent starts serving streams again: the machine recovers on its own.
    agent.allow_stream.store(true, Ordering::Relaxed);
    wait_info(&rt, "stream recovery", |i| i.mode == "stream").await;

    rt.shutdown().await;
}

// =================================================================================================
// PollOnly: unchanged by the streaming machine
// =================================================================================================

#[tokio::test]
async fn poll_only_never_requests_a_stream() {
    let agent = ScriptedAgent::start(PROBE, CURRENT).await;
    let rt = runtime_for(
        &agent,
        json!({ "streaming": "poll-only", "pollIntervalMs": 25 }),
    );
    let mut handle = rt.attach(DEVICE);
    rt.spawn().unwrap();

    wait_for(&mut handle.rx, "polled data", |e| {
        matches!(e, InstanceEvent::Snapshot(_))
    })
    .await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    let requests = agent.requests();
    assert!(
        requests
            .iter()
            .filter(|r| r.starts_with("/current"))
            .count()
            >= 2,
        "{requests:?}"
    );
    assert!(
        !requests.iter().any(|r| r.starts_with("/sample")),
        "poll-only must never open a stream: {requests:?}"
    );
    assert_eq!(rt.info().mode, "poll");

    rt.shutdown().await;
}

// =================================================================================================
// Multi-device demultiplexing on one stream
// =================================================================================================

#[tokio::test]
async fn one_stream_serves_many_devices_each_seeing_only_its_own() {
    let agent = ScriptedAgent::start(PROBE, CURRENT).await;
    // One part carrying observations for BOTH devices of the fixture.
    let both = r#"<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:2.7">
       <Header instanceId="1749000000" bufferSize="131072" nextSequence="60"/>
       <Streams>
         <DeviceStream uuid="OKUMA.123456" name="OKUMA-CNC">
           <ComponentStream component="Linear" name="X" componentId="x">
             <Samples><Position dataItemId="Xabs" name="Xabs" sequence="58"
               timestamp="2026-07-27T11:00:00Z">200.5</Position></Samples>
           </ComponentStream>
         </DeviceStream>
         <DeviceStream uuid="MAZAK.999" name="MAZAK-MILL">
           <ComponentStream component="Device" name="m" componentId="m">
             <Events><Availability dataItemId="m-avail" sequence="59"
               timestamp="2026-07-27T11:00:00Z">AVAILABLE</Availability></Events>
           </ComponentStream>
         </DeviceStream>
       </Streams>
     </MTConnectStreams>"#;
    agent.push_sample(SampleBehavior::Parts(vec![both.to_string()]));

    let rt = runtime_for(&agent, json!({}));
    let mut okuma = rt.attach(DEVICE);
    let mut mazak = rt.attach("MAZAK.999");
    rt.spawn().unwrap();

    let okuma_obs = wait_for(
        &mut okuma.rx,
        "the CNC's streamed observation",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 58)),
    )
    .await;
    if let InstanceEvent::Snapshot(obs) = &okuma_obs {
        assert!(
            obs.iter().all(|o| o.data_item_id != "m-avail"),
            "no cross-device leakage"
        );
    }
    let mazak_obs = wait_for(
        &mut mazak.rx,
        "the mill's streamed observation",
        |e| matches!(e, InstanceEvent::Snapshot(obs) if obs.iter().any(|o| o.sequence == 59)),
    )
    .await;
    if let InstanceEvent::Snapshot(obs) = &mazak_obs {
        assert!(
            obs.iter().all(|o| o.data_item_id == "m-avail"),
            "only its own items"
        );
    }

    rt.shutdown().await;
}
