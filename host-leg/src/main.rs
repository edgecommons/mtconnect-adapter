//! # The HOST-platform release leg driver for `mtconnect-adapter`
//!
//! This is a HARNESS, not product code. It runs the REAL `mtconnect-adapter` binary as a real OS
//! process on Linux (through WSL, so `SIGTERM` is a real signal), against the live cppagent and the
//! live EMQX broker, and evidences the leg from the bus itself:
//!
//! * a raw MQTT subscriber on `ecv1/#` that is not the publishing process, decoding the bytes that
//!   actually landed on the broker straight against the generated `edgecommons.v1` protobuf schema;
//! * a second, genuine `EdgeCommons` runtime used purely as a CONSOLE — it issues the `sb/*` verbs
//!   over the real request/reply command topics;
//! * the cppagent SHDR adapter feed (ports 7401/7402), served here so values actually flow;
//! * `docker pause`/`unpause` of the agent and of the broker for the two hard cases.
//!
//! Nothing in `src/**` of the component is touched or re-declared.

use std::collections::BTreeSet;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use edgecommons::messaging::MessageBuilder;
use edgecommons::prelude::*;
use edgecommons::proto::edgecommons::v1 as pb;
use prost::Message as _;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

// -------------------------------------------------------------------------------------------------
// Fixed coordinates of this machine's live harness
// -------------------------------------------------------------------------------------------------

const REPO_WSL: &str = "/mnt/c/Users/breis/source/edgecommons/mtconnect-adapter/.claude/worktrees/adversarial-remediation";
const ADAPTER_BIN: &str = "/tmp/mtc-target/debug/mtconnect-adapter";
const BROKER: &str = "localhost:1883";
const COMPONENT_TOKEN: &str = "mtconnect-adapter";
const INSTANCE: &str = "cnc-1";
const AGENT_CONTAINER: &str = "mtc-e2e-agent";
const BROKER_CONTAINER: &str = "edgecommons-emqx";
const SHDR_ONE: u16 = 7401;
const SHDR_TWO: u16 = 7402;

static SEQ: AtomicU32 = AtomicU32::new(0);

// -------------------------------------------------------------------------------------------------
// Output helpers
// -------------------------------------------------------------------------------------------------

fn wall() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        (secs / 3600) % 24,
        (secs / 60) % 60,
        secs % 60,
        d.subsec_millis()
    )
}

struct Clock(Instant);

impl Clock {
    fn t(&self) -> String {
        format!("{:>8.3}s", self.0.elapsed().as_secs_f64())
    }
}

macro_rules! ev {
    ($clock:expr, $($arg:tt)*) => {
        println!("[{} | t+{}] {}", wall(), $clock.t(), format!($($arg)*))
    };
}

struct Checks(Vec<(bool, String)>);

impl Checks {
    fn check(&mut self, clock: &Clock, ok: bool, label: &str, detail: &str) {
        let verdict = if ok { "PASS" } else { "FAIL" };
        println!(
            "[{} | t+{}] ==== {verdict} :: {label} :: {detail}",
            wall(),
            clock.t()
        );
        self.0.push((ok, label.to_string()));
    }

    fn report(&self) -> bool {
        println!("\n================ CHECK SUMMARY ================");
        for (ok, label) in &self.0 {
            println!("{} {label}", if *ok { "PASS" } else { "FAIL" });
        }
        let failed = self.0.iter().filter(|(ok, _)| !ok).count();
        println!("{} checks, {failed} failed", self.0.len());
        failed == 0
    }
}

// -------------------------------------------------------------------------------------------------
// Decoding the protobuf that landed
// -------------------------------------------------------------------------------------------------

fn ec_json(v: &pb::EcValue) -> Value {
    use pb::ec_value::Kind;
    match v.kind.as_ref() {
        None | Some(Kind::NullValue(_)) => Value::Null,
        Some(Kind::BoolValue(b)) => json!(b),
        Some(Kind::IntValue(i)) => json!(i),
        Some(Kind::UintValue(u)) => json!(u),
        Some(Kind::DoubleValue(d)) => json!(d),
        Some(Kind::StringValue(s)) => json!(s),
        Some(Kind::BytesValue(b)) => json!(format!("<{} bytes>", b.len())),
        Some(Kind::ListValue(l)) => Value::Array(l.values.iter().map(ec_json).collect()),
        Some(Kind::MapValue(m)) => Value::Object(
            m.fields
                .iter()
                .map(|(k, v)| (k.clone(), ec_json(v)))
                .collect(),
        ),
    }
}

fn map_json<'a, I>(m: I) -> Value
where
    I: IntoIterator<Item = (&'a String, &'a pb::EcValue)>,
{
    let mut out = serde_json::Map::new();
    for (k, v) in m {
        out.insert(k.clone(), ec_json(v));
    }
    Value::Object(out)
}

fn opt_ec(v: Option<&pb::EcValue>) -> Value {
    v.map_or(Value::Null, ec_json)
}

/// `(class, decoded body)` of one captured publication.
fn body_json(env: &pb::EdgeCommonsMessage) -> (&'static str, Value) {
    use pb::edge_commons_message::Body;
    match env.body.as_ref() {
        Some(Body::SouthboundSignalUpdate(u)) => (
            "SouthboundSignalUpdate",
            json!({
                "signal": u.signal.as_ref().map(|s| json!({
                    "id": s.id, "name": s.name, "address": opt_ec(s.address.as_ref()),
                    "extra": map_json(&s.extra),
                })),
                "samples": u.samples.iter().map(|s| json!({
                    "value": opt_ec(s.value.as_ref()),
                    "quality": s.quality,
                    "qualityRaw": opt_ec(s.quality_raw.as_ref()),
                    "sourceTs": s.source_ts,
                    "serverTs": s.server_ts,
                    "extra": map_json(&s.extra),
                })).collect::<Vec<_>>(),
                "extra": map_json(&u.extra),
            }),
        ),
        Some(Body::StateUpdate(s)) => (
            "StateUpdate",
            json!({
                "status": s.status,
                "uptimeSecs": s.uptime_secs,
                "instances": s.instances.iter().map(|i| json!({
                    "instance": i.instance, "connected": i.connected,
                    "detail": i.detail, "extra": map_json(&i.extra),
                })).collect::<Vec<_>>(),
                "extra": map_json(&s.extra),
            }),
        ),
        Some(Body::MetricUpdate(m)) => (
            "MetricUpdate",
            json!({
                "namespace": m.namespace,
                "metricName": m.metric_name,
                "dimensions": m.dimensions.iter().map(|(k, v)| (k.clone(), json!(v))).collect::<serde_json::Map<_, _>>(),
                "values": m.values.iter().map(|v| json!({
                    "name": v.name, "value": v.value, "unit": v.unit,
                })).collect::<Vec<_>>(),
                // The messaging metric target publishes the EMF object, whose keys are not the
                // MetricUpdate field names — the library's codec round-trips them through `extra`,
                // so this is where the actual measurements are.
                "extra": map_json(&m.extra),
                "emfProjection": opt_ec(m.emf_projection.as_ref()),
            }),
        ),
        Some(Body::Event(e)) => (
            "EventMessage",
            json!({
                "severity": e.severity, "type": e.r#type, "message": e.message,
                "timestamp": e.timestamp, "alarm": e.alarm, "active": e.active,
                "context": opt_ec(e.context.as_ref()), "extra": map_json(&e.extra),
            }),
        ),
        Some(Body::ConfigUpdate(c)) => ("ConfigUpdate", json!(format!("{c:?}"))),
        Some(Body::Command(c)) => ("CommandMessage", json!(format!("{c:?}"))),
        Some(Body::Structured(v)) => ("Structured", ec_json(v)),
        Some(Body::Opaque(b)) => ("Opaque", json!(format!("<{} bytes>", b.len()))),
        None => ("<no body>", Value::Null),
    }
}

#[derive(Clone)]
struct Captured {
    at: Duration,
    wall: String,
    topic: String,
    bytes: Vec<u8>,
}

impl Captured {
    fn envelope(&self) -> Option<pb::EdgeCommonsMessage> {
        pb::EdgeCommonsMessage::decode(self.bytes.as_slice()).ok()
    }

    fn decoded(&self) -> (&'static str, Value) {
        match self.envelope() {
            Some(e) => body_json(&e),
            None => ("<not protobuf>", json!(String::from_utf8_lossy(&self.bytes))),
        }
    }

    /// The UNS class token. Instance-scoped classes sit at index 4
    /// (`ecv1/{device}/{component}/{instance}/{class}`); the library's component-level keepalive
    /// and metric topics carry no instance token, so the class sits at index 3.
    fn class(&self) -> &str {
        const CLASSES: [&str; 8] = [
            "state", "metric", "cfg", "log", "data", "evt", "cmd", "app",
        ];
        let parts: Vec<&str> = self.topic.split('/').collect();
        for idx in [3usize, 4] {
            if let Some(p) = parts.get(idx)
                && CLASSES.contains(p)
            {
                return p;
            }
        }
        ""
    }

    fn dump(&self) -> String {
        let (kind, body) = self.decoded();
        format!(
            "[t+{:>8.3}s | {}] {}\n    {kind} {}",
            self.at.as_secs_f64(),
            self.wall,
            self.topic,
            serde_json::to_string(&body).unwrap_or_default()
        )
    }

    fn update(&self) -> Option<pb::SouthboundSignalUpdate> {
        match self.envelope()?.body {
            Some(pb::edge_commons_message::Body::SouthboundSignalUpdate(u)) => Some(u),
            _ => None,
        }
    }

    fn signal_id(&self) -> String {
        self.update()
            .and_then(|u| u.signal)
            .map(|s| s.id)
            .unwrap_or_default()
    }

    fn state(&self) -> Option<pb::StateUpdate> {
        match self.envelope()?.body {
            Some(pb::edge_commons_message::Body::StateUpdate(s)) => Some(s),
            _ => None,
        }
    }

    fn event(&self) -> Option<pb::EventMessage> {
        match self.envelope()?.body {
            Some(pb::edge_commons_message::Body::Event(e)) => Some(e),
            _ => None,
        }
    }

    fn metric(&self) -> Option<pb::MetricUpdate> {
        match self.envelope()?.body {
            Some(pb::edge_commons_message::Body::MetricUpdate(m)) => Some(m),
            _ => None,
        }
    }
}

// -------------------------------------------------------------------------------------------------
// The raw subscriber
// -------------------------------------------------------------------------------------------------

struct Sniffer {
    client: rumqttc::AsyncClient,
    filter: String,
    captured: Arc<Mutex<Vec<Captured>>>,
    pump: tokio::task::JoinHandle<()>,
}

impl Sniffer {
    async fn start(broker: &str, filter: &str, origin: Instant) -> Sniffer {
        let (host, port) = split_broker(broker);
        let id = format!(
            "mtc-host-leg-sniffer-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let mut opts = rumqttc::MqttOptions::new(id, host, port);
        opts.set_keep_alive(Duration::from_secs(15));
        opts.set_max_packet_size(4 * 1024 * 1024, 4 * 1024 * 1024);
        opts.set_clean_session(true);
        let (client, mut eventloop) = rumqttc::AsyncClient::new(opts, 4096);
        client
            .subscribe(filter, rumqttc::QoS::AtLeastOnce)
            .await
            .unwrap_or_else(|e| panic!("subscribe `{filter}` on {broker}: {e}"));
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::SubAck(_))) => return,
                    Ok(_) => {}
                    Err(e) => panic!("MQTT connect to {broker} failed: {e}"),
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the broker at {broker} never acknowledged `{filter}`"));

        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&captured);
        let pump = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                        sink.lock().expect("capture lock").push(Captured {
                            at: origin.elapsed(),
                            wall: wall(),
                            topic: p.topic,
                            bytes: p.payload.to_vec(),
                        });
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        });
        Sniffer {
            client,
            filter: filter.to_string(),
            captured,
            pump,
        }
    }

    fn snapshot(&self) -> Vec<Captured> {
        self.captured.lock().expect("capture lock").clone()
    }

    fn len(&self) -> usize {
        self.captured.lock().expect("capture lock").len()
    }

    /// Everything captured from `from` onwards.
    fn since(&self, from: usize) -> Vec<Captured> {
        let all = self.snapshot();
        all.into_iter().skip(from).collect()
    }

    /// Wait until some capture at or after `from` satisfies `pred`.
    async fn wait(
        &self,
        from: usize,
        secs: u64,
        pred: impl Fn(&Captured) -> bool,
    ) -> Option<Captured> {
        tokio::time::timeout(Duration::from_secs(secs), async {
            loop {
                if let Some(found) = self.since(from).into_iter().find(&pred) {
                    return found;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .ok()
    }

    async fn stop(self) {
        let _ = self.client.unsubscribe(&self.filter).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = self.client.disconnect().await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        self.pump.abort();
    }
}

fn split_broker(broker: &str) -> (String, u16) {
    match broker.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().expect("broker port")),
        None => (broker.to_string(), 1883),
    }
}

// -------------------------------------------------------------------------------------------------
// The SHDR feed (the cppagent adapter protocol)
// -------------------------------------------------------------------------------------------------

struct ShdrFeed {
    tx: tokio::sync::mpsc::UnboundedSender<String>,
    task: tokio::task::JoinHandle<()>,
}

impl ShdrFeed {
    async fn start(port: u16) -> Self {
        let listener = tokio::time::timeout(Duration::from_secs(15), async {
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
        ShdrFeed { tx, task }
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

// -------------------------------------------------------------------------------------------------
// docker
// -------------------------------------------------------------------------------------------------

async fn docker(clock: &Clock, args: &[&str]) {
    let out = tokio::process::Command::new("docker")
        .args(args)
        .output()
        .await
        .expect("docker on PATH");
    ev!(
        clock,
        "docker {} -> status {} {}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    );
}

// -------------------------------------------------------------------------------------------------
// The component under test: a REAL process, on Linux, over WSL
// -------------------------------------------------------------------------------------------------

struct Adapter {
    child: tokio::process::Child,
    log: Arc<Mutex<Vec<(Duration, String)>>>,
    pid_file: String,
    cmdline: String,
}

impl Adapter {
    async fn spawn(origin: Instant, thing: &str, tag: &str) -> Adapter {
        let pid_file = format!("/tmp/mtc-host-leg-{tag}.pid");
        let cmdline = format!(
            "{ADAPTER_BIN} --platform HOST --transport MQTT {REPO_WSL}/test-configs/host-leg-messaging.json -c FILE {REPO_WSL}/test-configs/host-leg.json -t {thing}"
        );
        let script = format!("echo $$ > {pid_file}; exec {cmdline}");
        let mut child = tokio::process::Command::new("wsl.exe")
            .arg("-e")
            .arg("bash")
            .arg("-c")
            .arg(&script)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false)
            .spawn()
            .expect("spawn the adapter through wsl.exe");

        let log = Arc::new(Mutex::new(Vec::new()));
        for (name, pipe) in [
            ("out", child.stdout.take().map(PipeKind::Out)),
            ("err", child.stderr.take().map(PipeKind::Err)),
        ] {
            let Some(pipe) = pipe else { continue };
            let sink = Arc::clone(&log);
            tokio::spawn(async move {
                let mut lines = match pipe {
                    PipeKind::Out(o) => BufReader::new(Box::pin(o) as PinnedRead).lines(),
                    PipeKind::Err(e) => BufReader::new(Box::pin(e) as PinnedRead).lines(),
                };
                while let Ok(Some(line)) = lines.next_line().await {
                    let at = origin.elapsed();
                    println!("    ADAPTER[{name}] t+{:>8.3}s | {line}", at.as_secs_f64());
                    sink.lock().expect("log lock").push((at, line));
                }
            });
        }

        Adapter {
            child,
            log,
            pid_file,
            cmdline,
        }
    }

    fn lines(&self) -> Vec<(Duration, String)> {
        self.log.lock().expect("log lock").clone()
    }

    async fn linux_pid(&self) -> String {
        let out = tokio::process::Command::new("wsl.exe")
            .args(["-e", "cat", &self.pid_file])
            .output()
            .await
            .expect("read the pid file");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// Deliver a real `SIGTERM` to the Linux process.
    async fn sigterm(&self) -> String {
        let pid = self.linux_pid().await;
        let out = tokio::process::Command::new("wsl.exe")
            .args(["-e", "kill", "-TERM", &pid])
            .output()
            .await
            .expect("kill -TERM");
        assert!(
            out.status.success(),
            "kill -TERM {pid} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        pid
    }

}

type PinnedRead = std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>;

enum PipeKind {
    Out(tokio::process::ChildStdout),
    Err(tokio::process::ChildStderr),
}

// -------------------------------------------------------------------------------------------------
// The console: a genuine EdgeCommons runtime issuing the sb/* verbs over request/reply
// -------------------------------------------------------------------------------------------------

async fn console(dir: &std::path::Path) -> EdgeCommons {
    let config = dir.join("console-config.json");
    std::fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "logging": { "level": "ERROR" },
            "hierarchy": { "levels": ["site", "device"] },
            "identity": { "site": "host-leg" },
            "heartbeat": { "enabled": false },
            "metricEmission": { "target": "log" },
            "tags": { "site": "host-leg" },
            "component": {
                "token": "host-leg-console",
                "global": {},
                "instances": [
                    { "id": "probe", "adapter": "sim", "connection": { "endpoint": "sim://probe" } }
                ]
            }
        }))
        .unwrap(),
    )
    .expect("write the console config");
    let messaging = dir.join("console-messaging.json");
    let (host, port) = split_broker(BROKER);
    std::fs::write(
        &messaging,
        serde_json::to_vec_pretty(&json!({
            "messaging": { "local": { "host": host, "port": port, "clientId": "mtc-host-leg-console" } }
        }))
        .unwrap(),
    )
    .expect("write the console messaging config");

    EdgeCommonsBuilder::new("com.mbreissi.edgecommons.HostLegConsole")
        .args([
            std::ffi::OsString::from("host-leg"),
            "--platform".into(),
            "HOST".into(),
            "--transport".into(),
            "MQTT".into(),
            messaging.into_os_string(),
            "-c".into(),
            "FILE".into(),
            config.into_os_string(),
            "-t".into(),
            "mtc-host-leg-console".into(),
        ])
        .build()
        .await
        .expect("the console runtime comes up against the broker")
}

/// Issue one verb at the adapter's instance-scoped command topic and return the decoded reply body.
async fn verb(
    clock: &Clock,
    gg: &EdgeCommons,
    thing: &str,
    name: &str,
    body: Value,
    secs: u64,
) -> Option<Value> {
    let topic = format!("ecv1/{thing}/{COMPONENT_TOKEN}/{INSTANCE}/cmd/{name}");
    let request = MessageBuilder::new(name, "1.0").command(body.clone()).build();
    ev!(clock, "REQUEST  {topic}\n    body {body}");
    let started = Instant::now();
    let reply = gg
        .messaging()
        .expect("a wired messaging transport")
        .request_with_timeout(&topic, request, Some(Duration::from_secs(secs)))
        .await;
    let reply = match reply {
        Ok(fut) => fut.await,
        Err(e) => {
            ev!(clock, "REPLY    <request failed to go out: {e}>");
            return None;
        }
    };
    match reply {
        Ok(msg) => {
            ev!(
                clock,
                "REPLY    ({:.0} ms) {}",
                started.elapsed().as_secs_f64() * 1000.0,
                serde_json::to_string(&msg.body).unwrap_or_default()
            );
            Some(msg.body)
        }
        Err(e) => {
            ev!(clock, "REPLY    <no reply: {e}>");
            None
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Shared: the SHDR values that make the fixture live
// -------------------------------------------------------------------------------------------------

fn seed(one: &ShdrFeed, two: &ShdrFeed) {
    one.send("|avail|AVAILABLE");
    one.send("|Xtravel|NORMAL||||");
    one.send("|Xabs|10.5");
    one.send("|exec|READY");
    two.send("|avail2|AVAILABLE");
    two.send("|Ypos|1.0");
}

fn instance_state(state: &pb::StateUpdate) -> Option<(bool, String, Value)> {
    let i = state.instances.iter().find(|i| i.instance == INSTANCE)?;
    let extra = map_json(&i.extra);
    let link = extra
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("<none>")
        .to_string();
    Some((i.connected, link, extra))
}

// -------------------------------------------------------------------------------------------------
// main
// -------------------------------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let phase = std::env::args().nth(1).unwrap_or_else(|| "1".to_string());
    let ok = match phase.as_str() {
        "1" => phase_one().await,
        "2" => phase_two(false).await,
        // The hardest stop: the agent is frozen mid-stream (so the acquisition task is parked in an
        // HTTP long-poll that will never answer) AND the broker is frozen.
        "3" => phase_two(true).await,
        other => panic!("unknown phase `{other}` (use 1, 2 or 3)"),
    };
    if !ok {
        std::process::exit(1);
    }
}

// =================================================================================================
// PHASE 1 — start, connect, publish, connectivity, commands, metrics, events, passive quality,
//           and the clean bounded shutdown
// =================================================================================================

async fn phase_one() -> bool {
    let origin = Instant::now();
    let clock = Clock(origin);
    let mut checks = Checks(Vec::new());
    let thing = "mtc-host-leg";
    let tmp = std::env::temp_dir().join(format!("mtc-host-leg-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).expect("temp dir");

    println!("=================================================================================");
    println!("PHASE 1 — HOST platform release leg, thing `{thing}`");
    println!("=================================================================================");

    // --- the SHDR feed ---------------------------------------------------------------------------
    let one = ShdrFeed::start(SHDR_ONE).await;
    let two = ShdrFeed::start(SHDR_TWO).await;
    ev!(clock, "SHDR feeds listening on {SHDR_ONE}/{SHDR_TWO}");
    seed(&one, &two);
    tokio::time::sleep(Duration::from_millis(2_000)).await;

    // --- freeze the agent so the CONNECTING/BACKOFF rungs are observable -------------------------
    docker(&clock, &["pause", AGENT_CONTAINER]).await;

    // --- the raw subscriber ----------------------------------------------------------------------
    let sniffer = Sniffer::start(BROKER, "ecv1/#", origin).await;
    ev!(clock, "raw subscriber on ecv1/# established");

    // --- the console -----------------------------------------------------------------------------
    let gg = console(&tmp).await;
    ev!(clock, "console runtime connected to {BROKER}");

    // --- the component under test ----------------------------------------------------------------
    let adapter = Adapter::spawn(origin, thing, "p1").await;
    ev!(clock, "SPAWNED: {}", adapter.cmdline);
    let started_at = Instant::now();

    // ============================================================================================
    // 2. the connectivity state model, BEFORE acquisition delivers
    // ============================================================================================
    // The `CONNECTING` rung is attempt 0 of the connect loop only, and the D-R1 gate fails that
    // attempt the instant it finds the shared runtime is not delivering — so it is far shorter than
    // the 1 s keepalive tick. Poll `sb/status` back to back (STRICTLY sequential: never more than
    // one outstanding reply subscription) for the first seconds, to see whether it is observable at
    // all from outside the process.
    println!("\n--- ITEM 2a-pre: back-to-back sb/status probe over the first 6 s ------------------");
    let probe_until = Instant::now() + Duration::from_secs(6);
    let mut probed: Vec<(f64, String, bool)> = Vec::new();
    while Instant::now() < probe_until {
        let topic = format!("ecv1/{thing}/{COMPONENT_TOKEN}/{INSTANCE}/cmd/sb/status");
        let request = MessageBuilder::new("sb/status", "1.0").command(json!({})).build();
        let sent = origin.elapsed().as_secs_f64();
        if let Ok(fut) = gg
            .messaging()
            .expect("messaging")
            .request_with_timeout(&topic, request, Some(Duration::from_millis(400)))
            .await
            && let Ok(msg) = fut.await
        {
            let state = msg.body["result"]["state"]
                .as_str()
                .unwrap_or("<none>")
                .to_string();
            let connected = msg.body["result"]["connected"].as_bool().unwrap_or(false);
            if probed.last().map(|(_, s, _)| s.as_str()) != Some(state.as_str()) {
                ev!(clock, "sb/status probe at t+{sent:.3}s -> state={state} connected={connected}");
                probed.push((sent, state, connected));
            }
        }
    }
    let probed_states: BTreeSet<String> = probed.iter().map(|(_, s, _)| s.clone()).collect();
    ev!(
        clock,
        "distinct sb/status states over the first 6 s: {probed_states:?} ({} replies with a change)",
        probed.len()
    );

    println!("\n--- ITEM 2a: state keepalive + sb/status while the agent is frozen ---------------");
    let first_state = sniffer
        .wait(0, 45, |c| {
            c.state()
                .and_then(|s| instance_state(&s))
                .is_some_and(|(connected, _, _)| !connected)
        })
        .await;
    match &first_state {
        Some(c) => {
            println!("{}", c.dump());
            let (connected, link, _) = instance_state(&c.state().unwrap()).unwrap();
            checks.check(
                &clock,
                !connected && (link == "CONNECTING" || link == "BACKOFF"),
                "state keepalive reports the pre-acquisition rung",
                &format!("topic={} connected={connected} state={link}", c.topic),
            );
        }
        None => checks.check(
            &clock,
            false,
            "state keepalive reports the pre-acquisition rung",
            "no state message with instances[] arrived in 45 s",
        ),
    }

    // every distinct rung the state keepalive published while the agent was frozen
    let mut rungs: BTreeSet<String> = BTreeSet::new();
    for c in sniffer.snapshot() {
        if let Some((_, link, _)) = c.state().and_then(|s| instance_state(&s)) {
            rungs.insert(link);
        }
    }

    // sb/status must agree with it, at the same moment
    let status = verb(&clock, &gg, thing, "sb/status", json!({}), 20).await;
    let mut state_now = None;
    if let Some(c) = sniffer
        .wait(sniffer.len(), 10, |c| c.state().is_some())
        .await
    {
        println!("{}", c.dump());
        state_now = c.state().and_then(|s| instance_state(&s));
    }
    match (&status, &state_now) {
        (Some(reply), Some((connected, link, _))) => {
            let r = &reply["result"];
            let status_link = r["state"].as_str();
            let status_connected = r["connected"].as_bool();
            rungs.insert(link.clone());
            checks.check(
                &clock,
                status_link == Some(link.as_str()) && status_connected == Some(*connected),
                "sb/status agrees with the state keepalive (pre-acquisition)",
                &format!(
                    "sb/status state={status_link:?} connected={status_connected:?} | state instances[] state={link} connected={connected}"
                ),
            );
        }
        _ => checks.check(
            &clock,
            false,
            "sb/status agrees with the state keepalive (pre-acquisition)",
            "one of the two answers was missing",
        ),
    }

    // ============================================================================================
    // 1. it starts, connects and publishes
    // ============================================================================================
    println!("\n--- ITEM 2b/1: unfreeze the agent, watch it reach ONLINE and publish -------------");
    let mark = sniffer.len();
    docker(&clock, &["unpause", AGENT_CONTAINER]).await;

    let online = sniffer
        .wait(mark, 90, |c| {
            c.state()
                .and_then(|s| instance_state(&s))
                .is_some_and(|(connected, link, _)| connected && link == "ONLINE")
        })
        .await;
    match &online {
        Some(c) => {
            println!("{}", c.dump());
            checks.check(
                &clock,
                true,
                "state keepalive reports ONLINE after acquisition delivers",
                &format!(
                    "at t+{:.3}s (started t+{:.3}s)",
                    c.at.as_secs_f64(),
                    started_at.duration_since(origin).as_secs_f64()
                ),
            );
        }
        None => checks.check(
            &clock,
            false,
            "state keepalive reports ONLINE after acquisition delivers",
            "never saw connected=true / ONLINE within 90 s",
        ),
    }
    for c in sniffer.snapshot() {
        if let Some((_, link, _)) = c.state().and_then(|s| instance_state(&s)) {
            rungs.insert(link);
        }
    }
    for r in &rungs {
        ev!(clock, "observed connectivity rung: {r}");
    }
    checks.check(
        &clock,
        rungs.contains("ONLINE") && (rungs.contains("CONNECTING") || rungs.contains("BACKOFF")),
        "the connectivity model walks CONNECTING/BACKOFF -> ONLINE",
        &format!(
            "rungs on the state keepalive: {rungs:?}; rungs seen by the back-to-back sb/status probe: {probed_states:?}"
        ),
    );

    let status = verb(&clock, &gg, thing, "sb/status", json!({}), 20).await;
    if let (Some(reply), Some(c)) = (&status, &online) {
        let (connected, link, _) = instance_state(&c.state().unwrap()).unwrap();
        let r = &reply["result"];
        checks.check(
            &clock,
            r["state"].as_str() == Some(link.as_str())
                && r["connected"].as_bool() == Some(connected),
            "sb/status agrees with the state keepalive (ONLINE)",
            &format!(
                "sb/status state={:?} connected={:?} | state keepalive state={link} connected={connected}",
                r["state"], r["connected"]
            ),
        );
    }

    // publications
    println!("\n--- ITEM 1: SouthboundSignalUpdate on the data class ------------------------------");
    let mark = sniffer.len();
    one.send("|Xabs|11.25");
    one.send("|exec|ACTIVE");
    let data = sniffer
        .wait(mark, 30, |c| {
            c.class() == "data" && c.signal_id() == "execution"
        })
        .await;
    let data_x = sniffer
        .wait(mark, 30, |c| {
            c.class() == "data" && c.signal_id() == "x-position"
        })
        .await;
    for c in [data.as_ref(), data_x.as_ref()].into_iter().flatten() {
        println!("{}", c.dump());
    }
    let expected_prefix = format!("ecv1/{thing}/{COMPONENT_TOKEN}/{INSTANCE}/data/");
    let topics: Vec<String> = sniffer
        .snapshot()
        .iter()
        .filter(|c| c.class() == "data")
        .map(|c| c.topic.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    checks.check(
        &clock,
        !topics.is_empty() && topics.iter().all(|t| t.starts_with(&expected_prefix)),
        "data-class topic grammar ecv1/{device}/{component}/{instance}/data/{channel}",
        &format!("{topics:?}"),
    );
    checks.check(
        &clock,
        data.is_some() && data_x.is_some(),
        "SouthboundSignalUpdate published for the configured signals",
        &format!(
            "execution={} x-position={}",
            data.is_some(),
            data_x.is_some()
        ),
    );

    // ============================================================================================
    // 3. the sb/* command surface over the real bus
    // ============================================================================================
    println!("\n--- ITEM 3: every sb/* verb over the request/reply command topics -----------------");
    let mut verbs_ok = Vec::new();

    let signals = verb(&clock, &gg, thing, "sb/signals", json!({}), 20).await;
    verbs_ok.push((
        "sb/signals",
        signals
            .as_ref()
            .is_some_and(|r| r["ok"] == json!(true) && r["result"]["signals"].is_array()),
    ));

    let paged = verb(&clock, &gg, thing, "sb/browse", json!({ "max": 3 }), 20).await;
    let cursor = paged
        .as_ref()
        .and_then(|r| r["result"]["cursor"].as_str().map(str::to_string));
    verbs_ok.push((
        "sb/browse (paged)",
        paged
            .as_ref()
            .is_some_and(|r| r["ok"] == json!(true) && r["result"]["entries"].is_array()),
    ));
    if let Some(cursor) = cursor {
        let page2 = verb(
            &clock,
            &gg,
            thing,
            "sb/browse",
            json!({ "cursor": cursor, "max": 3 }),
            20,
        )
        .await;
        verbs_ok.push((
            "sb/browse (paged, cursor page 2)",
            page2.as_ref().is_some_and(|r| r["ok"] == json!(true)),
        ));
    }

    let tree = verb(
        &clock,
        &gg,
        thing,
        "sb/browse",
        json!({ "ref": "root", "depth": 3 }),
        20,
    )
    .await;
    verbs_ok.push((
        "sb/browse (hierarchical)",
        tree.as_ref().is_some_and(|r| {
            r["ok"] == json!(true)
                && r["result"]["mode"] == json!("hierarchical")
                && r["result"]["root"]["refs"].is_array()
        }),
    ));

    let read = verb(
        &clock,
        &gg,
        thing,
        "sb/read",
        json!({ "signals": [{ "id": "x-position" }, { "id": "availability" }, { "id": "x-travel" }] }),
        25,
    )
    .await;
    verbs_ok.push((
        "sb/read",
        read.as_ref().is_some_and(|r| {
            r["ok"] == json!(true)
                && r["result"]["reads"]
                    .as_array()
                    .is_some_and(|rs| rs.len() == 3 && rs.iter().all(|e| e["quality"] == json!("GOOD")))
        }),
    ));

    let write = verb(
        &clock,
        &gg,
        thing,
        "sb/write",
        json!({ "writes": [{ "signal": { "id": "x-position" }, "value": 1.0 }] }),
        20,
    )
    .await;
    let refused = write.as_ref().is_some_and(|r| {
        r["ok"] == json!(false) && r["error"]["code"] == json!("WRITE_NOT_ALLOWED")
    });
    checks.check(
        &clock,
        refused,
        "sb/write is refused with WRITE_NOT_ALLOWED",
        &write
            .as_ref()
            .map(|r| serde_json::to_string(r).unwrap())
            .unwrap_or_else(|| "<no reply>".into()),
    );

    let mark = sniffer.len();
    let pause = verb(&clock, &gg, thing, "sb/pause", json!({}), 20).await;
    verbs_ok.push((
        "sb/pause",
        pause.as_ref().is_some_and(|r| r["ok"] == json!(true)),
    ));
    let paused_evt = sniffer
        .wait(mark, 15, |c| {
            c.event().is_some_and(|e| e.r#type == "adapter-paused")
        })
        .await;
    if let Some(c) = &paused_evt {
        println!("{}", c.dump());
    }
    let mark = sniffer.len();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resume = verb(&clock, &gg, thing, "sb/resume", json!({}), 20).await;
    verbs_ok.push((
        "sb/resume",
        resume.as_ref().is_some_and(|r| r["ok"] == json!(true)),
    ));
    let resumed_evt = sniffer
        .wait(mark, 15, |c| {
            c.event().is_some_and(|e| e.r#type == "adapter-resumed")
        })
        .await;
    if let Some(c) = &resumed_evt {
        println!("{}", c.dump());
    }

    let reconnect = verb(&clock, &gg, thing, "reconnect", json!({}), 30).await;
    verbs_ok.push((
        "reconnect",
        reconnect.as_ref().is_some_and(|r| r["ok"] == json!(true)),
    ));
    let repoll = verb(&clock, &gg, thing, "repoll", json!({}), 30).await;
    verbs_ok.push((
        "repoll",
        repoll.as_ref().is_some_and(|r| r["ok"] == json!(true)),
    ));

    let status2 = verb(&clock, &gg, thing, "sb/status", json!({}), 20).await;
    verbs_ok.push((
        "sb/status",
        status2.as_ref().is_some_and(|r| r["ok"] == json!(true)),
    ));

    for (name, ok) in &verbs_ok {
        checks.check(&clock, *ok, &format!("verb {name} answers over the bus"), "");
    }

    // ============================================================================================
    // 5. metrics and events reach the bus
    // ============================================================================================
    println!("\n--- ITEM 5: metric and evt classes ------------------------------------------------");
    // The agent metric ticker runs on a 30 s period; wait for a full cycle to be sure.
    let want = [
        "southbound_health",
        "MtconnectStream",
        "MtconnectProbe",
        "MtconnectParse",
    ];
    let deadline = Instant::now() + Duration::from_secs(75);
    let mut seen: BTreeSet<String> = BTreeSet::new();
    while Instant::now() < deadline {
        seen = sniffer
            .snapshot()
            .iter()
            .filter(|c| c.class() == "metric")
            .map(|c| {
                c.metric()
                    .map(|m| m.metric_name)
                    .filter(|n| !n.is_empty())
                    .unwrap_or_else(|| c.topic.rsplit('/').next().unwrap_or("").to_string())
            })
            .collect();
        if want.iter().all(|w| seen.iter().any(|s| s == w)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("---- every distinct metric-class topic captured so far ----");
    let metric_topics: BTreeSet<String> = sniffer
        .snapshot()
        .iter()
        .filter(|c| c.class() == "metric" || c.topic.contains("/metric"))
        .map(|c| c.topic.clone())
        .collect();
    for t in &metric_topics {
        println!("    {t}");
    }
    if let Some(c) = sniffer
        .snapshot()
        .into_iter()
        .find(|c| c.class() == "metric" || c.topic.contains("/metric"))
    {
        println!("---- one raw metric-class publication ----\n{}", c.dump());
    }

    for name in want {
        let sample = sniffer.snapshot().into_iter().find(|c| {
            c.metric().is_some_and(|m| m.metric_name == name) || c.topic.ends_with(name)
        });
        match sample {
            Some(c) => {
                // The `result` dimension splits each family into a success cell and an error cell,
                // and both are emitted every tick — so print the last few publications on the
                // topic rather than one, or the evidence is whichever cell happened to be last.
                let all: Vec<Captured> = sniffer
                    .snapshot()
                    .into_iter()
                    .filter(|x| x.topic == c.topic)
                    .collect();
                for latest in all.iter().rev().take(3).rev() {
                    println!("{}", latest.dump());
                }
                checks.check(
                    &clock,
                    true,
                    &format!("metric `{name}` on the bus"),
                    &format!("{} ({} publications)", c.topic, all.len()),
                );
            }
            None => checks.check(
                &clock,
                false,
                &format!("metric `{name}` on the bus"),
                &format!("metric names seen: {seen:?}"),
            ),
        }
    }

    let events: Vec<Captured> = sniffer
        .snapshot()
        .into_iter()
        .filter(|c| c.class() == "evt")
        .collect();
    for c in events.iter().take(6) {
        println!("{}", c.dump());
    }
    checks.check(
        &clock,
        !events.is_empty(),
        "at least one evt-class publication",
        &format!(
            "{} evt publications; types {:?}",
            events.len(),
            events
                .iter()
                .filter_map(|c| c.event().map(|e| e.r#type))
                .collect::<BTreeSet<_>>()
        ),
    );

    // the condition Fault: a bound CONDITION degrades the signal it is bound to
    println!("\n--- ITEM 5b: the CONDITION fault ---------------------------------------------------");
    let mark = sniffer.len();
    one.send("|Xtravel|FAULT|ALM-1|HIGH||X travel limit exceeded");
    let fault = sniffer
        .wait(mark, 30, |c| {
            c.class() == "data"
                && c.update().is_some_and(|u| {
                    u.samples.iter().any(|s| {
                        matches!(
                            s.quality_raw.as_ref().and_then(|q| q.kind.as_ref()),
                            Some(pb::ec_value::Kind::StringValue(v)) if v.contains("ALM-1")
                        )
                    })
                })
        })
        .await;
    for c in sniffer.since(mark).iter().filter(|c| c.class() == "data") {
        println!("{}", c.dump());
    }
    checks.check(
        &clock,
        fault.is_some(),
        "a bound CONDITION Fault degrades the signal on the wire",
        &fault
            .as_ref()
            .map(|c| c.topic.clone())
            .unwrap_or_else(|| "<not observed>".into()),
    );
    // ...and the BOUND signal takes the fault's native code on its next reading (conditionBinding).
    let mark = sniffer.len();
    one.send("|Xabs|55.5");
    let bound = sniffer
        .wait(mark, 30, |c| {
            c.class() == "data"
                && c.signal_id() == "x-position"
                && c.update().is_some_and(|u| {
                    u.samples.iter().any(|s| {
                        s.quality == "BAD"
                            && matches!(
                                s.quality_raw.as_ref().and_then(|q| q.kind.as_ref()),
                                Some(pb::ec_value::Kind::StringValue(v)) if v.contains("ALM-1")
                            )
                    })
                })
        })
        .await;
    if let Some(c) = &bound {
        println!("{}", c.dump());
    }
    checks.check(
        &clock,
        bound.is_some(),
        "conditionBinding degrades the BOUND signal (x-position) with the native code",
        &bound
            .as_ref()
            .map(|c| c.topic.clone())
            .unwrap_or_else(|| "<not observed>".into()),
    );

    one.send("|Xtravel|NORMAL||||");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ============================================================================================
    // 6. passive quality end to end, on a real process
    // ============================================================================================
    println!("\n--- ITEM 6: passive quality — freeze the agent, watch held signals degrade --------");
    let mark = sniffer.len();
    docker(&clock, &["pause", AGENT_CONTAINER]).await;
    let degraded = sniffer
        .wait(mark, 40, |c| {
            c.class() == "data"
                && c.update().is_some_and(|u| {
                    u.samples
                        .iter()
                        .any(|s| s.quality == "UNCERTAIN" || s.quality == "BAD")
                })
        })
        .await;
    tokio::time::sleep(Duration::from_secs(12)).await;
    let degrade_window: Vec<Captured> = sniffer
        .since(mark)
        .into_iter()
        .filter(|c| c.class() == "data")
        .collect();
    for c in degrade_window.iter().take(14) {
        println!("{}", c.dump());
    }
    let qualities: BTreeSet<String> = degrade_window
        .iter()
        .filter_map(|c| c.update())
        .flat_map(|u| {
            u.samples
                .iter()
                .map(|s| s.quality.clone())
                .collect::<Vec<_>>()
        })
        .collect();
    checks.check(
        &clock,
        degraded.is_some(),
        "held signals degrade on the bus while the agent is frozen",
        &format!("qualities published in the window: {qualities:?}"),
    );

    let mark = sniffer.len();
    docker(&clock, &["unpause", AGENT_CONTAINER]).await;
    one.send("|Xabs|22.75");
    let recovered = sniffer
        .wait(mark, 60, |c| {
            c.class() == "data"
                && c.update()
                    .is_some_and(|u| u.samples.iter().any(|s| s.quality == "GOOD"))
        })
        .await;
    if let Some(c) = &recovered {
        println!("{}", c.dump());
    }
    checks.check(
        &clock,
        recovered.is_some(),
        "signals recover to GOOD once the agent is unfrozen",
        &recovered
            .as_ref()
            .map(|c| c.topic.clone())
            .unwrap_or_else(|| "<not observed>".into()),
    );

    // ============================================================================================
    // 4. bounded structured shutdown — the clean case
    // ============================================================================================
    println!("\n--- ITEM 4a: SIGTERM with an OPEN batch window (clean broker) ---------------------");
    // Let any window that is already open expire, so the burst below opens a fresh one.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let burst = ["701.1", "702.2", "703.3", "704.4"];
    for v in burst {
        one.send(&format!("|Xabs|{v}"));
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    ev!(
        clock,
        "fed {burst:?} into the OPEN 5000 ms batch window; SIGTERM in 1.2 s"
    );
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let before_sigterm = sniffer.len();
    let sig_at = Instant::now();
    let pid = adapter.sigterm().await;
    ev!(clock, "SIGTERM delivered to linux pid {pid}");

    let mut adapter = adapter;
    let exit = tokio::time::timeout(Duration::from_secs(30), adapter.child.wait()).await;
    let elapsed = sig_at.elapsed();
    match &exit {
        Ok(Ok(status)) => {
            ev!(
                clock,
                "process exited {:?} after {:.3} s",
                status,
                elapsed.as_secs_f64()
            );
            checks.check(
                &clock,
                status.success(),
                "the process exits on its own (no SIGKILL needed)",
                &format!("exit status {status:?} after {:.3} s", elapsed.as_secs_f64()),
            );
        }
        _ => checks.check(
            &clock,
            false,
            "the process exits on its own (no SIGKILL needed)",
            "it was still running 30 s after SIGTERM",
        ),
    }
    checks.check(
        &clock,
        elapsed < Duration::from_secs(12),
        "teardown completes inside the 12 s budget (6 devices + 4 agents + 2 metrics)",
        &format!("{:.3} s", elapsed.as_secs_f64()),
    );

    // give the broker a moment to deliver whatever the flush published
    tokio::time::sleep(Duration::from_secs(2)).await;
    let tail = sniffer.since(before_sigterm);
    println!("\n---- everything that landed on the bus AT OR AFTER SIGTERM ----");
    for c in &tail {
        println!("{}", c.dump());
    }
    let flushed: Vec<&Captured> = tail
        .iter()
        .filter(|c| c.class() == "data" && c.signal_id() == "x-position")
        .collect();
    let flushed_values: Vec<String> = flushed
        .iter()
        .filter_map(|c| c.update())
        .flat_map(|u| {
            u.samples
                .iter()
                .map(|s| ec_json(s.value.as_ref().unwrap_or(&pb::EcValue::default())).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    checks.check(
        &clock,
        burst
            .iter()
            .all(|v| flushed_values.iter().any(|f| f.starts_with(&v[..5]))),
        "the OPEN batch window is flushed on SIGTERM and its samples reach the broker",
        &format!("fed {burst:?} -> flushed after SIGTERM {flushed_values:?}"),
    );

    let alarms: Vec<String> = tail
        .iter()
        .filter_map(|c| c.event())
        .filter(|e| e.r#type == "device-unreachable" && e.alarm == Some(true))
        .map(|e| format!("{} active={:?}", e.r#type, e.active))
        .collect();
    checks.check(
        &clock,
        alarms.is_empty(),
        "no device-unreachable alarm is raised on a clean stop",
        &format!("alarms after SIGTERM: {alarms:?}"),
    );

    // the component's own shutdown narration, with its timings
    println!("\n---- the component's own shutdown log ----");
    let sig_offset = sig_at.duration_since(origin);
    for (at, line) in adapter.lines() {
        if at >= sig_offset.saturating_sub(Duration::from_millis(200))
            || line.contains("shutdown")
            || line.contains("stopped")
        {
            println!(
                "    t+{:>8.3}s (SIGTERM+{:>7.3}s) | {line}",
                at.as_secs_f64(),
                at.saturating_sub(sig_offset).as_secs_f64()
            );
        }
    }
    let log = adapter.lines();
    let clean = log
        .iter()
        .any(|(_, l)| l.contains("shutdown complete: every task flushed and returned"));
    checks.check(
        &clock,
        clean,
        "shutdown reports every task flushed and returned",
        if clean {
            "logged `shutdown complete: every task flushed and returned`"
        } else {
            "the clean-shutdown line was not logged"
        },
    );

    // --- the whole bus census --------------------------------------------------------------------
    println!("\n---- BUS CENSUS: every distinct topic this run put on the broker ----");
    let mut census: std::collections::BTreeMap<String, (usize, String)> =
        std::collections::BTreeMap::new();
    for c in sniffer.snapshot() {
        let kind = c.decoded().0.to_string();
        let entry = census.entry(c.topic.clone()).or_insert((0, kind));
        entry.0 += 1;
    }
    for (topic, (count, kind)) in &census {
        println!("    {count:>5} x {topic}   [{kind}]");
    }

    // --- teardown --------------------------------------------------------------------------------
    drop(gg);
    sniffer.stop().await;
    drop(one);
    drop(two);
    checks.report()
}

// =================================================================================================
// PHASE 2 — the hard case: SIGTERM while the broker is dead
// =================================================================================================

async fn phase_two(freeze_agent: bool) -> bool {
    let origin = Instant::now();
    let clock = Clock(origin);
    let mut checks = Checks(Vec::new());
    let thing = "mtc-host-leg";
    let tmp = std::env::temp_dir().join(format!("mtc-host-leg-p2-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).expect("temp dir");

    println!("=================================================================================");
    if freeze_agent {
        println!("PHASE 3 — SIGTERM against a DEAD broker AND an agent frozen mid-stream");
    } else {
        println!("PHASE 2 — SIGTERM against a DEAD broker");
    }
    println!("=================================================================================");

    let one = ShdrFeed::start(SHDR_ONE).await;
    let two = ShdrFeed::start(SHDR_TWO).await;
    seed(&one, &two);
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let sniffer = Sniffer::start(BROKER, "ecv1/#", origin).await;
    let adapter = Adapter::spawn(origin, thing, "p2").await;
    ev!(clock, "SPAWNED: {}", adapter.cmdline);

    let online = sniffer
        .wait(0, 90, |c| {
            c.state()
                .and_then(|s| instance_state(&s))
                .is_some_and(|(connected, link, _)| connected && link == "ONLINE")
        })
        .await;
    match &online {
        Some(c) => {
            println!("{}", c.dump());
            ev!(clock, "the component is ONLINE and publishing");
        }
        None => ev!(clock, "WARNING: never reached ONLINE before the broker kill"),
    }
    checks.check(
        &clock,
        online.is_some(),
        "phase 2 preconditions: the component reached ONLINE before the broker was killed",
        "",
    );

    // fill an open batch window that can now never drain
    one.send("|Xabs|901.1");
    tokio::time::sleep(Duration::from_millis(250)).await;
    one.send("|Xabs|902.2");
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Optionally park the acquisition task in an HTTP long-poll that will never answer.
    if freeze_agent {
        docker(&clock, &["pause", AGENT_CONTAINER]).await;
        ev!(clock, "agent frozen mid-stream");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // KILL THE BROKER
    docker(&clock, &["pause", BROKER_CONTAINER]).await;
    ev!(clock, "broker frozen — nothing can drain from here on");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let sig_at = Instant::now();
    let pid = adapter.sigterm().await;
    ev!(clock, "SIGTERM delivered to linux pid {pid} (broker dead)");

    let mut adapter = adapter;
    let exit = tokio::time::timeout(Duration::from_secs(45), adapter.child.wait()).await;
    let elapsed = sig_at.elapsed();
    match &exit {
        Ok(Ok(status)) => {
            ev!(
                clock,
                "process exited {:?} after {:.3} s with the broker dead",
                status,
                elapsed.as_secs_f64()
            );
            checks.check(
                &clock,
                true,
                "the process exits on its own with a DEAD broker",
                &format!("exit status {status:?} after {:.3} s", elapsed.as_secs_f64()),
            );
        }
        _ => {
            checks.check(
                &clock,
                false,
                "the process exits on its own with a DEAD broker",
                "still running 45 s after SIGTERM — it hung",
            );
        }
    }
    checks.check(
        &clock,
        elapsed < Duration::from_secs(15),
        "the dead-broker teardown still finishes inside the orchestrator stop window",
        &format!("{:.3} s (12 s budget, 15 s Greengrass stop window)", elapsed.as_secs_f64()),
    );

    println!("\n---- the component's own shutdown log (dead broker) ----");
    let sig_offset = sig_at.duration_since(origin);
    for (at, line) in adapter.lines() {
        if at >= sig_offset.saturating_sub(Duration::from_millis(500)) {
            println!(
                "    t+{:>8.3}s (SIGTERM+{:>7.3}s) | {line}",
                at.as_secs_f64(),
                at.saturating_sub(sig_offset).as_secs_f64()
            );
        }
    }

    // ALWAYS restore the shared containers
    docker(&clock, &["unpause", BROKER_CONTAINER]).await;
    if freeze_agent {
        docker(&clock, &["unpause", AGENT_CONTAINER]).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    sniffer.stop().await;
    drop(one);
    drop(two);
    checks.report()
}
