//! # The KUBERNETES-platform release-leg driver for `mtconnect-adapter`
//!
//! This is a HARNESS, not product code. The component itself runs as a real pod on the local kind
//! cluster (`kind-ggcommons`); this process runs on the DEV MACHINE and supplies the two live
//! things the pod talks to, plus the independent observation of the bus:
//!
//! * the cppagent SHDR adapter feed on ports 7401/7402, served here so values actually flow into
//!   `mtc-e2e-agent` (the agents dial `host.docker.internal:7401`);
//! * a raw MQTT subscriber on `ecv1/#` against the same EMQX the pod publishes to, which is NOT
//!   the publishing process, decoding the bytes that actually landed on the broker straight
//!   against the generated `edgecommons.v1` protobuf schema.
//!
//! It is driven by a CONTROL FILE (appended-to from the shell) so the phases can be interleaved
//! with `kubectl` from outside. Every capture is printed as it lands, so stdout is the timeline.
//!
//! Commands (one per line, appended to the control file):
//!   mark <text>        print a labelled marker into the timeline
//!   shdr1 <shdr line>  send a raw SHDR line to the 7401 feed (e.g. `shdr1 |Xabs|11.25`)
//!   shdr2 <shdr line>  same, for the 7402 feed
//!   seed               re-send the baseline SHDR values
//!   live on|off        enable/disable printing each capture as it arrives
//!   topics             print distinct captured topics with counts
//!   dump <n>           print the last n captures, decoded
//!   grep <substr>      print every capture whose topic OR decoded body contains <substr>
//!   count              print the capture count
//!   quit               exit
//!
//! Nothing in `src/**` of the component is touched or re-declared.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use edgecommons::proto::edgecommons::v1 as pb;
use prost::Message as _;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const BROKER: &str = "127.0.0.1:1883";
const SHDR_ONE: u16 = 7401;
const SHDR_TWO: u16 = 7402;

static SEQ: AtomicU32 = AtomicU32::new(0);
static LIVE: AtomicBool = AtomicBool::new(true);

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
}

// -------------------------------------------------------------------------------------------------
// The raw subscriber
// -------------------------------------------------------------------------------------------------

struct Sniffer {
    captured: Arc<Mutex<Vec<Captured>>>,
    #[allow(dead_code)]
    client: rumqttc::AsyncClient,
    #[allow(dead_code)]
    pump: tokio::task::JoinHandle<()>,
}

impl Sniffer {
    async fn start(broker: &str, filter: &str, origin: Instant) -> Sniffer {
        let (host, port) = split_broker(broker);
        let id = format!(
            "mtc-k8s-leg-sniffer-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let mut opts = rumqttc::MqttOptions::new(id, host, port);
        opts.set_keep_alive(Duration::from_secs(15));
        opts.set_max_packet_size(4 * 1024 * 1024, 4 * 1024 * 1024);
        opts.set_clean_session(true);
        let (client, mut eventloop) = rumqttc::AsyncClient::new(opts, 8192);
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
                        let c = Captured {
                            at: origin.elapsed(),
                            wall: wall(),
                            topic: p.topic,
                            bytes: p.payload.to_vec(),
                        };
                        if LIVE.load(Ordering::Relaxed) {
                            println!("{}", c.dump());
                        }
                        sink.lock().expect("capture lock").push(c);
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        });
        Sniffer {
            captured,
            client,
            pump,
        }
    }

    fn snapshot(&self) -> Vec<Captured> {
        self.captured.lock().expect("capture lock").clone()
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
    #[allow(dead_code)]
    task: tokio::task::JoinHandle<()>,
}

impl ShdrFeed {
    /// Binds the SHDR adapter port, or returns `None` when it is already served — the GREENGRASS
    /// lab leg runs its own feeder (`lab-leg/shdr_feed.py`) on the very same 7401/7402, and taking
    /// those ports from a leg that is mid-run is not acceptable. Sniffer-only is then the mode:
    /// the agent still has live, ramping values from that feeder.
    async fn start(port: u16) -> Option<Self> {
        let listener = match TcpListener::bind(("0.0.0.0", port)).await {
            Ok(l) => l,
            Err(e) => {
                println!("[{}] SHDR port {port} is already served ({e}) — not binding", wall());
                return None;
            }
        };
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
        Some(ShdrFeed { tx, task })
    }

    fn send(&self, line: &str) {
        let _ = self.tx.send(line.to_string());
    }
}

/// Sends to a feed we own, or says so when the port belongs to another leg's feeder.
fn feed_send(feed: &Option<ShdrFeed>, port: u16, line: &str) -> String {
    match feed {
        Some(f) => {
            f.send(line);
            format!("SHDR {port} <- {line}")
        }
        None => format!("SHDR {port} NOT OWNED by this harness — `{line}` not sent"),
    }
}

fn seed(one: &Option<ShdrFeed>, two: &Option<ShdrFeed>) {
    for l in [
        "|d1-avail|AVAILABLE",
        "|d1-travel|NORMAL||||",
        "|d1-Xabs|10.5",
        "|d1-exec|READY",
    ] {
        let _ = feed_send(one, SHDR_ONE, l);
    }
    for l in ["|d2-avail|AVAILABLE", "|d2-Xabs|1.0"] {
        let _ = feed_send(two, SHDR_TWO, l);
    }
}

// -------------------------------------------------------------------------------------------------
// main — control-file driven
// -------------------------------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let control = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "control.txt".to_string());
    let origin = Instant::now();
    let clock = Clock(origin);

    let one = ShdrFeed::start(SHDR_ONE).await;
    let two = ShdrFeed::start(SHDR_TWO).await;
    ev!(clock, "SHDR feeds listening on {SHDR_ONE}/{SHDR_TWO}");
    seed(&one, &two);

    let sniffer = Sniffer::start(BROKER, "ecv1/#", origin).await;
    ev!(clock, "raw subscriber on ecv1/# established against {BROKER}");
    ev!(clock, "control file: {control}");

    // Re-seed periodically: the agent only receives values while a feed connection is up, and it
    // may reconnect at any time.
    let mut consumed = 0usize;
    loop {
        let text = tokio::fs::read_to_string(&control).await.unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        if lines.len() > consumed {
            for line in &lines[consumed..] {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let (cmd, rest) = line.split_once(' ').unwrap_or((line, ""));
                match cmd {
                    "mark" => {
                        println!(
                            "\n=================================================================="
                        );
                        ev!(clock, "MARK: {rest}");
                        println!(
                            "==================================================================\n"
                        );
                    }
                    "shdr1" => {
                        let m = feed_send(&one, SHDR_ONE, rest);
                        ev!(clock, "{m}");
                    }
                    "shdr2" => {
                        let m = feed_send(&two, SHDR_TWO, rest);
                        ev!(clock, "{m}");
                    }
                    "seed" => {
                        seed(&one, &two);
                        ev!(clock, "SHDR baseline re-seeded");
                    }
                    "live" => {
                        LIVE.store(rest.trim() == "on", Ordering::Relaxed);
                        ev!(clock, "live printing = {rest}");
                    }
                    "count" => ev!(clock, "captures: {}", sniffer.snapshot().len()),
                    "topics" => {
                        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
                        for c in sniffer.snapshot() {
                            *counts.entry(c.topic.clone()).or_default() += 1;
                        }
                        ev!(clock, "distinct topics ({}):", counts.len());
                        for (t, n) in counts {
                            println!("    {n:>6}  {t}");
                        }
                    }
                    "dump" => {
                        let n: usize = rest.trim().parse().unwrap_or(20);
                        let all = sniffer.snapshot();
                        let start = all.len().saturating_sub(n);
                        ev!(clock, "last {} of {} captures:", all.len() - start, all.len());
                        for c in &all[start..] {
                            println!("{}", c.dump());
                        }
                    }
                    "grep" => {
                        let needle = rest.trim();
                        let all = sniffer.snapshot();
                        let hits: Vec<&Captured> = all
                            .iter()
                            .filter(|c| {
                                c.topic.contains(needle) || c.dump().contains(needle)
                            })
                            .collect();
                        ev!(clock, "grep `{needle}` -> {} of {}", hits.len(), all.len());
                        for c in hits {
                            println!("{}", c.dump());
                        }
                    }
                    "quit" => {
                        ev!(clock, "quit");
                        return;
                    }
                    other => ev!(clock, "unknown control command `{other}`"),
                }
            }
            consumed = lines.len();
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}
