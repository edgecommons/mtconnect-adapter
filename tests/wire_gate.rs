//! # The local-MQTT wire gate (LLD §12 — `wire | local MQTT | exact envelope + extras assertions`)
//!
//! Every other suite in this repository stops at the `Wire` seam: `tests/publish_shaping.rs` and
//! `tests/passive_quality.rs` build the body and round-trip it through the library's codec
//! in-process, and `src/driver.rs` drives the whole poll loop over a recording wire. None of them
//! ever puts a byte on a broker. This one does, and it is the release gate that says so:
//!
//! * a **genuine `EdgeCommons` runtime**, built through `EdgeCommonsBuilder::build()` against the
//!   real local broker — the only construction path the library offers for `DataFacade`, whose
//!   constructor is `pub(crate)`;
//! * the **real MTConnect stack** underneath it — `AgentRuntime` streaming from the pinned
//!   cppagent, `MtcBackend`/`MtcSession` folding its observations, `crate::shaping`, the
//!   `QualityWatchdog`, and `driver::run_device` orchestrating all of it;
//! * a **raw MQTT subscriber** that is not the publishing runtime, reading the bytes that actually
//!   landed on the broker;
//! * decoded with **`prost`, straight against the generated `edgecommons.v1` schema**, not through
//!   the library's own envelope reader — so the assertions below are about the wire, not about a
//!   codec agreeing with itself.
//!
//! ## Opting in
//!
//! ```text
//! docker compose -f tests/compose.mtconnect-agent.yaml up -d
//! docker compose -f ../core/test-infra/compose.yaml up -d          # EMQX on :1883
//! EC_MTC_AGENT=http://localhost:5010 EC_MQTT_BROKER=localhost:1883 \
//!   cargo test --test wire_gate -- --test-threads=1 --nocapture
//! ```
//!
//! Without **both** variables every test self-skips, so an ordinary `cargo test` on a machine with
//! no Docker stays green. A run that is *supposed* to have the harness sets `EC_REQUIRE_LIVE`, and
//! the skip becomes a hard failure — a lab leg whose infrastructure never came up must report red,
//! not a green suite that exercised nothing.
//!
//! ## What is production code here and what is not
//!
//! Everything from `run_device` downwards is the shipped implementation. The one thing this file
//! re-declares is `supervisor::FacadeWire`, which is a private struct: [`GateWire`] performs the
//! *identical* three public calls (`DataFacade::build_body` ▸ `app::stamp_component_path` ▸
//! `DataFacade::publish_body_via`) on the *same* real facade minted by `gg.instance(id).data()`.
//! Nothing is mocked, and no body and no topic is hand-built anywhere below.
//!
//! ## The design the assertions are held to
//!
//! `signal.address` is deliberately **absent** from a published update: D-MtconnectAdapter-L13
//! weighed and rejected promoting the MTConnect address block onto the southbound data contract,
//! and put the canonical component path on the update-level `extra` map instead. The §5.3 address
//! is served by the control plane, so this gate proves it where it lives — over the same broker,
//! through a real `sb/signals` request/reply (see `the_5_3_address_round_trips_over_the_bus`).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use edgecommons::messaging::MessageBuilder;
use edgecommons::prelude::*;
use edgecommons::proto::edgecommons::v1 as pb;
use mtconnect_adapter::app::{
    ChannelBudgets, DeviceConfig, Health, compile_mtconnect, publish_defaults_of,
    stamp_component_path,
};
use mtconnect_adapter::commands::{DeviceHandle, ProtocolView, register_all};
use mtconnect_adapter::device::{DeviceBackend, MtcBackend, resolve_agent_credentials};
use mtconnect_adapter::driver::{Wire, run_device};
use mtconnect_adapter::metrics::{AgentTelemetry, DeviceMetrics};
use mtconnect_adapter::mtconnect::AgentRuntime;
use mtconnect_adapter::mtconnect::config::parse_agents;
use prost::Message as _;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// The tests share the SHDR port, the live agent container (one of them freezes it) and the device
/// fixture, so they run one at a time whatever `--test-threads` says.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Distinguishes the MQTT client ids of concurrently-lived connections within one run.
static CLIENT_SEQ: AtomicU32 = AtomicU32::new(0);

/// The device the fixture calls `dev-one`.
const DEVICE_UUID: &str = "MTC-E2E-001";
/// The SHDR adapter port `tests/fixtures/agent-e2e/agent.cfg` dials for `dev-one`.
const SHDR_PORT: u16 = 7401;
/// The compose container serving that device — frozen by the passive-quality leg.
const AGENT_CONTAINER: &str = "mtc-e2e-agent";

/// The component name `recipe.yaml` ships, so the runtime resolves exactly as production does.
const COMPONENT_NAME: &str = "com.mbreissi.edgecommons.MtconnectAdapter";
/// `component.token` — the `{component}` UNS token.
const COMPONENT_TOKEN: &str = "mtconnect-adapter";
/// The single configured instance — the `{instance}` UNS token.
const INSTANCE: &str = "cnc-1";
/// The first hierarchy level's value; the second (`device`) is the thing name.
const SITE: &str = "wire-gate";

/// The wire name/version the `data()` facade publishes a signal update under.
const DATA_MESSAGE_NAME: &str = "SouthboundSignalUpdate";
const DATA_MESSAGE_VERSION: &str = "1.0";

/// The switch a CI or lab leg sets to declare "the live harness is supposed to be up". It turns the
/// self-skip below into a hard failure, so a leg whose broker or agent never started cannot report
/// a green suite that exercised nothing.
const REQUIRE_LIVE: &str = "EC_REQUIRE_LIVE";

// =================================================================================================
// Opting in
// =================================================================================================

fn live_required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|v| {
        let v = v.trim();
        !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
    })
}

fn env_var(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        _ => None,
    }
}

/// `(agent base URL, broker `host:port`)`, or `None` when this run opted out.
///
/// Once both are named nothing in this file skips: every way of failing to reach either peer panics
/// and names it.
fn live_targets() -> Option<(String, String)> {
    match (env_var("EC_MTC_AGENT"), env_var("EC_MQTT_BROKER")) {
        (Some(agent), Some(broker)) => Some((agent, broker)),
        (agent, broker) => {
            assert!(
                !live_required(),
                "{REQUIRE_LIVE} is set, so this run is supposed to exercise the local-MQTT wire \
                 gate against the pinned cppagent AND the local broker - but EC_MTC_AGENT={agent:?} \
                 EC_MQTT_BROKER={broker:?}. Start both (`docker compose -f \
                 tests/compose.mtconnect-agent.yaml up -d` and the EMQX compose) and export \
                 EC_MTC_AGENT=http://localhost:5010 EC_MQTT_BROKER=localhost:1883. Refusing to \
                 report a pass for a gate that ran nothing."
            );
            eprintln!(
                "EC_MTC_AGENT / EC_MQTT_BROKER not both set - skipping the local-MQTT wire gate"
            );
            None
        }
    }
}

fn split_broker(broker: &str) -> (String, u16) {
    let (host, port) = broker
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("EC_MQTT_BROKER must be `host:port`, got `{broker}`"));
    (
        host.to_string(),
        port.parse()
            .unwrap_or_else(|e| panic!("EC_MQTT_BROKER port `{port}`: {e}")),
    )
}

// =================================================================================================
// The in-process SHDR feed (the cppagent adapter protocol)
// =================================================================================================

/// One SHDR adapter port: the agent dials in, we feed `|key|value` lines and answer `* PING`.
/// Lines sent while no agent is connected are queued and flushed on (re)connect.
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

/// The agent container, frozen for the lifetime of this value and **always** thawed.
///
/// The freeze is what the passive ladder is judged against, and the assertions that judge it run
/// while it is in effect — so a failing one must not leave a paused container behind to poison the
/// next run (or the rest of the live suite). `Drop` runs on the unwind too.
struct FrozenAgent;

impl FrozenAgent {
    async fn freeze(container: &str) -> Self {
        docker(&["pause", container]).await;
        Self
    }
}

impl Drop for FrozenAgent {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["unpause", AGENT_CONTAINER])
            .status();
    }
}

// =================================================================================================
// The raw subscriber — the bytes that actually landed
// =================================================================================================

/// One captured MQTT publication, kept as the exact bytes the broker delivered.
#[derive(Clone)]
struct Captured {
    topic: String,
    bytes: Vec<u8>,
}

impl Captured {
    /// Decode straight against the generated `edgecommons.v1` schema.
    fn envelope(&self) -> pb::EdgeCommonsMessage {
        pb::EdgeCommonsMessage::decode(self.bytes.as_slice()).unwrap_or_else(|e| {
            panic!(
                "`{}` did not decode as an EdgeCommonsMessage ({e}); first bytes: {:02x?}",
                self.topic,
                &self.bytes[..self.bytes.len().min(16)]
            )
        })
    }

    /// The `SouthboundSignalUpdate` this publication carries.
    fn update(&self) -> pb::SouthboundSignalUpdate {
        match self.envelope().body {
            Some(pb::edge_commons_message::Body::SouthboundSignalUpdate(u)) => u,
            other => panic!(
                "`{}` carries {other:?}, not a SouthboundSignalUpdate body",
                self.topic
            ),
        }
    }

    fn signal_id(&self) -> String {
        self.update().signal.unwrap_or_default().id
    }

    /// A one-line description — what a trace prints and a timeout names.
    fn describe(&self) -> String {
        let u = self.update();
        let signal = u.signal.clone().unwrap_or_default();
        let samples = u
            .samples
            .iter()
            .map(|s| {
                format!(
                    "{}/{}{}",
                    render(s.value.as_ref()),
                    s.quality,
                    s.quality_raw
                        .as_ref()
                        .map(|r| format!("({})", render(Some(r))))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("{}[{}: {samples}]", self.topic, signal.id)
    }
}

/// A raw MQTT client that is not the publishing runtime. It subscribes, records every delivery in
/// arrival order, and is closed down explicitly (UNSUBSCRIBE then DISCONNECT) so no subscription is
/// leaked against the shared broker's connection quota.
struct Sniffer {
    client: rumqttc::AsyncClient,
    filter: String,
    captured: Arc<Mutex<Vec<Captured>>>,
    pump: tokio::task::JoinHandle<()>,
}

impl Sniffer {
    async fn start(broker: &str, filter: &str) -> Sniffer {
        let (host, port) = split_broker(broker);
        let id = format!(
            "mtc-wire-gate-sniffer-{}-{}",
            std::process::id(),
            CLIENT_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let mut opts = rumqttc::MqttOptions::new(id, host, port);
        opts.set_keep_alive(Duration::from_secs(15));
        opts.set_max_packet_size(4 * 1024 * 1024, 4 * 1024 * 1024);
        opts.set_clean_session(true);
        let (client, mut eventloop) = rumqttc::AsyncClient::new(opts, 1024);
        client
            .subscribe(filter, rumqttc::QoS::AtLeastOnce)
            .await
            .unwrap_or_else(|e| panic!("subscribe `{filter}` on {broker}: {e}"));

        // Do not return until the broker has ACKed the subscription: a publish that raced an
        // un-established filter would be silently missing and would read as a product failure.
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
                            topic: p.topic,
                            bytes: p.payload.to_vec(),
                        });
                    }
                    Ok(_) => {}
                    // A disconnect during teardown is expected; anything else settles on the next
                    // poll. Never panic on a background task the assertions are not watching.
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

    /// Every `data`-class publication of this component, in arrival order.
    fn data_updates(&self) -> Vec<Captured> {
        self.snapshot()
            .into_iter()
            .filter(|c| c.topic.contains("/data/"))
            .collect()
    }

    /// The first `data`-class publication anywhere in the capture satisfying `pred`, waiting up to
    /// `secs` for one to arrive. For lookups that are not part of an ordered sequence — the cold
    /// `/current` snapshot's rows arrive in the agent's order, not ours, and an agent whose adapter
    /// link dropped between runs replays `UNAVAILABLE` before the fed value.
    async fn wait_any(&self, secs: u64, what: &str, pred: impl Fn(&Captured) -> bool) -> Captured {
        let outcome = tokio::time::timeout(Duration::from_secs(secs), async {
            loop {
                if let Some(found) = self.data_updates().into_iter().find(&pred) {
                    return found;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        outcome.unwrap_or_else(|_| {
            panic!(
                "timed out after {secs}s waiting for {what}; what did arrive: {}",
                self.data_updates()
                    .iter()
                    .map(Captured::describe)
                    .collect::<Vec<_>>()
                    .join(" | ")
            )
        })
    }

    async fn stop(self) {
        let _ = self.client.unsubscribe(&self.filter).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = self.client.disconnect().await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        self.pump.abort();
    }
}

/// An ordered reader over a [`Sniffer`]'s captures: each `next_where` resumes where the last one
/// stopped, so a sequence of transitions that repeat a value can be asserted **in order**.
struct Tap<'a> {
    sniffer: &'a Sniffer,
    cursor: usize,
}

impl<'a> Tap<'a> {
    fn new(sniffer: &'a Sniffer) -> Self {
        Self { sniffer, cursor: 0 }
    }

    /// The next `data`-class publication after the cursor satisfying `pred`, waiting up to `secs`.
    /// Panics naming everything that did arrive in the meantime.
    async fn next_where(
        &mut self,
        secs: u64,
        what: &str,
        pred: impl Fn(&Captured) -> bool,
    ) -> Captured {
        let start = self.cursor;
        let outcome = tokio::time::timeout(Duration::from_secs(secs), async {
            loop {
                let all = self.sniffer.data_updates();
                while self.cursor < all.len() {
                    let candidate = all[self.cursor].clone();
                    self.cursor += 1;
                    if pred(&candidate) {
                        return candidate;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        match outcome {
            Ok(found) => found,
            Err(_) => {
                let all = self.sniffer.data_updates();
                let seen = all[start.min(all.len())..]
                    .iter()
                    .map(Captured::describe)
                    .collect::<Vec<_>>()
                    .join(" | ");
                panic!(
                    "timed out after {secs}s waiting for {what}; what did arrive: {}",
                    if seen.is_empty() {
                        "<nothing>".to_string()
                    } else {
                        seen
                    }
                );
            }
        }
    }
}

// =================================================================================================
// Reading the decoded protobuf
// =================================================================================================

fn render(value: Option<&pb::EcValue>) -> String {
    use pb::ec_value::Kind;
    match value.and_then(|v| v.kind.as_ref()) {
        None => "<unset>".to_string(),
        Some(Kind::NullValue(_)) => "null".to_string(),
        Some(Kind::BoolValue(b)) => b.to_string(),
        Some(Kind::IntValue(i)) => i.to_string(),
        Some(Kind::UintValue(u)) => u.to_string(),
        Some(Kind::DoubleValue(d)) => d.to_string(),
        Some(Kind::StringValue(s)) => format!("{s:?}"),
        Some(Kind::BytesValue(b)) => format!("<{} bytes>", b.len()),
        Some(Kind::ListValue(l)) => format!(
            "[{}]",
            l.values
                .iter()
                .map(|v| render(Some(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Some(Kind::MapValue(m)) => format!(
            "{{{}}}",
            m.fields
                .iter()
                .map(|(k, v)| format!("{k}: {}", render(Some(v))))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn as_str(value: Option<&pb::EcValue>) -> Option<&str> {
    match value.and_then(|v| v.kind.as_ref()) {
        Some(pb::ec_value::Kind::StringValue(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn as_u64(value: Option<&pb::EcValue>) -> Option<u64> {
    match value.and_then(|v| v.kind.as_ref()) {
        Some(pb::ec_value::Kind::UintValue(u)) => Some(*u),
        Some(pb::ec_value::Kind::IntValue(i)) => u64::try_from(*i).ok(),
        Some(pb::ec_value::Kind::DoubleValue(d)) if d.fract() == 0.0 && *d >= 0.0 => {
            Some(*d as u64)
        }
        _ => None,
    }
}

fn as_f64(value: Option<&pb::EcValue>) -> Option<f64> {
    match value.and_then(|v| v.kind.as_ref()) {
        Some(pb::ec_value::Kind::DoubleValue(d)) => Some(*d),
        Some(pb::ec_value::Kind::IntValue(i)) => Some(*i as f64),
        Some(pb::ec_value::Kind::UintValue(u)) => Some(*u as f64),
        _ => None,
    }
}

fn is_null(value: Option<&pb::EcValue>) -> bool {
    matches!(
        value.and_then(|v| v.kind.as_ref()),
        Some(pb::ec_value::Kind::NullValue(_))
    )
}

fn map_of(value: Option<&pb::EcValue>) -> Option<BTreeMap<String, pb::EcValue>> {
    match value.and_then(|v| v.kind.as_ref()) {
        Some(pb::ec_value::Kind::MapValue(m)) => Some(m.fields.clone()),
        _ => None,
    }
}

/// The `sequence` extra as a number, whatever integral shape it took on the wire.
fn sequence_of(sample: &pb::Sample) -> u64 {
    as_u64(sample.extra.get("sequence")).unwrap_or_else(|| {
        panic!(
            "every sample carries the agent's `sequence` (D-MTC-6); extras were {:?}",
            sample.extra.keys().collect::<Vec<_>>()
        )
    })
}

fn quality_raw_of(sample: &pb::Sample) -> String {
    as_str(sample.quality_raw.as_ref())
        .unwrap_or_else(|| {
            panic!(
                "this adapter stamps `qualityRaw` on every sample; got {}",
                render(sample.quality_raw.as_ref())
            )
        })
        .to_string()
}

fn extra_str_of(sample: &pb::Sample, key: &str) -> Option<String> {
    as_str(sample.extra.get(key)).map(str::to_string)
}

/// The update-level canonical component path (D-MtconnectAdapter-L13). `Some(path)` — including
/// `Some("")` for a device-level data item — or `None` for the JSON `null` a signal no model
/// describes publishes. The key itself is never absent.
fn component_path_of(update: &pb::SouthboundSignalUpdate) -> Option<String> {
    let value = update
        .extra
        .get("componentPath")
        .unwrap_or_else(|| panic!("`componentPath` is stamped on EVERY update (D-L13)"));
    if is_null(Some(value)) {
        return None;
    }
    Some(
        as_str(Some(value))
            .unwrap_or_else(|| {
                panic!(
                    "`componentPath` is a string or null, got {}",
                    render(Some(value))
                )
            })
            .to_string(),
    )
}

/// Whole-milliseconds-since-epoch of an ISO-8601 UTC stamp, for ordering assertions on real
/// timestamps. Panics (naming the input) on anything that is not one.
fn iso_millis(stamp: &str, what: &str) -> i64 {
    // `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` — the only shape either the agent or this adapter emits.
    let bytes = stamp.as_bytes();
    assert!(
        stamp.len() >= 20 && bytes[4] == b'-' && bytes[10] == b'T' && stamp.ends_with('Z'),
        "{what} must be an ISO-8601 UTC stamp, got {stamp:?}"
    );
    let num = |range: std::ops::Range<usize>| -> i64 {
        stamp[range.clone()]
            .parse()
            .unwrap_or_else(|_| panic!("{what}: {stamp:?} is not ISO-8601 ({range:?})"))
    };
    let (y, mo, d) = (num(0..4), num(5..7), num(8..10));
    let (h, mi, s) = (num(11..13), num(14..16), num(17..19));
    let frac = stamp[19..stamp.len() - 1].strip_prefix('.').map_or(0, |f| {
        let digits: String = f
            .chars()
            .take(3)
            .chain(std::iter::repeat('0'))
            .take(3)
            .collect();
        digits.parse().unwrap_or(0)
    });
    // Days since a fixed civil epoch (Howard Hinnant's days_from_civil) — no chrono dependency.
    let (y, era_y) = if mo <= 2 {
        (y - 1, mo + 9)
    } else {
        (y, mo - 3)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * era_y + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    ((days * 86_400 + h * 3_600 + mi * 60 + s) * 1_000) + frac
}

fn now_millis() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_millis(),
    )
    .expect("a clock before the year 292 million")
}

/// A full, readable dump of one captured publication — the evidence a gate run leaves behind.
fn dump(c: &Captured) -> String {
    let envelope = c.envelope();
    let header = envelope.header.clone().unwrap_or_default();
    let identity = envelope.identity.clone().unwrap_or_default();
    let update = c.update();
    let signal = update.signal.clone().unwrap_or_default();

    let mut out = String::new();
    out.push_str(&format!("topic   : {}\n", c.topic));
    out.push_str(&format!(
        "bytes   : {} (protobuf; leading {:02x?})\n",
        c.bytes.len(),
        &c.bytes[..c.bytes.len().min(8)]
    ));
    out.push_str(&format!(
        "header  : name={:?} version={:?} timestampMs={} uuid={:?}\n",
        header.name, header.version, header.timestamp_ms, header.uuid
    ));
    out.push_str(&format!(
        "identity: path={:?} component={:?} instance={:?} hier=[{}]\n",
        identity.path,
        identity.component,
        identity.instance,
        identity
            .hier
            .iter()
            .map(|h| format!("{}={}", h.level, h.value))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    out.push_str(&format!(
        "signal  : id={:?} name={:?} address={}\n",
        signal.id,
        signal.name,
        render(signal.address.as_ref())
    ));
    for (key, value) in &update.extra {
        out.push_str(&format!("update  : {key} = {}\n", render(Some(value))));
    }
    for (i, s) in update.samples.iter().enumerate() {
        out.push_str(&format!(
            "sample{i} : value={} quality={:?} qualityRaw={} sourceTs={:?} serverTs={:?}\n",
            render(s.value.as_ref()),
            s.quality,
            render(s.quality_raw.as_ref()),
            s.source_ts,
            s.server_ts
        ));
        for (key, value) in &s.extra {
            out.push_str(&format!(
                "sample{i} :   extra {key} = {}\n",
                render(Some(value))
            ));
        }
    }
    out
}

// =================================================================================================
// The wire — the same three public calls `supervisor::FacadeWire` makes
// =================================================================================================

/// [`Wire`] over one instance's **real** `data()`/`events()` facades.
///
/// `supervisor::FacadeWire` is private, so this re-declares it: `DataFacade::build_body` applies the
/// whole §2.1 contract, `app::stamp_component_path` adds the one additive update-level key
/// (D-MtconnectAdapter-L13), and `DataFacade::publish_body_via` mints the `data/{channel}` topic and
/// stamps identity from the same `effective_signal_path`. No body and no topic is built here.
struct GateWire {
    data: DataFacade,
    events: EventsFacade,
}

#[async_trait]
impl Wire for GateWire {
    async fn publish(
        &self,
        update: &SignalUpdate,
        component_path: Option<&str>,
    ) -> edgecommons::Result<()> {
        if update.signal_id.as_deref().unwrap_or_default().is_empty() {
            return Err(EdgeCommonsError::Facade(
                "data publish requires a stable signal.id (the consumer key)".to_string(),
            ));
        }
        if update.samples.is_empty() {
            return Err(EdgeCommonsError::Facade(
                "data publish requires at least one sample".to_string(),
            ));
        }
        let mut body = self.data.build_body(update)?;
        stamp_component_path(&mut body, component_path);
        let path = update
            .effective_signal_path()
            .unwrap_or_default()
            .to_string();
        self.data
            .publish_body_via(&path, body, update.via.clone())
            .await
    }

    async fn emit(
        &self,
        severity: Severity,
        event_type: &str,
        message: Option<String>,
        context: Option<Value>,
    ) -> edgecommons::Result<()> {
        self.events
            .emit(severity, event_type, message, context)
            .await
    }

    async fn raise_alarm(
        &self,
        severity: Severity,
        event_type: &str,
        message: Option<String>,
        context: Option<Value>,
    ) -> edgecommons::Result<()> {
        self.events
            .raise_alarm(severity, event_type, message, context)
            .await
    }

    async fn clear_alarm(
        &self,
        severity: Severity,
        event_type: &str,
        context: Option<Value>,
    ) -> edgecommons::Result<()> {
        self.events.clear_alarm(severity, event_type, context).await
    }
}

// =================================================================================================
// The harness — a genuine EdgeCommons runtime over the real MTConnect stack
// =================================================================================================

/// The signals this gate publishes. `x-position` binds the condition data item, so the same signal
/// proves both the ordinary GOOD path and the D-MTC-8 binding degradation.
fn signals() -> Value {
    json!([
        {
            "id": "x-position",
            "name": "X axis actual position",
            "dataItemId": "d1-Xabs",
            "conditionBinding": ["d1-travel"]
        },
        { "id": "availability", "name": "Device availability", "dataItemId": "d1-avail" },
        { "id": "execution", "name": "Execution state", "dataItemId": "d1-exec" },
        { "id": "x-travel", "name": "X axis travel condition", "dataItemId": "d1-travel" }
    ])
}

/// The component configuration, in the shape `-c FILE` loads.
///
/// `staleSignalSecs` and `heartbeatMs` are deliberately short: the passive ladder is a wall-clock
/// state machine, and a gate that had to wait 30 s per rung would never be run. The two are
/// **ordered**, not merely small — see [`Harness::start`].
fn component_config(agent_url: &str, stale_signal_secs: u64, heartbeat_ms: u64) -> Value {
    json!({
        "logging": { "level": std::env::var("EC_WIRE_GATE_LOG").unwrap_or_else(|_| "WARN".into()) },
        "hierarchy": { "levels": ["site", "device"] },
        "identity": { "site": SITE },
        // Off, so the only `state`-class traffic on the broker is nothing at all and the capture is
        // this component's data and events.
        "heartbeat": { "enabled": false },
        "metricEmission": { "target": "log", "namespace": "edgecommons" },
        "tags": { "site": SITE },
        "component": {
            "token": COMPONENT_TOKEN,
            "global": {
                "agents": [{
                    "id": "live-agent",
                    "url": agent_url,
                    "streaming": "prefer",
                    "heartbeatMs": heartbeat_ms,
                    "pollIntervalMs": 250,
                    "requestTimeoutMs": 3_000,
                    "reconnect": { "initialMs": 200, "maxMs": 1_000 }
                }],
                "defaults": { "pollIntervalMs": 200, "publishMode": "on-change" },
                "healthThresholds": { "staleSignalSecs": stale_signal_secs }
            },
            "instances": [{
                "id": INSTANCE,
                "adapter": "mtconnect",
                "connection": { "agentId": "live-agent", "deviceUuid": DEVICE_UUID },
                "pollIntervalMs": 200,
                "signals": signals(),
                "writes": { "allow": [] }
            }]
        }
    })
}

/// A live component: the real runtime, the real agent runtime, and one real device task.
struct Harness {
    _tmp: tempfile::TempDir,
    gg: EdgeCommons,
    agent: Arc<AgentRuntime>,
    thing: String,
    /// Held for the lifetime of the device task: dropping it closes the control channel, which the
    /// poll loop reads as shutdown.
    _control: mpsc::Sender<mtconnect_adapter::app::DeviceControl>,
    device_token: CancellationToken,
    agent_token: CancellationToken,
    device_task: tokio::task::JoinHandle<()>,
    agent_task: Option<tokio::task::JoinHandle<()>>,
}

impl Harness {
    /// Build and start everything `supervisor::App` builds and starts for one instance, against the
    /// live broker and the live agent.
    ///
    /// `heartbeat_ms` is the agent's `heartbeatMs`, and it is what makes the passive ladder
    /// observable: while a stream is up the liveness window IS `heartbeatMs` (D-R12) and the stream
    /// is declared dead at **twice** it (LLD ladder 1). A silent agent therefore crosses
    /// `Stale` at `heartbeatMs`, `Expired` at `staleSignalSecs`, and `Unreachable` at
    /// `2 x heartbeatMs` — so a run that wants to watch all three rungs must order them
    /// `heartbeatMs < staleSignalSecs < 2 x heartbeatMs`. Anything else is a legitimate
    /// configuration that simply skips a rung (`PassivePhase::of` checks expiry before staleness
    /// precisely so a short `staleSignalSecs` lands straight on `Expired`).
    async fn start(
        agent_url: &str,
        broker: &str,
        stale_signal_secs: u64,
        heartbeat_ms: u64,
    ) -> Harness {
        let tmp = tempfile::tempdir().expect("a temp dir for the config pair");
        let thing = format!(
            "mtc-wire-gate-{}-{}",
            std::process::id(),
            CLIENT_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let (host, port) = split_broker(broker);

        let config_path = tmp.path().join("config.json");
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&component_config(
                agent_url,
                stale_signal_secs,
                heartbeat_ms,
            ))
            .unwrap(),
        )
        .expect("write the component config");

        let messaging_path = tmp.path().join("standalone-messaging.json");
        std::fs::write(
            &messaging_path,
            serde_json::to_vec_pretty(&json!({
                "messaging": { "local": { "host": host, "port": port, "clientId": thing } }
            }))
            .unwrap(),
        )
        .expect("write the messaging config");

        // The supported construction path, and the ONLY one that can mint a `DataFacade`: it
        // connects to the broker before it returns, so a failure here is a real infrastructure
        // failure and is named as one.
        let gg = EdgeCommonsBuilder::new(COMPONENT_NAME)
            .args([
                std::ffi::OsString::from("wire-gate"),
                "--platform".into(),
                "HOST".into(),
                "--transport".into(),
                "MQTT".into(),
                messaging_path.clone().into_os_string(),
                "-c".into(),
                "FILE".into(),
                config_path.clone().into_os_string(),
                "-t".into(),
                thing.clone().into(),
            ])
            .build()
            .await
            .unwrap_or_else(|e| {
                panic!("the EdgeCommons runtime did not come up against {broker}: {e}")
            });

        // --- exactly what `App::new` wires ----------------------------------------------------
        let config = gg.config();
        let mut devices: Vec<DeviceConfig> = config
            .instance_ids()
            .iter()
            .map(|id| {
                serde_json::from_value(
                    config
                        .instance(id)
                        .expect("the configured instance")
                        .clone(),
                )
                .expect("the instance parses as a DeviceConfig")
            })
            .collect();
        let agent_configs = parse_agents(config.global()).expect("the agents parse");
        let budgets = ChannelBudgets::resolve(&gg, devices.iter().map(|d| d.id.as_str()));
        let mtc_devices = compile_mtconnect(
            &mut devices,
            &agent_configs,
            publish_defaults_of(config.global()),
            &budgets,
        )
        .expect("the instances compile against the agents");

        let credentials = gg.credentials();
        let mut agents = std::collections::HashMap::new();
        for cfg in agent_configs {
            let creds = resolve_agent_credentials(&cfg, credentials.as_deref())
                .expect("no vault references in this config");
            let id = cfg.id.clone();
            agents.insert(
                id,
                AgentRuntime::new(cfg, &creds, edgecommons::facades::system_clock())
                    .expect("the agent runtime builds"),
            );
        }
        let agent = Arc::clone(agents.values().next().expect("one configured agent"));
        let backend = Arc::new(MtcBackend::new(agents.clone(), mtc_devices, budgets));
        let registry = backend.signals();

        // --- exactly what `App::run` spawns ---------------------------------------------------
        let agent_token = CancellationToken::new();
        let agent_task = agent.spawn(agent_token.clone());

        let device = devices.first().expect("one configured instance").clone();
        let health = Arc::new(Health::default());
        let dm = Arc::new(DeviceMetrics::new(
            gg.metrics(),
            Arc::clone(&config),
            device.id.clone(),
            Arc::clone(&health),
            stale_signal_secs,
            Some(Arc::clone(&agent) as Arc<dyn AgentTelemetry>),
        ));
        dm.define_all();
        let inventory = backend.inventory(&device.connection);
        health.set_signal_inventory(inventory.len() as u64);

        let (control_tx, control_rx) = mpsc::channel(16);
        let instance = gg.instance(&device.id).expect("the instance handle");
        let wire: Arc<dyn Wire> = Arc::new(GateWire {
            data: instance.data(),
            events: instance.events(),
        });

        // The southbound command surface, so `sb/signals` answers over the same broker.
        if let Some(commands) = gg.commands() {
            register_all(
                &commands,
                vec![DeviceHandle {
                    cfg: device.clone(),
                    control: control_tx.clone(),
                    health: Arc::clone(&health),
                    dm: Arc::clone(&dm),
                    signals: inventory,
                    protocol: ProtocolView::of(&device, &agents, &registry),
                }],
            )
            .expect("the sb/* verbs register");
        }

        let device_token = CancellationToken::new();
        let device_task = tokio::spawn(run_device(
            device,
            Arc::clone(&backend) as Arc<dyn DeviceBackend>,
            wire,
            dm,
            health,
            control_rx,
            stale_signal_secs,
            device_token.clone(),
        ));

        Harness {
            _tmp: tmp,
            gg,
            agent,
            thing,
            _control: control_tx,
            device_token,
            agent_token,
            device_task,
            agent_task,
        }
    }

    /// The `data`-class topic this instance publishes `signal_id` on.
    fn data_topic(&self, channel: &str) -> String {
        format!(
            "ecv1/{}/{COMPONENT_TOKEN}/{INSTANCE}/data/{channel}",
            self.thing
        )
    }

    /// The ordered teardown `App::run` performs: stop and JOIN the device task (so its open
    /// windows are flushed and its session detached while the facade is still alive), then the
    /// agent runtime, then drop the runtime, which unsubscribes the command inbox and disconnects
    /// the broker connection (RAII). Nothing is left leaking a subscription.
    async fn stop(self) {
        self.device_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(10), self.device_task).await;
        self.agent.shutdown().await;
        self.agent_token.cancel();
        if let Some(task) = self.agent_task {
            let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
        }
        drop(self.gg);
    }
}

/// Wait (bounded) until the shared agent runtime is genuinely delivering — D-R1: a cached probe
/// model is not liveness, and a device link cannot open before it.
async fn wait_until_delivering(agent: &Arc<AgentRuntime>, secs: u64) {
    let outcome = tokio::time::timeout(Duration::from_secs(secs), async {
        while !agent.info().connected {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "the agent runtime never began delivering: {:?}",
        agent.info()
    );
}

// =================================================================================================
// 1. The envelope, the extras and the quality vocabulary
// =================================================================================================

#[tokio::test]
async fn the_envelope_extras_and_quality_land_on_the_broker_exactly() {
    let Some((agent_url, broker)) = live_targets() else {
        return;
    };
    let _serial = SERIAL.lock().await;

    let feed = ShdrFeed::start(SHDR_PORT).await;
    // A clean condition slate (a keyless NORMAL is the standard's normal sweep), an available
    // device and a position — `exec` is deliberately never fed, so it stays UNAVAILABLE.
    feed.send("|Xtravel|NORMAL||||");
    feed.send("|avail|AVAILABLE");
    feed.send("|Xabs|10.5");
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let sniffer = Sniffer::start(&broker, "ecv1/#").await;
    let harness = Harness::start(&agent_url, &broker, 30, 1_000).await;
    wait_until_delivering(&harness.agent, 45).await;
    let mut tap = Tap::new(&sniffer);

    // A streamed value, so the update carries a capture stamp AND a distinct arrival stamp.
    let started_ms = now_millis();
    feed.send("|Xabs|11.5");

    let captured = tap
        .next_where(60, "the streamed x-position update", |c| {
            c.signal_id() == "x-position"
                && c.update()
                    .samples
                    .iter()
                    .any(|s| as_f64(s.value.as_ref()) == Some(11.5))
        })
        .await;

    println!(
        "EVIDENCE captured SouthboundSignalUpdate:\n{}",
        dump(&captured)
    );

    // --- the bytes ---------------------------------------------------------------------------
    // Field 1 (`header`), wire type 2 — a protobuf envelope, not the JSON a raw publish would be.
    assert_eq!(
        captured.bytes[0],
        0x0A,
        "the payload opens with protobuf field 1 (header), not {:?}",
        &captured.bytes[..captured.bytes.len().min(8)]
    );
    assert_ne!(captured.bytes[0], b'{', "the data class is never JSON");

    // --- the topic ---------------------------------------------------------------------------
    assert_eq!(
        captured.topic,
        harness.data_topic("x-position"),
        "ecv1/{{device}}/{{component}}/{{instance}}/data/{{channel}} - the channel is the \
         sanitized signal id when the signal configures none"
    );
    let tokens: Vec<&str> = captured.topic.split('/').collect();
    assert_eq!(tokens[0], "ecv1");
    assert_eq!(tokens[1], harness.thing);
    assert_eq!(tokens[2], COMPONENT_TOKEN);
    assert_eq!(tokens[3], INSTANCE);
    assert_eq!(tokens[4], "data");
    assert_eq!(tokens.len(), 6, "one channel token: {}", captured.topic);

    // --- the envelope ------------------------------------------------------------------------
    let envelope = captured.envelope();
    let header = envelope
        .header
        .clone()
        .expect("every envelope has a header");
    assert_eq!(header.name, DATA_MESSAGE_NAME);
    assert_eq!(header.version, DATA_MESSAGE_VERSION);
    assert!(
        !header.uuid.is_empty(),
        "the library stamps a message uuid: {header:?}"
    );

    let identity = envelope
        .identity
        .clone()
        .expect("config-built messages stamp the TOP-LEVEL identity element");
    assert_eq!(identity.component, COMPONENT_TOKEN);
    assert_eq!(identity.instance, INSTANCE);
    assert_eq!(identity.path, format!("{SITE}/{}", harness.thing));
    assert_eq!(
        identity
            .hier
            .iter()
            .map(|h| (h.level.as_str(), h.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("site", SITE), ("device", harness.thing.as_str())]
    );

    // --- the signal --------------------------------------------------------------------------
    let update = captured.update();
    let signal = update.signal.clone().expect("a signal block");
    assert_eq!(signal.id, "x-position");
    assert_eq!(signal.name, "X axis actual position");
    // D-MtconnectAdapter-L13 rejected promoting the MTConnect §5.3 address onto the southbound
    // data contract; the canonical path rides the update-level extra instead, and the address is
    // served by `sb/signals` (proven over this same broker below).
    assert!(
        signal.address.is_none(),
        "the data class carries no address block by design (D-L13); got {}",
        render(signal.address.as_ref())
    );
    assert_eq!(
        component_path_of(&update).as_deref(),
        Some("Axes[axes]/Linear[X]"),
        "the untruncated canonical component path, unconditionally, at update level"
    );

    // The `device` block rides the update-level extras (the body reserves only `signal`/`samples`).
    let device = map_of(update.extra.get("device")).expect("a device block");
    assert_eq!(as_str(device.get("adapter")), Some("mtconnect"));
    assert_eq!(as_str(device.get("instance")), Some(INSTANCE));
    let (agent_host, agent_port) = {
        let url = url::Url::parse(&agent_url).expect("EC_MTC_AGENT is a URL");
        (
            url.host_str().expect("a host").to_string(),
            url.port().expect("an explicit port"),
        )
    };
    assert_eq!(
        as_str(device.get("endpoint")),
        Some(format!("mtconnect://{agent_host}:{agent_port}/{DEVICE_UUID}").as_str()),
        "the endpoint is DERIVED from the agent URL and the device uuid, never configured"
    );

    // --- the sample: value, quality, and the extras this gate exists for ----------------------
    assert_eq!(update.samples.len(), 1, "an unshaped signal publishes one");
    let sample = &update.samples[0];
    assert_eq!(as_f64(sample.value.as_ref()), Some(11.5));
    assert_eq!(sample.quality, "GOOD");
    assert_eq!(quality_raw_of(sample), "MTC_OK");
    assert_eq!(
        sample.source_ts, None,
        "MTConnect has no device-authored time, so `sourceTs` is never synthesized"
    );

    let info = harness.agent.info();
    let (first, next) = (
        info.first_sequence.expect("a firstSequence"),
        info.next_sequence.expect("a nextSequence"),
    );
    let sequence = sequence_of(sample);
    assert!(
        (first..next).contains(&sequence),
        "`sequence` is the LIVE agent's own numbering: {sequence} outside {first}..{next}"
    );

    let server_ts = sample.server_ts.clone().expect("the agent's capture stamp");
    let received_ts = extra_str_of(sample, "receivedTs")
        .expect("a mediated protocol's arrival stamp rides as `receivedTs`");
    let server_ms = iso_millis(&server_ts, "serverTs");
    let received_ms = iso_millis(&received_ts, "receivedTs");
    // `receivedTs` is present at all only because the two moments DIFFER: for a mediated protocol
    // the agent's capture stamp is not this adapter's arrival stamp, and `sample_timestamps` emits
    // the extra exactly when it has something to add.
    assert_ne!(
        received_ts, server_ts,
        "a mediating server makes the receive moment differ from `serverTs`; that difference IS \
         the reason the extra exists"
    );
    // The two stamps come from two different CLOCKS — the agent's is minted inside the container,
    // `receivedTs` by this adapter on the host — so their relative order is a property of NTP, not
    // of the adapter, and is deliberately not asserted. What IS the adapter's own property: the
    // arrival stamp is drawn from the same clock this test reads, during this run.
    assert!(
        received_ms >= started_ms - 5_000 && received_ms <= now_millis() + 5_000,
        "`receivedTs` is this run's own arrival moment off this host's clock, not a replayed or \
         agent-supplied one: {received_ts} (the run reached this point at {started_ms} ms)"
    );
    assert!(
        !sample.extra.contains_key("passive"),
        "a delivered sample is not a synthetic transition"
    );

    println!(
        "EVIDENCE extras: sequence={sequence} (agent window {first}..{next}) serverTs={server_ts} \
         receivedTs={received_ts} (host-vs-container clock skew {} ms) componentPath={:?}",
        received_ms - server_ms,
        component_path_of(&update)
    );

    // --- UNAVAILABLE is a BAD explicit null (HLD §5.3) ----------------------------------------
    let unavailable = sniffer
        .wait_any(30, "the never-fed `execution` signal", |c| {
            c.signal_id() == "execution"
        })
        .await;
    let update = unavailable.update();
    let sample = &update.samples[0];
    assert!(
        is_null(sample.value.as_ref()),
        "UNAVAILABLE publishes an explicit null, never a 0: {}",
        render(sample.value.as_ref())
    );
    assert_eq!(
        sample.quality, "BAD",
        "the explicit-null opt-in gates the null's PERMISSION, not its quality"
    );
    assert_eq!(quality_raw_of(sample), "UNAVAILABLE");
    assert_eq!(
        component_path_of(&update).as_deref(),
        Some("Controller[controller]"),
        "the controller-level data item's canonical path"
    );

    // --- a device-level data item's component path is the empty string, not null ---------------
    let availability = sniffer
        .wait_any(30, "the AVAILABLE availability event", |c| {
            c.signal_id() == "availability"
                && as_str(c.update().samples[0].value.as_ref()) == Some("AVAILABLE")
        })
        .await;
    let update = availability.update();
    assert_eq!(
        as_str(update.samples[0].value.as_ref()),
        Some("AVAILABLE"),
        "an EVENT publishes its enum verbatim"
    );
    assert_eq!(update.samples[0].quality, "GOOD");
    assert_eq!(quality_raw_of(&update.samples[0]), "MTC_OK");
    assert_eq!(
        component_path_of(&update).as_deref(),
        Some(""),
        "a device-level data item hangs off no component - and the key is still present"
    );

    // --- a cleared condition publishes NORMAL with a zero activation count ----------------------
    let normal = sniffer
        .wait_any(30, "the swept condition", |c| {
            c.signal_id() == "x-travel"
                && as_str(c.update().samples[0].value.as_ref()) == Some("NORMAL")
        })
        .await;
    let update = normal.update();
    let sample = &update.samples[0];
    assert_eq!(as_str(sample.value.as_ref()), Some("NORMAL"));
    assert_eq!(sample.quality, "GOOD");
    assert_eq!(quality_raw_of(sample), "MTC_OK:NORMAL");
    assert_eq!(
        as_u64(sample.extra.get("activeConditions")),
        Some(0),
        "nothing is asserted"
    );

    println!(
        "EVIDENCE quality vocabulary proved on the wire: GOOD/MTC_OK, GOOD/MTC_OK:NORMAL, \
         BAD/UNAVAILABLE (explicit null)"
    );

    harness.stop().await;
    sniffer.stop().await;
}

// =================================================================================================
// 2. Concurrent condition activations, on the wire (the live half of P1-3)
// =================================================================================================

#[tokio::test]
async fn concurrent_condition_activations_survive_to_the_broker() {
    let Some((agent_url, broker)) = live_targets() else {
        return;
    };
    let _serial = SERIAL.lock().await;

    let feed = ShdrFeed::start(SHDR_PORT).await;
    feed.send("|Xtravel|NORMAL||||");
    feed.send("|avail|AVAILABLE");
    feed.send("|Xabs|20.5");
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let sniffer = Sniffer::start(&broker, "ecv1/#").await;
    let harness = Harness::start(&agent_url, &broker, 30, 1_000).await;
    wait_until_delivering(&harness.agent, 45).await;
    let mut tap = Tap::new(&sniffer);

    let travel = |c: &Captured| c.signal_id() == "x-travel";

    // --- one activation ------------------------------------------------------------------------
    feed.send("|Xtravel|FAULT|ALM-1|HIGH||X travel limit exceeded");
    let one = tap
        .next_where(45, "the FAULT activation", |c| {
            travel(c) && as_u64(c.update().samples[0].extra.get("activeConditions")) == Some(1)
        })
        .await;
    let sample = one.update().samples[0].clone();
    assert_eq!(as_str(sample.value.as_ref()), Some("FAULT"));
    assert_eq!(sample.quality, "BAD");
    assert_eq!(quality_raw_of(&sample), "MTC_CONDITION:FAULT:ALM-1");
    assert_eq!(
        extra_str_of(&sample, "conditionId").as_deref(),
        Some("ALM-1")
    );
    assert_eq!(
        extra_str_of(&sample, "nativeCode").as_deref(),
        Some("ALM-1")
    );
    assert_eq!(
        extra_str_of(&sample, "nativeSeverity").as_deref(),
        Some("HIGH")
    );

    // --- a SECOND, concurrent activation of the SAME data item ---------------------------------
    // The whole point: the Warning does not replace the Fault, and the aggregate stays BAD.
    feed.send("|Xtravel|WARNING|ALM-2|LOW||X drift");
    let two = tap
        .next_where(45, "the concurrent WARNING activation", |c| {
            travel(c) && as_u64(c.update().samples[0].extra.get("activeConditions")) == Some(2)
        })
        .await;
    let sample = two.update().samples[0].clone();
    assert_eq!(
        extra_str_of(&sample, "conditionId").as_deref(),
        Some("ALM-2"),
        "this observation's own activation identity rides in the extras"
    );
    assert_eq!(
        as_str(sample.value.as_ref()),
        Some("FAULT"),
        "the published value is the AGGREGATE, not this observation's own transition"
    );
    assert_eq!(sample.quality, "BAD");
    assert_eq!(
        quality_raw_of(&sample),
        "MTC_CONDITION:FAULT:ALM-1",
        "the worst activation names the alarm an operator acts on"
    );
    println!("EVIDENCE two concurrent activations:\n{}", dump(&two));

    // --- clear ONE of the two: the signal stays degraded by the one still asserted --------------
    feed.send("|Xtravel|NORMAL|ALM-2|||");
    feed.send("|Xabs|21.5");
    let still = tap
        .next_where(45, "the WARNING cleared while the FAULT stands", |c| {
            travel(c) && as_u64(c.update().samples[0].extra.get("activeConditions")) == Some(1)
        })
        .await;
    let sample = still.update().samples[0].clone();
    assert_eq!(
        extra_str_of(&sample, "conditionId").as_deref(),
        Some("ALM-2"),
        "the clearing observation identifies WHICH activation went away"
    );
    assert_eq!(
        as_str(sample.value.as_ref()),
        Some("FAULT"),
        "P1-3: clearing one activation must NOT promote the data item to NORMAL"
    );
    assert_eq!(sample.quality, "BAD");
    assert_eq!(quality_raw_of(&sample), "MTC_CONDITION:FAULT:ALM-1");
    println!(
        "EVIDENCE one cleared, one still asserted:\n{}",
        dump(&still)
    );

    // ...and the signal BOUND to that condition is degraded by it too (D-MTC-8), even though its
    // own value is a perfectly good number.
    let bound = tap
        .next_where(45, "the bound x-position reading", |c| {
            c.signal_id() == "x-position"
                && c.update()
                    .samples
                    .iter()
                    .any(|s| as_f64(s.value.as_ref()) == Some(21.5))
        })
        .await;
    let sample = bound.update().samples[0].clone();
    assert_eq!(as_f64(sample.value.as_ref()), Some(21.5));
    assert_eq!(sample.quality, "BAD");
    assert_eq!(
        quality_raw_of(&sample),
        "MTC_CONDITION:FAULT:ALM-1",
        "a bound condition can only make a reading worse, never better"
    );

    // --- now clear the FAULT and leave a fresh WARNING standing ---------------------------------
    feed.send("|Xtravel|WARNING|ALM-2|LOW||X drift");
    tap.next_where(45, "the re-raised WARNING", |c| {
        travel(c) && as_u64(c.update().samples[0].extra.get("activeConditions")) == Some(2)
    })
    .await;
    feed.send("|Xtravel|NORMAL|ALM-1|||");
    let warning = tap
        .next_where(45, "the WARNING aggregate after the FAULT cleared", |c| {
            travel(c)
                && as_u64(c.update().samples[0].extra.get("activeConditions")) == Some(1)
                && c.update().samples[0].quality == "UNCERTAIN"
        })
        .await;
    let sample = warning.update().samples[0].clone();
    assert_eq!(as_str(sample.value.as_ref()), Some("WARNING"));
    assert_eq!(sample.quality, "UNCERTAIN");
    assert_eq!(
        quality_raw_of(&sample),
        "MTC_CONDITION:WARNING:ALM-2",
        "the remaining activation now governs the signal"
    );
    println!(
        "EVIDENCE the surviving WARNING governs:\n{}",
        dump(&warning)
    );

    // --- and the standard's normal sweep clears everything --------------------------------------
    feed.send("|Xtravel|NORMAL||||");
    let cleared = tap
        .next_where(45, "the normal sweep", |c| {
            travel(c) && as_u64(c.update().samples[0].extra.get("activeConditions")) == Some(0)
        })
        .await;
    let sample = cleared.update().samples[0].clone();
    assert_eq!(as_str(sample.value.as_ref()), Some("NORMAL"));
    assert_eq!(sample.quality, "GOOD");
    assert_eq!(quality_raw_of(&sample), "MTC_OK:NORMAL");

    harness.stop().await;
    sniffer.stop().await;
}

// =================================================================================================
// 3. Passive quality, on the wire (the live half of P1-5)
// =================================================================================================

#[tokio::test]
async fn passive_quality_transitions_reach_the_broker_when_the_agent_goes_silent() {
    let Some((agent_url, broker)) = live_targets() else {
        return;
    };
    let _serial = SERIAL.lock().await;

    let feed = ShdrFeed::start(SHDR_PORT).await;
    feed.send("|Xtravel|NORMAL||||");
    feed.send("|avail|AVAILABLE");
    feed.send("|Xabs|30.5");
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let sniffer = Sniffer::start(&broker, "ecv1/#").await;
    // `heartbeatMs: 3000` and `staleSignalSecs: 4`, in that order for a reason: the three rungs are
    // crossed at 3 s (one missed heartbeat), 4 s (`staleSignalSecs`) and 6 s (2 x heartbeat — the
    // stream is declared dead and the connectivity authority marks the agent down), all measured
    // from the same moment the agent last vouched for currency. A 1 s heartbeat — the value the
    // other tests use, because it connects fastest — would declare the agent unreachable at 2 s and
    // no integral `staleSignalSecs` could ever sit between the two, so `Expired` would be skipped.
    const STALE_AFTER_MS: u64 = 4_000;
    const LIVENESS_WINDOW_MS: u64 = 3_000;
    let harness = Harness::start(
        &agent_url,
        &broker,
        STALE_AFTER_MS / 1_000,
        LIVENESS_WINDOW_MS,
    )
    .await;
    wait_until_delivering(&harness.agent, 45).await;
    let mut tap = Tap::new(&sniffer);

    // Something must be HELD before it can be degraded: this is the reading the ladder republishes.
    feed.send("|Xabs|31.5");
    let held = tap
        .next_where(60, "the reading the watchdog will hold", |c| {
            c.signal_id() == "x-position"
                && c.update()
                    .samples
                    .iter()
                    .any(|s| as_f64(s.value.as_ref()) == Some(31.5))
        })
        .await;
    let held_sample = held.update().samples[0].clone();
    let held_sequence = sequence_of(&held_sample);
    let held_server_ts = held_sample.server_ts.clone().expect("a capture stamp");
    assert_eq!(held_sample.quality, "GOOD");

    // Freeze the agent: the socket stays open, so this is silence rather than a closed connection —
    // exactly the failure a TCP-only view of liveness never notices.
    let frozen = FrozenAgent::freeze(AGENT_CONTAINER).await;
    let paused_at = std::time::Instant::now();
    let paused_ms = now_millis();

    let passive_of = |c: &Captured, marker: &str| -> bool {
        c.signal_id() == "x-position"
            && extra_str_of(&c.update().samples[0], "passive").as_deref() == Some(marker)
    };

    // --- rung 1: one missed heartbeat --------------------------------------------------------
    let stale = tap
        .next_where(45, "the `stale` transition", |c| passive_of(c, "stale"))
        .await;
    let sample = stale.update().samples[0].clone();
    assert_eq!(
        as_f64(sample.value.as_ref()),
        Some(31.5),
        "the HELD value is republished - this update says `I no longer vouch for it`"
    );
    assert_eq!(sample.quality, "UNCERTAIN");
    let raw = quality_raw_of(&sample);
    let age_ms: u64 = raw
        .strip_prefix("MTC_STALE:")
        .unwrap_or_else(|| panic!("`MTC_STALE:<ageMs>`, got {raw:?}"))
        .parse()
        .unwrap_or_else(|e| panic!("the age is whole milliseconds, got {raw:?}: {e}"));
    assert!(
        age_ms > LIVENESS_WINDOW_MS && age_ms <= STALE_AFTER_MS,
        "the liveness age is past the {LIVENESS_WINDOW_MS} ms window but not yet past \
         `staleSignalSecs` - a freshly measured age, not a placeholder: {raw}"
    );
    assert_eq!(
        sequence_of(&sample),
        held_sequence,
        "the held sequence still names the observation the sample describes (D-MTC-6)"
    );
    assert_eq!(
        sample.server_ts.as_deref(),
        Some(held_server_ts.as_str()),
        "the value's own truth-time is history and stays as it was"
    );
    // The emission moment rides beside the frozen `serverTs` as the adapter's receive stamp — and
    // it is the moment the TRANSITION was emitted, not the moment the value arrived, so it lands
    // inside the pause window rather than back with the held reading.
    let emitted_ts = extra_str_of(&sample, "receivedTs")
        .expect("the emission moment rides beside it as the adapter's receive stamp");
    let emitted_ms = iso_millis(&emitted_ts, "the transition's receivedTs");
    assert!(
        emitted_ms >= paused_ms && emitted_ms <= now_millis() + 5_000,
        "the stale transition was stamped while the agent was frozen: {emitted_ts} \
         (the pause began at {paused_ms} ms)"
    );
    assert_eq!(
        component_path_of(&stale.update()).as_deref(),
        Some("Axes[axes]/Linear[X]"),
        "a synthetic transition is a full, ordinary update"
    );
    println!("EVIDENCE passive `stale`:\n{}", dump(&stale));

    // --- rung 2: past `staleSignalSecs` ------------------------------------------------------
    let expired = tap
        .next_where(45, "the `expired` transition", |c| passive_of(c, "expired"))
        .await;
    let sample = expired.update().samples[0].clone();
    assert_eq!(
        as_f64(sample.value.as_ref()),
        Some(31.5),
        "still the held value"
    );
    assert_eq!(sample.quality, "BAD");
    let raw = quality_raw_of(&sample);
    let expired_age: u64 = raw
        .strip_prefix("MTC_STALE:")
        .unwrap_or_else(|| panic!("`MTC_STALE:<ageMs>`, got {raw:?}"))
        .parse()
        .expect("whole milliseconds");
    assert!(
        expired_age > STALE_AFTER_MS && expired_age > age_ms,
        "expiry is past `staleSignalSecs` ({STALE_AFTER_MS} ms) and later than the stale rung: \
         {expired_age} vs {age_ms}"
    );
    assert_eq!(sequence_of(&sample), held_sequence);
    println!("EVIDENCE passive `expired`: qualityRaw={raw}");

    // --- rung 3: the connectivity authority gives up ------------------------------------------
    let unreachable = tap
        .next_where(90, "the `unreachable` transition", |c| {
            passive_of(c, "unreachable")
        })
        .await;
    let sample = unreachable.update().samples[0].clone();
    assert_eq!(as_f64(sample.value.as_ref()), Some(31.5));
    assert_eq!(sample.quality, "BAD");
    assert_eq!(
        quality_raw_of(&sample),
        "MTC_AGENT_UNREACHABLE",
        "the link is named, not a clock: the agent stopped answering at all"
    );
    assert_eq!(sequence_of(&sample), held_sequence);
    println!(
        "EVIDENCE passive `unreachable` after {:?}:\n{}",
        paused_at.elapsed(),
        dump(&unreachable)
    );

    // The whole ladder is the CADENCE change consumers see: an on-change protocol that had said
    // nothing for seconds now speaks three times without the machine producing a single new value.
    drop(frozen);

    harness.stop().await;
    sniffer.stop().await;
}

// =================================================================================================
// 4. The §5.3 address, where the design puts it
// =================================================================================================

/// D-MtconnectAdapter-L13 keeps the MTConnect address block OFF the southbound data contract, so
/// the round-trippable §5.3 address is served by `sb/signals`. This proves it over the same broker,
/// through a real request/reply — the address a consumer actually gets, decoded from the reply
/// envelope rather than read out of the process that built it.
#[tokio::test]
async fn the_5_3_address_round_trips_over_the_bus() {
    let Some((agent_url, broker)) = live_targets() else {
        return;
    };
    let _serial = SERIAL.lock().await;

    let feed = ShdrFeed::start(SHDR_PORT).await;
    feed.send("|Xtravel|NORMAL||||");
    feed.send("|avail|AVAILABLE");
    feed.send("|Xabs|40.5");
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let harness = Harness::start(&agent_url, &broker, 30, 1_000).await;
    wait_until_delivering(&harness.agent, 45).await;
    // The address is enriched from the cached probe model; give the runtime its first probe.
    tokio::time::timeout(Duration::from_secs(30), async {
        while harness.agent.model(DEVICE_UUID).is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the runtime probed the device model");

    let topic = format!(
        "ecv1/{}/{COMPONENT_TOKEN}/{INSTANCE}/cmd/sb/signals",
        harness.thing
    );
    let request = MessageBuilder::new("sb/signals", "1.0")
        .command(json!({}))
        .build();
    let reply = harness
        .gg
        .messaging()
        .expect("a wired messaging transport")
        .request_with_timeout(&topic, request, Some(Duration::from_secs(20)))
        .await
        .expect("the request goes out")
        .await
        .expect("the sb/signals verb answers");

    assert_eq!(
        reply.body["ok"],
        json!(true),
        "the verb succeeded: {}",
        reply.body
    );
    assert_eq!(reply.body["verb"], json!("sb/signals"));
    assert_eq!(reply.body["result"]["id"], json!(INSTANCE));
    let rows = reply.body["result"]["signals"]
        .as_array()
        .unwrap_or_else(|| panic!("sb/signals answers with `result.signals[]`: {}", reply.body));
    let row = rows
        .iter()
        .find(|r| r["id"] == json!("x-position"))
        .unwrap_or_else(|| panic!("the configured signal is served: {}", reply.body));
    println!(
        "EVIDENCE sb/signals row over {topic}:\n{}",
        serde_json::to_string_pretty(row).unwrap()
    );

    assert_eq!(
        row["address"],
        json!({
            "protocol": "mtconnect",
            "agentId": "live-agent",
            "deviceUuid": DEVICE_UUID,
            "dataItemId": "d1-Xabs",
            "category": "SAMPLE",
            "type": "POSITION",
            "subType": "ACTUAL",
            "componentPath": "Axes[axes]/Linear[X]"
        }),
        "the whole §5.3 address, round-tripped through the protobuf envelope"
    );
    assert_eq!(row["units"], json!("MILLIMETER"));
    assert_eq!(row["conditionBinding"], json!(["d1-travel"]));
    assert_eq!(row["bound"], json!(true));
    assert_eq!(row["writable"], json!(false), "MTConnect is read-only");

    // The condition data item's own row, so the address of the item this gate drives is pinned too.
    let condition = rows
        .iter()
        .find(|r| r["id"] == json!("x-travel"))
        .expect("the condition signal is served");
    assert_eq!(condition["address"]["category"], json!("CONDITION"));
    assert_eq!(condition["address"]["dataItemId"], json!("d1-travel"));
    assert_eq!(
        condition["address"]["componentPath"],
        json!("Axes[axes]/Linear[X]")
    );

    harness.stop().await;
}
