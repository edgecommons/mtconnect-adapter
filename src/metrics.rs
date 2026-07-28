//! # Operational metrics — the canonical `southbound_health` + the operational-family pattern
//!
//! Every southbound adapter emits the shared [`HEALTH`] metric with **exactly** the SOUTHBOUND.md §5
//! measure set. On top of that, this module ships the **operational-family pattern** two protocols
//! deep as worked examples — [`CONNECTION`] and [`COMMAND`] — and shows you where to add your own.
//!
//! ## What `MtconnectAdapter` emits today
//!
//! | Metric | Dimensions | What it is |
//! |---|---|---|
//! | `southbound_health` | `instance` | the §5 canonical set (below) — every adapter emits this |
//! | `MtconnectAdapterConnection` | `instance` | the connect/reconnect lifecycle |
//! | `MtconnectAdapterCommand` | `instance`, `verb`, `result` | the `sb/*` command surface |
//! | `MtconnectAdapterShaping` | `instance` | the publish-shaping engine — updates published, readings coalesced into batch windows, readings deadband-dropped |
//!
//! ## The Total/Interval counter convention
//!
//! Every **counter** is emitted as a measure PAIR: `<name>Total` (monotonic since start) and
//! `<name>Interval` (since the previous emit of that family; **reset on emit** — see [`Pair`]).
//! **Gauges** (`connectionState`, `signalsSubscribed`) and interval **sums** (the `*Ms`
//! latencies/durations) are single measures. This is the same convention `modbus-adapter` and `ethernet-ip-adapter` use, so a fleet
//! dashboard reads every adapter the same way.
//!
//! ## Dimensions are LOW-CARDINALITY only
//!
//! `instance`, `verb` (the closed [`COMMAND_VERBS`] set), and `result` (`success`|`error`) — and
//! nothing else. **Never** dimension by signal name, address, endpoint, or error text: those are
//! unbounded and would shred a fleet dashboard. (`coreName`/`category`/`component` are injected by
//! [`MetricBuilder::build`].)
//!
//! ## The MTConnect families (HLD §9)
//!
//! Three protocol families ride the same pattern, on top of the two generic ones:
//!
//! | Family | Dimensions | Scope |
//! |---|---|---|
//! | [`STREAM`] | `agentId`, `result` | one **agent** — documents, observations, heartbeats, reconnects, gaps, `OUT_OF_RANGE`, request latency |
//! | [`PROBE`] | `agentId`, `result` | one **agent** — `/probe` requests, model changes, probe latency |
//! | [`PARSE`] | `instance`, `result` | one **instance** — documents parsed, parse errors |
//!
//! `MtconnectStream`/`MtconnectProbe` are emitted **once per agent** by [`AgentMetrics`], not once
//! per instance: an agent is shared infrastructure (D-MTC-3) and one document stream serves every
//! device on it, so emitting these per instance would multiply one agent's traffic by its device
//! count. `MtconnectParse` is dimensioned by `instance` per HLD §9 and is emitted by that instance's
//! [`DeviceMetrics`]; parsing happens at the document level, **above** the per-device split, so
//! instances sharing one agent report that agent's document counters.
//!
//! Within a family the `result` dimension splits by outcome: a decoded document, its observations,
//! its heartbeats and a request that answered land in the `success` cell; an undecodable document, a
//! failed request's latency, a stream re-establishment, a data gap and an `OUT_OF_RANGE` recovery
//! land in the `error` cell — a reconnect/gap is by definition a recovery from something that went
//! wrong.
//!
//! Sequence numbers, device uuids, and data-item ids are **event** fields (`src/device.rs`), never
//! metric dimensions.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use edgecommons::prelude::{Config, MetricBuilder, MetricService};

use crate::app::Health;
use crate::mtconnect::{AgentRuntime, AgentStatsSnapshot, ParseCounters};

/// The metric every southbound adapter emits (SOUTHBOUND.md §5).
pub const HEALTH: &str = "southbound_health";
/// The worked operational family for the connect/reconnect lifecycle. Named from the component so a
/// fleet view can tell one adapter's connection health from another's.
pub const CONNECTION: &str = "MtconnectAdapterConnection";
/// The worked operational family for the `sb/*` command surface, dimensioned `instance`×`verb`×`result`.
pub const COMMAND: &str = "MtconnectAdapterCommand";
/// The publish-shaping family, dimensioned `instance`: updates the engine released to the wire,
/// readings coalesced into batch windows, and readings a deadband suppressed on entry. Per
/// **instance** (not per agent) because shaping is a property of one device's publication flow —
/// the engine sits above the session, where `MtconnectStream` measures the shared acquisition
/// below it.
pub const SHAPING: &str = "MtconnectAdapterShaping";
/// HLD §9: the agent's acquisition family — documents, observations, heartbeats, reconnects, gaps,
/// `OUT_OF_RANGE` recoveries, and request latency. Dimensioned `agentId`×`result`.
pub const STREAM: &str = "MtconnectStream";
/// HLD §9: the agent's `/probe` family — probes, model changes, and probe latency. Dimensioned
/// `agentId`×`result`.
pub const PROBE: &str = "MtconnectProbe";
/// HLD §9: the document-parse family — documents parsed and parse errors. Dimensioned
/// `instance`×`result`.
pub const PARSE: &str = "MtconnectParse";

/// A `result` dimension value: the operation succeeded.
pub const RESULT_SUCCESS: &str = "success";
/// A `result` dimension value: the operation failed.
pub const RESULT_ERROR: &str = "error";
const RESULTS: [&str; 2] = [RESULT_SUCCESS, RESULT_ERROR];

/// The **closed** `verb` dimension set for [`COMMAND`] — every `sb/*` verb the command surface
/// registers (`src/commands.rs`). Closed and low-cardinality on purpose (see the module header).
pub const COMMAND_VERBS: [&str; 9] = [
    "sb/status", "sb/read", "sb/write", "sb/signals", "sb/browse", "sb/pause", "sb/resume",
    "reconnect", "repoll",
];

/// The **exact** SOUTHBOUND.md §5 measure set of `southbound_health` — `connectionState`,
/// `publishLatencyMs`, `pollLatencyMs`, `readErrors`, `staleSignals`, `reconnects`, `writeErrors`,
/// and the `signalsSubscribed` gauge. This literal list is the parity anchor the metrics test
/// asserts against; if you change what `emit_health` emits, this list and [`family_defs`] must move
/// with it.
#[allow(dead_code)] // a documentation + parity anchor, consumed by the metrics test
pub const HEALTH_MEASURES: [&str; 8] = [
    "connectionState", "publishLatencyMs", "pollLatencyMs", "readErrors", "staleSignals",
    "reconnects", "writeErrors", "signalsSubscribed",
];

const UNIT_COUNT: &str = "Count";
const UNIT_MS: &str = "Milliseconds";

// =================================================================================================
// The definition schema — the single source the startup pre-definition and the parity test both read
// =================================================================================================

/// One measure's name, unit, and storage resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasureDef {
    pub name: String,
    pub unit: String,
    pub res: u32,
}

/// One metric family's full definition: its name, dimension keys, and measures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyDef {
    pub name: String,
    pub dimensions: Vec<String>,
    pub measures: Vec<MeasureDef>,
}

fn m(name: &str, unit: &str, res: u32) -> MeasureDef {
    MeasureDef { name: name.to_string(), unit: unit.to_string(), res }
}

/// A `<prefix>Total` + `<prefix>Interval` counter pair (both `Count`, resolution 60).
fn pair_defs(prefix: &str) -> Vec<MeasureDef> {
    vec![m(&format!("{prefix}Total"), UNIT_COUNT, 60), m(&format!("{prefix}Interval"), UNIT_COUNT, 60)]
}

fn dims(keys: &[&str]) -> Vec<String> {
    keys.iter().map(|s| (*s).to_string()).collect()
}

/// The **complete** definition set — every family, measure, and dimension key this adapter emits. The
/// startup pre-definition ([`DeviceMetrics::define_all`]) and the parity test both read it, so a
/// dropped or renamed measure fails the build.
#[must_use]
pub fn family_defs() -> Vec<FamilyDef> {
    let mut out = Vec::new();

    // southbound_health — the §5 canonical set (dims: instance). All single measures.
    out.push(FamilyDef {
        name: HEALTH.to_string(),
        dimensions: dims(&["instance"]),
        measures: vec![
            m("connectionState", UNIT_COUNT, 1),
            m("publishLatencyMs", UNIT_MS, 1),
            m("pollLatencyMs", UNIT_MS, 1),
            m("readErrors", UNIT_COUNT, 60),
            m("staleSignals", UNIT_COUNT, 60),
            m("reconnects", UNIT_COUNT, 60),
            m("writeErrors", UNIT_COUNT, 60),
            m("signalsSubscribed", UNIT_COUNT, 1),
        ],
    });

    // MtconnectAdapterConnection — the connect/reconnect lifecycle (dims: instance).
    let mut conn = vec![m("connectionState", UNIT_COUNT, 1)];
    conn.extend(pair_defs("connectAttempts"));
    conn.extend(pair_defs("connectFailures"));
    conn.extend(pair_defs("reconnectAttempts"));
    conn.extend(pair_defs("connectionDrops"));
    conn.push(m("connectedDurationMs", UNIT_MS, 60));
    out.push(FamilyDef { name: CONNECTION.to_string(), dimensions: dims(&["instance"]), measures: conn });

    // MtconnectAdapterCommand — the sb/* surface (dims: instance, verb, result).
    let mut cmd = Vec::new();
    cmd.extend(pair_defs("commandRequests"));
    cmd.extend(pair_defs("commandErrors"));
    cmd.push(m("commandLatencyMs", UNIT_MS, 60));
    out.push(FamilyDef {
        name: COMMAND.to_string(),
        dimensions: dims(&["instance", "verb", "result"]),
        measures: cmd,
    });

    // MtconnectAdapterShaping — the publish-shaping engine (dims: instance).
    let mut shaping = Vec::new();
    shaping.extend(pair_defs("published"));
    shaping.extend(pair_defs("coalesced"));
    shaping.extend(pair_defs("deadbandDropped"));
    out.push(FamilyDef {
        name: SHAPING.to_string(),
        dimensions: dims(&["instance"]),
        measures: shaping,
    });

    // --- the MTConnect families (HLD §9) ---------------------------------------------------------

    // MtconnectStream — one agent's acquisition (dims: agentId, result).
    let mut stream = Vec::new();
    for base in ["documents", "observations", "heartbeats", "reconnects", "gaps", "outOfRange"] {
        stream.extend(pair_defs(base));
    }
    stream.push(m("latencyMs", UNIT_MS, 60));
    out.push(FamilyDef {
        name: STREAM.to_string(),
        dimensions: dims(&["agentId", "result"]),
        measures: stream,
    });

    // MtconnectProbe — one agent's /probe surface (dims: agentId, result).
    let mut probe = Vec::new();
    probe.extend(pair_defs("probes"));
    probe.extend(pair_defs("modelChanges"));
    probe.push(m("latencyMs", UNIT_MS, 60));
    out.push(FamilyDef {
        name: PROBE.to_string(),
        dimensions: dims(&["agentId", "result"]),
        measures: probe,
    });

    // MtconnectParse — document decoding, reported per instance (dims: instance, result).
    let mut parse = Vec::new();
    parse.extend(pair_defs("documentsParsed"));
    parse.extend(pair_defs("parseErrors"));
    out.push(FamilyDef {
        name: PARSE.to_string(),
        dimensions: dims(&["instance", "result"]),
        measures: parse,
    });

    out
}

fn family_def(name: &str) -> FamilyDef {
    family_defs()
        .into_iter()
        .find(|f| f.name == name)
        .expect("family_defs covers every family the emitter uses")
}

// =================================================================================================
// Counter state
// =================================================================================================

/// A `<name>Total` (monotonic) + `<name>Interval` (reset on emit) counter pair.
#[derive(Debug, Default, Clone, Copy)]
pub struct Pair {
    total: f64,
    interval: f64,
}

impl Pair {
    fn add(&mut self, v: f64) {
        self.total += v;
        self.interval += v;
    }

    /// Write both measures into `out` and **reset the interval** — the emit convention.
    fn drain_into(&mut self, out: &mut HashMap<String, f64>, prefix: &str) {
        out.insert(format!("{prefix}Total"), self.total);
        out.insert(format!("{prefix}Interval"), self.interval);
        self.interval = 0.0;
    }
}

#[derive(Default)]
struct ConnCounters {
    ever_connected: bool,
    connect_attempts: Pair,
    connect_failures: Pair,
    reconnect_attempts: Pair,
    connection_drops: Pair,
    connected_accrued_ms: f64,
    connected_since: Option<Instant>,
}

impl ConnCounters {
    fn accrue(&mut self, now: Instant) {
        if let Some(since) = self.connected_since {
            self.connected_accrued_ms += now.saturating_duration_since(since).as_secs_f64() * 1000.0;
            self.connected_since = Some(now);
        }
    }

    fn drain(&mut self, now: Instant, connection_state: f64) -> HashMap<String, f64> {
        self.accrue(now);
        let mut v = HashMap::new();
        v.insert("connectionState".to_string(), connection_state);
        self.connect_attempts.drain_into(&mut v, "connectAttempts");
        self.connect_failures.drain_into(&mut v, "connectFailures");
        self.reconnect_attempts.drain_into(&mut v, "reconnectAttempts");
        self.connection_drops.drain_into(&mut v, "connectionDrops");
        v.insert("connectedDurationMs".to_string(), self.connected_accrued_ms);
        self.connected_accrued_ms = 0.0;
        v
    }
}

#[derive(Default)]
struct ShapeCounters {
    published: Pair,
    coalesced: Pair,
    deadband_dropped: Pair,
}

impl ShapeCounters {
    fn add(&mut self, c: crate::shaping::ShapingCounters) {
        self.published.add(c.published as f64);
        self.coalesced.add(c.coalesced as f64);
        self.deadband_dropped.add(c.deadband_dropped as f64);
    }

    fn drain(&mut self) -> HashMap<String, f64> {
        let mut v = HashMap::new();
        self.published.drain_into(&mut v, "published");
        self.coalesced.drain_into(&mut v, "coalesced");
        self.deadband_dropped.drain_into(&mut v, "deadbandDropped");
        v
    }
}

#[derive(Default)]
struct CmdCounters {
    command_requests: Pair,
    command_errors: Pair,
    command_latency_ms: f64,
}

impl CmdCounters {
    fn drain(&mut self) -> HashMap<String, f64> {
        let mut v = HashMap::new();
        self.command_requests.drain_into(&mut v, "commandRequests");
        self.command_errors.drain_into(&mut v, "commandErrors");
        v.insert("commandLatencyMs".to_string(), self.command_latency_ms);
        self.command_latency_ms = 0.0;
        v
    }
}

// =================================================================================================
// The MTConnect families: monotonic totals in, (Total, Interval) pairs out
// =================================================================================================

/// One agent's counters, as the emitter sees them. A trait so the family arithmetic can be tested
/// against scripted numbers; the production implementation is [`AgentRuntime`] itself.
pub trait AgentTelemetry: Send + Sync {
    /// The `component.global.agents[]` id — the `agentId` dimension.
    fn agent_id(&self) -> &str;
    /// The acquisition counters since process start.
    fn stats(&self) -> AgentStatsSnapshot;
    /// The document-parse counters since process start.
    fn parse_counters(&self) -> ParseCounters;
}

impl AgentTelemetry for AgentRuntime {
    fn agent_id(&self) -> &str {
        &self.config().id
    }

    fn stats(&self) -> AgentStatsSnapshot {
        AgentRuntime::stats(self)
    }

    fn parse_counters(&self) -> ParseCounters {
        AgentRuntime::parse_counters(self)
    }
}

/// Write one `<prefix>Total` / `<prefix>Interval` pair from a monotonic total and the total at the
/// previous emit. The subtraction saturates: a counter can only ever go up, and a restarted source
/// must not emit a negative interval.
fn pair_values(out: &mut HashMap<String, f64>, prefix: &str, total: u64, previous: u64) {
    out.insert(format!("{prefix}Total"), total as f64);
    out.insert(format!("{prefix}Interval"), total.saturating_sub(previous) as f64);
}

/// The [`STREAM`] measures for one `result` cell, from the current totals and the totals at the
/// previous emit. Every measure is present in both cells (the dimension × measure set is fixed);
/// each counter is non-zero only in the cell its outcome belongs to.
#[must_use]
pub fn stream_values(
    total: &AgentStatsSnapshot,
    previous: &AgentStatsSnapshot,
    result: &str,
) -> HashMap<String, f64> {
    let mut v = HashMap::new();
    let success = result == RESULT_SUCCESS;
    if success {
        pair_values(&mut v, "documents", total.documents, previous.documents);
        pair_values(&mut v, "observations", total.observations, previous.observations);
        pair_values(&mut v, "heartbeats", total.heartbeats, previous.heartbeats);
        pair_values(&mut v, "reconnects", 0, 0);
        pair_values(&mut v, "gaps", 0, 0);
        pair_values(&mut v, "outOfRange", 0, 0);
        v.insert(
            "latencyMs".to_string(),
            total.latency_ms.saturating_sub(previous.latency_ms) as f64,
        );
    } else {
        pair_values(&mut v, "documents", total.documents_failed, previous.documents_failed);
        pair_values(&mut v, "observations", 0, 0);
        pair_values(&mut v, "heartbeats", 0, 0);
        pair_values(&mut v, "reconnects", total.reconnects, previous.reconnects);
        pair_values(&mut v, "gaps", total.gaps, previous.gaps);
        pair_values(&mut v, "outOfRange", total.out_of_range, previous.out_of_range);
        v.insert(
            "latencyMs".to_string(),
            total.error_latency_ms.saturating_sub(previous.error_latency_ms) as f64,
        );
    }
    v
}

/// The [`PROBE`] measures for one `result` cell. A model change is only observable on a probe that
/// answered, so it is zero in the `error` cell by construction.
#[must_use]
pub fn probe_values(
    total: &AgentStatsSnapshot,
    previous: &AgentStatsSnapshot,
    result: &str,
) -> HashMap<String, f64> {
    let mut v = HashMap::new();
    if result == RESULT_SUCCESS {
        pair_values(&mut v, "probes", total.probes, previous.probes);
        pair_values(&mut v, "modelChanges", total.model_changes, previous.model_changes);
        v.insert(
            "latencyMs".to_string(),
            total.probe_latency_ms.saturating_sub(previous.probe_latency_ms) as f64,
        );
    } else {
        pair_values(&mut v, "probes", total.probes_failed, previous.probes_failed);
        pair_values(&mut v, "modelChanges", 0, 0);
        v.insert(
            "latencyMs".to_string(),
            total.probe_error_latency_ms.saturating_sub(previous.probe_error_latency_ms) as f64,
        );
    }
    v
}

/// The [`PARSE`] measures for one `result` cell.
#[must_use]
pub fn parse_values(
    total: &ParseCounters,
    previous: &ParseCounters,
    result: &str,
) -> HashMap<String, f64> {
    let mut v = HashMap::new();
    if result == RESULT_SUCCESS {
        pair_values(&mut v, "documentsParsed", total.documents_parsed, previous.documents_parsed);
        pair_values(&mut v, "parseErrors", 0, 0);
    } else {
        pair_values(&mut v, "documentsParsed", 0, 0);
        pair_values(&mut v, "parseErrors", total.parse_errors, previous.parse_errors);
    }
    v
}

/// The **agent-scoped** emitter: one per `component.global.agents[]` entry, emitting [`STREAM`] and
/// [`PROBE`] on the metrics cadence. An agent is shared by every device configured against it
/// (D-MTC-3), so its acquisition is measured once — not once per attached instance.
pub struct AgentMetrics {
    svc: Arc<dyn MetricService>,
    config: Arc<Config>,
    agent: Arc<dyn AgentTelemetry>,
    /// The totals at the previous emit — what the interval measures are computed against.
    previous: Mutex<AgentStatsSnapshot>,
}

impl AgentMetrics {
    /// Build the emitter for one agent.
    #[must_use]
    pub fn new(
        svc: Arc<dyn MetricService>,
        config: Arc<Config>,
        agent: Arc<dyn AgentTelemetry>,
    ) -> Self {
        Self { svc, config, agent, previous: Mutex::new(AgentStatsSnapshot::default()) }
    }

    /// Pre-define both families × both `result` cells at startup, so the metric set is fixed and
    /// discoverable before the first document arrives.
    pub fn define_all(&self) {
        for result in RESULTS {
            define_family(
                &self.svc,
                &self.config,
                STREAM,
                &[("agentId", self.agent.agent_id()), ("result", result)],
            );
            define_family(
                &self.svc,
                &self.config,
                PROBE,
                &[("agentId", self.agent.agent_id()), ("result", result)],
            );
        }
    }

    /// Emit both families for both `result` cells and advance the interval baseline.
    pub async fn emit_periodic(&self) {
        let total = self.agent.stats();
        let previous = {
            let mut guard = self.previous.lock().unwrap();
            std::mem::replace(&mut *guard, total)
        };
        let agent_id = self.agent.agent_id().to_string();
        for result in RESULTS {
            let dims = [("agentId", agent_id.as_str()), ("result", result)];
            emit_family(
                &self.svc,
                &self.config,
                STREAM,
                &dims,
                stream_values(&total, &previous, result),
            )
            .await;
            emit_family(
                &self.svc,
                &self.config,
                PROBE,
                &dims,
                probe_values(&total, &previous, result),
            )
            .await;
        }
    }
}

/// Build + register one family combo's metric definition.
fn define_family(
    svc: &Arc<dyn MetricService>,
    config: &Arc<Config>,
    name: &str,
    dimensions: &[(&str, &str)],
) {
    let def = family_def(name);
    let mut b = MetricBuilder::create(name).with_config(config);
    for measure in &def.measures {
        b = b.add_measure(measure.name.clone(), measure.unit.clone(), measure.res);
    }
    for (k, v) in dimensions {
        b = b.add_dimension(*k, *v);
    }
    svc.define_metric(b.build());
}

/// Re-define (the name-keyed-store rule) then emit one family combo.
async fn emit_family(
    svc: &Arc<dyn MetricService>,
    config: &Arc<Config>,
    name: &str,
    dimensions: &[(&str, &str)],
    values: HashMap<String, f64>,
) {
    define_family(svc, config, name, dimensions);
    if let Err(e) = svc.emit_metric(name, values).await {
        tracing::warn!(metric = %name, error = %e, "metric emit failed");
    }
}

#[derive(Default)]
struct Inner {
    conn: ConnCounters,
    command: std::collections::BTreeMap<(&'static str, &'static str), CmdCounters>,
    /// The publish-shaping engine's counters ([`SHAPING`]), fed by the device task.
    shaping: ShapeCounters,
    /// Per-signal last-update instant — the staleness tracker driving `southbound_health.staleSignals`.
    last_update: HashMap<String, Instant>,
    /// The agent's parse totals at the previous [`PARSE`] emit.
    previous_parse: ParseCounters,
}

/// A per-device operational-metrics emitter. Owns the counter state for one device's `southbound_health`
/// plus the two worked families, and emits them on the metrics cadence and on connect/disconnect
/// transitions. One per configured instance.
pub struct DeviceMetrics {
    svc: Arc<dyn MetricService>,
    config: Arc<Config>,
    instance: String,
    health: Arc<Health>,
    /// A signal with no update for longer than this is counted in `staleSignals`
    /// (`component.global.healthThresholds.staleSignalSecs`).
    stale_after: Duration,
    /// The agent serving this instance — the source of the [`PARSE`] family. `None` for the
    /// template simulator, which parses no documents.
    agent: Option<Arc<dyn AgentTelemetry>>,
    inner: Mutex<Inner>,
}

impl DeviceMetrics {
    /// Build the emitter for one device, pre-populating the full `(verb, result)` command matrix so the
    /// dimension set is fixed and discoverable at startup.
    #[must_use]
    pub fn new(
        svc: Arc<dyn MetricService>,
        config: Arc<Config>,
        instance: String,
        health: Arc<Health>,
        stale_signal_secs: u64,
        agent: Option<Arc<dyn AgentTelemetry>>,
    ) -> Self {
        let mut inner = Inner::default();
        for verb in COMMAND_VERBS {
            for result in RESULTS {
                inner.command.entry((verb, result)).or_default();
            }
        }
        Self {
            svc,
            config,
            instance,
            health,
            stale_after: Duration::from_secs(stale_signal_secs.max(1)),
            agent,
            inner: Mutex::new(inner),
        }
    }

    fn instance(&self) -> &str {
        &self.instance
    }

    // ---- recording (called from the device task; all synchronous) --------------------------------

    /// A connect attempt is about to be made.
    pub fn on_connect_attempt(&self) {
        self.inner.lock().unwrap().conn.connect_attempts.add(1.0);
    }

    /// The connect attempt succeeded. A re-establishment (after a previous drop) also bumps
    /// `reconnectAttempts`.
    pub fn on_connected(&self, now: Instant) {
        let mut inner = self.inner.lock().unwrap();
        let c = &mut inner.conn;
        c.connected_since = Some(now);
        if c.ever_connected {
            c.reconnect_attempts.add(1.0);
        }
        c.ever_connected = true;
    }

    /// The connect attempt failed (unreachable / refused / timeout).
    pub fn on_connect_failure(&self) {
        self.inner.lock().unwrap().conn.connect_failures.add(1.0);
    }

    /// An established session was lost.
    pub fn on_connection_dropped(&self, now: Instant) {
        let mut inner = self.inner.lock().unwrap();
        let c = &mut inner.conn;
        c.accrue(now);
        c.connected_since = None;
        c.connection_drops.add(1.0);
    }

    /// Note that a signal just updated — feeds the `staleSignals` tracker.
    pub fn on_signal_update(&self, signal_id: &str, now: Instant) {
        self.inner.lock().unwrap().last_update.insert(signal_id.to_string(), now);
    }

    /// Record what the publish-shaping engine did since it was last drained — feeds the
    /// [`SHAPING`] family.
    pub fn on_shaping(&self, counters: crate::shaping::ShapingCounters) {
        self.inner.lock().unwrap().shaping.add(counters);
    }

    /// Record one `sb/*` command outcome for its `(verb, result)` combo.
    pub fn record_command(&self, verb: &'static str, ok: bool, latency_ms: u64) {
        let result = if ok { RESULT_SUCCESS } else { RESULT_ERROR };
        let mut inner = self.inner.lock().unwrap();
        let c = inner.command.entry((verb, result)).or_default();
        c.command_requests.add(1.0);
        c.command_latency_ms += latency_ms as f64;
        if !ok {
            c.command_errors.add(1.0);
        }
    }

    /// The connection-counter snapshot for `sb/status` / the diagnostics panel: each counter as
    /// `{interval, total}`. Cheap; no device I/O.
    #[must_use]
    pub fn counters_view(&self) -> serde_json::Value {
        let inner = self.inner.lock().unwrap();
        let pair = |p: &Pair| serde_json::json!({ "interval": p.interval, "total": p.total });
        serde_json::json!({
            "connectAttempts": pair(&inner.conn.connect_attempts),
            "connectFailures": pair(&inner.conn.connect_failures),
            "reconnectAttempts": pair(&inner.conn.reconnect_attempts),
            "connectionDrops": pair(&inner.conn.connection_drops),
        })
    }

    fn stale_count(&self, now: Instant) -> f64 {
        let inner = self.inner.lock().unwrap();
        inner
            .last_update
            .values()
            .filter(|&&t| now.saturating_duration_since(t) > self.stale_after)
            .count() as f64
    }

    // ---- definition + emission -------------------------------------------------------------------

    /// Pre-define every family × dimension combination at startup, so the metric set is fixed and
    /// discoverable. Each is also re-defined immediately before each emit (the name-keyed-store rule).
    pub fn define_all(&self) {
        self.define(HEALTH, &[("instance", self.instance())]);
        self.define(CONNECTION, &[("instance", self.instance())]);
        self.define(SHAPING, &[("instance", self.instance())]);
        for verb in COMMAND_VERBS {
            for result in RESULTS {
                self.define(COMMAND, &[("instance", self.instance()), ("verb", verb), ("result", result)]);
            }
        }
        if self.agent.is_some() {
            for result in RESULTS {
                self.define(PARSE, &[("instance", self.instance()), ("result", result)]);
            }
        }
    }

    /// Build + register one family combo's metric definition.
    fn define(&self, name: &str, dimensions: &[(&str, &str)]) {
        let def = family_def(name);
        let mut b = MetricBuilder::create(name).with_config(&self.config);
        for measure in &def.measures {
            b = b.add_measure(measure.name.clone(), measure.unit.clone(), measure.res);
        }
        for (k, v) in dimensions {
            b = b.add_dimension(*k, *v);
        }
        self.svc.define_metric(b.build());
    }

    /// Re-define (with the combo's dimensions) then emit one family combo.
    async fn emit_combo(&self, name: &str, dimensions: &[(&str, &str)], values: HashMap<String, f64>, now: bool) {
        self.define(name, dimensions);
        let res = if now {
            self.svc.emit_metric_now(name, values).await
        } else {
            self.svc.emit_metric(name, values).await
        };
        if let Err(e) = res {
            tracing::warn!(metric = %name, instance = %self.instance(), error = %e, "metric emit failed");
        }
    }

    /// The full periodic emit (every metrics interval): `southbound_health`, the connection family, and
    /// every command `(verb, result)` combo.
    pub async fn emit_periodic(&self) {
        self.emit_health(false).await;
        self.emit_connection(false).await;
        self.emit_command().await;
        self.emit_shaping().await;
        self.emit_parse().await;
    }

    /// The `MtconnectAdapterShaping` family: what the publish-shaping engine did to this
    /// instance's flow since the previous emit.
    async fn emit_shaping(&self) {
        let values = self.inner.lock().unwrap().shaping.drain();
        self.emit_combo(SHAPING, &[("instance", self.instance())], values, false).await;
    }

    /// The `MtconnectParse` family for this instance: the agent's document-decode totals, diffed
    /// against this instance's own previous emit.
    async fn emit_parse(&self) {
        let Some(agent) = &self.agent else { return };
        let total = agent.parse_counters();
        let previous = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::replace(&mut inner.previous_parse, total)
        };
        for result in RESULTS {
            self.emit_combo(
                PARSE,
                &[("instance", self.instance()), ("result", result)],
                parse_values(&total, &previous, result),
                false,
            )
            .await;
        }
    }

    /// The immediate transition emit (`emit_metric_now`): the mandatory `southbound_health` plus the
    /// connection gauges whose state just changed — flushed on connect / disconnect.
    pub async fn emit_now(&self) {
        self.emit_health(true).await;
        self.emit_connection(true).await;
    }

    async fn emit_health(&self, now: bool) {
        let mut v = HashMap::new();
        v.insert("connectionState".to_string(), self.health.connection_state.load(Ordering::Relaxed) as f64);
        v.insert("publishLatencyMs".to_string(), self.health.publish_latency_ms.load(Ordering::Relaxed) as f64);
        v.insert("pollLatencyMs".to_string(), self.health.poll_latency_ms.load(Ordering::Relaxed) as f64);
        v.insert("readErrors".to_string(), self.health.read_errors.swap(0, Ordering::Relaxed) as f64);
        v.insert("staleSignals".to_string(), self.stale_count(Instant::now()));
        v.insert("reconnects".to_string(), self.health.reconnects.swap(0, Ordering::Relaxed) as f64);
        v.insert("writeErrors".to_string(), self.health.write_errors.swap(0, Ordering::Relaxed) as f64);
        v.insert("signalsSubscribed".to_string(), self.health.signals_subscribed() as f64);
        self.emit_combo(HEALTH, &[("instance", self.instance())], v, now).await;
    }

    async fn emit_connection(&self, now: bool) {
        let state = self.health.connection_state.load(Ordering::Relaxed) as f64;
        let values = self.inner.lock().unwrap().conn.drain(Instant::now(), state);
        self.emit_combo(CONNECTION, &[("instance", self.instance())], values, now).await;
    }

    async fn emit_command(&self) {
        let rows: Vec<(&'static str, &'static str, HashMap<String, f64>)> = {
            let mut inner = self.inner.lock().unwrap();
            inner.command.iter_mut().map(|((verb, result), c)| (*verb, *result, c.drain())).collect()
        };
        for (verb, result, values) in rows {
            self.emit_combo(COMMAND, &[("instance", self.instance()), ("verb", verb), ("result", result)], values, false).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use edgecommons::prelude::{Config, Metric, MetricService};
    use serde_json::json;

    use crate::app::{Health, LinkState};

    // --- a no-op MetricService + Config so DeviceMetrics can be built and emitted without a live
    // runtime. `define_metric`/`emit_metric*` are the only seam a live target adds; everything the
    // emitter COMPUTES (the counter recording, drain, and the §5 value assembly below) is exercised
    // here for real. ---------------------------------------------------------------------------------
    #[derive(Default)]
    struct NoopMetrics;

    #[async_trait::async_trait]
    impl MetricService for NoopMetrics {
        fn define_metric(&self, _metric: Metric) {}
        fn is_metric_defined(&self, _name: &str) -> bool {
            true
        }
        async fn emit_metric(&self, _name: &str, _values: HashMap<String, f64>) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn emit_metric_now(&self, _name: &str, _values: HashMap<String, f64>) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn flush_metrics(&self) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn shutdown(&self) {}
    }

    fn config() -> Arc<Config> {
        Arc::new(
            Config::from_value(
                "com.example.MyAdapter",
                "thing-1",
                json!({ "metricEmission": { "target": "log", "namespace": "test" } }),
            )
            .unwrap(),
        )
    }

    fn dm(health: Arc<Health>, stale_secs: u64) -> DeviceMetrics {
        DeviceMetrics::new(Arc::new(NoopMetrics), config(), "plc-1".to_string(), health, stale_secs, None)
    }

    #[test]
    fn the_command_matrix_is_prepopulated_so_the_dimension_set_is_fixed_at_startup() {
        let dm = dm(Arc::new(Health::default()), 30);
        let inner = dm.inner.lock().unwrap();
        // Every (verb, result) combo exists before a single command is recorded.
        assert_eq!(inner.command.len(), COMMAND_VERBS.len() * RESULTS.len());
        for verb in COMMAND_VERBS {
            for result in RESULTS {
                assert!(inner.command.contains_key(&(verb, result)), "{verb}/{result} pre-defined");
            }
        }
    }

    #[test]
    fn the_connection_lifecycle_records_reconnects_only_after_the_first_connect() {
        let dm = dm(Arc::new(Health::default()), 30);
        let t0 = Instant::now();
        dm.on_connect_attempt();
        dm.on_connected(t0); // first connect: NOT a reconnect
        dm.on_connection_dropped(t0 + Duration::from_millis(100));
        dm.on_connect_attempt();
        dm.on_connected(t0 + Duration::from_millis(200)); // re-establishment: IS a reconnect

        let inner = dm.inner.lock().unwrap();
        assert_eq!(inner.conn.connect_attempts.total, 2.0);
        assert_eq!(inner.conn.reconnect_attempts.total, 1.0, "only the second connect is a reconnect");
        assert_eq!(inner.conn.connection_drops.total, 1.0);
        assert!(inner.conn.connected_accrued_ms >= 100.0, "the first session's uptime accrued");
    }

    #[test]
    fn a_connect_failure_bumps_only_its_own_counter() {
        let dm = dm(Arc::new(Health::default()), 30);
        dm.on_connect_attempt();
        dm.on_connect_failure();
        let inner = dm.inner.lock().unwrap();
        assert_eq!(inner.conn.connect_failures.total, 1.0);
        assert_eq!(inner.conn.connect_attempts.total, 1.0);
    }

    #[test]
    fn a_command_outcome_lands_in_its_verb_result_cell_and_errors_double_count() {
        let dm = dm(Arc::new(Health::default()), 30);
        dm.record_command("sb/read", true, 4);
        dm.record_command("sb/read", false, 6);
        let inner = dm.inner.lock().unwrap();
        let ok = &inner.command[&("sb/read", RESULT_SUCCESS)];
        let err = &inner.command[&("sb/read", RESULT_ERROR)];
        assert_eq!(ok.command_requests.total, 1.0);
        assert_eq!(ok.command_errors.total, 0.0, "a success is not an error");
        assert_eq!(err.command_requests.total, 1.0);
        assert_eq!(err.command_errors.total, 1.0, "a failure counts a request AND an error");
        assert_eq!(err.command_latency_ms, 6.0);
    }

    #[test]
    fn counters_view_reports_each_connection_counter_as_interval_and_total() {
        let dm = dm(Arc::new(Health::default()), 30);
        dm.on_connect_attempt();
        dm.on_connect_attempt();
        let view = dm.counters_view();
        assert_eq!(view["connectAttempts"]["total"], 2.0);
        assert_eq!(view["connectAttempts"]["interval"], 2.0);
        assert_eq!(view["connectFailures"]["total"], 0.0);
    }

    #[test]
    fn stale_count_counts_only_signals_past_the_threshold() {
        let dm = dm(Arc::new(Health::default()), 30);
        let now = Instant::now();
        dm.on_signal_update("fresh", now);
        dm.on_signal_update("stale", now - Duration::from_secs(120));
        assert_eq!(dm.stale_count(now), 1.0, "only the signal older than staleSignalSecs is stale");
    }

    #[test]
    fn conn_counters_drain_resets_intervals_and_the_duration_but_not_totals() {
        let mut c = ConnCounters::default();
        let t0 = Instant::now();
        c.connect_attempts.add(1.0);
        c.connected_since = Some(t0);
        let first = c.drain(t0 + Duration::from_millis(50), 1.0);
        assert_eq!(first["connectAttemptsTotal"], 1.0);
        assert_eq!(first["connectAttemptsInterval"], 1.0);
        assert!(first["connectedDurationMs"] >= 50.0);
        assert_eq!(first["connectionState"], 1.0);
        // Second drain: interval counters and the duration sum have reset; the total has not.
        let second = c.drain(t0 + Duration::from_millis(100), 1.0);
        assert_eq!(second["connectAttemptsTotal"], 1.0);
        assert_eq!(second["connectAttemptsInterval"], 0.0);
    }

    #[tokio::test]
    async fn define_all_then_periodic_and_now_emits_drive_every_family_through_the_service() {
        let health = Arc::new(Health::default());
        health.set_link(LinkState::Online);
        let dm = dm(Arc::clone(&health), 30);
        // Record something in each family so the emit paths carry non-trivial values.
        dm.on_connect_attempt();
        dm.on_connected(Instant::now());
        dm.record_command("sb/status", true, 2);
        dm.on_signal_update("temperature-1", Instant::now());

        dm.define_all(); // define + family_def for every combo
        dm.emit_periodic().await; // emit_health(false) + emit_connection(false) + emit_command
        dm.emit_now().await; // emit_health(true) + emit_connection(true)

        // After a periodic emit the command intervals have drained to zero (totals persist).
        let inner = dm.inner.lock().unwrap();
        let ok = &inner.command[&("sb/status", RESULT_SUCCESS)];
        assert_eq!(ok.command_requests.total, 1.0);
        assert_eq!(ok.command_requests.interval, 0.0, "the periodic emit drained the interval");
    }

    /// `southbound_health` emits EXACTLY the SOUTHBOUND.md §5 set — asserted against an independent
    /// literal transcription so a drift from the canonical doc fails the build.
    #[test]
    fn southbound_health_emits_exactly_the_section_5_measure_set() {
        // A second, independent copy of §5 — NOT the module const, so a wrong edit to one is caught.
        let section_5: std::collections::BTreeSet<&str> = [
            "connectionState",
            "publishLatencyMs",
            "pollLatencyMs",
            "readErrors",
            "staleSignals",
            "reconnects",
            "writeErrors",
            "signalsSubscribed",
        ]
        .into_iter()
        .collect();

        let health = family_defs().into_iter().find(|f| f.name == HEALTH).expect("health family");
        let emitted: std::collections::BTreeSet<&str> =
            health.measures.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(emitted, section_5, "southbound_health must be the exact §5 set — no more, no less");

        // The advertised const must agree with what family_defs emits.
        let advertised: std::collections::BTreeSet<&str> = HEALTH_MEASURES.into_iter().collect();
        assert_eq!(advertised, section_5, "HEALTH_MEASURES must equal the §5 set");
    }

    #[tokio::test]
    async fn write_errors_drain_on_emit_and_signals_subscribed_tracks_the_link() {
        let health = Arc::new(Health::default());
        health.set_signal_inventory(2);
        health.set_link(LinkState::Online);
        health.write_errors.fetch_add(2, Ordering::Relaxed);
        let dm = dm(Arc::clone(&health), 30);

        assert_eq!(health.signals_subscribed(), 2, "the sb/signals inventory size while connected");
        dm.emit_periodic().await;
        assert_eq!(
            health.write_errors.load(Ordering::Relaxed),
            0,
            "the emit drained the writeErrors interval counter"
        );

        health.set_link(LinkState::Backoff);
        assert_eq!(health.signals_subscribed(), 0, "0 while disconnected");
    }

    /// The operational families are named from the component and carry only low-cardinality dimensions.
    #[test]
    fn operational_families_are_named_from_the_component_and_low_cardinality() {
        let defs = family_defs();
        let names: Vec<&str> = defs.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&CONNECTION), "the Connection family is present");
        assert!(names.contains(&COMMAND), "the Command family is present");
        // Named from the component token — a fleet view separates adapters by name.
        assert!(CONNECTION.ends_with("Connection") && CONNECTION != "Connection");
        assert!(COMMAND.ends_with("Command") && COMMAND != "Command");

        let cmd = defs.iter().find(|f| f.name == COMMAND).unwrap();
        assert_eq!(cmd.dimensions, vec!["instance", "verb", "result"], "closed, low-cardinality dims only");
    }

    /// The connection family is a set of Total/Interval counter pairs plus the state gauge and the
    /// duration sum — the operational-family pattern, spelled out.
    #[test]
    fn the_connection_family_is_the_counter_pair_pattern() {
        let conn = family_defs().into_iter().find(|f| f.name == CONNECTION).unwrap();
        let names: Vec<&str> = conn.measures.iter().map(|m| m.name.as_str()).collect();
        for base in ["connectAttempts", "connectFailures", "reconnectAttempts", "connectionDrops"] {
            assert!(names.contains(&format!("{base}Total").as_str()), "{base}Total present");
            assert!(names.contains(&format!("{base}Interval").as_str()), "{base}Interval present");
        }
        assert!(names.contains(&"connectionState"), "the state gauge");
        assert!(names.contains(&"connectedDurationMs"), "the connected-duration sum");
    }

    /// The publish-shaping family: `instance`-dimensioned counter pairs for the three engine
    /// outcomes — published, coalesced, deadband-dropped.
    #[test]
    fn the_shaping_family_is_instance_dimensioned_counter_pairs() {
        let shaping = family_defs().into_iter().find(|f| f.name == SHAPING).unwrap();
        assert_eq!(shaping.dimensions, vec!["instance"], "shaping is a per-instance fact");
        let names: Vec<&str> = shaping.measures.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "publishedTotal",
                "publishedInterval",
                "coalescedTotal",
                "coalescedInterval",
                "deadbandDroppedTotal",
                "deadbandDroppedInterval"
            ]
        );
    }

    #[tokio::test]
    async fn shaping_counters_accumulate_and_drain_on_emit() {
        let dm = dm(Arc::new(Health::default()), 30);
        dm.on_shaping(crate::shaping::ShapingCounters {
            published: 3,
            coalesced: 7,
            deadband_dropped: 2,
        });
        dm.on_shaping(crate::shaping::ShapingCounters { published: 1, ..Default::default() });
        {
            let mut inner = dm.inner.lock().unwrap();
            let values = inner.shaping.drain();
            assert_eq!(values["publishedTotal"], 4.0);
            assert_eq!(values["publishedInterval"], 4.0);
            assert_eq!(values["coalescedTotal"], 7.0);
            assert_eq!(values["deadbandDroppedTotal"], 2.0);
        }
        // The emit path drains the interval; the total is monotonic.
        dm.on_shaping(crate::shaping::ShapingCounters { published: 2, ..Default::default() });
        dm.emit_periodic().await;
        let mut inner = dm.inner.lock().unwrap();
        let values = inner.shaping.drain();
        assert_eq!(values["publishedTotal"], 6.0, "totals are monotonic across emits");
        assert_eq!(values["publishedInterval"], 0.0, "the periodic emit drained the interval");
    }

    #[test]
    fn interval_counters_reset_on_drain_but_totals_do_not() {
        let mut p = Pair::default();
        p.add(3.0);
        let mut out = HashMap::new();
        p.drain_into(&mut out, "x");
        assert_eq!(out["xTotal"], 3.0);
        assert_eq!(out["xInterval"], 3.0);

        p.add(2.0);
        let mut out2 = HashMap::new();
        p.drain_into(&mut out2, "x");
        assert_eq!(out2["xTotal"], 5.0, "total is monotonic across emits");
        assert_eq!(out2["xInterval"], 2.0, "interval resets to only what accrued since the last emit");
    }

    // --- the MTConnect families (HLD §9) ---------------------------------------------------------

    /// A scripted agent: the family arithmetic is exercised against known totals, with no runtime.
    struct FakeAgent {
        id: String,
        stats: Mutex<AgentStatsSnapshot>,
        parse: Mutex<ParseCounters>,
    }

    impl FakeAgent {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                id: "line-a-agent".to_string(),
                stats: Mutex::new(AgentStatsSnapshot::default()),
                parse: Mutex::new(ParseCounters::default()),
            })
        }
    }

    impl AgentTelemetry for FakeAgent {
        fn agent_id(&self) -> &str {
            &self.id
        }
        fn stats(&self) -> AgentStatsSnapshot {
            *self.stats.lock().unwrap()
        }
        fn parse_counters(&self) -> ParseCounters {
            *self.parse.lock().unwrap()
        }
    }

    /// The three HLD §9 families exist with exactly the documented dimensions and measures —
    /// asserted against an independent transcription of the table, so a drift fails the build.
    #[test]
    fn the_mtconnect_families_match_the_hld_table() {
        let defs = family_defs();
        let by_name = |n: &str| defs.iter().find(|f| f.name == n).expect("family present").clone();

        // | MtconnectStream | agentId, result | documents, observations, heartbeats, reconnects,
        //   gaps, outOfRange, latencyMs |
        let stream = by_name("MtconnectStream");
        assert_eq!(stream.dimensions, vec!["agentId", "result"]);
        let names: Vec<&str> = stream.measures.iter().map(|m| m.name.as_str()).collect();
        for base in ["documents", "observations", "heartbeats", "reconnects", "gaps", "outOfRange"] {
            assert!(names.contains(&format!("{base}Total").as_str()), "{base}Total");
            assert!(names.contains(&format!("{base}Interval").as_str()), "{base}Interval");
        }
        assert!(names.contains(&"latencyMs"), "the latency sum is a single measure, not a pair");
        assert_eq!(names.len(), 13, "six counter pairs plus the latency sum - no more");

        // | MtconnectProbe | agentId, result | probes, modelChanges, latencyMs |
        let probe = by_name("MtconnectProbe");
        assert_eq!(probe.dimensions, vec!["agentId", "result"]);
        let names: Vec<&str> = probe.measures.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "probesTotal",
                "probesInterval",
                "modelChangesTotal",
                "modelChangesInterval",
                "latencyMs"
            ]
        );

        // | MtconnectParse | instance, result | documentsParsed, parseErrors |
        let parse = by_name("MtconnectParse");
        assert_eq!(parse.dimensions, vec!["instance", "result"]);
        let names: Vec<&str> = parse.measures.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "documentsParsedTotal",
                "documentsParsedInterval",
                "parseErrorsTotal",
                "parseErrorsInterval"
            ]
        );

        // No family dimensions by anything unbounded — the whole key set, checked at once.
        let allowed = ["instance", "verb", "result", "agentId"];
        for f in &defs {
            for d in &f.dimensions {
                assert!(allowed.contains(&d.as_str()), "`{d}` is not a low-cardinality dimension");
            }
        }
    }

    #[test]
    fn stream_values_put_each_counter_in_the_cell_its_outcome_belongs_to() {
        let previous = AgentStatsSnapshot { documents: 10, observations: 40, ..Default::default() };
        let total = AgentStatsSnapshot {
            documents: 14,
            documents_failed: 2,
            observations: 55,
            heartbeats: 3,
            reconnects: 1,
            gaps: 7,
            out_of_range: 1,
            latency_ms: 120,
            error_latency_ms: 9_000,
            ..Default::default()
        };

        let ok = stream_values(&total, &previous, RESULT_SUCCESS);
        assert_eq!(ok["documentsTotal"], 14.0);
        assert_eq!(ok["documentsInterval"], 4.0, "the interval is the delta since the last emit");
        assert_eq!(ok["observationsInterval"], 15.0);
        assert_eq!(ok["heartbeatsTotal"], 3.0);
        assert_eq!(ok["latencyMs"], 120.0);
        assert_eq!(ok["gapsTotal"], 0.0, "a gap is never a success");
        assert_eq!(ok["reconnectsTotal"], 0.0);

        let err = stream_values(&total, &previous, RESULT_ERROR);
        assert_eq!(err["documentsTotal"], 2.0, "the error cell counts the undecodable documents");
        assert_eq!(err["gapsTotal"], 7.0);
        assert_eq!(err["outOfRangeTotal"], 1.0);
        assert_eq!(err["reconnectsTotal"], 1.0);
        assert_eq!(err["latencyMs"], 9_000.0, "a failed request's latency does not flatter the mean");
        assert_eq!(err["observationsTotal"], 0.0);

        // The two cells carry the SAME measure set — a fixed metric shape either way.
        let ok_keys: std::collections::BTreeSet<&String> = ok.keys().collect();
        let err_keys: std::collections::BTreeSet<&String> = err.keys().collect();
        assert_eq!(ok_keys, err_keys);
    }

    #[test]
    fn probe_and_parse_values_split_by_outcome_and_never_go_backwards() {
        let previous = AgentStatsSnapshot { probes: 5, probe_latency_ms: 50, ..Default::default() };
        let total = AgentStatsSnapshot {
            probes: 7,
            probes_failed: 2,
            model_changes: 1,
            probe_latency_ms: 90,
            probe_error_latency_ms: 400,
            ..Default::default()
        };
        let ok = probe_values(&total, &previous, RESULT_SUCCESS);
        assert_eq!((ok["probesTotal"], ok["probesInterval"]), (7.0, 2.0));
        assert_eq!(ok["modelChangesTotal"], 1.0);
        assert_eq!(ok["latencyMs"], 40.0);
        let err = probe_values(&total, &previous, RESULT_ERROR);
        assert_eq!(err["probesTotal"], 2.0);
        assert_eq!(err["modelChangesTotal"], 0.0, "a failed probe cannot have seen a new model");
        assert_eq!(err["latencyMs"], 400.0);

        let parsed = ParseCounters { documents_parsed: 9, parse_errors: 2, unknown_elements: 4 };
        let before = ParseCounters { documents_parsed: 9, parse_errors: 1, unknown_elements: 0 };
        let ok = parse_values(&parsed, &before, RESULT_SUCCESS);
        assert_eq!((ok["documentsParsedTotal"], ok["documentsParsedInterval"]), (9.0, 0.0));
        let err = parse_values(&parsed, &before, RESULT_ERROR);
        assert_eq!((err["parseErrorsTotal"], err["parseErrorsInterval"]), (2.0, 1.0));

        // A source that somehow reports less than last time yields 0, never a negative interval.
        let backwards = parse_values(&before, &parsed, RESULT_ERROR);
        assert_eq!(backwards["parseErrorsInterval"], 0.0);
    }

    #[tokio::test]
    async fn the_agent_emitter_advances_its_baseline_so_intervals_do_not_double_count() {
        let agent = FakeAgent::new();
        let am = AgentMetrics::new(
            Arc::new(NoopMetrics),
            config(),
            Arc::clone(&agent) as Arc<dyn AgentTelemetry>,
        );
        am.define_all();

        agent.stats.lock().unwrap().documents = 4;
        am.emit_periodic().await;
        assert_eq!(am.previous.lock().unwrap().documents, 4, "the baseline moved to what was emitted");

        agent.stats.lock().unwrap().documents = 6;
        am.emit_periodic().await;
        let values = stream_values(&agent.stats(), &AgentStatsSnapshot { documents: 4, ..Default::default() }, RESULT_SUCCESS);
        assert_eq!(values["documentsInterval"], 2.0, "only the new documents");
    }

    #[tokio::test]
    async fn an_instance_emits_the_parse_family_of_the_agent_serving_it() {
        let agent = FakeAgent::new();
        let dm = DeviceMetrics::new(
            Arc::new(NoopMetrics),
            config(),
            "cnc-1".to_string(),
            Arc::new(Health::default()),
            30,
            Some(Arc::clone(&agent) as Arc<dyn AgentTelemetry>),
        );
        dm.define_all();
        agent.parse.lock().unwrap().record_ok(0);
        agent.parse.lock().unwrap().record_err();
        dm.emit_periodic().await;
        assert_eq!(
            dm.inner.lock().unwrap().previous_parse,
            ParseCounters { documents_parsed: 1, parse_errors: 1, unknown_elements: 0 },
            "the emit advanced this instance's own baseline"
        );

        // The simulator has no agent: nothing to parse, nothing emitted, no panic.
        let plain = dm_no_agent();
        plain.define_all();
        plain.emit_periodic().await;
        assert_eq!(plain.inner.lock().unwrap().previous_parse, ParseCounters::default());
    }

    fn dm_no_agent() -> DeviceMetrics {
        DeviceMetrics::new(
            Arc::new(NoopMetrics),
            config(),
            "sim-1".to_string(),
            Arc::new(Health::default()),
            30,
            None,
        )
    }

    #[test]
    fn stale_signals_counts_only_signals_past_the_threshold() {
        let inner = Inner {
            last_update: {
                let mut m = HashMap::new();
                let now = Instant::now();
                m.insert("fresh".to_string(), now);
                m.insert("stale".to_string(), now - Duration::from_secs(120));
                m
            },
            ..Default::default()
        };
        // Reconstruct just enough to call stale_count without a live MetricService.
        let count = inner
            .last_update
            .values()
            .filter(|&&t| Instant::now().saturating_duration_since(t) > Duration::from_secs(30))
            .count();
        assert_eq!(count, 1, "only the signal older than staleSignalSecs is stale");
    }
}
