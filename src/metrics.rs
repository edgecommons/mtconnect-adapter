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
//! ## Add your protocol's families HERE
//!
//! `MtconnectAdapterConnection`/`Command` are generic — every adapter has them. Your protocol also
//! has an **inventory** (configured signals), a **poll/subscribe** path, and a **publish** path worth
//! measuring. Add `MtconnectAdapterInventory` / `MtconnectAdapterPoll` / `MtconnectAdapterPublish`
//! families next to the two below — see `modbus-adapter/modbus_adapter/metrics.py` and
//! `ethernet-ip-adapter/crates/ethernet-ip-adapter/src/metrics.rs` for the full worked set (poll
//! cycles, samples good/bad/uncertain/changed/suppressed, batch flushes, …). Register each new family
//! in [`family_defs`] and pre-define it in [`DeviceMetrics::define_all`]; the rest of the pattern
//! (record → drain → emit) is copy-shaped from [`CmdCounters`].

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use edgecommons::prelude::{Config, MetricBuilder, MetricService};

use crate::app::Health;

/// The metric every southbound adapter emits (SOUTHBOUND.md §5).
pub const HEALTH: &str = "southbound_health";
/// The worked operational family for the connect/reconnect lifecycle. Named from the component so a
/// fleet view can tell one adapter's connection health from another's.
pub const CONNECTION: &str = "MtconnectAdapterConnection";
/// The worked operational family for the `sb/*` command surface, dimensioned `instance`×`verb`×`result`.
pub const COMMAND: &str = "MtconnectAdapterCommand";

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

    // ADD YOUR PROTOCOL'S FAMILIES HERE (Inventory / Poll / Publish — see the module header).

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

#[derive(Default)]
struct Inner {
    conn: ConnCounters,
    command: std::collections::BTreeMap<(&'static str, &'static str), CmdCounters>,
    /// Per-signal last-update instant — the staleness tracker driving `southbound_health.staleSignals`.
    last_update: HashMap<String, Instant>,
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
        for verb in COMMAND_VERBS {
            for result in RESULTS {
                self.define(COMMAND, &[("instance", self.instance()), ("verb", verb), ("result", result)]);
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
        DeviceMetrics::new(Arc::new(NoopMetrics), config(), "plc-1".to_string(), health, stale_secs)
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
