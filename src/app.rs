//! # MtconnectAdapter — a southbound protocol adapter
//!
//! An **adapter** connects to devices, reads signals, and publishes them onto the UNS in the
//! shape the rest of the fleet expects — so that a consumer can chart a Modbus register and an
//! OPC UA node without knowing either protocol.
//!
//! ```text
//!   connect ──► poll ──► publish SouthboundSignalUpdate ──► report health
//!      ▲                                                         │
//!      └──────────── reconnect with backoff ◄────────────────────┘
//! ```
//!
//! One task per instance: an instance is one device, and its connection lifecycle is its own. That
//! task also owns a **control channel** ([`DeviceControl`]) — every command that must touch the
//! (non-`Sync`) session or serialize with the poll loop is *sent* to the task, and *confirmed*
//! through the reply that rides it. The command surface itself lives in [`crate::commands`].
//!
//! ## The contract you are implementing (docs/SOUTHBOUND.md)
//!
//! * Publish `SouthboundSignalUpdate` on the `data` class, **via the `data()` facade** — never
//!   hand-build the body and never hand-write the topic.
//! * **Quality on every sample**, normalized to `GOOD | BAD | UNCERTAIN`, with the native code in
//!   `qualityRaw`.
//! * Emit **`southbound_health`** (the exact §5 set — see [`crate::metrics`]), dimensioned by
//!   instance, so an operator can see a link go down without reading logs.
//! * Report **per-instance connectivity** ([`connectivity_of`]).
//! * Serve **read/write/browse/reconnect/pause commands** — and allow-list the writes.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use edgecommons::prelude::*;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::oneshot;

use crate::device::{BrowseError, BrowsePage, ConnectionConfig, Reading};


/// One device == one entry of `component.instances[]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceConfig {
    /// The instance id. It is the `{instance}` token of this device's UNS topics, so it must be a
    /// valid UNS token (lower-kebab).
    pub id: String,
    /// Which backend to use. Matches [`crate::device::DeviceBackend::kind`]: `mtconnect` (the
    /// default) or `sim`.
    #[serde(default = "default_adapter")]
    pub adapter: String,
    pub connection: ConnectionConfig,
    /// How often to read, in milliseconds.
    #[serde(default = "default_poll_ms")]
    pub poll_interval_ms: u64,
    /// Writes are **allow-listed by stable `signal.id`**. An empty list means this adapter is
    /// read-only, which is the correct default for anything touching a control system. For
    /// MTConnect the schema pins it to the empty list: the protocol has no write path (D-MTC-7).
    #[serde(default)]
    pub writes: Writes,
    /// The signals this device publishes, each binding one MTConnect `dataItemId` (HLD §5.3).
    #[serde(default)]
    pub signals: Vec<crate::mtconnect::config::SignalConfig>,
    /// Probe-derived signal selection (R1.1): describe which data items to publish instead of (or
    /// beside) naming each one. Absent = only the explicit `signals[]` publish.
    #[serde(default)]
    pub selection: Option<crate::mtconnect::SelectionConfig>,
}

/// Compile the configured instances against the configured agents: bind each MTConnect device to
/// its agent, derive its published endpoint, and stamp the resolved defaults into its `selection`.
///
/// The endpoint is **derived, never configured** (HLD §5.1): an instance names an agent and a
/// device uuid, and `mtconnect://<host>[:<port>]/<uuid>` follows from them, so the two can never
/// disagree. `default_batch_ms` is `component.global.defaults.batchMs`, the coalescing window a
/// selection-derived SAMPLE signal publishes with.
///
/// # Errors
/// [`MtcError::Config`] naming the offending instance when a binding is missing, an agent is
/// unknown, two devices claim the same uuid on one agent, a selection pattern does not compile,
/// or a `sim` instance carries a `selection` (the simulator has no probe to derive from).
pub fn compile_mtconnect(
    devices: &mut [DeviceConfig],
    agents: &[crate::mtconnect::config::AgentConfig],
    default_batch_ms: u32,
) -> std::result::Result<Vec<crate::mtconnect::config::DeviceConfig>, crate::mtconnect::MtcError>
{
    use crate::mtconnect::MtcError;

    if let Some(bad) = devices
        .iter()
        .find(|d| d.adapter != crate::device::KIND && d.selection.is_some())
    {
        return Err(MtcError::Config(format!(
            "instance `{}`: `selection` requires the `{}` adapter - the `{}` backend has no probe \
             to derive signals from",
            bad.id,
            crate::device::KIND,
            bad.adapter
        )));
    }

    let mut compiled = Vec::new();
    for device in devices.iter_mut().filter(|d| d.adapter == crate::device::KIND) {
        let (agent_id, device_uuid) = crate::device::connection_binding(&device.connection)
            .map_err(|e| MtcError::Config(format!("instance `{}`: {e}", device.id)))?;
        let agent = agents.iter().find(|a| a.id == agent_id).ok_or_else(|| {
            MtcError::Config(format!(
                "instance `{}` references unknown agent `{agent_id}`",
                device.id
            ))
        })?;
        if device.connection.endpoint.is_empty() {
            device.connection.endpoint =
                crate::device::endpoint_description(&agent.url, &device_uuid);
        }
        let selection = device.selection.clone().map(|mut s| {
            s.default_batch_ms = default_batch_ms;
            s
        });
        compiled.push(crate::mtconnect::config::DeviceConfig {
            id: device.id.clone(),
            agent_id,
            device_uuid,
            signals: device.signals.clone(),
            selection,
        });
    }
    crate::mtconnect::config::validate_bindings(agents, &compiled)?;
    Ok(compiled)
}

/// The `component.global.defaults.batchMs` of a raw global object (`0` when unset) — the batch
/// window selection-derived SAMPLE signals publish with.
#[must_use]
pub fn default_batch_ms_of(global: &serde_json::Value) -> u32 {
    global
        .get("defaults")
        .and_then(|d| d.get("batchMs"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Writes {
    /// Signal ids this adapter is permitted to write. Nothing else is writable, whatever the
    /// command asks for.
    #[serde(default)]
    pub allow: Vec<String>,
}

impl Writes {
    #[must_use]
    pub fn permits(&self, signal_id: &str) -> bool {
        self.allow.iter().any(|s| s == signal_id)
    }
}

fn default_adapter() -> String {
    // This component's own protocol. The simulator is opt-in (`"sim"`), matching the schema's
    // default so configuration and code cannot disagree about what an unset `adapter` means.
    crate::device::KIND.into()
}
fn default_poll_ms() -> u64 {
    5_000
}

/// Reconnect backoff. Exponential with full jitter and a cap — so a site whose PLC reboots does
/// not get every adapter in the plant reconnecting in lockstep on the same second.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    pub base_ms: u64,
    pub max_ms: u64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self { base_ms: 1_000, max_ms: 60_000 }
    }
}

impl Backoff {
    #[must_use]
    pub fn delay(&self, attempt: u32, rand01: f64) -> Duration {
        let exp = self.base_ms.saturating_mul(1_u64 << attempt.min(20));
        let cap = exp.min(self.max_ms);
        Duration::from_millis((rand01.clamp(0.0, 1.0) * cap as f64) as u64)
    }
}

/// This adapter's **own vocabulary** for a link's condition — what it reports as
/// `InstanceConnectivity::state`. A boolean cannot tell "still trying" from "backing off after a
/// failure"; an operator needs to, so the richer token exists alongside the normalized flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LinkState {
    /// Connecting for the first time; nothing has failed yet.
    #[default]
    Connecting = 0,
    /// The session is up and being polled.
    Online = 1,
    /// The link failed; reconnecting with backoff.
    Backoff = 2,
}

impl LinkState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "CONNECTING",
            Self::Online => "ONLINE",
            Self::Backoff => "BACKOFF",
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Online,
            2 => Self::Backoff,
            _ => Self::Connecting,
        }
    }
}

/// The shared per-device state the metrics emitter reads and the connectivity provider renders.
/// The gauges (`connection_state`, `signals_subscribed`, latencies) and the interval counters
/// (`read_errors`, `write_errors`, `reconnects`) feed `southbound_health` ([`crate::metrics`]);
/// `paused` and `link` feed the connectivity token and `sb/status`. One source, several surfaces —
/// so a health dot, a metric, and a status reply can never disagree.
#[derive(Default)]
pub struct Health {
    /// 1 = connected, 0 = down.
    pub connection_state: AtomicU64,
    /// The [`LinkState`], as a `u8`. Read it through [`Health::link`].
    link: AtomicU8,
    /// 1 = telemetry production is paused (`sb/pause`). Read by the connectivity provider and
    /// `sb/status`; NOT a `southbound_health` measure (§5 has no `paused`).
    pub paused: AtomicBool,
    pub poll_latency_ms: AtomicU64,
    pub publish_latency_ms: AtomicU64,
    pub read_errors: AtomicU64,
    /// Write entries that failed on the device path (`sb/write`: rejected by the device, or
    /// aborted by an unavailable device) — drained into `southbound_health.writeErrors` on each
    /// emit.
    pub write_errors: AtomicU64,
    pub reconnects: AtomicU64,
    /// The size of this device's `sb/signals` inventory. Read it through
    /// [`Health::signals_subscribed`], which reports it only while the link is ONLINE.
    signal_inventory: AtomicU64,
}

impl Health {
    /// Record the link's condition. The metric's boolean and the reported state token move
    /// **together**, so the health dot and the label a console shows can never disagree.
    pub fn set_link(&self, state: LinkState) {
        self.link.store(state as u8, Ordering::Relaxed);
        self.connection_state
            .store(u64::from(state == LinkState::Online), Ordering::Relaxed);
    }

    #[must_use]
    pub fn link(&self) -> LinkState {
        LinkState::from_u8(self.link.load(Ordering::Relaxed))
    }

    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Record the size of the `sb/signals` inventory this device serves.
    pub fn set_signal_inventory(&self, count: u64) {
        self.signal_inventory.store(count, Ordering::Relaxed);
    }

    /// The `southbound_health.signalsSubscribed` gauge: the `sb/signals` inventory size while the
    /// link is ONLINE, and 0 while it is not — the gauge and the connection state come from the same
    /// source, so they can never disagree.
    #[must_use]
    pub fn signals_subscribed(&self) -> u64 {
        if self.link() == LinkState::Online {
            self.signal_inventory.load(Ordering::Relaxed)
        } else {
            0
        }
    }
}

// =================================================================================================
// Timestamps — the four-slot mapping (docs/SOUTHBOUND.md §2)
// =================================================================================================

/// Auto-stamp `received_ts` at read completion: every reading the backend did not stamp itself
/// gets the adapter's receive moment — ONE moment for the whole batch, taken when the read
/// completed, not when each sample is published (under batching those diverge). A backend's own
/// (earlier) receive stamp survives.
pub fn stamp_received(readings: &mut [Reading], now: &str) {
    for r in readings {
        if r.received_ts.is_none() {
            r.received_ts = Some(now.to_string());
        }
    }
}

/// Map a [`Reading`]'s timestamp slots onto the wire sample. Returns `(server_ts, received_extra)`:
///
/// * `server_ts` — the effective `serverTs`: the **capture** moment (`capture_ts`), falling back
///   to the adapter's receive moment (`received_ts`) — for a direct-client protocol the receive
///   moment IS the capture moment.
/// * `received_extra` — the `receivedTs` value to ride as a per-sample extra, present ONLY when a
///   mediating server makes the receive moment differ from the effective `serverTs`.
///
/// `source_ts` needs no mapping: it is published as `sourceTs` verbatim when present and is never
/// synthesized.
#[must_use]
pub fn sample_timestamps(r: &Reading) -> (Option<String>, Option<String>) {
    let server_ts = r.capture_ts.clone().or_else(|| r.received_ts.clone());
    let received_extra = match (&server_ts, &r.received_ts) {
        (Some(server), Some(received)) if server != received => Some(received.clone()),
        _ => None,
    };
    (server_ts, received_extra)
}

/// Build the wire [`Sample`] for one reading — the whole publish mapping, in one tested place so
/// the live poll loop only has to call it.
///
/// * a reading with a value publishes it; a reading **without** one publishes an explicit JSON
///   `null` ([`Sample::null_value`]) beside its quality — the MTConnect `UNAVAILABLE` case, which
///   is a *bad* null and must not be confused with a good one;
/// * quality and the protocol-native `qualityRaw` always ride along;
/// * the four-slot timestamp mapping is [`sample_timestamps`]: capture → `serverTs`, a distinct
///   receive moment → the `receivedTs` extra, `sourceTs` verbatim and never synthesized;
/// * protocol extras (MTConnect's `sequence`, `resetTriggered`, …) are copied last.
#[must_use]
pub fn build_sample(r: &Reading) -> Sample {
    let quality = match r.quality {
        crate::device::Quality::Good => edgecommons::facades::Quality::Good,
        crate::device::Quality::Bad => edgecommons::facades::Quality::Bad,
        crate::device::Quality::Uncertain => edgecommons::facades::Quality::Uncertain,
    };
    let mut sample = match &r.value {
        Some(value) => Sample::with_quality(value.clone(), quality),
        None => {
            // The explicit-null opt-in gates the null's PERMISSION, not its quality: this is a
            // published `"value": null` with a BAD quality and the agent's own reason.
            let mut s = Sample::null_value();
            s.quality = Some(quality);
            s
        }
    };
    if let Some(raw) = &r.quality_raw {
        sample = sample.quality_raw(raw);
    }
    let (server_ts, received_extra) = sample_timestamps(r);
    sample.source_ts = r.source_ts.clone();
    if let Some(ts) = server_ts {
        sample = sample.server_ts(ts);
    }
    if let Some(received) = received_extra {
        sample = sample.extra("receivedTs", received);
    }
    if let Some(extra) = &r.extra {
        for (key, value) in extra {
            sample = sample.extra(key.clone(), value.clone());
        }
    }
    sample
}

/// Flip the paused flag, returning whether the state actually changed (idempotent — pausing an
/// already-paused device is not an error). The event is emitted by the caller, which holds the
/// `events()` facade.
#[must_use]
pub fn set_paused(health: &Health, paused: bool) -> bool {
    health.paused.swap(paused, Ordering::Relaxed) != paused
}

/// One device's connectivity sample, for the instance-connectivity provider registered in
/// [`App::run`].
///
/// * `connected` is the **normalized** flag — always present.
/// * `state` is *this adapter's* vocabulary ([`LinkState`]) — `PAUSED` when paused and up, else the
///   raw link token (so a break while paused still reads `BACKOFF`, `connected` staying truthful).
/// * `attributes` is the **open** bag: domain data only this adapter understands.
#[must_use]
pub fn connectivity_of(cfg: &DeviceConfig, health: &Health) -> InstanceConnectivity {
    let link = health.link();
    let connected = link == LinkState::Online;
    let paused = health.is_paused();
    let state = if paused && connected { "PAUSED" } else { link.as_str() };

    let mut attributes = serde_json::Map::new();
    attributes.insert("adapter".to_string(), json!(cfg.adapter));
    attributes.insert("paused".to_string(), json!(paused));

    InstanceConnectivity::new(&cfg.id, connected, Some(cfg.connection.endpoint.clone()))
        .with_state(state)
        .with_attributes(attributes)
}

// =================================================================================================
// The device control channel
// =================================================================================================

/// A confirmed, allow-listed write of one signal, on its way from the command inbox to the device's
/// own task (`sb/write`).
pub struct WriteRequest {
    pub signal_id: String,
    pub value: serde_json::Value,
    /// The device's answer. A write is confirmed, not fire-and-forget.
    pub ack: oneshot::Sender<std::result::Result<(), String>>,
}

/// One message on a device's **control channel**. Every `sb/*` verb that must touch the session or
/// serialize with the poll loop is delivered as one of these, so the command inbox never touches the
/// (non-`Sync`) session directly — the device's own task services them one at a time. The reply
/// riding each variant is what makes reads/writes/reconnect *confirmed*.
pub enum DeviceControl {
    /// A confirmed, allow-listed write (`sb/write`). The allow-list is checked in the command layer
    /// BEFORE this is ever sent.
    Write(WriteRequest),
    /// Live-read these ids now (`sb/read`). Serializes with the loop and works while paused.
    ReadNow {
        ids: Vec<String>,
        reply: oneshot::Sender<std::result::Result<Vec<Reading>, String>>,
    },
    /// One page of address-space discovery (`sb/browse`).
    Browse {
        cursor: Option<String>,
        max: usize,
        reply: oneshot::Sender<std::result::Result<BrowsePage, BrowseError>>,
    },
    /// Pause telemetry production (`sb/pause`). Reply = whether the state changed.
    Pause { reply: oneshot::Sender<bool> },
    /// Resume telemetry production (`sb/resume`). Reply = whether the state changed.
    Resume { reply: oneshot::Sender<bool> },
    /// Drop + re-establish, one immediate attempt (`reconnect`). `Ok(())` ⇒ connected, `Err` ⇒
    /// failed (mapped to `RECONNECT_FAILED`).
    Reconnect { reply: oneshot::Sender<std::result::Result<(), String>> },
    /// Force an immediate poll now (`repoll`). Reply = signals read, or `Err` when refused (paused).
    Repoll { reply: oneshot::Sender<std::result::Result<u64, String>> },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_device_parses_from_its_instance_config() {
        let d: DeviceConfig = serde_json::from_value(json!({
            "id": "plc-1",
            "adapter": "sim",
            "connection": { "endpoint": "sim://plc-1", "unitId": 3 },
            "pollIntervalMs": 1000,
            "writes": { "allow": ["setpoint-1"] }
        }))
        .unwrap();

        assert_eq!(d.id, "plc-1");
        assert_eq!(d.poll_interval_ms, 1_000);
        // `connection` is deliberately open: every protocol needs different keys.
        assert_eq!(d.connection.extra["unitId"], 3);
    }

    #[test]
    fn an_adapter_is_read_only_until_a_write_is_allow_listed() {
        // The default must be read-only. An adapter that writes any address it is asked to is a
        // control-system vulnerability, not a convenience.
        let d: DeviceConfig = serde_json::from_value(json!({
            "id": "plc-1",
            "connection": { "endpoint": "sim://plc-1" }
        }))
        .unwrap();
        assert!(!d.writes.permits("setpoint-1"), "nothing is writable by default");

        let w = Writes { allow: vec!["setpoint-1".into()] };
        assert!(w.permits("setpoint-1"));
        assert!(!w.permits("setpoint-2"), "only the listed signal, not its neighbours");
    }

    #[test]
    fn reconnect_backoff_is_exponential_capped_and_jittered() {
        let b = Backoff { base_ms: 1_000, max_ms: 10_000 };
        assert_eq!(b.delay(0, 1.0).as_millis(), 1_000);
        assert_eq!(b.delay(2, 1.0).as_millis(), 4_000);
        assert_eq!(b.delay(20, 1.0).as_millis(), 10_000, "capped");
        // Jitter: the delay is a point in the window, not its edge.
        assert_eq!(b.delay(2, 0.5).as_millis(), 2_000);
        assert_eq!(b.delay(2, 0.0).as_millis(), 0);
    }

    #[test]
    fn an_unknown_config_key_is_rejected_rather_than_ignored() {
        let bad = serde_json::from_value::<DeviceConfig>(json!({
            "id": "plc-1",
            "connection": { "endpoint": "x" },
            "pollIntervalMS": 1000
        }));
        assert!(bad.is_err(), "a typo'd key is a mistake, not a no-op");
    }

    #[test]
    fn every_device_reports_its_own_connectivity() {
        let cfg: DeviceConfig = serde_json::from_value(json!({
            "id": "plc-1",
            "adapter": "sim",
            "connection": { "endpoint": "sim://plc-1" }
        }))
        .unwrap();
        let health = Health::default();

        // Before the first connect: not reachable, and the token says why — CONNECTING, not BACKOFF.
        let c = connectivity_of(&cfg, &health);
        assert_eq!(c.instance, "plc-1");
        assert!(!c.connected);
        assert_eq!(c.state.as_deref(), Some("CONNECTING"));
        assert_eq!(c.detail.as_deref(), Some("sim://plc-1"), "the endpoint, for a human");
        assert_eq!(c.attributes["adapter"], json!("sim"), "the open bag carries domain data");
        assert_eq!(c.attributes["paused"], json!(false));

        health.set_link(LinkState::Online);
        let c = connectivity_of(&cfg, &health);
        assert!(c.connected, "the normalized flag every console reads");
        // D-SC-7: this sample IS the keepalive's `instances[]` element (`supervisor.rs` registers
        // it as the provider), so the state reaches every passive fleet view — a live device is
        // distinguishable from a reconnecting one without knowing this adapter's internals.
        assert_eq!(c.state.as_deref(), Some("ONLINE"));
        assert_eq!(c.to_json()["state"], json!("ONLINE"), "the state rides the keepalive element");

        health.set_link(LinkState::Backoff);
        assert!(!connectivity_of(&cfg, &health).connected);
    }

    #[test]
    fn a_paused_online_device_reports_paused_but_stays_connected() {
        let cfg: DeviceConfig = serde_json::from_value(json!({
            "id": "plc-1", "connection": { "endpoint": "sim://plc-1" }
        }))
        .unwrap();
        let health = Health::default();
        health.set_link(LinkState::Online);

        assert!(set_paused(&health, true), "pausing changed the state");
        assert!(!set_paused(&health, true), "pausing again is idempotent");
        let c = connectivity_of(&cfg, &health);
        // D-SC-7: PAUSED reaches the keepalive too, so a deliberately paused instance is
        // distinguishable from a silently stale one on the passive surface.
        assert_eq!(c.state.as_deref(), Some("PAUSED"), "paused + online = PAUSED");
        assert_eq!(c.to_json()["state"], json!("PAUSED"), "the state rides the keepalive element");
        assert!(c.connected, "connected stays truthful while paused");
        assert_eq!(c.attributes["paused"], json!(true));

        // A break while paused reports BACKOFF (not PAUSED), `connected` false.
        health.set_link(LinkState::Backoff);
        let c = connectivity_of(&cfg, &health);
        assert_eq!(c.state.as_deref(), Some("BACKOFF"));
        assert!(!c.connected);
    }

    #[test]
    fn the_normalized_flag_and_the_health_metric_cannot_disagree() {
        let health = Health::default();
        health.set_link(LinkState::Online);
        assert_eq!(health.connection_state.load(Ordering::Relaxed), 1);
        health.set_link(LinkState::Backoff);
        assert_eq!(health.connection_state.load(Ordering::Relaxed), 0);
    }

    // --- timestamps: the four-slot mapping (docs/SOUTHBOUND.md §2) -------------------------------

    fn reading(id: &str) -> Reading {
        Reading::good(id, json!(1.0))
    }

    #[test]
    fn the_worker_auto_stamps_received_ts_at_read_completion() {
        let mut readings = vec![
            reading("a"),
            Reading { received_ts: Some("2026-01-01T00:00:00Z".into()), ..reading("b") },
        ];
        stamp_received(&mut readings, "2026-02-02T00:00:00Z");
        assert_eq!(
            readings[0].received_ts.as_deref(),
            Some("2026-02-02T00:00:00Z"),
            "a missing receive stamp is filled"
        );
        assert_eq!(
            readings[1].received_ts.as_deref(),
            Some("2026-01-01T00:00:00Z"),
            "a backend's own stamp survives"
        );
    }

    #[test]
    fn capture_maps_to_server_ts_and_a_distinct_receive_moment_rides_as_the_extra() {
        let r = Reading {
            source_ts: Some("S".into()),
            capture_ts: Some("C".into()),
            received_ts: Some("R".into()),
            ..reading("a")
        };
        // capture is the serverTs; a mediating server makes receive differ -> it rides as the
        // extra. (sourceTs needs no mapping: it is published verbatim, never synthesized.)
        assert_eq!(sample_timestamps(&r), (Some("C".into()), Some("R".into())));
    }

    #[test]
    fn the_receive_moment_is_the_server_ts_fallback_and_then_not_an_extra() {
        // No capture stamp: a direct client's receive moment IS the capture moment.
        let r = Reading { received_ts: Some("R".into()), ..reading("a") };
        assert_eq!(sample_timestamps(&r), (Some("R".into()), None));
    }

    #[test]
    fn an_equal_capture_and_receive_moment_omits_the_received_ts_extra() {
        let r = Reading {
            capture_ts: Some("X".into()),
            received_ts: Some("X".into()),
            ..reading("a")
        };
        assert_eq!(sample_timestamps(&r), (Some("X".into()), None));
    }

    #[test]
    fn a_reading_with_no_timestamp_slots_maps_to_nothing_synthesized() {
        assert_eq!(sample_timestamps(&reading("a")), (None, None));
    }

    // --- the publish mapping ------------------------------------------------------------------

    #[test]
    fn a_reading_with_a_value_publishes_it_with_its_quality_and_native_code() {
        let r = Reading {
            quality_raw: Some("MTC_OK".into()),
            capture_ts: Some("2026-07-27T10:00:04.250000Z".into()),
            received_ts: Some("2026-07-27T10:00:04.900000Z".into()),
            ..Reading::good("x-position", json!(123.456))
        };
        let s = build_sample(&r);
        assert_eq!(s.value, Some(json!(123.456)));
        assert_eq!(s.quality, Some(edgecommons::facades::Quality::Good));
        assert_eq!(s.quality_raw.as_deref(), Some("MTC_OK"));
        assert!(!s.explicit_null);
        // The agent's capture stamp is the serverTs; the adapter's receive moment differs (a
        // mediated protocol) and rides as the extra.
        assert_eq!(s.server_ts.as_deref(), Some("2026-07-27T10:00:04.250000Z"));
        assert_eq!(
            s.extra.as_ref().unwrap()["receivedTs"],
            json!("2026-07-27T10:00:04.900000Z")
        );
        assert!(s.source_ts.is_none(), "MTConnect has no device-authored time");
    }

    #[test]
    fn an_unavailable_reading_publishes_an_explicit_null_with_bad_quality() {
        // The §2 explicit-null rule gates the null's PERMISSION; the quality stays the protocol's.
        let r = Reading::bad("x-load", "UNAVAILABLE");
        let s = build_sample(&r);
        assert_eq!(s.value, None);
        assert!(s.explicit_null, "a legitimate protocol null, deliberately published");
        assert_eq!(s.quality, Some(edgecommons::facades::Quality::Bad));
        assert_eq!(s.quality_raw.as_deref(), Some("UNAVAILABLE"));
    }

    #[test]
    fn protocol_extras_ride_the_sample() {
        let r = Reading::good("x-position", json!(1.0))
            .with_extra("sequence", json!(37))
            .with_extra("resetTriggered", json!("MANUAL"));
        let extra = build_sample(&r).extra.expect("extras");
        assert_eq!(extra["sequence"], json!(37), "exact once-only ordering, on every sample");
        assert_eq!(extra["resetTriggered"], json!("MANUAL"));
    }

    #[test]
    fn an_uncertain_reading_keeps_its_value_and_says_why() {
        let r = Reading {
            quality: crate::device::Quality::Uncertain,
            quality_raw: Some("MTC_CONDITION:WARNING:ALM-2".into()),
            ..Reading::good("spindle-speed", json!(1200))
        };
        let s = build_sample(&r);
        assert_eq!(s.value, Some(json!(1200)), "a warned value is still a value");
        assert_eq!(s.quality, Some(edgecommons::facades::Quality::Uncertain));
        assert_eq!(s.quality_raw.as_deref(), Some("MTC_CONDITION:WARNING:ALM-2"));
    }

    #[test]
    fn signals_subscribed_reports_the_inventory_only_while_online() {
        let health = Health::default();
        health.set_signal_inventory(2);
        assert_eq!(health.signals_subscribed(), 0, "0 while disconnected");
        health.set_link(LinkState::Online);
        assert_eq!(health.signals_subscribed(), 2, "the sb/signals inventory size while connected");
        health.set_link(LinkState::Backoff);
        assert_eq!(health.signals_subscribed(), 0, "a broken link serves nothing");
    }
}
