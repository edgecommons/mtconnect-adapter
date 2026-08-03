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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::time::Duration;

use edgecommons::prelude::*;
use edgecommons::uns::{Uns, UnsClass};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::oneshot;

use crate::device::{BrowseError, BrowsePage, ConnectionConfig, Reading};
use crate::mtconnect::selection::ChannelBudget;

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
/// disagree. `defaults` carries `component.global.defaults.batchMs` and `.publishMode` — the
/// coalescing window and publish mode a selection-derived SAMPLE signal publishes with; `budgets`
/// carries each instance's resolved UNS channel budget, which shapes its derived channels.
///
/// # Errors
/// [`MtcError::Config`] naming the offending instance when a binding is missing, an agent is
/// unknown, two devices claim the same uuid on one agent, a selection pattern does not compile,
/// or a `sim` instance carries a `selection` (the simulator has no probe to derive from).
pub fn compile_mtconnect(
    devices: &mut [DeviceConfig],
    agents: &[crate::mtconnect::config::AgentConfig],
    defaults: PublishDefaults,
    budgets: &ChannelBudgets,
) -> std::result::Result<Vec<crate::mtconnect::config::DeviceConfig>, crate::mtconnect::MtcError> {
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
    for device in devices
        .iter_mut()
        .filter(|d| d.adapter == crate::device::KIND)
    {
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
            s.default_batch_ms = defaults.batch_ms;
            s.default_publish_mode = defaults.publish_mode;
            s.channel_budget = budgets.get(&device.id);
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

// =================================================================================================
// The UNS channel budget
// =================================================================================================

/// Every instance's resolved [`ChannelBudget`] — how much of its UNS data topic is left for a
/// channel once its own identity is spent.
///
/// Resolved once, at startup, from the live identity: a longer device, component or instance token
/// leaves fewer bytes, so the budget cannot be gamed by naming an instance
/// `line-3-cell-b-spindle-controller`. Adding or removing an instance is `RESTART_REQUIRED`
/// (D-MtconnectAdapter-L5) and the identity is fixed for the process, so these values are stable
/// for the lifetime of the component — a reload restamps the same map onto the recompiled
/// selections.
#[derive(Debug, Clone, Default)]
pub struct ChannelBudgets(std::collections::HashMap<String, ChannelBudget>);

impl ChannelBudgets {
    /// One instance's budget, or the conservative [`ChannelBudget::default`] for an instance that
    /// was never resolved (configuration-shape validation, tests).
    #[must_use]
    pub fn get(&self, instance_id: &str) -> ChannelBudget {
        self.0.get(instance_id).copied().unwrap_or_default()
    }

    /// Record one instance's budget.
    pub fn insert(&mut self, instance_id: impl Into<String>, budget: ChannelBudget) {
        self.0.insert(instance_id.into(), budget);
    }

    /// Resolve the budget of every named instance against the component's live identity.
    #[must_use]
    pub fn resolve<'a>(gg: &EdgeCommons, instance_ids: impl Iterator<Item = &'a str>) -> Self {
        let mut out = Self::default();
        for id in instance_ids {
            let budget = match gg.instance(id) {
                Ok(instance) => channel_budget_of(instance.uns()),
                // An instance id that is not a legal UNS token cannot mint a topic at all; the
                // floor budget makes that visible on the first derivation instead of hiding it.
                Err(e) => {
                    tracing::warn!(instance = %id, error = %e, "cannot resolve the UNS channel budget");
                    ChannelBudget {
                        max_tokens: 0,
                        max_bytes: 0,
                    }
                }
            };
            tracing::debug!(
                instance = %id, tokens = budget.max_tokens, bytes = budget.max_bytes,
                "resolved the UNS channel budget"
            );
            out.insert(id, budget);
        }
        out
    }
}

/// The room one instance's `data` topic leaves for a channel, measured against the library's own
/// topic builder rather than a copy of its rules: mint a topic with a one-token probe channel, and
/// what the prefix did not spend is the budget.
///
/// The 8-level (7-separator) IoT Core depth limit and the 256-byte publish limit are
/// [`Uns::MAX_TOPIC_SLASHES`] and [`Uns::MAX_TOPIC_UTF8_BYTES`]; a rootless, instance-scoped data
/// topic spends four separators on `ecv1/{device}/{component}/{instance}/data`, leaving three
/// channel tokens — two when `topic.includeRoot` adds a site level.
#[must_use]
pub fn channel_budget_of(uns: &Uns) -> ChannelBudget {
    /// A one-token probe channel: the shortest legal UNS token.
    const PROBE: &str = "x";

    let Ok(topic) = uns.topic_with_channel(UnsClass::Data, PROBE) else {
        // Not even a one-token channel is publishable: the identity itself has consumed the
        // topic. Nothing derives — every signal reports the pathological floor.
        return ChannelBudget {
            max_tokens: 0,
            max_bytes: 0,
        };
    };
    // `topic` is `<prefix>/x`, so the prefix (`ecv1/…/data`) is what this instance spends.
    let prefix_len = topic.len().saturating_sub(PROBE.len() + 1);
    let prefix_slashes = topic[..prefix_len].matches('/').count();
    ChannelBudget {
        max_tokens: Uns::MAX_TOPIC_SLASHES.saturating_sub(prefix_slashes),
        max_bytes: Uns::MAX_TOPIC_UTF8_BYTES.saturating_sub(prefix_len + 1),
    }
}

/// The publish defaults of `component.global.defaults` that selection-derived SAMPLE signals
/// compile with (D-MtconnectAdapter-L10): the coalescing window (`batchMs`) and the publish mode
/// (`publishMode`). Explicit signals set their own `publish` block instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishDefaults {
    pub batch_ms: u32,
    pub publish_mode: crate::mtconnect::config::PublishMode,
}

/// The [`PublishDefaults`] of a raw global object: `defaults.batchMs` (`0` when unset) and
/// `defaults.publishMode` (`on-change` when unset).
#[must_use]
pub fn publish_defaults_of(global: &serde_json::Value) -> PublishDefaults {
    let defaults = global.get("defaults");
    let batch_ms = defaults
        .and_then(|d| d.get("batchMs"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0);
    let publish_mode = match defaults
        .and_then(|d| d.get("publishMode"))
        .and_then(serde_json::Value::as_str)
    {
        Some("interval") => crate::mtconnect::config::PublishMode::Interval,
        _ => crate::mtconnect::config::PublishMode::OnChange,
    };
    PublishDefaults {
        batch_ms,
        publish_mode,
    }
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
        Self {
            base_ms: 1_000,
            max_ms: 60_000,
        }
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

/// The update-level extra key carrying the signal's canonical MTConnect component path.
///
/// It is not one of the library's reserved names at any level: the update body reserves only
/// `signal` and `samples` (everything else round-trips through the protobuf `extra` map), and the
/// seven reserved *sample* keys are `value`, `quality`, `qualityRaw`, `sourceTs`, `serverTs`,
/// `sourceTsMs`, `serverTsMs`.
pub const COMPONENT_PATH_KEY: &str = "componentPath";

/// Stamp the signal's canonical component path onto a facade-built `SouthboundSignalUpdate` body
/// as an **update-level** extra (D-MtconnectAdapter-L13).
///
/// * **Unconditional.** The key is written on every published update — for a derived channel that
///   was truncated to the topic budget, for one that was not, and for an explicitly configured
///   signal alike — so a consumer reads one place with no branch.
/// * **Once per update, not per sample.** The path is per-signal-static, and a batched update is
///   one signal's readings, so a flushed window carries exactly one `componentPath`.
/// * The value is the untruncated path as
///   [`ProbeModel::component_path_of`](crate::mtconnect::model::ProbeModel::component_path_of)
///   holds it — the same string `sb/signals` serves in `signal.address.componentPath`, including
///   the empty string for a device-level data item. `None` (no device model describes the signal)
///   stamps JSON `null`, again matching what `sb/signals` reports.
///
/// A non-object body is left alone: there is nowhere to put the key, and the facade never produces
/// one.
pub fn stamp_component_path(body: &mut serde_json::Value, component_path: Option<&str>) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let value = match component_path {
        Some(path) => serde_json::Value::String(path.to_string()),
        None => serde_json::Value::Null,
    };
    obj.insert(COMPONENT_PATH_KEY.to_string(), value);
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
    let state = if paused && connected {
        "PAUSED"
    } else {
        link.as_str()
    };

    let mut attributes = serde_json::Map::new();
    attributes.insert("adapter".to_string(), json!(cfg.adapter));
    attributes.insert("paused".to_string(), json!(paused));

    InstanceConnectivity::new(&cfg.id, connected, Some(cfg.connection.endpoint.clone()))
        .with_state(state)
        .with_attributes(attributes)
}

// =================================================================================================
// Structured lifecycle: the token tree and the bounded, ordered teardown (P1-7)
// =================================================================================================

/// How long ALL device tasks together get to flush their open batch windows, publish them, and
/// detach their sessions before the stragglers are aborted.
///
/// Generous, because this is the window in which buffered readings still reach the wire against a
/// merely *slow* broker; bounded, because against a *dead* one they never will, and an orchestrator
/// that is waiting to `SIGKILL` us would take the rest of the process with it.
///
/// Sized so the three budgets together total 12 s, leaving margin inside the tightest orchestrator
/// stop window this component ships into (Greengrass, 15 s).
pub const DEVICE_SHUTDOWN_BUDGET: Duration = Duration::from_secs(6);

/// How long ALL agent acquisition tasks and their metric tickers together get to acknowledge the
/// stop and unwind. Shorter than the device budget: by the time it starts, the data that was worth
/// saving has already been published by the device tasks.
pub const AGENT_SHUTDOWN_BUDGET: Duration = Duration::from_secs(4);

/// How long the final metric flush gets. Bounded for the same reason as the other two: the flush
/// rides the same messaging facade the acquisition path does, and a dead broker must not be able to
/// hold the process open past the orchestrator's stop window.
pub const METRICS_FLUSH_BUDGET: Duration = Duration::from_secs(2);

/// The cancellation-token tree the structured shutdown drives.
///
/// ```text
///   root
///    ├── devices ── one child token per device task
///    └── agents  ── one child token per acquisition task (+ the shared one every metric ticker
///                   selects on)
/// ```
///
/// Two families, not one flat token, because the **order** matters: the device tasks are cancelled
/// and drained first (their flush needs the messaging facade alive, and their `close()` detaches
/// cleanly from a still-running runtime), and only then are the agents that feed them stopped.
/// Cancelling a parent cancels every token below it, so `root` remains the one lever that stops
/// everything at once.
#[derive(Debug, Clone)]
pub struct TaskTokens {
    root: tokio_util::sync::CancellationToken,
    devices: tokio_util::sync::CancellationToken,
    agents: tokio_util::sync::CancellationToken,
}

impl Default for TaskTokens {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskTokens {
    #[must_use]
    pub fn new() -> Self {
        let root = tokio_util::sync::CancellationToken::new();
        let devices = root.child_token();
        let agents = root.child_token();
        Self {
            root,
            devices,
            agents,
        }
    }

    /// A token for one device task.
    #[must_use]
    pub fn device(&self) -> tokio_util::sync::CancellationToken {
        self.devices.child_token()
    }

    /// A token for one agent's acquisition task.
    #[must_use]
    pub fn agent(&self) -> tokio_util::sync::CancellationToken {
        self.agents.child_token()
    }

    /// The shared agent-family token — what the per-agent metric tickers select on.
    #[must_use]
    pub fn agents(&self) -> tokio_util::sync::CancellationToken {
        self.agents.clone()
    }

    /// Tell every device task to flush, detach, and return.
    pub fn cancel_devices(&self) {
        self.devices.cancel();
    }

    /// Tell every acquisition task and metric ticker to unwind.
    pub fn cancel_agents(&self) {
        self.agents.cancel();
    }

    /// Stop everything at once — the last-resort lever, and what a caller uses when it is
    /// abandoning the whole component rather than shutting it down in order.
    pub fn cancel_all(&self) {
        self.root.cancel();
    }
}

/// What the teardown could not stop in time: the tasks that were aborted, by name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    pub aborted_devices: Vec<String>,
    pub aborted_agents: Vec<String>,
}

impl ShutdownReport {
    /// Whether every task returned on its own before its budget ran out.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.aborted_devices.is_empty() && self.aborted_agents.is_empty()
    }
}

/// Join every task under ONE shared `budget`, aborting and naming whatever is still running when it
/// runs out.
///
/// The budget is shared, not per task: ten device tasks flushing in parallel are one shutdown, and
/// giving each its own window would multiply the worst case by the number of instances. Tasks are
/// joined in order, each against the same absolute deadline, so once the deadline has passed the
/// remaining stragglers are aborted immediately rather than each waiting again.
pub async fn join_all_within(
    tasks: Vec<(String, tokio::task::JoinHandle<()>)>,
    budget: Duration,
) -> Vec<String> {
    join_all_by(tasks, tokio::time::Instant::now() + budget).await
}

/// [`join_all_within`] against an absolute deadline — how a phase that has already spent part of
/// its budget joins what is left.
async fn join_all_by(
    tasks: Vec<(String, tokio::task::JoinHandle<()>)>,
    deadline: tokio::time::Instant,
) -> Vec<String> {
    let mut aborted = Vec::new();
    for (name, mut handle) in tasks {
        match tokio::time::timeout_at(deadline, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(task = %name, error = %e, "task did not unwind cleanly");
            }
            Err(_) => {
                handle.abort();
                tracing::warn!(
                    task = %name,
                    "task did not stop within the shutdown budget; aborting it"
                );
                aborted.push(name);
            }
        }
    }
    aborted
}

/// The whole teardown, in the one order that is safe, and bounded at every step (P1-7).
///
/// 1. **Cancel the device tasks** — each flushes its open batch windows, publishes them, and closes
///    (detaches) its session.
/// 2. **Join them** under [`DEVICE_SHUTDOWN_BUDGET`]; abort and name whatever is still running.
/// 3. **Cancel the agent family, then tell each agent to stop**, and join the acquisition tasks and
///    metric tickers — the whole phase inside [`AGENT_SHUTDOWN_BUDGET`].
/// 4. **Flush the metrics last**, inside [`METRICS_FLUSH_BUDGET`], so the final counters include the
///    shutdown's own work.
///
/// Devices come first because their flush needs the messaging facade alive and their detach is only
/// clean against a still-running runtime; the agents follow; the counters go out last. Every step is
/// bounded because the failure that makes shutdown matter — a dead broker or a dead Greengrass IPC
/// link — is exactly the one that makes an unbounded step never return, and an orchestrator that
/// gets tired of waiting sends `SIGKILL`, which loses everything still buffered.
///
/// Worst case: [`DEVICE_SHUTDOWN_BUDGET`] + [`AGENT_SHUTDOWN_BUDGET`] + [`METRICS_FLUSH_BUDGET`].
pub async fn shutdown_within(
    tokens: &TaskTokens,
    device_tasks: Vec<(String, tokio::task::JoinHandle<()>)>,
    agent_tasks: Vec<(String, tokio::task::JoinHandle<()>)>,
    stop_agents: impl std::future::Future<Output = ()>,
    flush_metrics: impl std::future::Future<Output = ()>,
) -> ShutdownReport {
    // 1-2. The devices: buffered readings are data, so they get the first and largest window.
    tokens.cancel_devices();
    let aborted_devices = join_all_within(device_tasks, DEVICE_SHUTDOWN_BUDGET).await;

    // 3. The agents: one shared window covering both the stop request and the join, so the phase
    //    cannot outrun its budget by way of an agent that will not acknowledge.
    let agents_by = tokio::time::Instant::now() + AGENT_SHUTDOWN_BUDGET;
    tokens.cancel_agents();
    if tokio::time::timeout_at(agents_by, stop_agents)
        .await
        .is_err()
    {
        tracing::warn!("agents did not acknowledge the stop within the budget");
    }
    let aborted_agents = join_all_by(agent_tasks, agents_by).await;

    // 4. The counters, including everything the two phases above just did.
    if tokio::time::timeout(METRICS_FLUSH_BUDGET, flush_metrics)
        .await
        .is_err()
    {
        tracing::warn!("the final metric flush did not complete within its budget");
    }

    ShutdownReport {
        aborted_devices,
        aborted_agents,
    }
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
    Reconnect {
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    /// Force an immediate poll now (`repoll`). Reply = signals read, or `Err` when refused (paused).
    Repoll {
        reply: oneshot::Sender<std::result::Result<u64, String>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- the UNS channel budget -----------------------------------------------------------------

    /// A `Uns` bound to an identity, exactly as the library builds one for an instance.
    fn uns(device: &str, component: &str, instance: Option<&str>, rooted: bool) -> Uns {
        use edgecommons::messaging::message::{HierEntry, MessageIdentity};
        let hier = if rooted {
            vec![
                HierEntry {
                    level: "site".into(),
                    value: "plant1".into(),
                },
                HierEntry {
                    level: "device".into(),
                    value: device.into(),
                },
            ]
        } else {
            vec![HierEntry {
                level: "device".into(),
                value: device.into(),
            }]
        };
        let identity = MessageIdentity::new(hier, component, instance.map(str::to_string)).unwrap();
        Uns::new(identity, rooted)
    }

    #[test]
    fn the_channel_budget_is_what_the_identity_did_not_spend() {
        // The ordinary adapter shape: `ecv1/{device}/{component}/{instance}/data/…`. Four of the
        // seven separators are spent, so three channel tokens remain.
        let u = uns("gw-01", "mtconnect-adapter", Some("cnc-1"), false);
        let b = channel_budget_of(&u);
        assert_eq!(b.max_tokens, 3);
        let prefix = "ecv1/gw-01/mtconnect-adapter/cnc-1/data/";
        assert_eq!(b.max_bytes, Uns::MAX_TOPIC_UTF8_BYTES - prefix.len());

        // The budget is not a copy of the rules — it agrees with the builder itself, at the edge.
        assert!(u.topic_with_channel(UnsClass::Data, "a/b/c").is_ok());
        assert!(
            u.topic_with_channel(UnsClass::Data, "a/b/c/d").is_err(),
            "one token over"
        );
        assert!(u
            .topic_with_channel(UnsClass::Data, &"x".repeat(b.max_bytes))
            .is_ok());
        assert!(
            u.topic_with_channel(UnsClass::Data, &"x".repeat(b.max_bytes + 1))
                .is_err(),
            "one byte over"
        );
    }

    #[test]
    fn the_mazak_stock_path_is_exactly_one_token_over_the_budget() {
        // The regression this rule exists for, pinned as a fact rather than a memory: the live
        // demo Mazak's `Resources[resources]/Materials[materials]/Stock[stock]` plus its id is
        // four channel tokens, and the library refuses that topic. Dropping the root-side segment
        // is what makes the signal publishable at all.
        let u = uns("gw-01", "MtconnectAdapter", Some("cnc-1"), false);
        let untruncated = "resources-resources/materials-materials/stock-stock/stock";
        assert!(
            u.topic_with_channel(UnsClass::Data, untruncated).is_err(),
            "the whole path is unpublishable - this is the bug"
        );
        let leaf_preserving = "materials-materials/stock-stock/stock";
        assert_eq!(
            u.topic_with_channel(UnsClass::Data, leaf_preserving)
                .unwrap(),
            "ecv1/gw-01/MtconnectAdapter/cnc-1/data/materials-materials/stock-stock/stock"
        );

        // ... and it is what the derivation rule produces from this instance's own budget.
        let derived = crate::mtconnect::selection::derive_channel(
            "Resources[resources]/Materials[materials]/Stock[stock]",
            "stock",
            channel_budget_of(&u),
        );
        assert_eq!(derived.channel, leaf_preserving);
        assert_eq!(derived.dropped, 1);
        assert!(derived.fits);
    }

    #[test]
    fn a_longer_identity_leaves_a_smaller_budget() {
        let short = channel_budget_of(&uns("gw", "mtc", Some("a"), false));
        let long = channel_budget_of(&uns(
            "packaging-line-3-edge-gateway",
            "mtconnect-adapter",
            Some("okuma-genos-vertical-machining-center-1"),
            false,
        ));
        // The token budget is a property of the grammar; the BYTE budget is what a long identity
        // spends, so a derived channel cannot be bought with a verbose instance name.
        assert_eq!(short.max_tokens, long.max_tokens);
        assert!(
            long.max_bytes < short.max_bytes - 60,
            "{} vs {}",
            long.max_bytes,
            short.max_bytes
        );

        // A rooted (site) topic spends one more level: two channel tokens, not three.
        assert_eq!(
            channel_budget_of(&uns("gw", "mtc", Some("a"), true)).max_tokens,
            2
        );
    }

    #[test]
    fn an_unpublishable_identity_reports_the_floor_budget() {
        // A device token long enough that not even a one-token channel fits: the budget says so
        // rather than pretending there is room.
        let u = uns(&"d".repeat(240), "mtconnect-adapter", Some("cnc-1"), false);
        assert!(u.topic_with_channel(UnsClass::Data, "x").is_err());
        assert_eq!(
            channel_budget_of(&u),
            ChannelBudget {
                max_tokens: 0,
                max_bytes: 0
            }
        );
    }

    #[test]
    fn budgets_default_for_an_instance_that_was_never_resolved() {
        let mut budgets = ChannelBudgets::default();
        assert_eq!(budgets.get("cnc-1"), ChannelBudget::default());
        budgets.insert(
            "cnc-1",
            ChannelBudget {
                max_tokens: 2,
                max_bytes: 40,
            },
        );
        assert_eq!(
            budgets.get("cnc-1"),
            ChannelBudget {
                max_tokens: 2,
                max_bytes: 40
            }
        );
        assert_eq!(
            budgets.get("cnc-2"),
            ChannelBudget::default(),
            "unknown ids fall back"
        );
    }

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
        assert!(
            !d.writes.permits("setpoint-1"),
            "nothing is writable by default"
        );

        let w = Writes {
            allow: vec!["setpoint-1".into()],
        };
        assert!(w.permits("setpoint-1"));
        assert!(
            !w.permits("setpoint-2"),
            "only the listed signal, not its neighbours"
        );
    }

    #[test]
    fn reconnect_backoff_is_exponential_capped_and_jittered() {
        let b = Backoff {
            base_ms: 1_000,
            max_ms: 10_000,
        };
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
        assert_eq!(
            c.detail.as_deref(),
            Some("sim://plc-1"),
            "the endpoint, for a human"
        );
        assert_eq!(
            c.attributes["adapter"],
            json!("sim"),
            "the open bag carries domain data"
        );
        assert_eq!(c.attributes["paused"], json!(false));

        health.set_link(LinkState::Online);
        let c = connectivity_of(&cfg, &health);
        assert!(c.connected, "the normalized flag every console reads");
        // D-SC-7: this sample IS the keepalive's `instances[]` element (`supervisor.rs` registers
        // it as the provider), so the state reaches every passive fleet view — a live device is
        // distinguishable from a reconnecting one without knowing this adapter's internals.
        assert_eq!(c.state.as_deref(), Some("ONLINE"));
        assert_eq!(
            c.to_json()["state"],
            json!("ONLINE"),
            "the state rides the keepalive element"
        );

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
        assert_eq!(
            c.state.as_deref(),
            Some("PAUSED"),
            "paused + online = PAUSED"
        );
        assert_eq!(
            c.to_json()["state"],
            json!("PAUSED"),
            "the state rides the keepalive element"
        );
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
            Reading {
                received_ts: Some("2026-01-01T00:00:00Z".into()),
                ..reading("b")
            },
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
        let r = Reading {
            received_ts: Some("R".into()),
            ..reading("a")
        };
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
        assert!(
            s.source_ts.is_none(),
            "MTConnect has no device-authored time"
        );
    }

    #[test]
    fn an_unavailable_reading_publishes_an_explicit_null_with_bad_quality() {
        // The §2 explicit-null rule gates the null's PERMISSION; the quality stays the protocol's.
        let r = Reading::bad("x-load", "UNAVAILABLE");
        let s = build_sample(&r);
        assert_eq!(s.value, None);
        assert!(
            s.explicit_null,
            "a legitimate protocol null, deliberately published"
        );
        assert_eq!(s.quality, Some(edgecommons::facades::Quality::Bad));
        assert_eq!(s.quality_raw.as_deref(), Some("UNAVAILABLE"));
    }

    #[test]
    fn protocol_extras_ride_the_sample() {
        let r = Reading::good("x-position", json!(1.0))
            .with_extra("sequence", json!(37))
            .with_extra("resetTriggered", json!("MANUAL"));
        let extra = build_sample(&r).extra.expect("extras");
        assert_eq!(
            extra["sequence"],
            json!(37),
            "exact once-only ordering, on every sample"
        );
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
        assert_eq!(
            s.value,
            Some(json!(1200)),
            "a warned value is still a value"
        );
        assert_eq!(s.quality, Some(edgecommons::facades::Quality::Uncertain));
        assert_eq!(
            s.quality_raw.as_deref(),
            Some("MTC_CONDITION:WARNING:ALM-2")
        );
    }

    // --- the canonical componentPath, on every update (D-MtconnectAdapter-L13) ------------------

    /// A body in the shape `DataFacade::build_body` produces — `{device, signal, samples}`.
    fn facade_body() -> serde_json::Value {
        json!({
            "device": { "adapter": "mtconnect", "instance": "cnc-1", "endpoint": "http://agent" },
            "signal": { "id": "x-position", "name": "X position" },
            "samples": [ { "value": 1.0, "quality": "GOOD", "serverTs": "2026-07-27T10:00:00Z" } ]
        })
    }

    #[test]
    fn the_component_path_is_stamped_at_the_update_level_under_the_agreed_key() {
        let mut body = facade_body();
        stamp_component_path(&mut body, Some("Axes/Linear[X]"));
        assert_eq!(body[COMPONENT_PATH_KEY], json!("Axes/Linear[X]"));
        assert_eq!(
            COMPONENT_PATH_KEY, "componentPath",
            "the agreed key, not an alias"
        );
        // Beside the canonical members, never inside one of them.
        assert_eq!(body["signal"]["id"], json!("x-position"));
        assert_eq!(body["samples"].as_array().expect("samples").len(), 1);
        assert!(
            body["samples"][0].get(COMPONENT_PATH_KEY).is_none(),
            "per-signal-static: it rides the update, not every sample"
        );
        assert!(body["signal"].get(COMPONENT_PATH_KEY).is_none());
        assert!(
            body.get("device").is_some(),
            "the facade's own members are untouched"
        );
    }

    #[test]
    fn the_key_collides_with_no_reserved_name_at_any_level() {
        // The library reserves `signal`/`samples` at the update level (everything else round-trips
        // through the protobuf `extra` map) and these seven at the sample level.
        const RESERVED_SAMPLE_KEYS: [&str; 7] = [
            "value",
            "quality",
            "qualityRaw",
            "sourceTs",
            "serverTs",
            "sourceTsMs",
            "serverTsMs",
        ];
        assert!(!RESERVED_SAMPLE_KEYS.contains(&COMPONENT_PATH_KEY));
        assert_ne!(COMPONENT_PATH_KEY, "signal");
        assert_ne!(COMPONENT_PATH_KEY, "samples");
        assert_ne!(COMPONENT_PATH_KEY, "device");
    }

    #[test]
    fn a_device_level_item_stamps_the_empty_path_rather_than_omitting_it() {
        // `sb/signals` serves `""` for a data item that hangs off no component. The update says
        // the same thing, so presence is unconditional even at the root of the model.
        let mut body = facade_body();
        stamp_component_path(&mut body, Some(""));
        assert_eq!(body[COMPONENT_PATH_KEY], json!(""));
        assert!(body
            .as_object()
            .expect("object")
            .contains_key(COMPONENT_PATH_KEY));
    }

    #[test]
    fn a_signal_no_model_describes_stamps_null_exactly_as_sb_signals_reports_it() {
        // The permanent-BAD case: an explicit signal whose `dataItemId` is not in the probe.
        // `sb/signals` answers `address.componentPath: null`; the update carries the same null,
        // so the key is still there and a consumer never branches on its absence.
        let mut body = facade_body();
        stamp_component_path(&mut body, None);
        assert_eq!(body[COMPONENT_PATH_KEY], json!(null));
        assert!(body
            .as_object()
            .expect("object")
            .contains_key(COMPONENT_PATH_KEY));
    }

    #[test]
    fn truncated_and_untruncated_and_explicit_signals_all_stamp_the_same_way() {
        // The derived channel may be shortened to the topic budget (L12); the stamped path never
        // is, and the three provenances are indistinguishable in the body.
        let deep = "Resources[resources]/Materials[materials]/Stock[stock]";
        for path in [deep, "Axes/Linear[X]", "Controller[cnc]"] {
            let mut body = facade_body();
            stamp_component_path(&mut body, Some(path));
            assert_eq!(
                body[COMPONENT_PATH_KEY],
                json!(path),
                "the untruncated path, verbatim"
            );
        }
    }

    #[test]
    fn stamping_twice_replaces_rather_than_duplicates() {
        let mut body = facade_body();
        stamp_component_path(&mut body, Some("Axes/Linear[X]"));
        stamp_component_path(&mut body, Some("Axes/Rotary[C]"));
        assert_eq!(body[COMPONENT_PATH_KEY], json!("Axes/Rotary[C]"));
        assert_eq!(
            body.as_object()
                .expect("object")
                .keys()
                .filter(|k| *k == COMPONENT_PATH_KEY)
                .count(),
            1
        );
    }

    #[test]
    fn a_non_object_body_is_left_alone_rather_than_replaced() {
        let mut body = json!("not a body");
        stamp_component_path(&mut body, Some("Axes/Linear[X]"));
        assert_eq!(body, json!("not a body"));
    }

    #[test]
    fn signals_subscribed_reports_the_inventory_only_while_online() {
        let health = Health::default();
        health.set_signal_inventory(2);
        assert_eq!(health.signals_subscribed(), 0, "0 while disconnected");
        health.set_link(LinkState::Online);
        assert_eq!(
            health.signals_subscribed(),
            2,
            "the sb/signals inventory size while connected"
        );
        health.set_link(LinkState::Backoff);
        assert_eq!(
            health.signals_subscribed(),
            0,
            "a broken link serves nothing"
        );
    }

    // --- the structured lifecycle (P1-7) --------------------------------------------------------

    /// A shared, ordered trace: what happened, in the order it happened.
    #[derive(Clone, Default)]
    struct Trace(std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>);

    impl Trace {
        fn record(&self, what: &'static str) {
            self.0.lock().expect("trace").push(what);
        }
        fn steps(&self) -> Vec<&'static str> {
            self.0.lock().expect("trace").clone()
        }
    }

    /// A task that finishes on its own after `after`.
    fn finishes_in(
        after: Duration,
        trace: Trace,
        what: &'static str,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            trace.record(what);
        })
    }

    /// A task that can never finish — a device whose publish will never complete because the
    /// transport under it is dead.
    fn never_finishes(trace: Trace) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            std::future::pending::<()>().await;
            trace.record("the stuck task somehow finished");
        })
    }

    #[tokio::test(start_paused = true)]
    async fn joining_tasks_that_all_finish_in_time_aborts_nothing() {
        let trace = Trace::default();
        let tasks = vec![
            (
                "one".to_string(),
                finishes_in(Duration::from_millis(100), trace.clone(), "one"),
            ),
            (
                "two".to_string(),
                finishes_in(Duration::from_millis(300), trace.clone(), "two"),
            ),
        ];
        let started = tokio::time::Instant::now();
        let aborted = join_all_within(tasks, DEVICE_SHUTDOWN_BUDGET).await;
        assert!(aborted.is_empty(), "{aborted:?}");
        assert_eq!(trace.steps(), vec!["one", "two"]);
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(300),
            "the join costs what the slowest task costs, not the whole budget"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_task_that_cannot_finish_is_aborted_at_the_budget_and_named() {
        let trace = Trace::default();
        let tasks = vec![
            (
                "instance `spindle`".to_string(),
                finishes_in(Duration::from_millis(50), trace.clone(), "flushed"),
            ),
            (
                "instance `stuck`".to_string(),
                never_finishes(trace.clone()),
            ),
            // A second straggler behind the first one costs NO extra time: the deadline is shared.
            (
                "instance `also-stuck`".to_string(),
                never_finishes(trace.clone()),
            ),
        ];
        let started = tokio::time::Instant::now();
        let aborted = join_all_within(tasks, DEVICE_SHUTDOWN_BUDGET).await;
        assert_eq!(
            aborted,
            vec![
                "instance `stuck`".to_string(),
                "instance `also-stuck`".to_string()
            ],
            "the stragglers are named so an operator learns which instance hung"
        );
        assert_eq!(
            started.elapsed(),
            DEVICE_SHUTDOWN_BUDGET,
            "one shared budget, however many stragglers"
        );
        assert_eq!(
            trace.steps(),
            vec!["flushed"],
            "the healthy task flushed; the aborted ones never ran past their await"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn joining_nothing_costs_nothing() {
        let started = tokio::time::Instant::now();
        assert!(join_all_within(Vec::new(), DEVICE_SHUTDOWN_BUDGET)
            .await
            .is_empty());
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn a_panicking_task_is_reported_but_not_counted_as_a_straggler() {
        let task = tokio::spawn(async { panic!("the device task fell over") });
        let aborted = join_all_within(vec![("instance `bad`".to_string(), task)], SECOND).await;
        assert!(
            aborted.is_empty(),
            "it stopped; it just did not stop nicely"
        );
    }

    const SECOND: Duration = Duration::from_secs(1);

    #[test]
    fn the_token_tree_stops_the_devices_without_stopping_the_agents_that_feed_them() {
        let tokens = TaskTokens::default();
        let device = tokens.device();
        let agent = tokens.agent();
        let tickers = tokens.agents();

        tokens.cancel_devices();
        assert!(device.is_cancelled(), "the device task is told to unwind");
        assert!(
            !agent.is_cancelled() && !tickers.is_cancelled(),
            "its agent keeps running: a device's last flush still needs the runtime it detaches from"
        );

        tokens.cancel_agents();
        assert!(agent.is_cancelled());
        assert!(tickers.is_cancelled(), "the metric tickers stop with them");

        // Sibling tokens are independent, and the root stops every family at once.
        let tokens = TaskTokens::new();
        let (device, agent) = (tokens.device(), tokens.agent());
        tokens.cancel_all();
        assert!(device.is_cancelled() && agent.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn the_teardown_drains_the_devices_before_it_stops_the_agents_and_flushes_last() {
        let tokens = TaskTokens::new();
        let trace = Trace::default();

        let device_token = tokens.device();
        let device_trace = trace.clone();
        let device = tokio::spawn(async move {
            device_token.cancelled().await;
            // Flushing an open batch window and detaching the session takes a moment.
            tokio::time::sleep(Duration::from_millis(200)).await;
            device_trace.record("device flushed and detached");
        });

        let agent_token = tokens.agent();
        let agent_trace = trace.clone();
        let agent = tokio::spawn(async move {
            agent_token.cancelled().await;
            agent_trace.record("acquisition unwound");
        });

        let stop_trace = trace.clone();
        let flush_trace = trace.clone();
        let report = shutdown_within(
            &tokens,
            vec![("instance `spindle`".to_string(), device)],
            vec![("agent `line-a` acquisition".to_string(), agent)],
            async move { stop_trace.record("agents told to stop") },
            async move { flush_trace.record("metrics flushed") },
        )
        .await;

        assert!(report.is_clean(), "{report:?}");
        assert_eq!(
            trace.steps(),
            vec![
                "device flushed and detached",
                "agents told to stop",
                "acquisition unwound",
                "metrics flushed",
            ],
            "devices drain first, agents second, counters last"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_teardown_completes_inside_its_budget_when_nothing_can_drain() {
        // The failure this whole sequence exists for: the broker (or the Greengrass IPC link) is
        // gone, so the device task's publish never completes and the acquisition task is stuck
        // behind it. Shutdown must still END — an orchestrator that gets tired of waiting sends
        // SIGKILL, and then even the batches that COULD have been flushed are lost.
        let tokens = TaskTokens::new();
        let trace = Trace::default();
        let flush_trace = trace.clone();
        let started = tokio::time::Instant::now();

        let report = shutdown_within(
            &tokens,
            vec![(
                "instance `spindle`".to_string(),
                never_finishes(trace.clone()),
            )],
            vec![(
                "agent `line-a` acquisition".to_string(),
                never_finishes(trace.clone()),
            )],
            // Even the stop request is bounded: an agent that never acknowledges cannot hold the
            // process open.
            std::future::pending::<()>(),
            async move { flush_trace.record("metrics flushed") },
        )
        .await;

        assert_eq!(
            report.aborted_devices,
            vec!["instance `spindle`".to_string()]
        );
        assert_eq!(
            report.aborted_agents,
            vec!["agent `line-a` acquisition".to_string()]
        );
        assert!(!report.is_clean());
        assert_eq!(
            started.elapsed(),
            DEVICE_SHUTDOWN_BUDGET + AGENT_SHUTDOWN_BUDGET,
            "the two phases' budgets, and not one millisecond more"
        );
        assert_eq!(
            trace.steps(),
            vec!["metrics flushed"],
            "the counters still went out, and nothing stuck ever ran again"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_metric_flush_that_never_returns_cannot_hold_the_process_open() {
        let tokens = TaskTokens::new();
        let started = tokio::time::Instant::now();
        let report = shutdown_within(
            &tokens,
            Vec::new(),
            Vec::new(),
            std::future::ready(()),
            std::future::pending::<()>(),
        )
        .await;
        assert!(report.is_clean());
        assert_eq!(started.elapsed(), METRICS_FLUSH_BUDGET);
    }

    #[test]
    fn the_shutdown_budgets_are_the_documented_ones() {
        assert_eq!(DEVICE_SHUTDOWN_BUDGET, Duration::from_secs(6));
        assert_eq!(AGENT_SHUTDOWN_BUDGET, Duration::from_secs(4));
        assert_eq!(METRICS_FLUSH_BUDGET, Duration::from_secs(2));
        assert_eq!(
            DEVICE_SHUTDOWN_BUDGET + AGENT_SHUTDOWN_BUDGET + METRICS_FLUSH_BUDGET,
            Duration::from_secs(12),
            "the whole teardown fits inside the tightest stop window this component ships into \
             (Greengrass, 15 s) with margin to spare"
        );
    }
}
