//! # The southbound command surface — the `sb/*` verbs + the five edge-console panels
//!
//! This module owns the whole `gg.commands()` registration for `MtconnectAdapter`: `sb/status`,
//! `sb/read`, `sb/write`, `sb/signals`, `sb/browse`, `sb/pause`, `sb/resume`, `reconnect`, `repoll`.
//! It is the generic southbound command family (SOUTHBOUND.md §2.2) every adapter serves, mapped
//! onto MTConnect by LLD §7 / HLD §7.
//!
//! ## Conventions every verb follows
//!
//! * **Instance addressing — the library resolves it, this module only looks the device up.** Every
//!   verb declares [`CommandScope::Instance`]. Before a handler runs, the inbox resolves the
//!   delivery's addressing: the topic's instance token
//!   (`ecv1/{device}/{component}/{instance}/cmd/{verb}`) is authoritative, a body `instance` that
//!   disagrees with it is refused with `BAD_ARGS`, and the handler is handed the resolved
//!   `addressed_instance`. What is left here is the part that needs *this component's
//!   configuration*: an unknown id is `NO_SUCH_INSTANCE`, and `None` (a component-addressed
//!   delivery that named no instance) resolves to the sole configured device — with two or more it
//!   is `BAD_ARGS`.
//! * **Standardized error codes:** `BAD_ARGS`, `NO_SUCH_INSTANCE`, `WRITE_NOT_ALLOWED`,
//!   `DEVICE_UNAVAILABLE`, `RECONNECT_FAILED`, `BROWSE_UNSUPPORTED`, `BROWSE_FAILED`, `PAUSED`.
//! * **The session is never touched here.** Every verb that reads/reconnects/pauses is sent to the
//!   device's own task as a [`DeviceControl`] and *confirmed* through the reply that rides it,
//!   because the session lives in that task and is not `Sync`.
//! * **`sb/status` and `sb/browse` never block on acquisition.** Both answer from the agent
//!   runtime's *published* state — an `ArcSwap<AgentInfo>` and the cached [`ProbeModel`] — so a
//!   status call cannot queue behind a poll, and the address space stays browsable while the agent
//!   is unreachable (LLD §7).
//! * **`sb/write` is refused unconditionally, before any entry is looked at.** MTConnect's API is
//!   read-only by specification (Part 1 Fundamentals §5.1, D-MTC-7) — this is a capability
//!   statement, not a policy setting, so there is no allow-list path and no device I/O at all.
//! * Every verb records into the `MtconnectAdapterCommand` metric family (`instance`×`verb`×`result`).
//!
//! ## Registration order (LLD §7, the generated pre-activation window)
//!
//! [`register_all`] runs **register every verb → mark `sb/write` `unsupported` → register the
//! panels**, and only then does the component activate. Availability applies to registered verbs
//! only, and a console that reads the manifest before the availability entry exists would render a
//! write surface that can never work.
//!
//! Five panels (`overview`, `device-structure`, `signals`, `conditions`, `diagnostics`) are
//! registered via `commands.register_panel` — each `scope: "instance"`, `order` 10/20/30/40/50, and
//! **none** of them advertises a `writeVerb` (HLD §8).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use edgecommons::commands::AVAILABILITY_UNSUPPORTED;
use edgecommons::messaging::Message;
use edgecommons::prelude::{command_handler, CommandError, CommandHandler, CommandInbox, CommandScope};
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, oneshot};

use crate::app::{DeviceConfig, DeviceControl, Health, LinkState, Writes};
use crate::device::{BrowseError, Quality, Reading, SignalInfo, QUALITY_UNAVAILABLE, READ_ONLY_MESSAGE};
use crate::metrics::DeviceMetrics;
use crate::mtconnect::model::{BrowseNode, Category, NodeKind, ProbeModel, ROOT_NODE_ID};
use crate::mtconnect::selection::{served_set, Provenance, ServedSet};
use crate::mtconnect::{AgentInfo, AgentRuntime};
use crate::reload::{SignalRegistry, SignalSlot};

/// The per-entry `sb/read` failure code for an observation the agent has no value for (HLD §7).
/// The *published sample* uses the protocol's own `UNAVAILABLE` in `qualityRaw`; a read entry names
/// the adapter's code namespace, so an operator can tell a read failure from a published quality.
pub const READ_UNAVAILABLE: &str = "MTC_UNAVAILABLE";
/// The `sb/browse` detail code when no probe has ever been cached for this device (LLD §7).
pub const BROWSE_NO_PROBE: &str = "MTC_NO_PROBE";
/// The `sb/browse` detail code for a cursor minted against a superseded probe model.
pub const BROWSE_VIEW_CHANGED: &str = "MTC_VIEW_CHANGED";
/// The `sb/browse` detail code for a malformed cursor.
pub const BROWSE_BAD_CURSOR: &str = "MTC_BAD_CURSOR";
/// The capability token `sb/status.result.protocol.capability` publishes (HLD §7).
pub const CAPABILITY: &str = "MTCONNECT_CLIENT";
/// The closed, permanent capability limitations of this client (HLD §7).
pub const LIMITATIONS: [&str; 3] = ["READ_ONLY", "XML_ONLY", "NO_ASSETS"];

// =================================================================================================
// The protocol view — what a verb may read without touching acquisition
// =================================================================================================

/// The **published** state of one agent, as the command surface sees it: a snapshot of the agent's
/// info and its cached probe model, both lock-free reads.
///
/// It is a trait so the command tests can drive every verb against a known agent state and probe
/// model without an HTTP client — the production implementation is [`AgentRuntime`] itself.
pub trait AgentView: Send + Sync {
    /// The agent's published state (never a ctl round-trip — LLD §7).
    fn info(&self) -> Arc<AgentInfo>;
    /// The cached probe model for one device, when it has been probed at least once.
    fn model(&self, device_uuid: &str) -> Option<Arc<ProbeModel>>;
}

impl AgentView for AgentRuntime {
    fn info(&self) -> Arc<AgentInfo> {
        AgentRuntime::info(self)
    }

    fn model(&self, device_uuid: &str) -> Option<Arc<ProbeModel>> {
        AgentRuntime::model(self, device_uuid)
    }
}

/// One instance's MTConnect identity plus the agent state behind it. Present for every device whose
/// `adapter` is `mtconnect`; `None` for the template simulator, which has no agent.
pub struct ProtocolView {
    pub agent: Arc<dyn AgentView>,
    /// The `component.global.agents[]` id serving this device.
    pub agent_id: String,
    /// The MTConnect `Device/@uuid` this instance represents.
    pub device_uuid: String,
    /// The **live** signal set, in configuration order (`sb/signals`, browse `Configured` flags).
    /// Read through the slot on every call, so a reloaded signal set is visible immediately and a
    /// browse cursor minted against the old one is refused (LLD §8).
    pub signals: Arc<SignalSlot>,
}

impl ProtocolView {
    /// Build the view for one configured device, or `None` when it is not an MTConnect device (or
    /// its binding does not resolve to a configured agent — the supervisor already refused to start
    /// such an instance, so there is nothing to publish for it).
    #[must_use]
    pub fn of(
        cfg: &DeviceConfig,
        agents: &HashMap<String, Arc<AgentRuntime>>,
        signals: &SignalRegistry,
    ) -> Option<Self> {
        if cfg.adapter != crate::device::KIND {
            return None;
        }
        let (agent_id, device_uuid) = crate::device::connection_binding(&cfg.connection).ok()?;
        let agent = agents.get(&agent_id)?;
        Some(Self {
            agent: Arc::clone(agent) as Arc<dyn AgentView>,
            agent_id,
            device_uuid,
            signals: signals.slot(&cfg.id)?,
        })
    }

    /// The live signal set and the generation token identifying it.
    #[must_use]
    pub fn signals(&self) -> Arc<crate::reload::InstanceSignals> {
        self.signals.load()
    }

    /// The cached probe model for this device, if one has been fetched.
    #[must_use]
    pub fn model(&self) -> Option<Arc<ProbeModel>> {
        self.agent.model(&self.device_uuid)
    }

    /// The **closed** `sb/status.result.protocol` object of HLD §7 — every field nullable until the
    /// agent has told us, assembled from the runtime's published [`AgentInfo`] without ever waiting
    /// on the acquisition task.
    #[must_use]
    pub fn status_object(&self) -> Value {
        let info = self.agent.info();
        json!({
            "capability": CAPABILITY,
            "standardVersion": info.standard_version,
            "schemaNamespace": info.schema_namespace,
            "agentId": self.agent_id,
            "agentVersion": info.agent_version,
            "instanceId": info.instance_id,
            "bufferSize": info.buffer_size,
            "firstSequence": info.first_sequence,
            "nextSequence": info.next_sequence,
            "mode": info.mode,
            "heartbeatMs": info.heartbeat_ms,
            // Every document — including the empty heartbeat document — proves liveness, so the
            // last document IS the last heartbeat (HLD §5.2).
            "lastHeartbeatAt": info.last_document_at,
            "probeDigest": info.probe_digests.get(&self.device_uuid),
            "limitations": LIMITATIONS,
        })
    }

    /// The served set in force: the live explicit signals merged with the selection-derived half
    /// against the cached model (R1.1). The same pure merge the session serves from, so this view
    /// and acquisition can never disagree. With no model yet, only the explicit signals are known.
    #[must_use]
    pub fn served(&self) -> ServedSet {
        let live = self.signals();
        served_set(&live.signals, live.selection.as_ref(), self.model().as_deref())
    }

    /// The `sb/signals` inventory: the **served** signals — explicit and selection-derived alike —
    /// with the §5.3 address, enriched from the cached model when it is there and honestly null
    /// when it is not, and each row's `provenance` (`configured` vs `discovered`). No network I/O,
    /// ever.
    #[must_use]
    pub fn signal_rows(&self, writes: &Writes) -> Vec<Value> {
        let model = self.model();
        self.served()
            .signals
            .iter()
            .map(|served| {
                let s = &served.signal;
                let meta = model.as_ref().and_then(|m| m.item(&s.data_item_id));
                let address = model
                    .as_ref()
                    .and_then(|m| m.address_of(&self.agent_id, &s.data_item_id))
                    .unwrap_or_else(|| self.unlearned_address(&s.data_item_id));
                json!({
                    "id": s.id,
                    "name": s.name,
                    // Structurally false: the schema pins `writes.allow` empty (D-MTC-7). It is
                    // still asked, so a hand-edited config cannot make the reply lie.
                    "writable": writes.permits(&s.id),
                    "address": address,
                    "units": meta.and_then(|m| m.units.clone()),
                    "conditionBinding": s.condition_bindings(),
                    // Whether the `dataItemId` exists in the current device model.
                    "bound": meta.is_some(),
                    // Where the entry came from: an explicit `signals[]` entry, or the probe via
                    // the `selection` block.
                    "provenance": served.provenance.as_str(),
                })
            })
            .collect()
    }

    /// The §5.3 address for a data item the model has not taught us about yet: the binding keys are
    /// configuration and always known; everything the probe supplies stays null.
    fn unlearned_address(&self, data_item_id: &str) -> Value {
        json!({
            "protocol": "mtconnect",
            "agentId": self.agent_id,
            "deviceUuid": self.device_uuid,
            "dataItemId": data_item_id,
            "category": Value::Null,
            "type": Value::Null,
            "subType": Value::Null,
            "componentPath": Value::Null,
        })
    }

    /// The `dataItemId`s the served set binds, each with the *strongest* provenance claiming it
    /// (an item both configured and matched by the selection reads `configured`) — the browse
    /// `configured` flag plus its `provenance` refinement.
    fn served_provenance(served: &ServedSet) -> HashMap<&str, Provenance> {
        let mut map: HashMap<&str, Provenance> = HashMap::new();
        for s in &served.signals {
            map.entry(s.signal.data_item_id.as_str())
                .and_modify(|p| {
                    if s.provenance == Provenance::Configured {
                        *p = Provenance::Configured;
                    }
                })
                .or_insert(s.provenance);
        }
        map
    }
}

/// The per-device handles the command surface routes on: the config (routing, inventory), the
/// control channel (session-touching verbs), the shared health (status/paused), the metrics emitter
/// (per-verb command counters), and — for an MTConnect device — the published protocol view.
pub struct DeviceHandle {
    pub cfg: DeviceConfig,
    pub control: mpsc::Sender<DeviceControl>,
    pub health: Arc<Health>,
    pub dm: Arc<DeviceMetrics>,
    /// The signal inventory `sb/signals` falls back to when there is no [`ProtocolView`] (the
    /// template simulator) — a config/backend view, no device round-trip.
    pub signals: Vec<SignalInfo>,
    /// The MTConnect view: the shared agent runtime's published state + this device's uuid.
    pub protocol: Option<ProtocolView>,
}

/// The verbs this module puts on the inbox — every one of them [`CommandScope::Instance`].
pub const VERBS: [&str; 9] = [
    "sb/status",
    "sb/read",
    "sb/write",
    "sb/signals",
    "sb/browse",
    "sb/pause",
    "sb/resume",
    "reconnect",
    "repoll",
];

/// Register every `sb/*` verb, the permanent `sb/write` refusal, and the five edge-console panels.
///
/// The order is the contract (LLD §7): **verbs first** (availability applies only to a registered
/// verb), **then** `sb/write` → `unsupported`, **then** the panels — all inside the generated
/// pre-activation window, so no console can ever see a manifest that advertises a write surface
/// without the refusal beside it.
///
/// # Errors
/// Propagates [`CommandInbox::register`] / [`CommandInbox::set_command_availability`] /
/// [`CommandInbox::register_panel`] failures (a verb/panel name clash or an invalid token).
pub fn register_all(commands: &CommandInbox, handles: Vec<DeviceHandle>) -> anyhow::Result<()> {
    let commander = Arc::new(Commander::new(handles));
    for (verb, scope, handler) in registrations(&commander) {
        commands.register(verb, scope, handler)?;
    }
    // D-MTC-7: advertised as permanently unsupported, not merely disabled — the protocol has no
    // write path, so no configuration can ever turn this on.
    commands.set_command_availability("sb/write", AVAILABILITY_UNSUPPORTED, Some("MTConnect is read-only"))?;
    for panel in panels() {
        commands.register_panel(panel)?;
    }
    Ok(())
}

/// The `(verb, scope, handler)` triples [`register_all`] installs, in [`VERBS`] order.
///
/// Every handler takes the `addressed_instance` the inbox resolved and passes it straight to the
/// commander — none of them reads `body.instance`.
fn registrations(
    commander: &Arc<Commander>,
) -> Vec<(&'static str, CommandScope, Arc<dyn CommandHandler>)> {
    macro_rules! verb {
        ($name:expr, $method:ident) => {{
            let c = Arc::clone(commander);
            (
                $name,
                CommandScope::Instance,
                command_handler(move |req: Message, addressed| {
                    let c = Arc::clone(&c);
                    async move { c.$method(addressed.as_deref(), &req.body).await }
                }),
            )
        }};
        ($name:expr, $method:ident, no_body) => {{
            let c = Arc::clone(commander);
            (
                $name,
                CommandScope::Instance,
                command_handler(move |_req: Message, addressed| {
                    let c = Arc::clone(&c);
                    async move { c.$method(addressed.as_deref()).await }
                }),
            )
        }};
    }

    // `sb/pause` additionally carries the requester identity path (the `by` field of the event).
    let pause = {
        let c = Arc::clone(commander);
        (
            "sb/pause",
            CommandScope::Instance,
            command_handler(move |req: Message, addressed| {
                let c = Arc::clone(&c);
                async move {
                    let by = req.identity.as_ref().map(|i| i.path().to_string());
                    c.pause(addressed.as_deref(), by).await
                }
            }),
        )
    };

    vec![
        verb!("sb/status", status, no_body),
        verb!("sb/read", read),
        // The body is never read: the refusal precedes entry processing (LLD §7).
        verb!("sb/write", write, no_body),
        verb!("sb/signals", signals, no_body),
        verb!("sb/browse", browse),
        pause,
        verb!("sb/resume", resume, no_body),
        verb!("reconnect", reconnect, no_body),
        verb!("repoll", repoll, no_body),
    ]
}

// =================================================================================================
// Panels (HLD §8) — five instance-scoped views, none of them a write surface
// =================================================================================================

/// The five edge-console panel descriptors of HLD §8. Core validates `id`/`title`/uniqueness; the
/// widget kinds, columns, and bound verbs are console-interpreted, so they ride verbatim.
///
/// Every view declares the `rendererRequirements` tokens it needs from the shared contract's closed
/// set (`docs/adapters/edge-console-panels.md` §4), sorted and unique — edge-console refuses to
/// mount a view whose requirements it cannot meet rather than silently degrading it. Command-backed
/// widgets repeat `"scope": "instance"`. **No widget names a `writeVerb`**: MTConnect has nothing to
/// write, and `sb/write`'s permanent refusal rides the command-availability surface instead.
#[must_use]
pub fn panels() -> Vec<Value> {
    vec![overview_view(), device_structure_view(), signals_view(), conditions_view(), diagnostics_view()]
}

fn overview_view() -> Value {
    json!({
        "id": "overview", "title": "Overview", "order": 10, "scope": "instance",
        "rendererRequirements": [
            "action-bar.v1", "command-availability.v1", "instance-selector.v1",
            "metric-series.v1", "status-dashboard.v1"
        ],
        "widgets": [
            {
                "kind": "statusDashboard", "id": "agent-status", "title": "Agent and link",
                "scope": "instance", "verb": "sb/status",
                "refresh": { "onEnter": true, "manual": true, "intervalMs": 5000 },
                "fields": [
                    { "label": "Adapter state", "path": "state", "format": "badge" },
                    { "label": "Connected", "path": "connected", "format": "boolean" },
                    { "label": "Paused", "path": "paused", "format": "boolean" },
                    { "label": "Endpoint", "path": "endpoint", "format": "address" },
                    { "label": "Agent version", "path": "protocol.agentVersion", "format": "text" },
                    { "label": "Standard version", "path": "protocol.standardVersion", "format": "text" },
                    { "label": "Mode", "path": "protocol.mode", "format": "badge" },
                    { "label": "Instance ID", "path": "protocol.instanceId", "format": "text" },
                    { "label": "Next sequence", "path": "protocol.nextSequence", "format": "number" },
                    { "label": "Heartbeat age", "path": "protocol.lastHeartbeatAt", "format": "timestamp" },
                    { "label": "Probe digest", "path": "protocol.probeDigest", "format": "text" }
                ]
            },
            {
                "kind": "actionBar", "id": "lifecycle-actions", "title": "Lifecycle",
                "scope": "instance",
                "actions": [
                    { "verb": "sb/pause", "label": "Pause publication", "role": "operator", "confirm": true },
                    { "verb": "sb/resume", "label": "Resume publication", "role": "operator" },
                    { "verb": "reconnect", "label": "Reconnect device", "role": "operator", "confirm": true },
                    { "verb": "repoll", "label": "Refresh now", "role": "operator" }
                ]
            },
            {
                "kind": "metricSeries", "id": "overview-health", "title": "Link health",
                "scope": "instance",
                "series": [
                    { "label": "Connection state", "metric": "southbound_health", "measure": "connectionState" },
                    { "label": "Poll latency", "metric": "southbound_health", "measure": "pollLatencyMs", "unit": "ms" },
                    { "label": "Signals subscribed", "metric": "southbound_health", "measure": "signalsSubscribed" },
                    { "label": "Reconnects", "metric": "southbound_health", "measure": "reconnects" }
                ]
            }
        ],
        "verbs": ["sb/status", "sb/pause", "sb/resume", "reconnect", "repoll"]
    })
}

fn device_structure_view() -> Value {
    json!({
        "id": "device-structure", "title": "Device Structure", "order": 20, "scope": "instance",
        "rendererRequirements": ["instance-selector.v1", "tree-columns.v1"],
        "widgets": [
            {
                "kind": "treeBrowser", "id": "probe-tree", "title": "Probe tree",
                "scope": "instance", "mode": "hierarchical", "rootRef": "root",
                "depth": 1, "maxRefs": 200,
                "browseVerb": "sb/browse", "readVerb": "sb/read",
                "columns": [
                    { "label": "Name", "path": "name" },
                    { "label": "Kind", "path": "kind" },
                    { "label": "Type", "path": "type" },
                    { "label": "SubType", "path": "subType" },
                    { "label": "Category", "path": "category" },
                    { "label": "DataItem", "path": "dataItemId" },
                    { "label": "Configured", "path": "configured", "format": "boolean" }
                ]
            }
        ],
        "verbs": ["sb/browse", "sb/read"]
    })
}

fn signals_view() -> Value {
    json!({
        "id": "signals", "title": "Signals", "order": 30, "scope": "instance",
        "rendererRequirements": ["instance-selector.v1", "signal-columns.v1"],
        "widgets": [
            // Descriptor-compat hint: the shipped edge-console signalGrid reads `subscriptionsVerb`
            // (falling back to the removed `sb/subscriptions`). Point that key at the `sb/signals`
            // verb too. This is a descriptor field alias, NOT a wire-verb alias — no
            // `sb/subscriptions` verb exists.
            {
                "kind": "signalGrid", "id": "configured-signals", "title": "Configured signals",
                "scope": "instance",
                "signalsVerb": "sb/signals",
                "subscriptionsVerb": "sb/signals",
                "readVerb": "sb/read",
                "columns": [
                    { "label": "Signal", "path": "id" },
                    { "label": "Name", "path": "name" },
                    { "label": "DataItem", "path": "address.dataItemId" },
                    { "label": "Category", "path": "address.category" },
                    { "label": "Type", "path": "address.type" },
                    { "label": "Units", "path": "units" },
                    { "label": "Quality binding", "path": "conditionBinding" }
                ]
            }
        ],
        "verbs": ["sb/signals", "sb/read"]
    })
}

fn conditions_view() -> Value {
    json!({
        "id": "conditions", "title": "Conditions & Events", "order": 40, "scope": "instance",
        "rendererRequirements": ["event-feed.v1", "instance-selector.v1", "metric-series.v1"],
        "widgets": [
            {
                "kind": "eventFeed", "id": "condition-events",
                "title": "Conditions, data loss, and agent events", "scope": "instance",
                "families": ["MtconnectConditionEvent", "MtconnectDataLossEvent", "MtconnectAgentEvent"],
                "limit": 100
            },
            {
                "kind": "metricSeries", "id": "condition-metrics", "title": "Observation flow",
                "scope": "instance",
                "series": [
                    { "label": "Observations", "metric": "MtconnectStream", "measure": "observationsInterval" },
                    { "label": "Documents", "metric": "MtconnectStream", "measure": "documentsInterval" },
                    { "label": "Stale signals", "metric": "southbound_health", "measure": "staleSignals" }
                ]
            }
        ],
        "verbs": []
    })
}

fn diagnostics_view() -> Value {
    json!({
        "id": "diagnostics", "title": "Diagnostics", "order": 50, "scope": "instance",
        "rendererRequirements": [
            "event-feed.v1", "instance-selector.v1", "metric-series.v1", "status-dashboard.v1"
        ],
        "widgets": [
            {
                "kind": "statusDashboard", "id": "sequence-status", "title": "Sequence and buffer",
                "scope": "instance", "verb": "sb/status",
                "refresh": { "onEnter": true, "manual": true, "intervalMs": 10000 },
                "fields": [
                    { "label": "Instance ID", "path": "protocol.instanceId", "format": "text" },
                    { "label": "Buffer size", "path": "protocol.bufferSize", "format": "number" },
                    { "label": "First sequence", "path": "protocol.firstSequence", "format": "number" },
                    { "label": "Next sequence", "path": "protocol.nextSequence", "format": "number" },
                    { "label": "Heartbeat", "path": "protocol.heartbeatMs", "format": "duration-ms" },
                    { "label": "Schema namespace", "path": "protocol.schemaNamespace", "format": "text" },
                    { "label": "Probe digest", "path": "protocol.probeDigest", "format": "text" },
                    { "label": "Limitations", "path": "protocol.limitations", "format": "text" }
                ]
            },
            {
                "kind": "eventFeed", "id": "diagnostic-events", "title": "Agent and model events",
                "scope": "instance",
                "families": ["MtconnectAgentEvent", "MtconnectModelDriftEvent", "MtconnectDataLossEvent"],
                "limit": 50
            },
            {
                "kind": "metricSeries", "id": "diagnostic-metrics", "title": "Stream diagnostics",
                "scope": "instance",
                "series": [
                    { "label": "Stream gaps", "metric": "MtconnectStream", "measure": "gapsInterval" },
                    { "label": "Reconnects", "metric": "MtconnectStream", "measure": "reconnectsInterval" },
                    { "label": "Heartbeats", "metric": "MtconnectStream", "measure": "heartbeatsInterval" },
                    { "label": "Parse errors", "metric": "MtconnectParse", "measure": "parseErrorsInterval" }
                ]
            }
        ],
        "verbs": ["sb/status"]
    })
}

// =================================================================================================
// The dispatcher
// =================================================================================================

/// The command dispatcher: owns the per-device handles + the config order (for the single-instance
/// default).
struct Commander {
    devices: HashMap<String, DeviceHandle>,
    ids: Vec<String>,
}

type Reply = std::result::Result<Option<Value>, CommandError>;

impl Commander {
    fn new(handles: Vec<DeviceHandle>) -> Self {
        let ids: Vec<String> = handles.iter().map(|h| h.cfg.id.clone()).collect();
        let devices = handles.into_iter().map(|h| (h.cfg.id.clone(), h)).collect();
        Self { devices, ids }
    }

    /// Map the library-resolved `addressed_instance` onto a configured device.
    ///
    /// The addressing itself (topic token vs body `instance`, and the conflict between them) was
    /// settled by the inbox before this ran. Only the configuration-dependent half lives here: an
    /// instance this component does not have is `NO_SUCH_INSTANCE`, and `None` — a
    /// component-addressed delivery that named no instance — resolves to the sole configured
    /// device, or is `BAD_ARGS` when there are two or more.
    fn resolve(&self, instance: Option<&str>) -> std::result::Result<&DeviceHandle, CommandError> {
        match instance {
            Some(id) => self
                .devices
                .get(id)
                .ok_or_else(|| CommandError::new("NO_SUCH_INSTANCE", format!("no configured device `{id}`"))),
            None => {
                if self.ids.len() == 1 {
                    Ok(self.devices.get(&self.ids[0]).expect("one device"))
                } else {
                    Err(CommandError::new(
                        "BAD_ARGS",
                        "field `instance` is required when multiple devices are configured",
                    ))
                }
            }
        }
    }

    // --- sb/status (HLD §7: the closed `protocol` object, never a ctl round-trip) -----------------

    async fn status(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let link = h.health.link();
        let connected = link == LinkState::Online;
        let paused = h.health.is_paused();
        let state = if paused && connected { "PAUSED" } else { link.as_str() };
        let mut out = json!({
            "id": h.cfg.id,
            "adapter": h.cfg.adapter,
            "connected": connected,
            "state": state,
            "paused": paused,
            "endpoint": h.cfg.connection.endpoint,
            "metrics": h.dm.counters_view(),
        });
        if let Some(p) = &h.protocol {
            out["protocol"] = p.status_object();
        }
        h.dm.record_command("sb/status", true, ms(started));
        Ok(Some(out))
    }

    // --- sb/signals (the configured inventory + the §5.3 address, no device I/O) -------------------

    async fn signals(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let signals: Vec<Value> = match &h.protocol {
            Some(p) => p.signal_rows(&h.cfg.writes),
            None => h
                .signals
                .iter()
                .map(|s| {
                    json!({
                        "id": s.id,
                        "name": s.name,
                        "writable": h.cfg.writes.permits(&s.id),
                    })
                })
                .collect(),
        };
        h.dm.record_command("sb/signals", true, ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "signals": signals })))
    }

    // --- sb/read (a scoped `/current` read through the agent's control channel) --------------------

    /// `"mode": "current"` — an MTConnect read is always answered from `/current`, never from a
    /// cache (HLD §7). A failure of the *agent* degrades the entries, not the command: the reply
    /// stays `ok` and every requested entry carries `MTC_AGENT_ERROR:<code>` (the item-vs-session
    /// rule). Only an absent device task — the runtime itself is gone — is `DEVICE_UNAVAILABLE`.
    async fn read(&self, instance: Option<&str>, body: &Value) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let refs = body
            .get("signals")
            .and_then(Value::as_array)
            .ok_or_else(|| CommandError::new("BAD_ARGS", "expected a `signals` array"))?;

        // Resolve each ref to a stable id (keeping the request order for the reply).
        let plan: Vec<std::result::Result<String, String>> =
            refs.iter().map(|r| self.resolve_ref(h, r)).collect();
        let ids: Vec<String> = plan.iter().filter_map(|r| r.clone().ok()).collect();

        // The per-entry code every resolved entry gets when the read itself could not be served.
        let mut failure: Option<String> = None;
        let readings: HashMap<String, Reading> = if ids.is_empty() {
            HashMap::new()
        } else {
            let (tx, rx) = oneshot::channel();
            h.control
                .send(DeviceControl::ReadNow { ids, reply: tx })
                .await
                .map_err(|_| {
                    h.dm.record_command("sb/read", false, ms(started));
                    device_unavailable()
                })?;
            match rx.await {
                Ok(Ok(rs)) => rs.into_iter().map(|r| (r.signal_id.clone(), r)).collect(),
                Ok(Err(e)) => {
                    failure = Some(agent_failure_code(&e));
                    HashMap::new()
                }
                Err(_) => {
                    h.dm.record_command("sb/read", false, ms(started));
                    return Err(device_unavailable());
                }
            }
        };

        let reads: Vec<Value> = plan
            .into_iter()
            .map(|entry| match entry {
                Ok(id) => match readings.get(&id) {
                    Some(r) => json!({
                        "signal": { "id": id },
                        // A protocol null is published as an explicit null, not as a missing key.
                        "value": r.value.clone().unwrap_or(Value::Null),
                        "quality": quality_str(r.quality),
                        "qualityRaw": read_quality_raw(r),
                        "extra": r.extra,
                    }),
                    None => bad_read(&id, failure.as_deref().unwrap_or("NO_DATA")),
                },
                Err(label) => bad_read(&label, "UNRESOLVED_REF"),
            })
            .collect();

        h.dm.record_command("sb/read", failure.is_none(), ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "mode": "current", "reads": reads })))
    }

    // --- sb/write: registered, permanently refused (D-MTC-7) --------------------------------------

    /// MTConnect's API is **read-only by specification** (Part 1 Fundamentals §5.1): there is no
    /// write path to allow-list, so the refusal is unconditional and precedes any entry processing.
    /// The body is never inspected — a malformed batch and a perfectly formed one are refused
    /// identically, and nothing ever reaches a device.
    async fn write(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        h.dm.record_command("sb/write", false, ms(started));
        Err(CommandError::new("WRITE_NOT_ALLOWED", READ_ONLY_MESSAGE))
    }

    // --- sb/browse (the probe tree: paged + hierarchical, served from the cached model) -----------

    async fn browse(&self, instance: Option<&str>, body: &Value) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        // The two request forms are mutually exclusive: `ref`/`depth`/`maxRefs` select the
        // hierarchical panel mode, `cursor`/`max` the paged one — and the hierarchical-only
        // arguments are meaningless without a `ref`.
        let hierarchical_keys = ["ref", "depth", "maxRefs"].iter().any(|k| body.get(*k).is_some());
        let paged_keys = ["cursor", "max"].iter().any(|k| body.get(*k).is_some());
        if hierarchical_keys && paged_keys {
            h.dm.record_command("sb/browse", false, ms(started));
            return Err(CommandError::new(
                "BAD_ARGS",
                "`ref`/`depth`/`maxRefs` (hierarchical) and `cursor`/`max` (paged) are mutually exclusive",
            ));
        }
        if hierarchical_keys && body.get("ref").is_none() {
            h.dm.record_command("sb/browse", false, ms(started));
            return Err(CommandError::new(
                "BAD_ARGS",
                "`depth`/`maxRefs` are hierarchical-mode arguments and require `ref`",
            ));
        }

        let result = match &h.protocol {
            // The MTConnect address space IS the cached probe model: no ctl round-trip, and it
            // keeps answering while the agent is unreachable (LLD §7, D-MTC-5).
            Some(p) => match p.model() {
                Some(model) => browse_model(&h.cfg.id, p, &model, body),
                None => Err(CommandError::new(
                    "BROWSE_FAILED",
                    format!("{BROWSE_NO_PROBE}: no probe model is cached for device `{}` yet", p.device_uuid),
                )),
            },
            // The template simulator has no probe: the generated seam serves it through the device
            // task, exactly as it did before.
            None => self.browse_seam(h, body).await,
        };
        h.dm.record_command("sb/browse", result.is_ok(), ms(started));
        result
    }

    /// The generated, control-channel-backed browse (paged + hierarchical) — the path a non-MTConnect
    /// backend (the simulator) is served by.
    async fn browse_seam(&self, h: &DeviceHandle, body: &Value) -> Reply {
        if body.get("ref").is_some() {
            return self.browse_seam_hierarchical(h, body).await;
        }
        let cursor = body.get("cursor").and_then(Value::as_str).map(str::to_string);
        let max = body.get("max").and_then(Value::as_u64).unwrap_or(200).clamp(1, 1000) as usize;

        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Browse { cursor, max, reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        match rx.await {
            Ok(Ok(page)) => {
                let entries: Vec<Value> = page
                    .entries
                    .iter()
                    .map(|e| json!({ "id": e.id, "name": e.name, "type": e.type_name }))
                    .collect();
                let mut out = json!({ "id": h.cfg.id, "entries": entries });
                if let Some(cursor) = page.next_cursor {
                    out["cursor"] = json!(cursor);
                }
                Ok(Some(out))
            }
            Ok(Err(BrowseError::Unsupported)) => {
                Err(CommandError::new("BROWSE_UNSUPPORTED", "this adapter has no discovery service"))
            }
            Ok(Err(BrowseError::Failed(e))) => Err(CommandError::new("BROWSE_FAILED", e)),
            Err(_) => Err(device_unavailable()),
        }
    }

    /// The `treeBrowser` mode of the generated seam: `"root"` is the device node whose `contains`
    /// refs are the inventory, a known id is a leaf, an unknown ref is `BAD_ARGS`.
    async fn browse_seam_hierarchical(&self, h: &DeviceHandle, body: &Value) -> Reply {
        let Some(ref_id) = body.get("ref").and_then(Value::as_str).filter(|r| !r.is_empty()) else {
            return Err(CommandError::new("BAD_ARGS", "`ref` must be a non-empty string"));
        };
        let depth = body.get("depth").and_then(Value::as_u64).unwrap_or(1).clamp(1, 4);
        let max_refs = body.get("maxRefs").and_then(Value::as_u64).unwrap_or(200).clamp(1, 1000) as usize;

        // Collect the whole inventory through the same control channel the paged mode uses,
        // following its cursors — one source, both browse modes.
        let mut entries = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let (tx, rx) = oneshot::channel();
            h.control
                .send(DeviceControl::Browse { cursor: cursor.clone(), max: 1000, reply: tx })
                .await
                .map_err(|_| device_unavailable())?;
            let page = match rx.await {
                Ok(Ok(page)) => page,
                Ok(Err(BrowseError::Unsupported)) => {
                    return Err(CommandError::new("BROWSE_UNSUPPORTED", "this adapter has no discovery service"));
                }
                Ok(Err(BrowseError::Failed(e))) => return Err(CommandError::new("BROWSE_FAILED", e)),
                Err(_) => return Err(device_unavailable()),
            };
            entries.extend(page.entries);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        if ref_id == "root" {
            let refs: Vec<Value> = entries
                .iter()
                .take(max_refs)
                .map(|e| {
                    json!({
                        "referenceType": "contains",
                        "target": { "nodeId": e.id, "name": e.name, "nodeClass": "signal",
                                    "dataType": e.type_name }
                    })
                })
                .collect();
            let ref_count = refs.len();
            return Ok(Some(json!({
                "id": h.cfg.id,
                "mode": "hierarchical",
                "root": { "nodeId": "root", "name": h.cfg.id, "nodeClass": "device",
                          "dataType": Value::Null, "refs": refs },
                "refCount": ref_count,
                "depth": depth,
                "truncated": entries.len() > max_refs
            })));
        }

        let Some(node) = entries.iter().find(|e| e.id == ref_id) else {
            return Err(CommandError::new("BAD_ARGS", format!("unknown browse ref `{ref_id}`")));
        };
        Ok(Some(json!({
            "id": h.cfg.id,
            "mode": "hierarchical",
            "root": { "nodeId": node.id, "name": node.name, "nodeClass": "signal",
                      "dataType": node.type_name, "refs": [] },
            "refCount": 0,
            "depth": depth,
            "truncated": false
        })))
    }

    // --- sb/pause + sb/resume (idempotent {paused, changed}) --------------------------------------

    async fn pause(&self, instance: Option<&str>, _by: Option<String>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Pause { reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        let changed = rx.await.map_err(|_| device_unavailable())?;
        h.dm.record_command("sb/pause", true, ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "paused": true, "changed": changed })))
    }

    async fn resume(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Resume { reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        let changed = rx.await.map_err(|_| device_unavailable())?;
        h.dm.record_command("sb/resume", true, ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "paused": false, "changed": changed })))
    }

    // --- reconnect ---------------------------------------------------------------------------------

    async fn reconnect(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Reconnect { reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        match rx.await.map_err(|_| device_unavailable())? {
            Ok(()) => {
                h.dm.record_command("reconnect", true, ms(started));
                Ok(Some(json!({ "id": h.cfg.id, "connected": true })))
            }
            Err(e) => {
                h.dm.record_command("reconnect", false, ms(started));
                Err(CommandError::new("RECONNECT_FAILED", e))
            }
        }
    }

    // --- repoll (a forced snapshot publish; refused with PAUSED while paused) ----------------------

    /// `polled` is the number of signal results **published**, including the `BAD` ones — a device
    /// that answers "unavailable" for every signal still polled them (HLD §7).
    async fn repoll(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        if h.health.is_paused() {
            h.dm.record_command("repoll", false, ms(started));
            return Err(CommandError::new("PAUSED", "instance is paused - resume first"));
        }
        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Repoll { reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        match rx.await.map_err(|_| device_unavailable())? {
            Ok(polled) => {
                h.dm.record_command("repoll", true, ms(started));
                Ok(Some(json!({ "id": h.cfg.id, "polled": polled })))
            }
            Err(e) if e.contains("paused") => {
                // The device task raced a pause in ahead of us — same refusal, same code.
                h.dm.record_command("repoll", false, ms(started));
                Err(CommandError::new("PAUSED", e))
            }
            Err(e) => {
                h.dm.record_command("repoll", false, ms(started));
                Err(CommandError::new("DEVICE_UNAVAILABLE", e))
            }
        }
    }

    /// Resolve an `sb/read` signal-ref to its stable id: `{"signalId"}` / `{"id"}` directly, or
    /// `{"name"}` looked up against the configured inventory. `Err` carries a label for the BAD /
    /// unresolved entry.
    fn resolve_ref(&self, h: &DeviceHandle, r: &Value) -> std::result::Result<String, String> {
        if let Some(id) = r.get("signalId").and_then(Value::as_str) {
            return Ok(id.to_string());
        }
        if let Some(id) = r.get("id").and_then(Value::as_str) {
            return Ok(id.to_string());
        }
        if let Some(name) = r.get("name").and_then(Value::as_str) {
            // The MTConnect inventory is the SERVED signal set (explicit + selection-derived);
            // the template inventory is the fallback for the simulator.
            if let Some(p) = &h.protocol {
                return p
                    .served()
                    .signals
                    .iter()
                    .find(|s| s.signal.name.as_deref() == Some(name))
                    .map(|s| s.signal.id.clone())
                    .ok_or_else(|| name.to_string());
            }
            return h
                .signals
                .iter()
                .find(|s| s.name.as_deref() == Some(name))
                .map(|s| s.id.clone())
                .ok_or_else(|| name.to_string());
        }
        Err("<invalid ref>".to_string())
    }
}

// =================================================================================================
// sb/browse over the cached probe model (LLD §7)
// =================================================================================================

/// Serve one browse request from the cached [`ProbeModel`] — paged when there is no `ref`,
/// hierarchical when there is. Both modes publish the model's digest as `viewGeneration`, so a
/// consumer can tell that the address space it paged through is the one it is still looking at.
fn browse_model(instance_id: &str, p: &ProtocolView, model: &ProbeModel, body: &Value) -> Reply {
    let signals = p.signals();
    // BOTH generations: the entries come from the probe model, their `Configured` flags from the
    // signal configuration (explicit + selection), so a reload voids a cursor exactly as a
    // re-probe does (LLD §8).
    let generation = crate::reload::view_generation(&model.digest_hex(), &signals.generation);
    // The `configured` flag covers the served UNION; `provenance` distinguishes an explicitly
    // configured binding from a selection-discovered one (R1.1).
    let served = served_set(&signals.signals, signals.selection.as_ref(), Some(model));
    let prov = ProtocolView::served_provenance(&served);
    let bound: HashSet<&str> = prov.keys().copied().collect();
    let flags = configured_flags(model, &bound);
    match body.get("ref") {
        Some(_) => browse_model_hierarchical(instance_id, model, &flags, &prov, &generation, body),
        None => browse_model_paged(instance_id, model, &flags, &prov, &generation, body),
    }
}

/// The paged mode: the pre-ordered browse projection (Device → its data items → each component
/// subtree), sliced by a cursor that carries the view generation it was minted against.
fn browse_model_paged(
    instance_id: &str,
    model: &ProbeModel,
    flags: &[bool],
    prov: &HashMap<&str, Provenance>,
    generation: &str,
    body: &Value,
) -> Reply {
    let start = match body.get("cursor").and_then(Value::as_str) {
        None => 0usize,
        Some(cursor) => parse_cursor(cursor, generation)?,
    };
    let max = body.get("max").and_then(Value::as_u64).unwrap_or(200).clamp(1, 1000) as usize;

    let entries: Vec<Value> = model
        .tree
        .iter()
        .enumerate()
        .skip(start)
        .take(max)
        .map(|(i, node)| browse_entry(node, flags[i], prov))
        .collect();
    let next = start + entries.len();

    let mut out = json!({
        "id": instance_id,
        "entries": entries,
        "viewGeneration": generation,
    });
    if next < model.tree.len() {
        out["cursor"] = json!(format!("{generation}#{next}"));
    }
    Ok(Some(out))
}

/// The `treeBrowser` mode: one node and its `contains` refs, expanded `depth` levels and bounded by
/// `maxRefs`. `"root"` is accepted as an alias of the device node's own id, so the shared panel
/// descriptor's `rootRef: "root"` works while every emitted `nodeId` stays a real, round-trippable
/// `mtc:/…` reference.
fn browse_model_hierarchical(
    instance_id: &str,
    model: &ProbeModel,
    flags: &[bool],
    prov: &HashMap<&str, Provenance>,
    generation: &str,
    body: &Value,
) -> Reply {
    let Some(ref_id) = body.get("ref").and_then(Value::as_str).filter(|r| !r.is_empty()) else {
        return Err(CommandError::new("BAD_ARGS", "`ref` must be a non-empty string"));
    };
    let ref_id = if ref_id == "root" { ROOT_NODE_ID } else { ref_id };
    let depth = body.get("depth").and_then(Value::as_u64).unwrap_or(1).clamp(1, 4);
    let max_refs = body.get("maxRefs").and_then(Value::as_u64).unwrap_or(200).clamp(1, 1000) as usize;

    let Some(index) = model.tree.iter().position(|n| n.id == ref_id) else {
        return Err(CommandError::new("BAD_ARGS", format!("unknown browse ref `{ref_id}`")));
    };

    let children = children_by_parent(model);
    let mut budget = max_refs;
    let mut truncated = false;
    let refs =
        refs_of(index, model, flags, prov, &children, depth as usize, &mut budget, &mut truncated);
    let ref_count = refs.len();

    let mut root = browse_target(&model.tree[index], flags[index], prov);
    root.insert("refs".to_string(), Value::Array(refs));

    Ok(Some(json!({
        "id": instance_id,
        "mode": "hierarchical",
        "root": Value::Object(root),
        "refCount": ref_count,
        "depth": depth,
        "truncated": truncated,
        "viewGeneration": generation,
    })))
}

/// `<viewGeneration>#<index>` — a cursor is tied to the model it was minted against, so paging on
/// through a probe that changed underneath is refused rather than silently mixing two address
/// spaces (D-MTC-5: drift is surfaced, never papered over).
fn parse_cursor(cursor: &str, generation: &str) -> std::result::Result<usize, CommandError> {
    let Some((gen, index)) = cursor.split_once('#') else {
        return Err(CommandError::new(
            "BROWSE_FAILED",
            format!("{BROWSE_BAD_CURSOR}: cursor `{cursor}` is malformed"),
        ));
    };
    if gen != generation {
        return Err(CommandError::new(
            "BROWSE_FAILED",
            format!("{BROWSE_VIEW_CHANGED}: the probe model changed; restart the browse"),
        ));
    }
    index.parse::<usize>().map_err(|_| {
        CommandError::new("BROWSE_FAILED", format!("{BROWSE_BAD_CURSOR}: cursor `{cursor}` is malformed"))
    })
}

/// Which nodes a configured signal binds — data items directly, components and the device because a
/// configured item lives beneath them. Pre-order guarantees a parent precedes its children, so one
/// reverse pass propagates the flag all the way up.
fn configured_flags(model: &ProbeModel, bound: &HashSet<&str>) -> Vec<bool> {
    let index: HashMap<&str, usize> =
        model.tree.iter().enumerate().map(|(i, n)| (n.id.as_str(), i)).collect();
    let mut flags: Vec<bool> = model
        .tree
        .iter()
        .map(|n| data_item_id(n).is_some_and(|id| bound.contains(id)))
        .collect();
    for i in (0..model.tree.len()).rev() {
        if flags[i] {
            if let Some(parent) = model.tree[i].parent_id.as_deref().and_then(|p| index.get(p)) {
                flags[*parent] = true;
            }
        }
    }
    flags
}

/// Every node's direct children, keyed by parent id.
fn children_by_parent(model: &ProbeModel) -> HashMap<&str, Vec<usize>> {
    let mut out: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, node) in model.tree.iter().enumerate() {
        if let Some(parent) = node.parent_id.as_deref() {
            out.entry(parent).or_default().push(i);
        }
    }
    out
}

/// The `contains` refs of one node, expanded `depth` levels and drawing on a shared `budget` so
/// `maxRefs` bounds the whole reply rather than each level.
#[allow(clippy::too_many_arguments)]
fn refs_of(
    index: usize,
    model: &ProbeModel,
    flags: &[bool],
    prov: &HashMap<&str, Provenance>,
    children: &HashMap<&str, Vec<usize>>,
    depth: usize,
    budget: &mut usize,
    truncated: &mut bool,
) -> Vec<Value> {
    let Some(kids) = children.get(model.tree[index].id.as_str()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for &child in kids {
        if *budget == 0 {
            *truncated = true;
            break;
        }
        *budget -= 1;
        let mut target = browse_target(&model.tree[child], flags[child], prov);
        let grandchildren = children.get(model.tree[child].id.as_str()).is_some();
        if !grandchildren {
            // A node known to be a leaf says so explicitly (panel contract §3.2).
            target.insert("refs".to_string(), Value::Array(Vec::new()));
        } else if depth > 1 {
            let nested =
                refs_of(child, model, flags, prov, children, depth - 1, budget, truncated);
            target.insert("refs".to_string(), Value::Array(nested));
        }
        // else: a node that MAY have children omits `refs` — the console expands it on demand.
        out.push(json!({ "referenceType": "contains", "target": Value::Object(target) }));
    }
    out
}

/// A data-item node's provenance token: how its binding came to be served (`configured` vs
/// `discovered`), `null` for an unserved item and for component/device nodes.
fn provenance_of(node: &BrowseNode, prov: &HashMap<&str, Provenance>) -> Value {
    match data_item_id(node).and_then(|id| prov.get(id)) {
        Some(p) => json!(p.as_str()),
        None => Value::Null,
    }
}

/// One paged browse entry: the node plus everything the Device Structure columns bind
/// (Kind/Type/SubType/Category/DataItem/Configured — HLD §8). `configured` covers the served
/// union; `provenance` says which half serves a data item.
fn browse_entry(node: &BrowseNode, configured: bool, prov: &HashMap<&str, Provenance>) -> Value {
    json!({
        "id": node.id,
        "name": node.name,
        "kind": node.kind.as_str(),
        "type": node.type_name,
        "subType": node.sub_type,
        "category": node.category.map(Category::as_str),
        "units": node.units,
        "dataItemId": data_item_id(node),
        "parentId": node.parent_id,
        "depth": node.depth,
        "configured": configured,
        "provenance": provenance_of(node, prov),
    })
}

/// One hierarchical target: the panel contract's `{nodeId, name, nodeClass, dataType}` plus the same
/// column data the paged entries carry.
fn browse_target(
    node: &BrowseNode,
    configured: bool,
    prov: &HashMap<&str, Provenance>,
) -> Map<String, Value> {
    let mut t = Map::new();
    t.insert("nodeId".to_string(), json!(node.id));
    t.insert("name".to_string(), json!(node.name));
    t.insert("nodeClass".to_string(), json!(node_class(node.kind)));
    t.insert(
        "dataType".to_string(),
        match node.kind {
            NodeKind::DataItem => json!(node.type_name),
            _ => Value::Null,
        },
    );
    t.insert("kind".to_string(), json!(node.kind.as_str()));
    t.insert("type".to_string(), json!(node.type_name));
    t.insert("subType".to_string(), json!(node.sub_type));
    t.insert("category".to_string(), json!(node.category.map(Category::as_str)));
    t.insert("units".to_string(), json!(node.units));
    t.insert("dataItemId".to_string(), json!(data_item_id(node)));
    t.insert("configured".to_string(), json!(configured));
    t.insert("provenance".to_string(), provenance_of(node, prov));
    t
}

fn node_class(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Device => "device",
        NodeKind::Component => "component",
        NodeKind::DataItem => "dataItem",
    }
}

/// The `dataItemId` a browse node addresses — `Some` only for data-item nodes.
fn data_item_id(node: &BrowseNode) -> Option<&str> {
    node.id.strip_prefix("mtc:/item/")
}

// =================================================================================================
// Helpers
// =================================================================================================

fn ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn device_unavailable() -> CommandError {
    CommandError::new("DEVICE_UNAVAILABLE", "device task is unavailable")
}

fn quality_str(q: Quality) -> &'static str {
    match q {
        Quality::Good => "GOOD",
        Quality::Bad => "BAD",
        Quality::Uncertain => "UNCERTAIN",
    }
}

/// A read entry's `qualityRaw`. The publish path stamps the protocol's own `UNAVAILABLE`; a read
/// entry names the adapter's own code for it (HLD §7's per-entry failure codes), and everything else
/// — `MTC_OK`, `MTC_NO_SUCH_DATAITEM`, `MTC_CONDITION:…` — rides through unchanged.
fn read_quality_raw(r: &Reading) -> Value {
    match r.quality_raw.as_deref() {
        Some(QUALITY_UNAVAILABLE) => json!(READ_UNAVAILABLE),
        Some(other) => json!(other),
        None => Value::Null,
    }
}

/// The per-entry code for a read the agent could not serve. The device seam hands the command layer
/// a message rather than the client's own [`MtcError`](crate::mtconnect::MtcError) (the control
/// channel is protocol-agnostic), so the stable code is recovered from it here — defaulting to
/// `UNREACHABLE`, the honest answer for an agent that is simply not answering.
fn agent_failure_code(error: &str) -> String {
    let lower = error.to_ascii_lowercase();
    if lower.contains("no dataitem") {
        return crate::device::QUALITY_NO_SUCH_DATAITEM.to_string();
    }
    if let Some(rest) = error.split("agent error ").nth(1) {
        if let Some(code) = rest.split(':').next() {
            let code = code.trim();
            if !code.is_empty() {
                return format!("MTC_AGENT_ERROR:{code}");
            }
        }
    }
    if lower.contains("xml error") {
        return "MTC_PARSE".to_string();
    }
    let code = if lower.contains("timed out") {
        "TIMEOUT"
    } else if lower.contains("http") {
        "HTTP"
    } else if lower.contains("tls error") {
        "TLS"
    } else if lower.contains("authentication error") {
        "AUTH"
    } else {
        "UNREACHABLE"
    };
    format!("MTC_AGENT_ERROR:{code}")
}

fn bad_read(id: &str, raw: &str) -> Value {
    json!({ "signal": { "id": id }, "value": Value::Null, "quality": "BAD", "qualityRaw": raw })
}

#[cfg(test)]
mod tests {
    //! Every verb's happy path + each error code + the single-instance default; the write refusal
    //! proven to precede any device I/O; browse served from a cached probe model in both modes
    //! (including with no model at all, and with a stale cursor); and the full panel manifest.
    //! A mock device task services the control channel and RECORDS every write that reaches it — no
    //! device, no socket, no agent.
    use super::*;
    use std::sync::Mutex;

    use edgecommons::prelude::{Config, Metric, MetricService};

    use crate::app::{set_paused, Health};
    use crate::device::{BrowsePage, BrowsedSignal};
    use crate::mtconnect::config::{AgentCredentials, SignalConfig};
    use crate::mtconnect::xml::parse_devices;

    const DEVICES_2_7: &str = include_str!("../tests/fixtures/devices_2.7.xml");
    const DEVICE_UUID: &str = "OKUMA.123456";

    // --- a no-op MetricService + Config so DeviceMetrics can be built without a live runtime --------

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

    fn dev(v: Value) -> DeviceConfig {
        serde_json::from_value(v).unwrap()
    }

    fn a_device() -> DeviceConfig {
        dev(json!({
            "id": "plc-1",
            "adapter": "sim",
            "connection": { "endpoint": "sim://plc-1" },
            "writes": { "allow": ["setpoint-1"] }
        }))
    }

    fn sim_signals() -> Vec<SignalInfo> {
        vec![
            SignalInfo { id: "temperature-1".into(), name: Some("Ambient temperature".into()) },
            SignalInfo { id: "setpoint-1".into(), name: Some("Setpoint".into()) },
        ]
    }

    // --- the MTConnect protocol view: a known agent state + a known probe model -------------------

    fn probe_model() -> Arc<ProbeModel> {
        Arc::new(ProbeModel::from_devices(&parse_devices(DEVICES_2_7).unwrap(), DEVICE_UUID).unwrap())
    }

    struct FakeAgent {
        info: Arc<AgentInfo>,
        model: Option<Arc<ProbeModel>>,
    }

    impl AgentView for FakeAgent {
        fn info(&self) -> Arc<AgentInfo> {
            Arc::clone(&self.info)
        }
        fn model(&self, device_uuid: &str) -> Option<Arc<ProbeModel>> {
            (device_uuid == DEVICE_UUID).then(|| self.model.clone()).flatten()
        }
    }

    /// The state a runtime publishes before it has ever reached the agent: everything the agent
    /// teaches us is still unknown.
    fn unlearned_info() -> AgentInfo {
        AgentInfo {
            agent_id: "line-a-agent".into(),
            url: "http://agent:5000/".into(),
            connected: false,
            mode: "poll",
            heartbeat_ms: 10_000,
            ..AgentInfo::default()
        }
    }

    /// The state after a probe and a first document.
    fn learned_info() -> AgentInfo {
        let mut digests = std::collections::BTreeMap::new();
        digests.insert(DEVICE_UUID.to_string(), probe_model().digest_hex());
        AgentInfo {
            connected: true,
            mode: "stream",
            instance_id: Some(1_749_000_000),
            agent_version: Some("2.7.0.12".into()),
            standard_version: Some("2.7".into()),
            schema_namespace: Some("urn:mtconnect.org:MTConnectDevices:2.7".into()),
            buffer_size: Some(131_072),
            first_sequence: Some(1),
            next_sequence: Some(43),
            last_document_at: Some("2026-07-27T10:00:00Z".into()),
            probe_digests: digests,
            ..unlearned_info()
        }
    }

    fn mtc_signal(id: &str, data_item_id: &str, condition_binding: &[&str]) -> SignalConfig {
        SignalConfig {
            id: id.into(),
            name: Some(format!("{id} label")),
            channel: None,
            data_item_id: data_item_id.into(),
            condition_binding: (!condition_binding.is_empty())
                .then(|| condition_binding.iter().map(|s| (*s).to_string()).collect()),
            publish: None,
        }
    }

    fn mtc_signals() -> Vec<SignalConfig> {
        vec![
            mtc_signal("x-position", "Xabs", &["Xtravel"]),
            mtc_signal("spindle-speed", "Sspeed", &[]),
            // A configured signal the device model does not have — never silently dropped.
            mtc_signal("ghost-signal", "not-in-the-model", &[]),
        ]
    }

    fn protocol_view(info: AgentInfo, model: Option<Arc<ProbeModel>>) -> ProtocolView {
        ProtocolView {
            agent: Arc::new(FakeAgent { info: Arc::new(info), model }),
            agent_id: "line-a-agent".into(),
            device_uuid: DEVICE_UUID.into(),
            signals: Arc::new(SignalSlot::new(mtc_signals(), None)),
        }
    }

    /// An MTConnect instance whose agent has been probed.
    fn mtc_device() -> DeviceConfig {
        dev(json!({
            "id": "cnc-1",
            "adapter": "mtconnect",
            "connection": { "endpoint": "mtconnect://agent:5000/OKUMA.123456",
                            "agentId": "line-a-agent", "deviceUuid": DEVICE_UUID },
            "writes": { "allow": [] }
        }))
    }

    #[derive(Clone)]
    enum BrowseKind {
        One,
        /// Two entries over two pages, so cursor-following and truncation are observable.
        Paged,
        Unsupported,
        Failed,
    }

    #[derive(Clone)]
    struct MockOpts {
        read_ok: bool,
        /// The device task answers the read with this error (an agent failure, not a dead task).
        read_error: &'static str,
        reconnect_ok: bool,
        repoll_ok: bool,
        /// The device task answers a repoll with the paused refusal (a pause raced in ahead).
        repoll_paused: bool,
        browse: BrowseKind,
    }

    impl Default for MockOpts {
        fn default() -> Self {
            Self {
                read_ok: true,
                read_error: "agent unreachable: transport error: connection refused",
                reconnect_ok: true,
                repoll_ok: true,
                repoll_paused: false,
                browse: BrowseKind::One,
            }
        }
    }

    struct Harness {
        commander: Arc<Commander>,
        /// Every write that REACHED the device — empty proves the refusal happened first.
        writes: Arc<Mutex<Vec<(String, Value)>>>,
        health: Arc<Health>,
        _task: tokio::task::JoinHandle<()>,
    }

    fn make_dm(cfg: &DeviceConfig, health: Arc<Health>) -> Arc<DeviceMetrics> {
        Arc::new(DeviceMetrics::new(Arc::new(NoopMetrics), config(), cfg.id.clone(), health, 30, None))
    }

    fn harness(cfg: DeviceConfig, opts: MockOpts) -> Harness {
        harness_with(cfg, opts, None)
    }

    /// Build a single-device commander whose control channel is served by a mock device task.
    fn harness_with(cfg: DeviceConfig, opts: MockOpts, protocol: Option<ProtocolView>) -> Harness {
        let (tx, mut rx) = mpsc::channel::<DeviceControl>(16);
        let health = Arc::new(Health::default());
        health.set_link(LinkState::Online);
        let dm = make_dm(&cfg, Arc::clone(&health));
        let writes = Arc::new(Mutex::new(Vec::new()));

        let t_health = Arc::clone(&health);
        let t_writes = Arc::clone(&writes);
        let task = tokio::spawn(async move {
            while let Some(ctrl) = rx.recv().await {
                match ctrl {
                    DeviceControl::Write(req) => {
                        t_writes.lock().unwrap().push((req.signal_id.clone(), req.value.clone()));
                        let _ = req.ack.send(Err(crate::device::READ_ONLY_MESSAGE.to_string()));
                    }
                    DeviceControl::ReadNow { ids, reply } => {
                        if opts.read_ok {
                            let rs = ids
                                .iter()
                                .map(|id| {
                                    if id == "unavailable-1" {
                                        Reading::bad(id.clone(), crate::device::QUALITY_UNAVAILABLE)
                                    } else if id == "unbound-1" {
                                        Reading::bad(id.clone(), crate::device::QUALITY_NO_SUCH_DATAITEM)
                                    } else {
                                        Reading {
                                            quality_raw: Some(crate::device::QUALITY_OK.into()),
                                            ..Reading::good(id.clone(), json!(42.0))
                                        }
                                    }
                                })
                                .collect();
                            let _ = reply.send(Ok(rs));
                        } else {
                            let _ = reply.send(Err(opts.read_error.to_string()));
                        }
                    }
                    DeviceControl::Browse { cursor, reply, .. } => {
                        let temperature = BrowsedSignal {
                            id: "temperature-1".into(),
                            name: Some("Ambient temperature".into()),
                            type_name: "REAL".into(),
                        };
                        let r = match opts.browse {
                            BrowseKind::One => {
                                Ok(BrowsePage { entries: vec![temperature], next_cursor: None })
                            }
                            BrowseKind::Paged => {
                                if cursor.is_none() {
                                    Ok(BrowsePage {
                                        entries: vec![temperature],
                                        next_cursor: Some("page-2".into()),
                                    })
                                } else {
                                    Ok(BrowsePage {
                                        entries: vec![BrowsedSignal {
                                            id: "pressure-1".into(),
                                            name: Some("Line pressure".into()),
                                            type_name: "REAL".into(),
                                        }],
                                        next_cursor: None,
                                    })
                                }
                            }
                            BrowseKind::Unsupported => Err(BrowseError::Unsupported),
                            BrowseKind::Failed => Err(BrowseError::Failed("mid-browse error".into())),
                        };
                        let _ = reply.send(r);
                    }
                    DeviceControl::Pause { reply } => {
                        let _ = reply.send(set_paused(&t_health, true));
                    }
                    DeviceControl::Resume { reply } => {
                        let _ = reply.send(set_paused(&t_health, false));
                    }
                    DeviceControl::Reconnect { reply } => {
                        let _ = reply.send(if opts.reconnect_ok { Ok(()) } else { Err("no route to host".into()) });
                    }
                    DeviceControl::Repoll { reply } => {
                        let r = if opts.repoll_paused {
                            Err("instance is paused - resume first".to_string())
                        } else if opts.repoll_ok {
                            Ok(2)
                        } else {
                            Err("link error".into())
                        };
                        let _ = reply.send(r);
                    }
                }
            }
        });

        let handle = DeviceHandle {
            cfg,
            control: tx,
            health: Arc::clone(&health),
            dm,
            signals: sim_signals(),
            protocol,
        };
        let commander = Arc::new(Commander::new(vec![handle]));
        Harness { commander, writes, health, _task: task }
    }

    /// An MTConnect harness whose agent has probed the device.
    fn mtc_harness() -> Harness {
        harness_with(
            mtc_device(),
            MockOpts::default(),
            Some(protocol_view(learned_info(), Some(probe_model()))),
        )
    }

    fn ok(reply: Reply) -> Value {
        reply.expect("command succeeded").expect("a result object")
    }
    fn err_code(reply: Reply) -> String {
        reply.expect_err("command failed").code
    }
    fn err(reply: Reply) -> CommandError {
        reply.expect_err("command failed")
    }

    // --- routing: the library hands over the addressed instance, this maps it to a device ---------

    #[tokio::test]
    async fn instance_defaults_to_the_sole_device_and_unknown_or_missing_ids_error() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.status(None).await);
        assert_eq!(out["id"], json!("plc-1"));
        assert_eq!(err_code(h.commander.status(Some("nope")).await), "NO_SUCH_INSTANCE");

        // Two devices: a missing `instance` is BAD_ARGS.
        let mk = |cfg: DeviceConfig| {
            let (tx, _rx) = mpsc::channel(1);
            let health = Arc::new(Health::default());
            let dm = make_dm(&cfg, Arc::clone(&health));
            DeviceHandle { cfg, control: tx, health, dm, signals: sim_signals(), protocol: None }
        };
        let mut b = a_device();
        b.id = "plc-2".into();
        let multi = Commander::new(vec![mk(a_device()), mk(b)]);
        assert_eq!(err_code(multi.status(None).await), "BAD_ARGS");
        assert_eq!(ok(multi.status(Some("plc-2")).await)["id"], json!("plc-2"));
    }

    // --- sb/status ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn status_reports_connected_state_paused_and_a_counter_snapshot() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.status(None).await);
        assert_eq!(out["connected"], json!(true));
        assert_eq!(out["state"], json!("ONLINE"));
        assert_eq!(out["paused"], json!(false));
        assert_eq!(out["adapter"], json!("sim"));
        assert!(out["metrics"].get("connectAttempts").is_some());
        assert!(out.get("protocol").is_none(), "a device with no agent publishes no protocol object");
    }

    #[tokio::test]
    async fn status_publishes_the_closed_mtconnect_protocol_object() {
        let h = mtc_harness();
        let out = ok(h.commander.status(None).await);
        let p = &out["protocol"];

        // The HLD §7 object, exactly — no more keys, no fewer.
        let mut keys: Vec<&str> = p.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "agentId", "agentVersion", "bufferSize", "capability", "firstSequence",
                "heartbeatMs", "instanceId", "lastHeartbeatAt", "limitations", "mode",
                "nextSequence", "probeDigest", "schemaNamespace", "standardVersion",
            ]
        );
        assert_eq!(p["capability"], json!("MTCONNECT_CLIENT"));
        assert_eq!(p["agentId"], json!("line-a-agent"));
        assert_eq!(p["standardVersion"], json!("2.7"));
        assert_eq!(p["schemaNamespace"], json!("urn:mtconnect.org:MTConnectDevices:2.7"));
        assert_eq!(p["agentVersion"], json!("2.7.0.12"));
        assert_eq!(p["instanceId"], json!(1_749_000_000_u64));
        assert_eq!(p["bufferSize"], json!(131_072));
        assert_eq!(p["firstSequence"], json!(1));
        assert_eq!(p["nextSequence"], json!(43));
        assert_eq!(p["mode"], json!("stream"));
        assert_eq!(p["heartbeatMs"], json!(10_000));
        assert_eq!(p["lastHeartbeatAt"], json!("2026-07-27T10:00:00Z"));
        assert_eq!(p["probeDigest"], json!(probe_model().digest_hex()));
        assert!(p["probeDigest"].as_str().unwrap().starts_with("sha256:"));
        assert_eq!(p["limitations"], json!(["READ_ONLY", "XML_ONLY", "NO_ASSETS"]));
    }

    #[tokio::test]
    async fn the_protocol_object_is_nullable_until_the_agent_has_taught_us() {
        // Nothing has been learned yet: every learned field is an explicit null, and the object is
        // still the same closed shape — a console binds the same paths from the first second.
        let h = harness_with(mtc_device(), MockOpts::default(), Some(protocol_view(unlearned_info(), None)));
        let p = ok(h.commander.status(None).await)["protocol"].clone();
        for key in [
            "standardVersion", "schemaNamespace", "agentVersion", "instanceId", "bufferSize",
            "firstSequence", "nextSequence", "lastHeartbeatAt", "probeDigest",
        ] {
            assert_eq!(p[key], Value::Null, "{key} is null until the agent tells us");
        }
        // What configuration already knows is never null.
        assert_eq!(p["capability"], json!("MTCONNECT_CLIENT"));
        assert_eq!(p["agentId"], json!("line-a-agent"));
        assert_eq!(p["mode"], json!("poll"));
        assert_eq!(p["heartbeatMs"], json!(10_000));
        assert_eq!(p["limitations"], json!(["READ_ONLY", "XML_ONLY", "NO_ASSETS"]));
    }

    // --- sb/signals --------------------------------------------------------------------------------

    #[tokio::test]
    async fn signals_lists_the_inventory_with_the_writable_flag() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.signals(None).await);
        let sigs = out["signals"].as_array().unwrap();
        assert_eq!(sigs.len(), 2);
        let setpoint = sigs.iter().find(|s| s["id"] == json!("setpoint-1")).unwrap();
        assert_eq!(setpoint["writable"], json!(true), "setpoint-1 is on the allow-list");
        let temp = sigs.iter().find(|s| s["id"] == json!("temperature-1")).unwrap();
        assert_eq!(temp["writable"], json!(false), "temperature-1 is not");
    }

    #[tokio::test]
    async fn signals_carries_the_round_trippable_mtconnect_address() {
        let h = mtc_harness();
        let out = ok(h.commander.signals(None).await);
        let sigs = out["signals"].as_array().unwrap();
        assert_eq!(sigs.len(), 3, "every configured signal, bound or not");

        let x = sigs.iter().find(|s| s["id"] == json!("x-position")).unwrap();
        assert_eq!(
            x["address"],
            json!({
                "protocol": "mtconnect",
                "agentId": "line-a-agent",
                "deviceUuid": DEVICE_UUID,
                "dataItemId": "Xabs",
                "category": "SAMPLE",
                "type": "POSITION",
                "subType": "ACTUAL",
                "componentPath": "Axes/Linear[X]",
            })
        );
        assert_eq!(x["units"], json!("MILLIMETER"));
        assert_eq!(x["conditionBinding"], json!(["Xtravel"]));
        assert_eq!(x["bound"], json!(true));
        assert_eq!(x["writable"], json!(false), "MTConnect is read-only: nothing is writable");

        // A signal whose dataItemId is not in the model keeps its binding keys and says so.
        let ghost = sigs.iter().find(|s| s["id"] == json!("ghost-signal")).unwrap();
        assert_eq!(ghost["address"]["dataItemId"], json!("not-in-the-model"));
        assert_eq!(ghost["address"]["category"], Value::Null);
        assert_eq!(ghost["bound"], json!(false));
        assert_eq!(ghost["units"], Value::Null);
    }

    /// A protocol view whose slot carries a `selection` block beside the explicit signals.
    fn selecting_view(selection: Value, model: Option<Arc<ProbeModel>>) -> ProtocolView {
        ProtocolView {
            agent: Arc::new(FakeAgent { info: Arc::new(learned_info()), model }),
            agent_id: "line-a-agent".into(),
            device_uuid: DEVICE_UUID.into(),
            signals: Arc::new(SignalSlot::new(
                vec![mtc_signal("x-position", "Xabs", &["Xtravel"])],
                Some(serde_json::from_value(selection).unwrap()),
            )),
        }
    }

    #[tokio::test]
    async fn signals_serves_the_union_and_says_where_each_row_came_from() {
        // One explicit signal plus `mode: "all"`: every data item is served, the explicit row reads
        // `configured`, the derived rows `discovered` — and each derived row carries a full
        // model-enriched address (R1.1 provenance surfacing).
        let h = harness_with(
            mtc_device(),
            MockOpts::default(),
            Some(selecting_view(json!({ "mode": "all" }), Some(probe_model()))),
        );
        let out = ok(h.commander.signals(None).await);
        let sigs = out["signals"].as_array().unwrap();
        assert_eq!(sigs.len(), 14, "1 explicit (merged over its item) + 13 derived");

        let x = sigs.iter().find(|s| s["id"] == json!("x-position")).unwrap();
        assert_eq!(x["provenance"], json!("configured"));
        assert_eq!(x["conditionBinding"], json!(["Xtravel"]));
        assert_eq!(x["bound"], json!(true));

        let load = sigs.iter().find(|s| s["id"] == json!("xload")).unwrap();
        assert_eq!(load["provenance"], json!("discovered"));
        assert_eq!(load["address"]["dataItemId"], json!("Xload"));
        assert_eq!(load["address"]["category"], json!("SAMPLE"));
        assert_eq!(load["units"], json!("PERCENT"));
        assert_eq!(load["bound"], json!(true), "a derived signal came FROM the model");
        assert_eq!(load["conditionBinding"], json!(["Xtravel"]), "auto-bound to its component's condition");
        assert_eq!(load["writable"], json!(false));

        // Before the first probe the derived half honestly does not exist yet.
        let h = harness_with(
            mtc_device(),
            MockOpts::default(),
            Some(selecting_view(json!({ "mode": "all" }), None)),
        );
        let sigs = ok(h.commander.signals(None).await)["signals"].clone();
        assert_eq!(sigs.as_array().unwrap().len(), 1, "explicit only until the model is known");
        assert_eq!(sigs[0]["provenance"], json!("configured"));
    }

    #[tokio::test]
    async fn browse_flags_the_served_union_and_distinguishes_its_provenance() {
        let h = harness_with(
            mtc_device(),
            MockOpts::default(),
            Some(selecting_view(
                json!({ "mode": "include", "include": [{ "idMatch": "Xload" }] }),
                Some(probe_model()),
            )),
        );
        let out = ok(h.commander.browse(None, &json!({ "max": 1000 })).await);
        let entries = out["entries"].as_array().unwrap();

        // The explicit binding: configured, provenance `configured`.
        let x = entries.iter().find(|e| e["id"] == json!("mtc:/item/Xabs")).unwrap();
        assert_eq!(x["configured"], json!(true));
        assert_eq!(x["provenance"], json!("configured"));
        // The selection-derived binding: the union flags it, provenance says how it got there.
        let load = entries.iter().find(|e| e["id"] == json!("mtc:/item/Xload")).unwrap();
        assert_eq!(load["configured"], json!(true));
        assert_eq!(load["provenance"], json!("discovered"));
        // An unserved item is neither.
        let estop = entries.iter().find(|e| e["id"] == json!("mtc:/item/estop")).unwrap();
        assert_eq!(estop["configured"], json!(false));
        assert_eq!(estop["provenance"], Value::Null);
        // Components carry the union flag and no provenance (they are not data items).
        let linear = entries.iter().find(|e| e["id"] == json!("mtc:/component/Axes/Linear[X]")).unwrap();
        assert_eq!(linear["configured"], json!(true));
        assert_eq!(linear["provenance"], Value::Null);

        // The hierarchical mode carries the same fields on its targets.
        let out = ok(h
            .commander
            .browse(None, &json!({ "ref": "mtc:/component/Axes/Linear[X]", "depth": 1 }))
            .await);
        let refs = out["root"]["refs"].as_array().unwrap();
        let load = refs.iter().find(|r| r["target"]["nodeId"] == json!("mtc:/item/Xload")).unwrap();
        assert_eq!(load["target"]["provenance"], json!("discovered"));
    }

    #[tokio::test]
    async fn signals_answers_from_configuration_before_any_probe() {
        // No model cached (the agent has never answered): the inventory still lists every signal
        // with its binding keys — `sb/signals` does no network I/O, by contract.
        let h = harness_with(mtc_device(), MockOpts::default(), Some(protocol_view(unlearned_info(), None)));
        let sigs = ok(h.commander.signals(None).await)["signals"].clone();
        let sigs = sigs.as_array().unwrap();
        assert_eq!(sigs.len(), 3);
        assert_eq!(sigs[0]["address"]["dataItemId"], json!("Xabs"));
        assert_eq!(sigs[0]["address"]["type"], Value::Null, "nullable until the probe lands");
        assert_eq!(sigs[0]["bound"], json!(false));
    }

    // --- sb/read -----------------------------------------------------------------------------------

    #[tokio::test]
    async fn read_returns_values_by_id_and_by_name_and_marks_unresolved_refs() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h
            .commander
            .read(None, &json!({ "signals": [ { "signalId": "temperature-1" }, { "name": "Setpoint" }, { "name": "ghost" } ] }))
            .await);
        assert_eq!(out["mode"], json!("current"), "an MTConnect read is always a /current read");
        let reads = out["reads"].as_array().unwrap();
        assert_eq!(reads[0]["signal"]["id"], json!("temperature-1"));
        assert_eq!(reads[0]["quality"], json!("GOOD"));
        assert_eq!(reads[1]["signal"]["id"], json!("setpoint-1"), "resolved by name");
        assert_eq!(reads[2]["quality"], json!("BAD"), "an unknown name is a BAD/unresolved entry");
        assert_eq!(reads[2]["qualityRaw"], json!("UNRESOLVED_REF"));
    }

    #[tokio::test]
    async fn read_resolves_names_against_the_configured_mtconnect_signals() {
        let h = mtc_harness();
        let out = ok(h.commander.read(None, &json!({ "signals": [ { "name": "x-position label" } ] })).await);
        assert_eq!(out["reads"][0]["signal"]["id"], json!("x-position"));
    }

    #[tokio::test]
    async fn an_unavailable_observation_reads_as_the_mtc_unavailable_entry_code() {
        // The published sample carries the protocol's own `UNAVAILABLE`; a read entry names the
        // adapter's per-entry code (HLD §7).
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.read(None, &json!({ "signals": [ { "signalId": "unavailable-1" } ] })).await);
        let entry = &out["reads"][0];
        assert_eq!(entry["value"], Value::Null);
        assert_eq!(entry["quality"], json!("BAD"));
        assert_eq!(entry["qualityRaw"], json!("MTC_UNAVAILABLE"));
    }

    #[tokio::test]
    async fn a_signal_bound_to_a_missing_data_item_reads_as_no_such_dataitem() {
        // A configured signal whose `dataItemId` is not in the device model is named every time it
        // is read, never silently dropped (D-MTC-5).
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.read(None, &json!({ "signals": [ { "signalId": "unbound-1" } ] })).await);
        assert_eq!(out["reads"][0]["quality"], json!("BAD"));
        assert_eq!(out["reads"][0]["qualityRaw"], json!("MTC_NO_SUCH_DATAITEM"));
    }

    #[tokio::test]
    async fn read_without_a_signals_array_is_bad_args() {
        let h = harness(a_device(), MockOpts::default());
        assert_eq!(err_code(h.commander.read(None, &json!({})).await), "BAD_ARGS");
    }

    #[tokio::test]
    async fn an_unreachable_agent_degrades_the_entries_not_the_command() {
        // The item-vs-session rule (LLD §7): the session answered, the agent did not.
        let h = harness(a_device(), MockOpts { read_ok: false, ..MockOpts::default() });
        let out = ok(h
            .commander
            .read(None, &json!({ "signals": [ { "signalId": "temperature-1" }, { "name": "ghost" } ] }))
            .await);
        assert_eq!(out["mode"], json!("current"));
        assert_eq!(out["reads"][0]["quality"], json!("BAD"));
        assert_eq!(out["reads"][0]["qualityRaw"], json!("MTC_AGENT_ERROR:UNREACHABLE"));
        assert_eq!(out["reads"][1]["qualityRaw"], json!("UNRESOLVED_REF"), "an unresolved ref is still its own failure");
    }

    #[tokio::test]
    async fn an_agent_reported_error_code_rides_through_to_the_entry() {
        let h = harness(
            a_device(),
            MockOpts {
                read_ok: false,
                read_error: "agent error UNAUTHORIZED: not permitted",
                ..MockOpts::default()
            },
        );
        let out = ok(h.commander.read(None, &json!({ "signals": [ { "signalId": "temperature-1" } ] })).await);
        assert_eq!(out["reads"][0]["qualityRaw"], json!("MTC_AGENT_ERROR:UNAUTHORIZED"));
    }

    #[test]
    fn every_seam_failure_maps_to_a_stable_per_entry_code() {
        assert_eq!(agent_failure_code("agent error OUT_OF_RANGE: past the buffer"), "MTC_AGENT_ERROR:OUT_OF_RANGE");
        assert_eq!(agent_failure_code("no dataItem `Xabs` in the device model"), "MTC_NO_SUCH_DATAITEM");
        assert_eq!(agent_failure_code("xml error: unexpected eof"), "MTC_PARSE");
        assert_eq!(agent_failure_code("request timed out after 10000 ms"), "MTC_AGENT_ERROR:TIMEOUT");
        assert_eq!(agent_failure_code("agent returned HTTP 503"), "MTC_AGENT_ERROR:HTTP");
        assert_eq!(agent_failure_code("TLS error: handshake refused"), "MTC_AGENT_ERROR:TLS");
        assert_eq!(agent_failure_code("authentication error: rejected"), "MTC_AGENT_ERROR:AUTH");
        assert_eq!(agent_failure_code("device is disconnected"), "MTC_AGENT_ERROR:UNREACHABLE");
    }

    // --- sb/write: registered, permanently refused, never any device I/O --------------------------

    #[tokio::test]
    async fn every_write_is_refused_before_any_entry_is_looked_at() {
        let h = harness(a_device(), MockOpts::default());

        // Even a signal that IS on this (template) device's allow-list: the protocol has no write
        // path at all, so the allow-list never even gets consulted.
        let e = err(h.commander.write(None).await);
        assert_eq!(e.code, "WRITE_NOT_ALLOWED");
        assert!(e.message.contains("read-only"), "the refusal names the standard's mandate: {}", e.message);
        assert!(e.message.contains("5.1"), "and the clause that says so: {}", e.message);
        assert!(h.writes.lock().unwrap().is_empty(), "no write may ever reach a device");
    }

    #[tokio::test]
    async fn a_refused_write_is_not_a_device_write_error() {
        // `southbound_health.writeErrors` counts DEVICE-path failures. There is no device path, so
        // it stays structurally 0 (HLD §9).
        let h = mtc_harness();
        for _ in 0..3 {
            assert_eq!(err_code(h.commander.write(None).await), "WRITE_NOT_ALLOWED");
        }
        assert_eq!(h.health.write_errors.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(h.writes.lock().unwrap().is_empty());
    }

    // --- sb/browse over the cached probe model -----------------------------------------------------

    #[tokio::test]
    async fn browse_pages_the_probe_tree_with_its_digest_as_the_view_generation() {
        let h = mtc_harness();
        let out = ok(h.commander.browse(None, &json!({})).await);
        // The view generation covers BOTH halves of what a browse page says: the probe model the
        // entries come from, and the signal set their `Configured` flags come from (LLD §8).
        let generation = crate::reload::view_generation(
            &probe_model().digest_hex(),
            &crate::reload::generation_of(&mtc_signals(), None),
        );
        assert_eq!(out["viewGeneration"], json!(generation));
        assert!(generation.starts_with(&probe_model().digest_hex()));

        let entries = out["entries"].as_array().unwrap();
        // Pre-order: the device, its own data items, then each component subtree.
        assert_eq!(entries[0]["id"], json!("mtc:/component/"));
        assert_eq!(entries[0]["kind"], json!("DEVICE"));
        assert_eq!(entries[0]["name"], json!("OKUMA-CNC"));
        assert_eq!(entries[1]["id"], json!("mtc:/item/avail"));
        assert_eq!(entries[1]["kind"], json!("DATA_ITEM"));
        assert_eq!(entries[1]["dataItemId"], json!("avail"));
        assert_eq!(entries[1]["category"], json!("EVENT"));

        // A configured data item is flagged, and so is every ancestor holding one.
        let x = entries.iter().find(|e| e["id"] == json!("mtc:/item/Xabs")).unwrap();
        assert_eq!(x["configured"], json!(true));
        assert_eq!(x["type"], json!("POSITION"));
        assert_eq!(x["subType"], json!("ACTUAL"));
        assert_eq!(x["units"], json!("MILLIMETER"));
        assert_eq!(x["parentId"], json!("mtc:/component/Axes/Linear[X]"));
        let linear = entries.iter().find(|e| e["id"] == json!("mtc:/component/Axes/Linear[X]")).unwrap();
        assert_eq!(linear["configured"], json!(true), "a component holding a configured item is flagged");
        assert_eq!(linear["kind"], json!("COMPONENT"));
        assert_eq!(linear["dataItemId"], Value::Null);

        // An unconfigured item is not.
        let estop = entries.iter().find(|e| e["id"] == json!("mtc:/item/estop")).unwrap();
        assert_eq!(estop["configured"], json!(false));
    }

    #[tokio::test]
    async fn browse_pages_forward_with_a_generation_bound_cursor() {
        let h = mtc_harness();
        let first = ok(h.commander.browse(None, &json!({ "max": 3 })).await);
        assert_eq!(first["entries"].as_array().unwrap().len(), 3);
        let cursor = first["cursor"].as_str().expect("more to come").to_string();
        assert!(cursor.starts_with(&probe_model().digest_hex()), "the cursor carries its view generation");

        let second = ok(h.commander.browse(None, &json!({ "cursor": cursor, "max": 3 })).await);
        let entries = second["entries"].as_array().unwrap();
        assert_eq!(entries[0]["id"], json!("mtc:/component/Axes"), "paging resumed where it stopped");

        // The last page has no cursor.
        let all = ok(h.commander.browse(None, &json!({ "max": 1000 })).await);
        assert!(all.get("cursor").is_none());
        assert_eq!(all["entries"].as_array().unwrap().len(), probe_model().tree.len());
    }

    #[tokio::test]
    async fn a_cursor_from_a_superseded_probe_model_is_refused() {
        let h = mtc_harness();
        let stale = format!("sha256:{}#2", "0".repeat(64));
        let e = err(h.commander.browse(None, &json!({ "cursor": stale })).await);
        assert_eq!(e.code, "BROWSE_FAILED");
        assert!(e.message.starts_with("MTC_VIEW_CHANGED"), "{}", e.message);

        // A cursor that is not a cursor at all is refused too — never silently restarted.
        let e = err(h.commander.browse(None, &json!({ "cursor": "17" })).await);
        assert!(e.message.starts_with("MTC_BAD_CURSOR"), "{}", e.message);
    }

    #[tokio::test]
    async fn browse_with_no_cached_probe_is_browse_failed_with_no_probe() {
        let h = harness_with(mtc_device(), MockOpts::default(), Some(protocol_view(unlearned_info(), None)));
        for body in [json!({}), json!({ "ref": "root" })] {
            let e = err(h.commander.browse(None, &body).await);
            assert_eq!(e.code, "BROWSE_FAILED");
            assert!(e.message.starts_with("MTC_NO_PROBE"), "{}", e.message);
        }
    }

    #[tokio::test]
    async fn browse_keeps_answering_from_cache_while_the_agent_is_down() {
        // The agent is unreachable (nothing connected) but the probe was cached earlier: the
        // address space is still browsable, and no ctl round-trip was made to find that out.
        let h = harness_with(
            mtc_device(),
            MockOpts::default(),
            Some(protocol_view(AgentInfo { connected: false, ..learned_info() }, Some(probe_model()))),
        );
        let out = ok(h.commander.browse(None, &json!({ "ref": "root" })).await);
        assert_eq!(out["root"]["nodeId"], json!("mtc:/component/"));
    }

    #[tokio::test]
    async fn hierarchical_browse_of_the_root_lists_the_devices_children() {
        let h = mtc_harness();
        let out = ok(h.commander.browse(None, &json!({ "ref": "root" })).await);
        assert_eq!(out["mode"], json!("hierarchical"));
        assert_eq!(
            out["viewGeneration"],
            json!(crate::reload::view_generation(
                &probe_model().digest_hex(),
                &crate::reload::generation_of(&mtc_signals(), None)
            ))
        );
        let root = &out["root"];
        assert_eq!(root["nodeId"], json!("mtc:/component/"), "`root` is an alias of the device node id");
        assert_eq!(root["nodeClass"], json!("device"));
        assert_eq!(root["dataType"], Value::Null);

        let refs = root["refs"].as_array().unwrap();
        assert_eq!(refs.len(), 4, "two device-level data items + Axes + Controller");
        assert_eq!(out["refCount"], json!(4));
        assert_eq!(out["depth"], json!(1));
        assert_eq!(out["truncated"], json!(false));

        let avail = &refs[0];
        assert_eq!(avail["referenceType"], json!("contains"));
        assert_eq!(avail["target"]["nodeId"], json!("mtc:/item/avail"));
        assert_eq!(avail["target"]["nodeClass"], json!("dataItem"));
        assert_eq!(avail["target"]["dataType"], json!("AVAILABILITY"));
        assert_eq!(avail["target"]["refs"], json!([]), "a data item is a known leaf");

        // A component that may have children omits `refs` — the console expands it on demand.
        let axes = refs.iter().find(|r| r["target"]["nodeId"] == json!("mtc:/component/Axes")).unwrap();
        assert!(axes["target"].get("refs").is_none());
        assert_eq!(axes["target"]["nodeClass"], json!("component"));
        assert_eq!(axes["target"]["configured"], json!(true));
    }

    #[tokio::test]
    async fn hierarchical_browse_expands_the_requested_depth_and_round_trips_node_ids() {
        let h = mtc_harness();
        // Expanding a component uses the nodeId the previous reply handed out.
        let out = ok(h.commander.browse(None, &json!({ "ref": "mtc:/component/Axes", "depth": 2 })).await);
        let refs = out["root"]["refs"].as_array().unwrap();
        assert_eq!(refs.len(), 2, "Linear[X] and Rotary[C]");
        let linear = &refs[0]["target"];
        assert_eq!(linear["nodeId"], json!("mtc:/component/Axes/Linear[X]"));
        let nested = linear["refs"].as_array().expect("depth 2 expands the grandchildren");
        assert_eq!(nested.len(), 4, "Xabs, Xload, Xfreq, Xtravel");
        assert_eq!(nested[0]["target"]["nodeId"], json!("mtc:/item/Xabs"));
        assert_eq!(nested[3]["target"]["category"], json!("CONDITION"));
    }

    #[tokio::test]
    async fn hierarchical_browse_of_a_data_item_is_a_known_leaf() {
        let h = mtc_harness();
        let out = ok(h.commander.browse(None, &json!({ "ref": "mtc:/item/Xabs" })).await);
        let root = &out["root"];
        assert_eq!(root["nodeId"], json!("mtc:/item/Xabs"));
        assert_eq!(root["nodeClass"], json!("dataItem"));
        assert_eq!(root["dataType"], json!("POSITION"));
        assert_eq!(root["configured"], json!(true));
        assert_eq!(root["refs"], json!([]));
        assert_eq!(out["refCount"], json!(0));
    }

    #[tokio::test]
    async fn hierarchical_browse_bounds_depth_and_max_refs() {
        let h = mtc_harness();
        let out = ok(h.commander.browse(None, &json!({ "ref": "root", "depth": 99, "maxRefs": 5000 })).await);
        assert_eq!(out["depth"], json!(4), "clamped into 1..4");

        let out = ok(h.commander.browse(None, &json!({ "ref": "root", "maxRefs": 2 })).await);
        assert_eq!(out["refCount"], json!(2));
        assert_eq!(out["truncated"], json!(true), "the budget bounds the whole reply");

        // The budget spans the levels, not each of them.
        let out = ok(h.commander.browse(None, &json!({ "ref": "root", "depth": 4, "maxRefs": 6 })).await);
        assert_eq!(out["truncated"], json!(true));
    }

    #[tokio::test]
    async fn browse_rejects_unknown_refs_and_mode_mixing() {
        let h = mtc_harness();
        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": "mtc:/item/ghost" })).await), "BAD_ARGS");
        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": "root", "cursor": "x" })).await), "BAD_ARGS");
        assert_eq!(err_code(h.commander.browse(None, &json!({ "depth": 2, "max": 10 })).await), "BAD_ARGS");
        assert_eq!(err_code(h.commander.browse(None, &json!({ "depth": 2 })).await), "BAD_ARGS");
        assert_eq!(err_code(h.commander.browse(None, &json!({ "maxRefs": 10 })).await), "BAD_ARGS");
        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": 7 })).await), "BAD_ARGS");
    }

    // --- sb/browse over the generated seam (the simulator, which has no probe) ---------------------

    #[tokio::test]
    async fn the_seam_browse_returns_a_page_or_the_right_error_code() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.browse(None, &json!({})).await);
        assert_eq!(out["entries"].as_array().unwrap().len(), 1);
        assert_eq!(out["entries"][0]["id"], json!("temperature-1"));

        let h = harness(a_device(), MockOpts { browse: BrowseKind::Unsupported, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.browse(None, &json!({})).await), "BROWSE_UNSUPPORTED");

        let h = harness(a_device(), MockOpts { browse: BrowseKind::Failed, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.browse(None, &json!({})).await), "BROWSE_FAILED");
    }

    #[tokio::test]
    async fn the_seam_hierarchical_browse_lists_the_inventory_as_contains_refs() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.browse(None, &json!({ "ref": "root" })).await);
        assert_eq!(out["mode"], json!("hierarchical"));
        assert_eq!(out["root"]["name"], json!("plc-1"));
        assert_eq!(out["refCount"], json!(1));

        let out = ok(h.commander.browse(None, &json!({ "ref": "temperature-1" })).await);
        assert_eq!(out["root"]["nodeClass"], json!("signal"));
        assert_eq!(out["root"]["refs"], json!([]));

        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": "ghost" })).await), "BAD_ARGS");
    }

    #[tokio::test]
    async fn the_seam_hierarchical_browse_truncates_across_pages() {
        let h = harness(a_device(), MockOpts { browse: BrowseKind::Paged, ..MockOpts::default() });
        let out = ok(h.commander.browse(None, &json!({ "ref": "root", "maxRefs": 1 })).await);
        assert_eq!(out["refCount"], json!(1));
        assert_eq!(out["truncated"], json!(true));
        let out = ok(h.commander.browse(None, &json!({ "ref": "pressure-1" })).await);
        assert_eq!(out["root"]["nodeClass"], json!("signal"));
    }

    // --- pause / resume / repoll -------------------------------------------------------------------

    #[tokio::test]
    async fn pause_is_idempotent_and_repoll_is_refused_while_paused() {
        let h = harness(a_device(), MockOpts::default());

        // repoll works while running, and answers with what it published (BAD results included).
        assert_eq!(ok(h.commander.repoll(None).await)["polled"], json!(2));

        let out = ok(h.commander.pause(None, None).await);
        assert_eq!(out["paused"], json!(true));
        assert_eq!(out["changed"], json!(true));
        assert!(h.health.is_paused());

        // repoll is refused while paused, with the dedicated PAUSED code.
        assert_eq!(err_code(h.commander.repoll(None).await), "PAUSED");

        // pausing again is idempotent.
        assert_eq!(ok(h.commander.pause(None, None).await)["changed"], json!(false));

        // resume clears it and repoll works again.
        let out = ok(h.commander.resume(None).await);
        assert_eq!(out["paused"], json!(false));
        assert_eq!(out["changed"], json!(true));
        assert!(!h.health.is_paused());
        assert_eq!(ok(h.commander.repoll(None).await)["polled"], json!(2));
    }

    #[tokio::test]
    async fn a_paused_refusal_from_the_device_task_is_also_paused() {
        // The command layer saw an unpaused instance, but a pause raced in ahead of the repoll —
        // the device task's refusal maps to the same PAUSED code.
        let h = harness(a_device(), MockOpts { repoll_paused: true, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.repoll(None).await), "PAUSED");
    }

    // --- reconnect ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn reconnect_confirms_or_reports_reconnect_failed() {
        let h = harness(a_device(), MockOpts::default());
        assert_eq!(ok(h.commander.reconnect(None).await)["connected"], json!(true));

        let h = harness(a_device(), MockOpts { reconnect_ok: false, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.reconnect(None).await), "RECONNECT_FAILED");
    }

    #[tokio::test]
    async fn device_unavailable_when_the_task_is_gone() {
        // Drop the receiver so the control channel is closed: the runtime itself is absent, which
        // IS a session failure (unlike an unreachable agent).
        let (tx, rx) = mpsc::channel::<DeviceControl>(1);
        drop(rx);
        let cfg = a_device();
        let health = Arc::new(Health::default());
        let dm = make_dm(&cfg, Arc::clone(&health));
        let handle = DeviceHandle {
            cfg,
            control: tx,
            health: Arc::clone(&health),
            dm,
            signals: sim_signals(),
            protocol: None,
        };
        let commander = Commander::new(vec![handle]);
        assert_eq!(err_code(commander.reconnect(None).await), "DEVICE_UNAVAILABLE");
        assert_eq!(
            err_code(commander.read(None, &json!({ "signals": [ { "signalId": "temperature-1" } ] })).await),
            "DEVICE_UNAVAILABLE"
        );
    }

    // --- the protocol view's construction ---------------------------------------------------------

    #[test]
    fn the_protocol_view_is_built_for_mtconnect_devices_only() {
        let agents: HashMap<String, Arc<AgentRuntime>> = [(
            "line-a-agent".to_string(),
            AgentRuntime::new(
                crate::mtconnect::config::parse_agents(
                    &json!({ "agents": [{ "id": "line-a-agent", "url": "http://agent:5000" }] }),
                )
                .unwrap()
                .remove(0),
                &AgentCredentials::default(),
            )
            .unwrap(),
        )]
        .into_iter()
        .collect();

        let mut cfg = mtc_device();
        cfg.signals = mtc_signals();
        let registry = SignalRegistry::new(&[crate::mtconnect::config::DeviceConfig {
            id: cfg.id.clone(),
            agent_id: "line-a-agent".into(),
            device_uuid: DEVICE_UUID.into(),
            signals: mtc_signals(),
            selection: None,
        }], crate::app::ChannelBudgets::default());
        let view =
            ProtocolView::of(&cfg, &agents, &registry).expect("an mtconnect device gets a view");
        assert_eq!(view.agent_id, "line-a-agent");
        assert_eq!(view.device_uuid, DEVICE_UUID);
        assert_eq!(view.signals().signals.len(), 3);
        assert!(view.model().is_none(), "nothing is cached before the first probe");
        assert_eq!(view.status_object()["agentId"], json!("line-a-agent"));

        // The simulator has no agent, and an unknown agent id resolves to nothing.
        assert!(ProtocolView::of(&a_device(), &agents, &registry).is_none());
        let mut orphan = mtc_device();
        orphan.connection.extra.insert("agentId".into(), json!("nope"));
        assert!(ProtocolView::of(&orphan, &agents, &registry).is_none());
    }

    // --- the registered wiring: verbs, declared scope, and scoped delivery ------------------------

    /// A well-formed request carrying `body`.
    fn request(body: Value) -> Message {
        edgecommons::messaging::MessageBuilder::new("sb/status", "1.0")
            .payload(body)
            .build()
    }

    #[tokio::test]
    async fn every_verb_is_registered_once_and_declares_instance_scope() {
        // Every sb/* verb acts on ONE device, so each declares CommandScope::Instance — the inbox
        // then resolves the topic/body addressing before the handler runs and hands the instance
        // over. Registering the same set twice on one inbox would clash, so the list is also the
        // uniqueness check.
        let h = harness(a_device(), MockOpts::default());
        let regs = registrations(&h.commander);
        let verbs: Vec<&str> = regs.iter().map(|(v, _, _)| *v).collect();
        assert_eq!(verbs, VERBS.to_vec(), "the nine verbs, in the documented order");
        for (verb, scope, _) in &regs {
            assert_eq!(*scope, CommandScope::Instance, "{verb} is instance-scoped");
        }
    }

    #[tokio::test]
    async fn a_registered_handler_acts_on_the_addressed_instance_the_library_resolved() {
        // The handler objects the inbox invokes, driven exactly as the inbox drives them: the
        // component-addressed delivery (`None`) resolves to the sole device, the instance-addressed
        // one (`Some(token)`) names it.
        let h = harness(a_device(), MockOpts::default());
        let regs = registrations(&h.commander);
        let status = &regs.iter().find(|(v, _, _)| *v == "sb/status").unwrap().2;

        let out = status.handle(request(json!({})), None).await.unwrap().unwrap();
        assert_eq!(out["id"], json!("plc-1"), "component-addressed: the sole configured device");
        let out = status
            .handle(request(json!({})), Some("plc-1".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out["id"], json!("plc-1"), "instance-addressed: the topic's token");
    }

    #[tokio::test]
    async fn the_registered_write_handler_refuses_whatever_body_it_is_handed() {
        let h = harness(a_device(), MockOpts::default());
        let regs = registrations(&h.commander);
        let write = &regs.iter().find(|(v, _, _)| *v == "sb/write").unwrap().2;
        for body in [
            json!({}),
            json!({ "signalId": "setpoint-1", "value": 42 }),
            json!({ "writes": [ { "signalId": "setpoint-1", "value": 42 } ] }),
        ] {
            let e = write
                .handle(request(body), Some("plc-1".to_string()))
                .await
                .expect_err("MTConnect never writes");
            assert_eq!(e.code, "WRITE_NOT_ALLOWED");
        }
        assert!(h.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_registered_handler_never_reads_body_instance() {
        // With two devices and no addressed instance the answer is BAD_ARGS even though the body
        // names one: the library owns that fold-in, so the handler must not second-guess it. An
        // instance this component does not have is NO_SUCH_INSTANCE.
        let mk = |cfg: DeviceConfig| {
            let (tx, _rx) = mpsc::channel(1);
            let health = Arc::new(Health::default());
            let dm = make_dm(&cfg, Arc::clone(&health));
            DeviceHandle { cfg, control: tx, health, dm, signals: sim_signals(), protocol: None }
        };
        let mut b = a_device();
        b.id = "plc-2".into();
        let commander = Arc::new(Commander::new(vec![mk(a_device()), mk(b)]));
        let regs = registrations(&commander);
        let status = &regs.iter().find(|(v, _, _)| *v == "sb/status").unwrap().2;

        let err = status
            .handle(request(json!({ "instance": "plc-2" })), None)
            .await
            .expect_err("a body instance is not addressing");
        assert_eq!(err.code, "BAD_ARGS");
        let out = status
            .handle(request(json!({})), Some("plc-2".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out["id"], json!("plc-2"));
        let err = status
            .handle(request(json!({})), Some("ghost".to_string()))
            .await
            .expect_err("an unknown instance");
        assert_eq!(err.code, "NO_SUCH_INSTANCE");
    }

    // --- panels: the HLD §8 manifest, pinned -------------------------------------------------------

    #[test]
    fn the_five_views_are_registered_with_the_right_ids_orders_scope_and_requirements() {
        let ps = panels();
        let ids: Vec<&str> = ps.iter().map(|p| p["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["overview", "device-structure", "signals", "conditions", "diagnostics"]);
        let titles: Vec<&str> = ps.iter().map(|p| p["title"].as_str().unwrap()).collect();
        assert_eq!(
            titles,
            vec!["Overview", "Device Structure", "Signals", "Conditions & Events", "Diagnostics"]
        );
        let orders: Vec<u64> = ps.iter().map(|p| p["order"].as_u64().unwrap()).collect();
        assert_eq!(orders, vec![10, 20, 30, 40, 50]);

        let requirements: Vec<Value> = ps.iter().map(|p| p["rendererRequirements"].clone()).collect();
        assert_eq!(
            requirements,
            vec![
                json!(["action-bar.v1", "command-availability.v1", "instance-selector.v1",
                       "metric-series.v1", "status-dashboard.v1"]),
                json!(["instance-selector.v1", "tree-columns.v1"]),
                json!(["instance-selector.v1", "signal-columns.v1"]),
                json!(["event-feed.v1", "instance-selector.v1", "metric-series.v1"]),
                json!(["event-feed.v1", "instance-selector.v1", "metric-series.v1",
                       "status-dashboard.v1"]),
            ]
        );

        for p in &ps {
            assert_eq!(p["scope"], json!("instance"), "every view is instance-scoped");
            let tokens: Vec<&str> =
                p["rendererRequirements"].as_array().unwrap().iter().map(|t| t.as_str().unwrap()).collect();
            let mut sorted = tokens.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(tokens, sorted, "requirement tokens are unique and sorted");
            for widget in p["widgets"].as_array().unwrap() {
                assert!(widget["id"].is_string() && widget["kind"].is_string());
                if widget.get("verb").is_some() || widget.get("browseVerb").is_some()
                    || widget.get("signalsVerb").is_some() || widget.get("actions").is_some()
                {
                    assert_eq!(widget["scope"], json!("instance"), "command-backed widgets repeat the scope");
                }
            }
        }
    }

    #[test]
    fn no_view_anywhere_advertises_a_write_surface() {
        // D-MTC-7: there is nothing to write, so no widget names a `writeVerb` and no view binds
        // `sb/write` — the permanent refusal rides the command-availability surface instead.
        let manifest = serde_json::to_string(&panels()).unwrap();
        assert!(!manifest.contains("writeVerb"), "no widget may advertise a write surface");
        assert!(!manifest.contains("sb/write"), "no view may bind the refused verb");
    }

    #[test]
    fn the_overview_binds_status_the_lifecycle_actions_and_health_metrics() {
        let ps = panels();
        let widgets = ps[0]["widgets"].as_array().unwrap();
        assert_eq!(widgets.len(), 3);

        let status = &widgets[0];
        assert_eq!(status["kind"], json!("statusDashboard"));
        assert_eq!(status["verb"], json!("sb/status"));
        assert_eq!(status["refresh"], json!({ "onEnter": true, "manual": true, "intervalMs": 5000 }));
        let labels: Vec<&str> =
            status["fields"].as_array().unwrap().iter().map(|f| f["label"].as_str().unwrap()).collect();
        assert_eq!(
            labels,
            vec![
                "Adapter state", "Connected", "Paused", "Endpoint", "Agent version",
                "Standard version", "Mode", "Instance ID", "Next sequence", "Heartbeat age",
                "Probe digest",
            ]
        );
        // Every protocol field binds a path that `sb/status` really publishes.
        let object = ProtocolView {
            agent: Arc::new(FakeAgent { info: Arc::new(learned_info()), model: Some(probe_model()) }),
            agent_id: "line-a-agent".into(),
            device_uuid: DEVICE_UUID.into(),
            signals: Arc::new(SignalSlot::new(Vec::new(), None)),
        }
        .status_object();
        for field in status["fields"].as_array().unwrap() {
            if let Some(key) = field["path"].as_str().unwrap().strip_prefix("protocol.") {
                assert!(object.get(key).is_some(), "sb/status publishes `{key}`");
            }
        }

        let actions = &widgets[1];
        assert_eq!(actions["kind"], json!("actionBar"));
        assert_eq!(
            actions["actions"],
            json!([
                { "verb": "sb/pause", "label": "Pause publication", "role": "operator", "confirm": true },
                { "verb": "sb/resume", "label": "Resume publication", "role": "operator" },
                { "verb": "reconnect", "label": "Reconnect device", "role": "operator", "confirm": true },
                { "verb": "repoll", "label": "Refresh now", "role": "operator" }
            ])
        );
        assert_eq!(widgets[2]["kind"], json!("metricSeries"));
    }

    #[test]
    fn the_device_structure_tree_declares_the_mtconnect_columns() {
        let tree = &panels()[1]["widgets"][0];
        assert_eq!(tree["kind"], json!("treeBrowser"));
        assert_eq!(tree["mode"], json!("hierarchical"));
        assert_eq!(tree["rootRef"], json!("root"));
        assert_eq!(tree["depth"], json!(1));
        assert_eq!(tree["maxRefs"], json!(200));
        assert_eq!(tree["browseVerb"], json!("sb/browse"));
        assert_eq!(tree["readVerb"], json!("sb/read"));
        let columns: Vec<&str> =
            tree["columns"].as_array().unwrap().iter().map(|c| c["label"].as_str().unwrap()).collect();
        assert_eq!(
            columns,
            vec!["Name", "Kind", "Type", "SubType", "Category", "DataItem", "Configured"]
        );
    }

    #[test]
    fn the_signal_grid_binds_sb_signals_through_both_verb_keys_with_its_own_columns() {
        let grid = &panels()[2]["widgets"][0];
        assert_eq!(grid["kind"], json!("signalGrid"));
        assert_eq!(grid["signalsVerb"], json!("sb/signals"));
        assert_eq!(grid["subscriptionsVerb"], json!("sb/signals"), "the renderer-compat alias binds the same verb");
        assert_eq!(grid["readVerb"], json!("sb/read"));
        let columns: Vec<&str> =
            grid["columns"].as_array().unwrap().iter().map(|c| c["label"].as_str().unwrap()).collect();
        assert_eq!(
            columns,
            vec!["Signal", "Name", "DataItem", "Category", "Type", "Units", "Quality binding"]
        );
        // Each column binds a path the reply really carries.
        let rows = ProtocolView {
            agent: Arc::new(FakeAgent { info: Arc::new(learned_info()), model: Some(probe_model()) }),
            agent_id: "line-a-agent".into(),
            device_uuid: DEVICE_UUID.into(),
            signals: Arc::new(SignalSlot::new(mtc_signals(), None)),
        }
        .signal_rows(&Writes::default());
        for column in grid["columns"].as_array().unwrap() {
            let path = column["path"].as_str().unwrap();
            let mut value = &rows[0];
            for key in path.split('.') {
                value = value.get(key).unwrap_or_else(|| panic!("sb/signals publishes `{path}`"));
            }
        }
    }

    #[test]
    fn the_condition_and_diagnostic_views_bind_the_protocol_event_families() {
        let ps = panels();
        let conditions = &ps[3]["widgets"][0];
        assert_eq!(conditions["kind"], json!("eventFeed"));
        assert_eq!(
            conditions["families"],
            json!(["MtconnectConditionEvent", "MtconnectDataLossEvent", "MtconnectAgentEvent"])
        );
        assert_eq!(conditions["limit"], json!(100));

        let diagnostics = ps[4]["widgets"].as_array().unwrap();
        assert_eq!(diagnostics[0]["kind"], json!("statusDashboard"));
        assert_eq!(diagnostics[1]["kind"], json!("eventFeed"));
        assert_eq!(
            diagnostics[1]["families"],
            json!(["MtconnectAgentEvent", "MtconnectModelDriftEvent", "MtconnectDataLossEvent"])
        );
        let series: Vec<&str> = diagnostics[2]["series"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["label"].as_str().unwrap())
            .collect();
        assert_eq!(series, vec!["Stream gaps", "Reconnects", "Heartbeats", "Parse errors"]);
    }
}
