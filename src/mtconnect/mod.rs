//! # The owned MTConnect client
//!
//! Everything this adapter knows about the MTConnect protocol lives under this module, and
//! **nothing under it imports `edgecommons`** (LLD §1; `tests/isolation.rs` enforces it). The
//! translation into EdgeCommons vocabulary — readings, qualities, samples, topics, metrics —
//! happens above the seam, in `src/device.rs`.
//!
//! ```text
//!   config.rs   what an agent and a signal are
//!   client.rs   HTTP: /probe, /current, /sample (+ the streaming seam)
//!   xml.rs      namespace-tolerant parsing of the three document types
//!   model.rs    probe -> device model + browse tree + content digest
//!   observations.rs   one streamed element -> one Observation
//!   sequence.rs streaming/polling sequence integrity + dedupe floors
//!   stream.rs   heartbeat supervision for the multipart stream
//!   multipart.rs multipart framing
//!   mod.rs      AgentRuntime: one shared runtime per agent, many device instances
//! ```
//!
//! ## One agent, many devices (D-MTC-3)
//!
//! [`AgentRuntime`] is created once per `component.global.agents[]` entry. Each device instance
//! *attaches* to it ([`AgentRuntime::attach`]) and receives an [`InstanceEvent`] stream filtered to
//! its own device uuid. The instances own no socket: one acquisition task serves all of them, and a
//! failure of one device's observations cannot tear down another's session.
//!
//! ## Acquisition: streaming first, polling as the floor (LLD §5)
//!
//! Under [`StreamPolicy::Prefer`] the acquisition task runs the full §5 state machine:
//! Connecting → probe → `/current` snapshot → a multipart `/sample?interval=…&heartbeat=…&from=…`
//! stream, with the three recovery ladders —
//!
//! 1. **heartbeat missed** (2× `heartbeatMs` of silence) or any transport/framing failure →
//!    drop the stream and re-establish from the same `nextSequence`;
//! 2. **`OUT_OF_RANGE`** (the agent's buffer overran our position) → a [`InstanceEvent::DataLoss`]
//!    with the provably-skipped count, a `/current` snapshot republished as fresh, resume from the
//!    snapshot's `nextSequence`;
//! 3. **`instanceId` changed** (the agent restarted) → full resync: re-probe every attached
//!    device ([`InstanceEvent::ModelDrift`] on a digest change), snapshot, resume.
//!
//! After [`STREAM_ESTABLISH_FAILURE_LIMIT`] consecutive stream-establish failures the task
//! degrades to `/current` polling ([`InstanceEvent::StreamDegraded`]) and keeps retrying the
//! stream on the reconnect backoff. Under [`StreamPolicy::PollOnly`] the task only ever polls:
//! [`AgentRuntime::poll_once`] fetches `/current`, demultiplexes by device uuid, decodes each
//! observation against the cached probe model, and dispatches what is **new** (the per-data-item
//! dedupe floor in [`sequence`]). Both paths share the same [`SequenceState`](sequence::SequenceState).

pub mod client;
pub mod config;
pub mod error;
pub mod model;
pub mod multipart;
pub mod observations;
pub mod selection;
pub mod sequence;
pub mod stats;
pub mod stream;
pub mod xml;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_util::sync::CancellationToken;

use multipart::MultipartReader;
use stream::{classify_part, PartDoc};

pub use client::{MtcClient, StreamRequest, StreamResponse};
pub use config::{
    AgentConfig, AgentCredentials, AuthMaterial, AuthRef, DeviceConfig, PublishCfg, PublishMode,
    SignalConfig, StreamPolicy, TlsMaterial, TlsRef,
};
pub use error::{MtcError, ParseCounters};
pub use model::{BrowseNode, Category, DataItemMeta, DeviceNode, NodeKind, ProbeModel, Repr};
pub use observations::{CondState, ObsValue, Observation};
pub use selection::{
    served_set, ChannelBudget, DerivedChannel, Matcher, Provenance, SelectionConfig, SelectionMode,
    ServedSet, ServedSignal,
};
pub use sequence::{AcqState, HeaderOutcome, SequenceState};
pub use stats::{AgentStats, AgentStatsSnapshot};
pub use stream::{ChunkSource, HeartbeatWatch, PartOutcome, StreamExit};

/// An ISO-8601 UTC "now" supplier. The runtime stamps observation arrival with it without importing
/// `edgecommons`; production passes the library's own clock down from the supervisor.
pub type ClockFn = Arc<dyn Fn() -> String + Send + Sync>;

/// Data-lane capacity (coalescible Sample/Event observations).
pub const INSTANCE_QUEUE_DEPTH: usize = 1024;

/// Loss-intolerant lane capacity (Condition observations, lifecycle events, snapshots).
pub const CRITICAL_QUEUE_DEPTH: usize = 256;

/// How long a loss-intolerant send may wait for room before it is dropped and counted (D-R2).
pub const CRITICAL_SEND_BUDGET: Duration = Duration::from_secs(5);

/// How often one instance's queue-drop warning may repeat. A lagging consumer loses events in
/// bursts; the counters carry the volume, so the log only has to say *that* it is happening.
pub const DROP_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// The `interval=` a streaming request asks the agent for. The LLD (§14 Q2) floors the interval at
/// 250 ms; per-signal publish cadences below that are shaped client-side by the publish policy.
pub const STREAM_INTERVAL_MS: u32 = 250;

/// Consecutive stream-establish failures before acquisition degrades to `/current` polling
/// (LLD §5). The stream keeps being retried on the reconnect backoff while degraded.
pub const STREAM_ESTABLISH_FAILURE_LIMIT: u32 = 3;

/// Consecutive undecodable parts before the stream is dropped and re-established (LLD §9: a doc
/// that cannot be parsed is dropped and counted; three in a row means the stream itself is bad).
pub const MAX_CONSECUTIVE_UNDECODABLE: u32 = 3;

/// The latched down-reason before the agent has ever answered.
pub const NOT_YET_REACHABLE: &str = "not yet reachable";

/// What the runtime tells one device instance.
#[derive(Debug, Clone)]
pub enum InstanceEvent {
    /// One new observation for this device — **ordinary on-change flow**, the shape every poll
    /// cycle and every stream part delivers. Boxed: a single observation is by far the largest
    /// thing this enum carries, and every other variant would pay for it in the queue.
    Obs(Box<Observation>),
    /// A **re-baseline**: the fleet's whole view of this device is being rebuilt, so the batch is
    /// delivered together and the session treats it as a resync (it re-arms the deadband) rather
    /// than as on-change flow. Reserved for the attach snapshot, a forced republish (resume,
    /// repoll, `OUT_OF_RANGE` recovery) and the post-restart resync snapshot — never for ordinary
    /// delivery, which would re-baseline the session every cycle and leave the deadband inert.
    Snapshot(Vec<Observation>),
    /// The agent is reachable and its model verified.
    AgentUp(Arc<AgentInfo>),
    /// The agent became unreachable; the reason is for logs and events, never for a metric label.
    AgentDown(String),
    /// The agent's buffer ran past our position: this many observations are provably lost.
    DataLoss { skipped: u64 },
    /// The device's probe model changed under us — browse cursors are void and signals recompile.
    ModelDrift { old: String, new: String },
    /// Streaming could not be established after this many consecutive attempts; acquisition
    /// degraded to `/current` polling until a later stream attempt succeeds (LLD §5).
    StreamDegraded { failures: u32 },
}

/// One agent's published state, refreshed by the acquisition task and read without blocking it
/// (`sb/status` must never wait on a poll).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentInfo {
    pub agent_id: String,
    /// The agent's base URL — derived, non-secret.
    pub url: String,
    pub connected: bool,
    /// The acquisition mode in force: `poll` or `stream`.
    pub mode: &'static str,
    pub instance_id: Option<u64>,
    /// The agent's own version string, from a document header.
    pub agent_version: Option<String>,
    /// The MTConnect schema version observed on the wire.
    pub standard_version: Option<String>,
    pub schema_namespace: Option<String>,
    pub buffer_size: Option<u64>,
    pub first_sequence: Option<u64>,
    pub next_sequence: Option<u64>,
    pub heartbeat_ms: u32,
    /// When the last document (of any kind) arrived.
    pub last_document_at: Option<String>,
    /// `sha256:<hex>` of each attached device's probe model.
    pub probe_digests: std::collections::BTreeMap<String, String>,
}

impl AgentInfo {
    /// The non-secret status view `sb/status` publishes as its `protocol` object (HLD §7).
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "capability": "MTCONNECT_CLIENT",
            "agentId": self.agent_id,
            "endpoint": self.url,
            "connected": self.connected,
            "mode": self.mode,
            "standardVersion": self.standard_version,
            "schemaNamespace": self.schema_namespace,
            "agentVersion": self.agent_version,
            "instanceId": self.instance_id,
            "bufferSize": self.buffer_size,
            "firstSequence": self.first_sequence,
            "nextSequence": self.next_sequence,
            "heartbeatMs": self.heartbeat_ms,
            "lastDocumentAt": self.last_document_at,
            "probeDigests": self.probe_digests,
            "limitations": ["READ_ONLY", "XML_ONLY", "NO_ASSETS"],
        })
    }
}

/// A control message for the acquisition task. Every verb that must serialize with acquisition
/// rides this channel rather than touching the client directly.
#[derive(Debug)]
pub enum AgentCtl {
    /// Read these data items of this device **now** (`sb/read`, `repoll`).
    Snapshot {
        device_uuid: String,
        data_item_ids: Vec<String>,
        reply: oneshot::Sender<Result<Vec<Observation>, MtcError>>,
    },
    /// Drop and re-establish acquisition (`reconnect`): the model cache is refreshed and every
    /// dedupe floor reset, so the next poll republishes as fresh.
    Reconnect {
        reply: oneshot::Sender<Result<(), MtcError>>,
    },
    /// Stop the acquisition task.
    Shutdown,
}

// =================================================================================================
// The two-lane instance queue (LLD §3, D-R2)
// =================================================================================================

/// Whether an event may never be silently dropped.
///
/// Loss-intolerant ⇔ `AgentUp | AgentDown | DataLoss | ModelDrift | StreamDegraded | Snapshot(_)`,
/// or `Obs(o)` where `o.category == Category::Condition` — a condition transition is a state
/// machine's input, not a resamplable value. Everything else rides the coalescible data lane.
#[must_use]
pub fn is_loss_intolerant(event: &InstanceEvent) -> bool {
    match event {
        InstanceEvent::AgentUp(_)
        | InstanceEvent::AgentDown(_)
        | InstanceEvent::DataLoss { .. }
        | InstanceEvent::ModelDrift { .. }
        | InstanceEvent::StreamDegraded { .. }
        | InstanceEvent::Snapshot(_) => true,
        InstanceEvent::Obs(obs) => obs.category == Category::Condition,
    }
}

/// Counters the runtime folds into [`AgentRuntime::dropped_events`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueCounters {
    /// Data-lane observations lost because the consumer lagged.
    pub dropped_data: u64,
    /// Loss-intolerant events lost because the consumer lagged past the send budget, the send was
    /// cancelled, or the receiver went away.
    pub dropped_critical: u64,
    /// Data-lane observations replaced in place by a newer reading of the same data item.
    pub coalesced: u64,
}

/// What both ends of one instance queue share.
#[derive(Debug, Default)]
struct QueueState {
    /// The loss-intolerant lane, drained FIRST so a condition transition is applied before the
    /// values that accompanied it.
    critical: VecDeque<InstanceEvent>,
    /// The coalescible data lane.
    data: VecDeque<Box<Observation>>,
    /// The receiver is gone (the session closed): every further send is a counted no-op.
    detached: bool,
    counters: QueueCounters,
}

#[derive(Debug)]
struct Queue {
    state: Mutex<QueueState>,
    /// Signalled by a drain: what a loss-intolerant send waits on for room.
    room: Notify,
}

/// One instance's queue: a coalescible data lane and a reserved loss-intolerant lane.
#[must_use]
pub fn instance_queue() -> (InstanceSender, InstanceReceiver) {
    let shared = Arc::new(Queue {
        state: Mutex::new(QueueState::default()),
        room: Notify::new(),
    });
    (
        InstanceSender {
            queue: Arc::clone(&shared),
        },
        InstanceReceiver { queue: shared },
    )
}

/// The acquisition task's end of one instance queue.
#[derive(Clone, Debug)]
pub struct InstanceSender {
    queue: Arc<Queue>,
}

/// The session's end of one instance queue. Dropping it marks the queue detached, so the
/// acquisition task counts rather than blocks once a session is gone.
#[derive(Debug)]
pub struct InstanceReceiver {
    queue: Arc<Queue>,
}

/// What one synchronous push onto the loss-intolerant lane did.
enum CriticalPush {
    /// Queued — the lane had room.
    Queued,
    /// No room. The event comes back so the sender can wait for a drain and offer it again.
    Full(InstanceEvent),
    /// The receiver is gone: no drain will ever come, so waiting for room would be a lie.
    Detached,
}

impl InstanceSender {
    /// Data lane. Never blocks.
    ///
    /// A full lane keeps its depth by **latest-value coalescing** (LLD §3): a queued reading of the
    /// same `data_item_id` is replaced in place, counted `coalesced` — the consumer was going to
    /// act on the newer number anyway, so nothing it could still have used is lost, and the
    /// signal keeps its place in line. Only when there is no entry to supersede does the lane evict
    /// its OLDEST entry, which IS a loss and is counted `dropped_data`. A detached receiver makes
    /// the send a counted no-op.
    pub fn send_data(&self, obs: Box<Observation>) {
        let mut state = self.queue.state.lock().expect("instance queue");
        if state.detached {
            state.counters.dropped_data += 1;
            return;
        }
        if state.data.len() >= INSTANCE_QUEUE_DEPTH {
            let queued = state
                .data
                .iter()
                .position(|q| q.data_item_id == obs.data_item_id);
            if let Some(at) = queued {
                state.data[at] = obs;
                state.counters.coalesced += 1;
                return;
            }
            state.data.pop_front();
            state.counters.dropped_data += 1;
        }
        state.data.push_back(obs);
    }

    /// Loss-intolerant lane. Enqueues immediately when there is room; when the lane is **full it
    /// waits** for a drain to make room, up to [`CRITICAL_SEND_BUDGET`], preempted by `cancel`
    /// — genuine consumer lag backpressures acquisition instead of silently discarding a condition
    /// transition or a lifecycle event.
    ///
    /// Past the budget, on cancellation, or against a detached receiver the event is dropped and
    /// counted (`dropped_critical`). This bound is the recorded, justified deviation from the LLD's
    /// literal unbounded `send().await` (D-R2): the publish path below is shared by every agent, so
    /// an unbounded wait would let one stalled consumer freeze all acquisition — while the
    /// backpressured events could not be published anyway. It is never an error the caller must
    /// handle, because there is nothing a caller could usefully do with one: the counter and a
    /// rate-limited warning are the surface.
    pub async fn send_critical(&self, event: InstanceEvent, cancel: &CancellationToken) {
        // Checked before the push, not after: once shutdown has been asked for, nothing new goes
        // into a queue nobody will drain.
        if cancel.is_cancelled() {
            self.count_critical_drop();
            return;
        }
        let mut event = match self.try_push_critical(event) {
            CriticalPush::Queued => return,
            CriticalPush::Detached => {
                self.count_critical_drop();
                return;
            }
            CriticalPush::Full(event) => event,
        };
        let deadline = tokio::time::Instant::now() + CRITICAL_SEND_BUDGET;
        loop {
            // Register interest BEFORE re-offering: a drain landing between the offer and the wait
            // would otherwise go unnoticed and the event would sit out its whole budget for nothing.
            let room = self.queue.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            match self.try_push_critical(event) {
                CriticalPush::Queued => return,
                CriticalPush::Detached => break,
                CriticalPush::Full(pending) => event = pending,
            }
            tokio::select! {
                () = &mut room => {}
                () = cancel.cancelled() => break,
                () = tokio::time::sleep_until(deadline) => break,
            }
        }
        self.count_critical_drop();
    }

    /// Drain-and-reset the counters (the runtime aggregates them).
    #[must_use]
    pub fn take_counters(&self) -> QueueCounters {
        std::mem::take(&mut self.queue.state.lock().expect("instance queue").counters)
    }

    /// The synchronous critical push: `false` when there was no room (or no receiver). This is what
    /// [`AgentRuntime::attach`] seeds a newborn queue through — an empty queue always has room.
    fn push_critical(&self, event: InstanceEvent) -> bool {
        matches!(self.try_push_critical(event), CriticalPush::Queued)
    }

    fn try_push_critical(&self, event: InstanceEvent) -> CriticalPush {
        let mut state = self.queue.state.lock().expect("instance queue");
        if state.detached {
            return CriticalPush::Detached;
        }
        if state.critical.len() >= CRITICAL_QUEUE_DEPTH {
            return CriticalPush::Full(event);
        }
        state.critical.push_back(event);
        CriticalPush::Queued
    }

    fn count_critical_drop(&self) {
        self.queue
            .state
            .lock()
            .expect("instance queue")
            .counters
            .dropped_critical += 1;
    }
}

impl InstanceReceiver {
    /// Everything queued: the loss-intolerant lane FIRST (FIFO), then the data lane (FIFO, with
    /// coalesced entries in the positions their predecessors held). Non-blocking — the session
    /// drains on its own cadence. Draining is also what signals room to a blocked critical send.
    pub fn drain(&mut self) -> Vec<InstanceEvent> {
        let drained = {
            let mut state = self.queue.state.lock().expect("instance queue");
            let mut out = Vec::with_capacity(state.critical.len() + state.data.len());
            out.extend(state.critical.drain(..));
            out.extend(state.data.drain(..).map(InstanceEvent::Obs));
            out
        };
        // The drain made room: release anyone waiting to enqueue a loss-intolerant event.
        self.queue.room.notify_waiters();
        drained
    }

    /// Whether anything is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let state = self.queue.state.lock().expect("instance queue");
        state.critical.is_empty() && state.data.is_empty()
    }
}

impl Drop for InstanceReceiver {
    fn drop(&mut self) {
        {
            let mut state = self.queue.state.lock().expect("instance queue");
            state.detached = true;
            state.critical.clear();
            state.data.clear();
        }
        // A sender waiting for room has to learn there will never be any: without this it would
        // hold its whole send budget against a consumer that is already gone.
        self.queue.room.notify_waiters();
    }
}

/// One device instance's attachment to a shared agent runtime: it owns no socket, only a queue.
#[derive(Debug)]
pub struct AgentHandle {
    pub agent: Arc<AgentRuntime>,
    pub device_uuid: String,
    pub rx: InstanceReceiver,
}

/// What one established stream did before it ended.
#[derive(Debug)]
pub struct StreamRun {
    pub exit: StreamExit,
    /// Liveness-proving parts ingested (observations, heartbeats, agent-error documents). ZERO
    /// means the stream died before proving anything — the headers-then-EOF case — and counts as an
    /// establish failure (D-R4).
    pub liveness_parts: u64,
}

/// What one polling cycle did — the numbers the `MtconnectStream`/`MtconnectParse` families record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollReport {
    /// Device streams present in the document.
    pub device_streams: usize,
    /// Observations decoded across every attached device.
    pub observations: usize,
    /// Observations that were new and were dispatched.
    pub published: usize,
    /// Elements the parser did not recognize.
    pub unknown_elements: u64,
    /// Observations were decoded but NOT dispatched because the document revealed (or arrived
    /// under) a pending `instanceId` resync — they will be covered by the post-resync snapshot.
    pub deferred: bool,
}

/// One MTConnect agent, shared by every device instance configured against it.
pub struct AgentRuntime {
    cfg: AgentConfig,
    client: MtcClient,
    /// The wall-clock seam: an ISO-8601 UTC "now" with no `edgecommons` import.
    clock: ClockFn,
    models: RwLock<HashMap<String, Arc<ProbeModel>>>,
    sinks: RwLock<HashMap<String, InstanceSender>>,
    info: ArcSwap<AgentInfo>,
    seq: Mutex<SequenceState>,
    parse: Mutex<ParseCounters>,
    /// The acquisition counters the `MtconnectStream`/`MtconnectProbe` families report (HLD §9).
    stats: AgentStats,
    ctl_tx: mpsc::Sender<AgentCtl>,
    ctl_rx: Mutex<Option<mpsc::Receiver<AgentCtl>>>,
    /// Queue accounting folded out of every instance queue: what the coalescible data lane threw
    /// away, what the loss-intolerant lane lost past its budget, and what coalescing superseded.
    dropped_data: AtomicU64,
    dropped_critical: AtomicU64,
    coalesced_events: AtomicU64,
    /// When each instance was last warned about queue losses — one warning per
    /// [`DROP_WARN_INTERVAL`] per instance, never one per lost event.
    drop_warned: Mutex<HashMap<String, Instant>>,
    /// The acquisition task's cancellation token — installed by [`AgentRuntime::spawn`], cancelled
    /// by [`AgentRuntime::shutdown`], and the token every loss-intolerant send is preempted by, so
    /// a full queue can never stall shutdown.
    cancel: Mutex<CancellationToken>,
    /// The latched reason the agent is unreachable — "not yet reachable" before first contact.
    last_down: Mutex<String>,
    /// Millis since [`AgentRuntime::epoch`] at which the agent last VOUCHED for data currency (a
    /// Streams document, or a successful `/current`). `u64::MAX` = never.
    last_liveness: AtomicU64,
    /// The monotonic origin `last_liveness` is measured from.
    epoch: Instant,
    /// Whether an acquisition task is servicing the control channel.
    task_started: AtomicBool,
    /// Ladder 3: the agent restarted, so every attached device must be re-probed before its model
    /// is trusted again.
    resync_needed: AtomicBool,
    /// Whether a multipart stream is currently established — what `sb/status` reports as `mode`.
    streaming_active: AtomicBool,
    /// Devices attached since the last service pass. A live stream carries only *changes*, so a
    /// freshly attached instance is owed a `/current` snapshot of its device (the poll path needs
    /// no list: its periodic `/current` covers a new attachment through the unset dedupe floors).
    attach_pending: Mutex<Vec<String>>,
    attach_notify: Notify,
}

/// Hand-written because the injected clock is a closure and has no `Debug`. What a log line wants
/// from a runtime is which agent it is and whether it is delivering.
impl std::fmt::Debug for AgentRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let info = self.info();
        f.debug_struct("AgentRuntime")
            .field("agent_id", &self.cfg.id)
            .field("url", &info.url)
            .field("connected", &info.connected)
            .field("mode", &info.mode)
            .finish_non_exhaustive()
    }
}

impl AgentRuntime {
    /// Build the runtime for one agent. Credentials are already resolved — this constructor cannot
    /// reach a vault, which is the point. `clock` is the wall-clock seam: production passes the
    /// library's own clock, tests pin a fixed one.
    ///
    /// # Errors
    /// [`MtcError::Tls`]/[`MtcError::Transport`] when the HTTP client cannot be built.
    pub fn new(
        cfg: AgentConfig,
        creds: &AgentCredentials,
        clock: ClockFn,
    ) -> Result<Arc<Self>, MtcError> {
        let client = MtcClient::new(&cfg, creds)?;
        let (ctl_tx, ctl_rx) = mpsc::channel(32);
        let info = AgentInfo {
            agent_id: cfg.id.clone(),
            url: cfg.url.to_string(),
            connected: false,
            mode: AcqState::Connecting.mode(),
            heartbeat_ms: cfg.heartbeat_ms,
            ..AgentInfo::default()
        };
        Ok(Arc::new(Self {
            cfg,
            client,
            clock,
            models: RwLock::new(HashMap::new()),
            sinks: RwLock::new(HashMap::new()),
            info: ArcSwap::from_pointee(info),
            seq: Mutex::new(SequenceState::new()),
            parse: Mutex::new(ParseCounters::default()),
            stats: AgentStats::default(),
            ctl_tx,
            ctl_rx: Mutex::new(Some(ctl_rx)),
            dropped_data: AtomicU64::new(0),
            dropped_critical: AtomicU64::new(0),
            coalesced_events: AtomicU64::new(0),
            drop_warned: Mutex::new(HashMap::new()),
            cancel: Mutex::new(CancellationToken::new()),
            last_down: Mutex::new(NOT_YET_REACHABLE.to_string()),
            last_liveness: AtomicU64::new(u64::MAX),
            epoch: Instant::now(),
            task_started: AtomicBool::new(false),
            resync_needed: AtomicBool::new(false),
            streaming_active: AtomicBool::new(false),
            attach_pending: Mutex::new(Vec::new()),
            attach_notify: Notify::new(),
        }))
    }

    /// This agent's configuration.
    #[must_use]
    pub fn config(&self) -> &AgentConfig {
        &self.cfg
    }

    /// The agent's published state — a snapshot, never a lock the caller holds.
    #[must_use]
    pub fn info(&self) -> Arc<AgentInfo> {
        self.info.load_full()
    }

    /// Parse counters since start.
    #[must_use]
    pub fn parse_counters(&self) -> ParseCounters {
        *self.parse.lock().expect("parse counters")
    }

    /// The acquisition counters since start — what `src/metrics.rs` diffs into the
    /// `MtconnectStream`/`MtconnectProbe` interval measures.
    #[must_use]
    pub fn stats(&self) -> AgentStatsSnapshot {
        self.stats.snapshot()
    }

    /// Events dropped because an instance's consumer lagged — both lanes.
    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        self.dropped_data.load(Ordering::Relaxed) + self.dropped_critical.load(Ordering::Relaxed)
    }

    /// The queue accounting in full, since start: which lane lost what, and how many stale readings
    /// coalescing superseded. Coalescing is not a loss and is deliberately NOT a metric measure —
    /// the `MtconnectStream` family's measure set is closed (D-R6), so this accessor plus the debug
    /// log are where it surfaces.
    #[must_use]
    pub fn queue_counters(&self) -> QueueCounters {
        QueueCounters {
            dropped_data: self.dropped_data.load(Ordering::Relaxed),
            dropped_critical: self.dropped_critical.load(Ordering::Relaxed),
            coalesced: self.coalesced_events.load(Ordering::Relaxed),
        }
    }

    /// ISO-8601 UTC "now" from the injected clock — the arrival stamp an ingested observation
    /// carries (C-6), with no `edgecommons` import below the seam.
    #[must_use]
    pub fn now(&self) -> String {
        (self.clock)()
    }

    /// The latched reason the agent is not delivering. [`NOT_YET_REACHABLE`] before first contact,
    /// so a caller never has to distinguish "never up" from "no reason recorded".
    #[must_use]
    pub fn last_down_reason(&self) -> String {
        self.last_down.lock().expect("down reason").clone()
    }

    /// Time since the agent last VOUCHED for data currency (a Streams document ingested — data or
    /// heartbeat — or a successful `/current` cycle). `None` before first contact.
    #[must_use]
    pub fn liveness_age(&self, now: Instant) -> Option<Duration> {
        let millis = self.last_liveness.load(Ordering::Relaxed);
        if millis == u64::MAX {
            return None;
        }
        Some(now.saturating_duration_since(self.epoch + Duration::from_millis(millis)))
    }

    /// "One missed heartbeat/poll": `heartbeatMs` while a stream is established, else
    /// `2 × pollIntervalMs` (D-R12).
    #[must_use]
    pub fn liveness_window(&self) -> Duration {
        if self.streaming_active.load(Ordering::Relaxed) {
            Duration::from_millis(u64::from(self.cfg.heartbeat_ms))
        } else {
            Duration::from_millis(u64::from(self.cfg.poll_interval_ms).saturating_mul(2))
        }
    }

    /// Record that the agent vouched for currency. Does NOT touch `connected`: only the ingest /
    /// mark-down pair writes that (D-R5).
    fn touch_liveness(&self) {
        let millis = u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX - 1);
        self.last_liveness.store(millis, Ordering::Relaxed);
    }

    /// The acquisition task's cancellation token.
    fn cancel_token(&self) -> CancellationToken {
        self.cancel.lock().expect("cancel token").clone()
    }

    /// The cached model for a device, when it has been probed.
    #[must_use]
    pub fn model(&self, device_uuid: &str) -> Option<Arc<ProbeModel>> {
        self.models
            .read()
            .expect("models")
            .get(device_uuid)
            .cloned()
    }

    /// Attach a device instance: it gets its own bounded queue of [`InstanceEvent`]s. Attaching the
    /// same uuid twice replaces the previous sink (a reconnecting instance, not a second device).
    ///
    /// The newborn queue is **seeded with the current connectivity truth**, so an instance that
    /// attaches after the agent went down still learns it — the `AgentDown` broadcast fires once,
    /// on the transition, and a session created afterwards would otherwise never hear about it.
    pub fn attach(self: &Arc<Self>, device_uuid: &str) -> AgentHandle {
        let (tx, rx) = instance_queue();
        let info = self.info();
        let seed = if info.connected {
            InstanceEvent::AgentUp(info)
        } else {
            InstanceEvent::AgentDown(self.last_down_reason())
        };
        // An empty queue always has room, so the seed needs no bounded wait.
        tx.push_critical(seed);
        self.sinks
            .write()
            .expect("sinks")
            .insert(device_uuid.to_string(), tx);
        // A live stream carries only changes: the streaming task owes this instance a `/current`
        // snapshot of its device. (A one-permit notify: many attaches collapse into one pass.)
        self.attach_pending
            .lock()
            .expect("attach queue")
            .push(device_uuid.to_string());
        self.attach_notify.notify_one();
        AgentHandle {
            agent: Arc::clone(self),
            device_uuid: device_uuid.to_string(),
            rx,
        }
    }

    /// Detach a device instance (its session closed).
    pub fn detach(&self, device_uuid: &str) {
        self.sinks.write().expect("sinks").remove(device_uuid);
    }

    /// The uuids currently attached.
    #[must_use]
    pub fn attached(&self) -> Vec<String> {
        let mut v: Vec<String> = self.sinks.read().expect("sinks").keys().cloned().collect();
        v.sort();
        v
    }

    /// The probe model for a device, fetching and caching it on first use.
    ///
    /// # Errors
    /// [`MtcError::NoSuchDevice`] when the agent's probe has no such uuid, plus any client/parse
    /// error.
    pub async fn ensure_model(&self, device_uuid: &str) -> Result<Arc<ProbeModel>, MtcError> {
        if let Some(cached) = self.model(device_uuid) {
            return Ok(cached);
        }
        Ok(self.refresh_model(device_uuid).await?.0)
    }

    /// Re-probe a device and replace its cached model. Returns the model and whether its digest
    /// changed — a changed digest is model drift, never a silent remap (D-MTC-5).
    ///
    /// # Errors
    /// [`MtcError::NoSuchDevice`], plus any client/parse error.
    pub async fn refresh_model(
        &self,
        device_uuid: &str,
    ) -> Result<(Arc<ProbeModel>, bool), MtcError> {
        let text = self.probe_text().await?;
        let doc = match xml::parse_devices(&text) {
            Ok(doc) => doc,
            Err(e) => {
                self.parse.lock().expect("parse counters").record_err();
                self.stats.record_document_failed();
                return Err(e);
            }
        };
        self.parse
            .lock()
            .expect("parse counters")
            .record_ok(doc.unknown_elements);

        let model = Arc::new(ProbeModel::from_devices(&doc, device_uuid)?);
        let digest = model.digest_hex();
        let previous = self
            .models
            .write()
            .expect("models")
            .insert(device_uuid.to_string(), Arc::clone(&model))
            .map(|m| m.digest_hex());
        let changed = previous.as_ref().is_some_and(|old| old != &digest);

        self.update_info(|info| {
            info.standard_version = doc.ns_version.map(|v| v.to_string());
            info.schema_namespace = doc
                .ns_version
                .map(|v| format!("urn:mtconnect.org:MTConnectDevices:{v}"));
            if doc.header.version.is_some() {
                info.agent_version = doc.header.version.clone();
            }
            info.probe_digests
                .insert(device_uuid.to_string(), digest.clone());
        });

        if changed {
            self.stats.record_model_change();
            self.dispatch(
                device_uuid,
                InstanceEvent::ModelDrift {
                    old: previous.unwrap_or_default(),
                    new: digest,
                },
            )
            .await;
        }
        Ok((model, changed))
    }

    async fn probe_text(&self) -> Result<String, MtcError> {
        let started = Instant::now();
        let result = self.client.probe().await;
        self.stats.record_probe(elapsed_ms(started), result.is_ok());
        match result {
            Ok(text) => Ok(text),
            Err(e) => {
                self.mark_down(&e).await;
                Err(e)
            }
        }
    }

    /// One polling cycle: `GET /current`, demultiplex, decode, dedupe, dispatch.
    ///
    /// # Errors
    /// Any client or parse error; the runtime marks itself down and tells every attached instance
    /// before returning.
    pub async fn poll_once(&self) -> Result<PollReport, MtcError> {
        self.snapshot_cycle(false).await
    }

    /// One `/current` cycle — the polling tick, the post-connect snapshot, and the ladder-2
    /// recovery snapshot are all this, differing only in whether the dedupe floors are bypassed
    /// (`republish_all`: a recovery snapshot deliberately says everything again, as fresh).
    ///
    /// It is also where **ladder 3 completes**, in the order LLD §5 mandates: re-probe → recompile
    /// → THEN snapshot. Poll-only and streaming share this one path, so resync-first holds for both.
    ///
    /// # Errors
    /// Any client or parse error; the runtime marks itself down and tells every attached instance
    /// before returning. A failed re-probe returns the probe's error with the resync still pending.
    pub async fn snapshot_cycle(&self, republish_all: bool) -> Result<PollReport, MtcError> {
        // Read BEFORE the cycle touches anything — and, above all, before the resync below clears
        // the flag: a pending resync means this `/current` IS the ladder-3 re-baseline, even though
        // the floors were already cleared for it by `reset_for_new_instance` and no `republish_all`
        // is needed to make it republish. The `Snapshot` event is the MARKER a session needs (it
        // re-arms the deadband), not the mechanism that refills it.
        let re_baseline = republish_all || self.needs_resync();
        // Ladder 3 FIRST: a restarted agent may have come back with a different device model, so
        // the model is re-verified (drift surfaced, never remapped) before this cycle fetches
        // anything. Publishing observations decoded against the dead incarnation's model is what
        // P1-4 forbids, and doing the re-probe after the dispatch is how it used to happen.
        if self.needs_resync() {
            for uuid in self.attached() {
                if let Err(e) = self.refresh_model(&uuid).await {
                    // The flag STAYS set: the next cycle re-enters resync-first, and until a probe
                    // answers, nothing is published against a model that may already be void.
                    tracing::warn!(
                        agent = %self.cfg.id, device = %uuid, error = %e,
                        "re-probe failed; the resync stays pending and nothing is published"
                    );
                    return Err(e);
                }
            }
            self.resync_needed.store(false, Ordering::Relaxed);
        }
        let started = Instant::now();
        let fetched = self.client.current(None).await;
        self.stats
            .record_latency(elapsed_ms(started), fetched.is_ok());
        let text = match fetched {
            Ok(text) => text,
            Err(e) => {
                self.mark_down(&e).await;
                return Err(e);
            }
        };
        let report = self
            .ingest_streams_as(&text, republish_all, re_baseline)
            .await?;
        if report.deferred {
            // The agent restarted AGAIN mid-recovery: this document was decoded against a model
            // that is void once more, so nothing was dispatched. The next cycle re-enters
            // resync-first against the newer incarnation, and the attach debts stay owed.
            return Ok(report);
        }
        // A `/current` document covers every attached device, so any snapshots owed to freshly
        // attached instances were just served (their dedupe floors were unset).
        self.attach_pending.lock().expect("attach queue").clear();
        Ok(report)
    }

    /// Whether a re-probe is pending after an agent restart.
    #[must_use]
    pub fn needs_resync(&self) -> bool {
        self.resync_needed.load(Ordering::Relaxed)
    }

    /// Parse a Streams document and dispatch what is new. `republish_all` bypasses the dedupe
    /// floors (a resume, an `OUT_OF_RANGE` recovery snapshot). A parse failure here marks the
    /// agent down — this is the *polling* entry, where the document IS the whole cycle. (A
    /// streamed part that fails to parse is only counted; see [`MAX_CONSECUTIVE_UNDECODABLE`].)
    ///
    /// # Errors
    /// Any parse error, counted into [`Self::parse_counters`] first.
    pub async fn ingest_streams(
        &self,
        text: &str,
        republish_all: bool,
    ) -> Result<PollReport, MtcError> {
        self.ingest_streams_as(text, republish_all, republish_all)
            .await
    }

    /// [`Self::ingest_streams`] with the re-baseline decision made by the caller: only
    /// [`Self::snapshot_cycle`] knows that a cycle is a ladder-3 recovery, which republishes
    /// everything off already-cleared floors rather than through `republish_all`.
    async fn ingest_streams_as(
        &self,
        text: &str,
        republish_all: bool,
        re_baseline: bool,
    ) -> Result<PollReport, MtcError> {
        let doc = match xml::parse_streams(text) {
            Ok(doc) => doc,
            Err(e) => {
                self.parse.lock().expect("parse counters").record_err();
                self.stats.record_document_failed();
                self.mark_down(&e).await;
                return Err(e);
            }
        };
        self.parse
            .lock()
            .expect("parse counters")
            .record_ok(doc.unknown_elements);
        Ok(self
            .ingest_streams_doc(&doc, republish_all, re_baseline)
            .await)
    }

    /// Fold one already-parsed Streams document into the runtime: sequence header, dedupe,
    /// dispatch, published state. Infallible — parsing (and its failure policy) is the caller's.
    ///
    /// A document that reveals — or arrives under — a pending `instanceId` resync is **deferred**:
    /// it updates liveness, the header facts and the counters, but dispatches nothing and touches
    /// no dedupe floor, because the model generation it was decoded against is void
    /// ([`PollReport::deferred`]). [`Self::snapshot_cycle`] re-probes and then covers those
    /// observations with the post-resync snapshot.
    async fn ingest_streams_doc(
        &self,
        doc: &xml::StreamsDoc,
        republish_all: bool,
        re_baseline: bool,
    ) -> PollReport {
        // The header first: an agent restart voids every sequence number we hold, and it must do so
        // BEFORE anything from this document is measured against a floor.
        let outcome = {
            let mut seq = self.seq.lock().expect("sequence state");
            if republish_all {
                seq.reset_dedupe();
            }
            seq.observe_header(&doc.header)
        };
        let instance_changed = matches!(outcome, HeaderOutcome::InstanceChanged { .. });
        if let HeaderOutcome::InstanceChanged { old, new } = outcome {
            // Ladder 3: the numbers are already void (the state reset itself). The MODEL is now
            // suspect too, so a re-probe is scheduled rather than assumed unnecessary. What this
            // document carries is a fresh view of a device whose model has yet to be verified —
            // the gate below is what stops it from being published as one.
            tracing::warn!(agent = %self.cfg.id, old, new, "agent restarted; resequencing");
            self.resync_needed.store(true, Ordering::Relaxed);
        }
        // The generation gate (P1-4). This document was decoded against a model generation the
        // runtime now knows is void — either because this very header revealed the restart, or
        // because an earlier document did and the re-probe has not run yet. LLD §5 ladder 3 is
        // re-probe → recompile → THEN snapshot, so nothing here may be dispatched: an update
        // decoded against the old model and routed by the new one mixes generations. The document
        // still proves the NEW incarnation is alive and still feeds the counters; its observations
        // are covered by the post-resync `/current`, whose floors this document must not touch.
        let deferred = instance_changed || self.needs_resync();

        let mut report = PollReport {
            device_streams: doc.device_streams.len(),
            unknown_elements: doc.unknown_elements,
            deferred,
            ..PollReport::default()
        };

        for ds in &doc.device_streams {
            if !self.is_attached(&ds.uuid) {
                continue;
            }
            let model = self.model(&ds.uuid);
            let mut fresh = Vec::new();
            for entry in &ds.entries {
                let meta = entry
                    .elem
                    .attr("dataItemId")
                    .and_then(|id| model.as_ref().and_then(|m| m.item(id).cloned()));
                let Some(obs) = observations::decode(entry, meta.as_ref()) else {
                    continue;
                };
                report.observations += 1;
                if deferred {
                    // Counted, never dispatched — and deliberately never measured against a dedupe
                    // floor: a floor recorded here would claim the instance already has an
                    // observation it was never sent, and would suppress the post-resync snapshot's
                    // own copy of it forever.
                    continue;
                }
                let is_new = {
                    let mut seq = self.seq.lock().expect("sequence state");
                    seq.should_publish(&dedupe_key(&ds.uuid, &obs.data_item_id), obs.sequence)
                };
                if is_new {
                    fresh.push(obs);
                }
            }
            report.published += fresh.len();
            if fresh.is_empty() {
                continue;
            }
            if re_baseline {
                // A re-baseline is ONE event on purpose: the session must treat the whole batch as
                // a fresh view of the device — which re-arms its deadband — rather than as
                // on-change flow. Losing one would break the resync guarantee, so it rides the
                // loss-intolerant lane.
                self.dispatch(&ds.uuid, InstanceEvent::Snapshot(fresh))
                    .await;
            } else {
                // Ordinary flow is per observation, so each one takes the lane its class earns:
                // condition transitions are loss-intolerant, values are coalescible. (F-N1: a
                // per-batch `Snapshot` re-baselined the session on EVERY cycle, which reset the
                // deadband entry state before it could ever suppress anything.)
                for obs in fresh {
                    self.dispatch(&ds.uuid, InstanceEvent::Obs(Box::new(obs)))
                        .await;
                }
            }
        }

        self.stats.record_document(report.observations as u64);

        let mode = if self.streaming_active.load(Ordering::Relaxed) {
            AcqState::Streaming { next: 0 }.mode()
        } else {
            AcqState::Polling.mode()
        };
        // A Streams document IS delivery: this is the one place `connected` is set true (D-R5).
        self.mark_up(|info| {
            info.mode = mode;
            info.instance_id = Some(doc.header.instance_id);
            info.buffer_size = doc.header.buffer_size;
            info.first_sequence = doc.header.first_sequence;
            info.next_sequence = doc.header.next_sequence;
            info.last_document_at = doc.header.creation_time.clone();
            if doc.header.version.is_some() {
                info.agent_version = doc.header.version.clone();
            }
            if let Some(v) = doc.ns_version {
                info.standard_version = Some(v.to_string());
            }
        })
        .await;
        report
    }

    /// Record that the agent is **delivering**: refresh the liveness clock, fold the document's
    /// header facts into the published state, flip `connected`, and announce the transition once.
    ///
    /// This and [`Self::mark_down`] are the only writers of `connected` (D-R1/D-R5) — a probe that
    /// answers, a cached model, or a served command read are none of them proof of delivery.
    async fn mark_up(&self, facts: impl FnOnce(&mut AgentInfo)) {
        self.touch_liveness();
        let was_down = !self.info().connected;
        self.update_info(|info| {
            info.connected = true;
            facts(info);
        });
        if was_down {
            let info = self.info();
            self.broadcast(&InstanceEvent::AgentUp(info)).await;
        }
    }

    /// A scoped `/current` read: every configured data item of one device, or just the named ones.
    /// Never deduped — a read answers with what the agent has *now*, by definition.
    ///
    /// # Errors
    /// Any client or parse error.
    pub async fn snapshot(
        &self,
        device_uuid: &str,
        data_item_ids: &[String],
    ) -> Result<Vec<Observation>, MtcError> {
        let started = Instant::now();
        let fetched = self.client.current(None).await;
        self.stats
            .record_latency(elapsed_ms(started), fetched.is_ok());
        let text = match fetched {
            Ok(text) => text,
            Err(e) => {
                self.mark_down(&e).await;
                return Err(e);
            }
        };
        let doc = match xml::parse_streams(&text) {
            Ok(doc) => doc,
            Err(e) => {
                self.parse.lock().expect("parse counters").record_err();
                self.stats.record_document_failed();
                return Err(e);
            }
        };
        self.parse
            .lock()
            .expect("parse counters")
            .record_ok(doc.unknown_elements);
        // A served read vouches for currency — but never for connectivity: one writer family owns
        // `connected`, and this is not it (D-R5).
        self.touch_liveness();

        let Some(ds) = doc.device_streams.iter().find(|d| d.uuid == device_uuid) else {
            return Err(MtcError::NoSuchDevice(device_uuid.to_string()));
        };
        let model = self.model(device_uuid);
        let mut out = Vec::new();
        for entry in &ds.entries {
            let Some(id) = entry.elem.attr("dataItemId") else {
                continue;
            };
            if !data_item_ids.is_empty() && !data_item_ids.iter().any(|w| w == id) {
                continue;
            }
            let meta = model.as_ref().and_then(|m| m.item(id).cloned());
            if let Some(obs) = observations::decode(entry, meta.as_ref()) {
                out.push(obs);
            }
        }
        Ok(out)
    }

    /// The control-channel form of [`Self::snapshot`] — how command verbs read, so a read
    /// serializes with acquisition instead of racing it. Falls back to a direct read when no
    /// acquisition task is running.
    ///
    /// # Errors
    /// Any client/parse error, or [`MtcError::Timeout`] when the acquisition task does not answer.
    pub async fn request_snapshot(
        &self,
        device_uuid: &str,
        data_item_ids: &[String],
    ) -> Result<Vec<Observation>, MtcError> {
        if !self.task_started.load(Ordering::Relaxed) {
            return self.snapshot(device_uuid, data_item_ids).await;
        }
        let (tx, rx) = oneshot::channel();
        if self
            .ctl_tx
            .send(AgentCtl::Snapshot {
                device_uuid: device_uuid.to_string(),
                data_item_ids: data_item_ids.to_vec(),
                reply: tx,
            })
            .await
            .is_err()
        {
            return self.snapshot(device_uuid, data_item_ids).await;
        }
        let budget =
            Duration::from_millis(u64::from(self.cfg.request_timeout_ms)) + Duration::from_secs(2);
        match tokio::time::timeout(budget, rx).await {
            Ok(Ok(result)) => result,
            // The task went away mid-request: answer from a direct read rather than failing.
            Ok(Err(_)) => self.snapshot(device_uuid, data_item_ids).await,
            Err(_) => Err(MtcError::Timeout {
                ms: budget.as_millis() as u64,
            }),
        }
    }

    /// Ask the acquisition task to re-establish: models are re-probed and dedupe floors reset.
    ///
    /// # Errors
    /// Any probe error, or [`MtcError::Timeout`] when the task does not answer.
    pub async fn request_reconnect(&self) -> Result<(), MtcError> {
        if !self.task_started.load(Ordering::Relaxed) {
            return self.reconnect().await;
        }
        let (tx, rx) = oneshot::channel();
        if self
            .ctl_tx
            .send(AgentCtl::Reconnect { reply: tx })
            .await
            .is_err()
        {
            return self.reconnect().await;
        }
        let budget =
            Duration::from_millis(u64::from(self.cfg.request_timeout_ms)) + Duration::from_secs(2);
        match tokio::time::timeout(budget, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => self.reconnect().await,
            Err(_) => Err(MtcError::Timeout {
                ms: budget.as_millis() as u64,
            }),
        }
    }

    /// Re-probe every attached device and reset the dedupe floors, so the next cycle republishes
    /// everything as fresh.
    ///
    /// # Errors
    /// The first probe error encountered.
    pub async fn reconnect(&self) -> Result<(), MtcError> {
        self.seq.lock().expect("sequence state").reset_dedupe();
        for uuid in self.attached() {
            self.refresh_model(&uuid).await?;
        }
        Ok(())
    }

    /// Stop the acquisition task (idempotent; a runtime with no task is already stopped): cancel
    /// its token AND offer it a `Shutdown`, belt and braces — the token preempts every await point,
    /// the message ends the loop that is between them.
    ///
    /// The message is offered with `try_send`, never awaited. The control channel is 32 deep, so
    /// against a *blocked* task the send would either succeed into a buffer nobody will ever drain
    /// or — once those 32 are spent — wait forever for room: exactly the stall this method exists to
    /// end. The token is the guarantee; the message is only the courtesy that lets a healthy task
    /// finish the iteration it is in.
    pub async fn shutdown(&self) {
        self.cancel_token().cancel();
        if self.ctl_tx.try_send(AgentCtl::Shutdown).is_err() {
            tracing::debug!(
                agent = %self.cfg.id,
                "the shutdown message had nowhere to go; the cancellation token stops the task"
            );
        }
        // Let a task that is already awake on the cancelled token run its exit path before the
        // caller starts joining. Purely an optimization: `join_all_within` waits either way.
        tokio::task::yield_now().await;
    }

    /// Start the acquisition task. Calling it twice is a no-op: the receiver is taken once.
    ///
    /// **Acquisition mode:** under [`StreamPolicy::Prefer`] the task runs the LLD §5 streaming
    /// state machine with polling as its degradation floor; under [`StreamPolicy::PollOnly`] it
    /// only ever polls `/current`.
    ///
    /// `cancel` is the task's own token: it is installed on the runtime, so [`Self::shutdown`] can
    /// cancel it, every loss-intolerant send is preempted by it, and every await point in the
    /// acquisition state machine gives way to it. The returned [`JoinHandle`](tokio::task::JoinHandle)
    /// is the caller's to keep — the structured shutdown joins it rather than abandoning it.
    pub fn spawn(
        self: &Arc<Self>,
        cancel: CancellationToken,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let ctl = self.ctl_rx.lock().expect("ctl receiver").take()?;
        *self.cancel.lock().expect("cancel token") = cancel;
        self.task_started.store(true, Ordering::Relaxed);
        let me = Arc::clone(self);
        Some(tokio::spawn(async move { me.run(ctl).await }))
    }

    /// The task body, under the cancellation token installed by [`Self::spawn`].
    ///
    /// The acquisition state machine has its own cancel arms at every `select!` it owns, so the
    /// ordinary stop is a clean one — the loop exits between two awaits. This outer arm is the
    /// guarantee for the awaits that are NOT selects: a `/current` fetch against an agent that
    /// accepted the connection and then went silent holds the task for the whole
    /// `requestTimeoutMs`, and a control channel full of queued snapshots would make the task work
    /// through every one of them before noticing. Shutdown must not have to wait for either
    /// (P1-7), so the whole body gives way to the token.
    async fn run(self: Arc<Self>, mut ctl: mpsc::Receiver<AgentCtl>) {
        let cancel = self.cancel_token();
        let acquisition = async {
            match self.cfg.streaming {
                StreamPolicy::PollOnly => self.run_poll_only(&mut ctl).await,
                StreamPolicy::Prefer => self.run_streaming(&mut ctl).await,
            }
        };
        tokio::select! {
            () = acquisition => {}
            () = cancel.cancelled() => {
                tracing::info!(agent = %self.cfg.id, "acquisition cancelled; the task is exiting");
            }
        }
        self.task_started.store(false, Ordering::Relaxed);
    }

    /// The `poll-only` acquisition loop: `/current` on the configured cadence, until it is told to
    /// stop — by a `Shutdown` message, a closed control channel, or the cancellation token.
    async fn run_poll_only(&self, ctl: &mut mpsc::Receiver<AgentCtl>) {
        let cancel = self.cancel_token();
        let mut ticker =
            tokio::time::interval(Duration::from_millis(u64::from(self.cfg.poll_interval_ms)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                msg = ctl.recv() => match msg {
                    None | Some(AgentCtl::Shutdown) => return,
                    Some(AgentCtl::Snapshot { device_uuid, data_item_ids, reply }) => {
                        let result = self.snapshot(&device_uuid, &data_item_ids).await;
                        let _ = reply.send(result);
                    }
                    Some(AgentCtl::Reconnect { reply }) => {
                        let result = self.reconnect().await;
                        let _ = reply.send(result);
                    }
                },
                _ = ticker.tick() => {
                    if let Err(e) = self.poll_once().await {
                        tracing::warn!(agent = %self.cfg.id, error = %e, "poll failed");
                    }
                }
            }
        }
    }

    /// The LLD §5 streaming state machine:
    ///
    /// ```text
    /// Connecting ──probe ok──▶ Snapshot(/current) ──▶ Streaming(from = nextSequence)
    ///   heartbeat missed / transport / framing → re-establish from the same next   [ladder 1]
    ///   OUT_OF_RANGE → DataLoss + snapshot republished as fresh + resume           [ladder 2]
    ///   instanceId changed → re-probe (ModelDrift on digest change) + snapshot     [ladder 3]
    ///   N consecutive establish failures → degrade to polling, retry per backoff
    /// ```
    async fn run_streaming(&self, ctl: &mut mpsc::Receiver<AgentCtl>) {
        let cancel = self.cancel_token();
        // Consecutive failures to *establish* the stream (the degradation counter, LLD §5).
        let mut establish_failures: u32 = 0;
        // Whether acquisition has degraded to `/current` polling between stream attempts.
        let mut degraded = false;
        // Consecutive connect/probe failures (the plain reconnect backoff).
        let mut connect_failures: u32 = 0;
        // Ladder 2 wants the next snapshot to bypass the dedupe floors.
        let mut republish_next_snapshot = false;

        'connect: loop {
            // A cancelled task starts no new probe/snapshot round: whatever the recovery ladder
            // was about to re-establish, nobody is going to consume it.
            if cancel.is_cancelled() {
                return;
            }
            // --- Connecting: probe (models for every attached device) + /current snapshot ------
            for uuid in self.attached() {
                if let Err(e) = self.ensure_model(&uuid).await {
                    tracing::warn!(agent = %self.cfg.id, device = %uuid, error = %e, "probe failed");
                }
            }
            match self.snapshot_cycle(republish_next_snapshot).await {
                Ok(_) => {
                    republish_next_snapshot = false;
                    connect_failures = 0;
                }
                Err(e) => {
                    let wait = self.backoff_delay(connect_failures);
                    connect_failures = connect_failures.saturating_add(1);
                    tracing::warn!(
                        agent = %self.cfg.id, error = %e, wait_ms = wait.as_millis() as u64,
                        "connect failed; backing off"
                    );
                    match self.wait_serving_ctl(ctl, wait, false).await {
                        CtlFlow::Shutdown => return,
                        CtlFlow::Reconnected | CtlFlow::Elapsed => continue 'connect,
                    }
                }
            }

            // --- Streaming (with ladder-1 re-establishment and the degradation floor) ----------
            'stream: loop {
                if cancel.is_cancelled() {
                    return;
                }
                let from = self.seq.lock().expect("sequence state").next;
                let request = StreamRequest {
                    from: Some(from),
                    interval_ms: STREAM_INTERVAL_MS,
                    heartbeat_ms: self.cfg.heartbeat_ms,
                    path: None,
                };
                // The *request* (headers in, status + headers out) is bounded like any one-shot;
                // only the body is open-ended, and liveness there is the heartbeat's job.
                let open_timeout = Duration::from_millis(u64::from(self.cfg.request_timeout_ms));
                let open_started = Instant::now();
                let opened = tokio::select! {
                    // Opening a stream against a silent agent costs the whole request timeout;
                    // shutdown does not wait it out.
                    () = cancel.cancelled() => return,
                    outcome = tokio::time::timeout(
                        open_timeout,
                        self.client.open_sample_stream(&request),
                    ) => match outcome {
                        Ok(result) => result,
                        Err(_) => Err(MtcError::Timeout {
                            ms: open_timeout.as_millis() as u64,
                        }),
                    },
                };
                self.stats
                    .record_latency(elapsed_ms(open_started), opened.is_ok());
                if opened.is_ok() {
                    // Every established stream comes back through here - the ladders and the
                    // degradation floor alike - so this is where a re-establishment is counted.
                    self.stats.record_stream_opened();
                }
                let opened = opened.and_then(|response| {
                    let reader = MultipartReader::from_content_type(
                        response.content_type(),
                        response.max_document_bytes(),
                    )?;
                    Ok((response, reader))
                });

                let (mut response, mut reader) = match opened {
                    Ok(pair) => pair,
                    Err(e) => {
                        establish_failures = establish_failures.saturating_add(1);
                        tracing::warn!(
                            agent = %self.cfg.id, error = %e, failures = establish_failures,
                            "stream establish failed"
                        );
                        self.degrade_if_exhausted(&mut degraded, establish_failures)
                            .await;
                        let wait = self.backoff_delay(establish_failures.saturating_sub(1));
                        // While degraded, the wait IS the polling window: /current keeps data
                        // flowing between stream retries.
                        match self.wait_serving_ctl(ctl, wait, degraded).await {
                            CtlFlow::Shutdown => return,
                            CtlFlow::Reconnected => {
                                establish_failures = 0;
                                degraded = false;
                                continue 'connect;
                            }
                            CtlFlow::Elapsed => continue 'stream,
                        }
                    }
                };

                // NOTE: opening the response is NOT establishing the stream. A stream counts as
                // established only once it has proved liveness, which is what stops a
                // headers-then-immediate-EOF agent from spinning a tight, never-backing-off,
                // never-degrading reconnect loop (D-R4).
                self.set_streaming(true);
                let run = self.drive_stream(&mut response, &mut reader, ctl).await;
                self.set_streaming(false);

                if run.liveness_parts > 0 {
                    // The stream delivered: this attempt was a real establishment.
                    establish_failures = 0;
                    if degraded {
                        degraded = false;
                        tracing::info!(agent = %self.cfg.id, "streaming re-established");
                    }
                }

                match run.exit {
                    StreamExit::Shutdown => return,
                    StreamExit::CtlReconnect => continue 'connect,
                    // Ladder 1: the agent stopped delivering. It is DOWN — every exit here means
                    // the link that was proving liveness is gone (D-R3) — and the attempt only
                    // counts as an establish failure when the stream never proved itself.
                    StreamExit::HeartbeatMissed => {
                        tracing::warn!(
                            agent = %self.cfg.id, window_ms = u64::from(self.cfg.heartbeat_ms) * 2,
                            "heartbeat missed; re-establishing the stream"
                        );
                        self.mark_down(&MtcError::Timeout {
                            ms: u64::from(self.cfg.heartbeat_ms) * 2,
                        })
                        .await;
                    }
                    StreamExit::TransportLost(e) | StreamExit::Malformed(e) => {
                        tracing::warn!(agent = %self.cfg.id, error = %e, "stream dropped; re-establishing");
                        self.mark_down(&e).await;
                    }
                    StreamExit::EndOfStream => {
                        tracing::info!(agent = %self.cfg.id, "agent closed the stream; re-establishing");
                        self.mark_down(&MtcError::Transport("agent closed the stream".into()))
                            .await;
                    }
                    // Ladder 2: the buffer provably ran past us. Say how much was lost, then
                    // snapshot-republish and resume from the snapshot's position. NOT a mark-down:
                    // the document that said so proves the agent is alive and answering, and the
                    // recovery I/O marks down itself if it fails (D-R3).
                    StreamExit::OutOfRange { first_sequence } => {
                        let skipped = self
                            .seq
                            .lock()
                            .expect("sequence state")
                            .skipped_before(first_sequence);
                        tracing::warn!(
                            agent = %self.cfg.id, first_sequence, skipped,
                            "agent buffer overran our position; snapshot recovery"
                        );
                        self.stats.record_gap(skipped);
                        self.broadcast(&InstanceEvent::DataLoss { skipped }).await;
                        republish_next_snapshot = true;
                        continue 'connect;
                    }
                    // Ladder 3: the agent restarted. The sequence state already reset itself; the
                    // connect phase re-probes (surfacing ModelDrift) and snapshots.
                    StreamExit::InstanceChanged => {
                        tracing::warn!(agent = %self.cfg.id, "agent restarted; full resync");
                        continue 'connect;
                    }
                }

                // A ladder-1 exit. A stream that delivered gets ONE immediate re-establish (the
                // prompt resume from `nextSequence` ladder 1 is for); one that never delivered is a
                // failed attempt, and waits out the growing backoff before trying again.
                if run.liveness_parts > 0 {
                    continue 'stream;
                }
                establish_failures = establish_failures.saturating_add(1);
                tracing::warn!(
                    agent = %self.cfg.id, failures = establish_failures,
                    "the stream ended before delivering anything; backing off"
                );
                self.degrade_if_exhausted(&mut degraded, establish_failures)
                    .await;
                let wait = self.backoff_delay(establish_failures.saturating_sub(1));
                match self.wait_serving_ctl(ctl, wait, degraded).await {
                    CtlFlow::Shutdown => return,
                    CtlFlow::Reconnected => {
                        establish_failures = 0;
                        degraded = false;
                        continue 'connect;
                    }
                    CtlFlow::Elapsed => continue 'stream,
                }
            }
        }
    }

    /// Degrade to `/current` polling once the establish-failure budget is spent — announced once
    /// per degradation, never once per attempt.
    async fn degrade_if_exhausted(&self, degraded: &mut bool, failures: u32) {
        if *degraded || failures < STREAM_ESTABLISH_FAILURE_LIMIT {
            return;
        }
        *degraded = true;
        tracing::warn!(
            agent = %self.cfg.id,
            "degrading to /current polling; streaming retried on backoff"
        );
        self.broadcast(&InstanceEvent::StreamDegraded { failures })
            .await;
    }

    /// Read one established stream until it ends, servicing the control channel and the heartbeat
    /// window as it goes. Public so the virtual-clock sequence tests can drive every ladder with a
    /// scripted [`ChunkSource`] instead of a socket.
    ///
    /// The [`StreamRun`] reports both how the stream ended and **how many liveness-proving parts it
    /// ingested** — the evidence the state machine needs to tell a stream that worked and then
    /// broke from one that never worked at all (D-R4).
    ///
    /// A stream is by design a long silence between parts, so shutdown is an arm of the same
    /// `select!` (P1-7): cancelling the task's token ends the stream **now** with
    /// [`StreamExit::Shutdown`], instead of after up to two heartbeat windows.
    pub async fn drive_stream(
        &self,
        source: &mut impl ChunkSource,
        reader: &mut MultipartReader,
        ctl: &mut mpsc::Receiver<AgentCtl>,
    ) -> StreamRun {
        let cancel = self.cancel_token();
        let mut watch = HeartbeatWatch::new(
            self.cfg.heartbeat_ms,
            tokio::time::Instant::now().into_std(),
        );
        let mut undecodable: u32 = 0;
        let mut liveness_parts: u64 = 0;
        let exit = 'drive: loop {
            let now = tokio::time::Instant::now();
            tokio::select! {
                () = cancel.cancelled() => break 'drive StreamExit::Shutdown,
                msg = ctl.recv() => match msg {
                    None | Some(AgentCtl::Shutdown) => break 'drive StreamExit::Shutdown,
                    Some(AgentCtl::Snapshot { device_uuid, data_item_ids, reply }) => {
                        let result = self.snapshot(&device_uuid, &data_item_ids).await;
                        let _ = reply.send(result);
                    }
                    Some(AgentCtl::Reconnect { reply }) => {
                        let _ = reply.send(self.reconnect().await);
                        break 'drive StreamExit::CtlReconnect;
                    }
                },
                () = self.attach_notify.notified() => {
                    self.service_attach_snapshots().await;
                }
                () = tokio::time::sleep(watch.remaining(now.into_std())) => {
                    if watch.is_expired(tokio::time::Instant::now().into_std()) {
                        break 'drive StreamExit::HeartbeatMissed;
                    }
                }
                chunk = source.next_chunk() => {
                    let bytes = match chunk {
                        Err(e) => break 'drive StreamExit::TransportLost(e),
                        Ok(None) => break 'drive StreamExit::EndOfStream,
                        Ok(Some(bytes)) => bytes,
                    };
                    if let Err(e) = reader.push(&bytes) {
                        break 'drive StreamExit::Malformed(e);
                    }
                    loop {
                        let part = match reader.next_part() {
                            Err(e) => break 'drive StreamExit::Malformed(e),
                            Ok(None) => break,
                            Ok(Some(part)) => part,
                        };
                        let outcome = self.handle_part(&part).await;
                        if outcome.is_liveness() {
                            liveness_parts += 1;
                            undecodable = 0;
                            watch.touch(tokio::time::Instant::now().into_std());
                        }
                        // Ladder 3 supersedes everything a part could also say.
                        if self.needs_resync() {
                            break 'drive StreamExit::InstanceChanged;
                        }
                        match outcome {
                            PartOutcome::OutOfRange { first_sequence } => {
                                break 'drive StreamExit::OutOfRange { first_sequence };
                            }
                            PartOutcome::Undecodable => {
                                undecodable += 1;
                                if undecodable >= MAX_CONSECUTIVE_UNDECODABLE {
                                    break 'drive StreamExit::Malformed(MtcError::Xml(format!(
                                        "{MAX_CONSECUTIVE_UNDECODABLE} consecutive undecodable parts"
                                    )));
                                }
                            }
                            PartOutcome::Observations { .. }
                            | PartOutcome::Heartbeat
                            | PartOutcome::AgentError { .. } => {}
                        }
                    }
                    if reader.is_finished() {
                        break 'drive StreamExit::EndOfStream;
                    }
                }
            }
        };
        StreamRun {
            exit,
            liveness_parts,
        }
    }

    /// Classify and fold one multipart part into the runtime. A Streams document (observations or
    /// heartbeat) is ingested through the same dedupe/dispatch path as a poll; an Errors document
    /// is inspected for `OUT_OF_RANGE` and an agent restart; anything else is counted, never
    /// guessed at.
    async fn handle_part(&self, part: &multipart::Part) -> PartOutcome {
        match classify_part(part) {
            PartDoc::Streams(doc) => {
                self.parse
                    .lock()
                    .expect("parse counters")
                    .record_ok(doc.unknown_elements);
                // A stream part is on-change flow by definition; only its own header can make it a
                // re-baseline (an agent that restarted under the open stream).
                let report = self.ingest_streams_doc(&doc, false, false).await;
                if report.observations == 0 {
                    PartOutcome::Heartbeat
                } else {
                    PartOutcome::Observations {
                        count: report.observations,
                        next_sequence: doc.header.next_sequence,
                    }
                }
            }
            PartDoc::Errors(doc) => {
                self.parse
                    .lock()
                    .expect("parse counters")
                    .record_ok(doc.unknown_elements);
                self.stats.record_document(0);
                // An error document's header still names the agent incarnation: a restart
                // discovered here is still a restart (ladder 3 beats ladder 2).
                if doc.header.instance_id != 0 {
                    let outcome = self
                        .seq
                        .lock()
                        .expect("sequence state")
                        .observe_header(&doc.header);
                    if let HeaderOutcome::InstanceChanged { old, new } = outcome {
                        tracing::warn!(agent = %self.cfg.id, old, new, "agent restarted (error doc)");
                        self.resync_needed.store(true, Ordering::Relaxed);
                    }
                }
                if let Some(first_sequence) = doc.out_of_range() {
                    PartOutcome::OutOfRange { first_sequence }
                } else {
                    let code = doc
                        .errors
                        .first()
                        .map_or_else(|| "UNKNOWN".to_string(), |e| e.code.clone());
                    tracing::warn!(agent = %self.cfg.id, code = %code, "agent error document in stream");
                    PartOutcome::AgentError { code }
                }
            }
            PartDoc::Unexpected(name) => {
                tracing::warn!(agent = %self.cfg.id, root = %name, "unexpected document in stream");
                self.parse.lock().expect("parse counters").record_err();
                self.stats.record_document_failed();
                PartOutcome::Undecodable
            }
            PartDoc::Unreadable(e) => {
                tracing::warn!(agent = %self.cfg.id, error = %e, "undecodable part");
                self.parse.lock().expect("parse counters").record_err();
                self.stats.record_document_failed();
                PartOutcome::Undecodable
            }
        }
    }

    /// Serve freshly attached instances the `/current` snapshot a change-only stream owes them.
    async fn service_attach_snapshots(&self) {
        let pending: Vec<String> =
            std::mem::take(&mut *self.attach_pending.lock().expect("attach queue"));
        for uuid in pending {
            if !self.is_attached(&uuid) {
                continue;
            }
            match self.snapshot(&uuid, &[]).await {
                Ok(observations) if !observations.is_empty() => {
                    let floors: Vec<(String, u64)> = observations
                        .iter()
                        .map(|obs| (obs.data_item_id.clone(), obs.sequence))
                        .collect();
                    let delivered = self
                        .dispatch(&uuid, InstanceEvent::Snapshot(observations))
                        .await;
                    // The floors say "the instance has these already", so they are recorded only
                    // once the snapshot is KNOWN to have landed. Recording them against a dropped
                    // dispatch would suppress those observations forever; leaving them unset costs
                    // one repeat on the next cycle, which is what the dedupe floor is for (F-N2).
                    if delivered {
                        let mut seq = self.seq.lock().expect("sequence state");
                        for (data_item_id, sequence) in floors {
                            seq.should_publish(&dedupe_key(&uuid, &data_item_id), sequence);
                        }
                    } else {
                        tracing::warn!(
                            agent = %self.cfg.id, device = %uuid,
                            "attach snapshot was not delivered; it republishes next cycle"
                        );
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(agent = %self.cfg.id, device = %uuid, error = %e, "attach snapshot failed");
                }
            }
        }
    }

    /// Wait out a backoff window while servicing the control channel — and, while degraded,
    /// keep `/current` polling so data still flows between stream retries.
    ///
    /// A backoff window runs to the agent's `reconnect.maxMs`, so shutdown is an arm of the same
    /// `select!`: cancellation ends the wait at once rather than after it (P1-7).
    async fn wait_serving_ctl(
        &self,
        ctl: &mut mpsc::Receiver<AgentCtl>,
        wait: Duration,
        poll_while_waiting: bool,
    ) -> CtlFlow {
        let cancel = self.cancel_token();
        let deadline = tokio::time::Instant::now() + wait;
        let mut ticker =
            tokio::time::interval(Duration::from_millis(u64::from(self.cfg.poll_interval_ms)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancel.cancelled() => return CtlFlow::Shutdown,
                msg = ctl.recv() => match msg {
                    None | Some(AgentCtl::Shutdown) => return CtlFlow::Shutdown,
                    Some(AgentCtl::Snapshot { device_uuid, data_item_ids, reply }) => {
                        let result = self.snapshot(&device_uuid, &data_item_ids).await;
                        let _ = reply.send(result);
                    }
                    Some(AgentCtl::Reconnect { reply }) => {
                        let _ = reply.send(self.reconnect().await);
                        return CtlFlow::Reconnected;
                    }
                },
                () = tokio::time::sleep_until(deadline) => return CtlFlow::Elapsed,
                _ = ticker.tick(), if poll_while_waiting => {
                    if let Err(e) = self.poll_once().await {
                        tracing::warn!(agent = %self.cfg.id, error = %e, "degraded poll failed");
                    }
                }
            }
        }
    }

    /// Record whether a stream is established — flips the `mode` that `sb/status` publishes.
    fn set_streaming(&self, active: bool) {
        self.streaming_active.store(active, Ordering::Relaxed);
        let mode = if active {
            AcqState::Streaming { next: 0 }.mode()
        } else {
            AcqState::Polling.mode()
        };
        self.update_info(|info| info.mode = mode);
    }

    /// Capped exponential backoff with full jitter over this agent's `reconnect` bounds.
    fn backoff_delay(&self, attempt: u32) -> Duration {
        let exp = self
            .cfg
            .reconnect
            .initial_ms
            .saturating_mul(1_u64 << attempt.min(20));
        let cap = exp.min(self.cfg.reconnect.max_ms);
        Duration::from_millis((rand01() * cap as f64) as u64)
    }

    fn is_attached(&self, device_uuid: &str) -> bool {
        self.sinks.read().expect("sinks").contains_key(device_uuid)
    }

    /// Deliver one event to one instance. `false` when the instance is gone or its queue could not
    /// take the event — the answer [`Self::service_attach_snapshots`] needs before it records
    /// dedupe floors against observations that may never have arrived.
    async fn dispatch(&self, device_uuid: &str, event: InstanceEvent) -> bool {
        let sink = self.sinks.read().expect("sinks").get(device_uuid).cloned();
        match sink {
            Some(tx) => self.send_on(device_uuid, &tx, event).await,
            None => false,
        }
    }

    async fn broadcast(&self, event: &InstanceEvent) {
        let sinks: Vec<(String, InstanceSender)> = self
            .sinks
            .read()
            .expect("sinks")
            .iter()
            .map(|(uuid, tx)| (uuid.clone(), tx.clone()))
            .collect();
        for (uuid, tx) in &sinks {
            self.send_on(uuid, tx, event.clone()).await;
        }
    }

    /// Route one event onto its lane — loss-intolerant events onto the reserved lane that waits for
    /// room, resamplable values onto the coalescible one — fold whatever the queue counted back
    /// into the runtime, and say whether the event actually landed.
    async fn send_on(&self, device_uuid: &str, tx: &InstanceSender, event: InstanceEvent) -> bool {
        if is_loss_intolerant(&event) {
            tx.send_critical(event, &self.cancel_token()).await;
        } else if let InstanceEvent::Obs(obs) = event {
            tx.send_data(obs);
        }
        let counted = tx.take_counters();
        self.fold_queue_counters(device_uuid, counted);
        counted.dropped_data == 0 && counted.dropped_critical == 0
    }

    /// Fold one queue's accounting into the runtime's, naming the lane that lost something at most
    /// once per instance per [`DROP_WARN_INTERVAL`] — a lagging consumer loses events in bursts,
    /// and the counters, not the log, carry the volume.
    fn fold_queue_counters(&self, device_uuid: &str, counted: QueueCounters) {
        if counted.coalesced > 0 {
            self.coalesced_events
                .fetch_add(counted.coalesced, Ordering::Relaxed);
            tracing::debug!(
                agent = %self.cfg.id, device = %device_uuid, coalesced = counted.coalesced,
                "a lagging consumer's stale reading was superseded in place"
            );
        }
        if counted.dropped_data == 0 && counted.dropped_critical == 0 {
            return;
        }
        self.dropped_data
            .fetch_add(counted.dropped_data, Ordering::Relaxed);
        self.dropped_critical
            .fetch_add(counted.dropped_critical, Ordering::Relaxed);
        if !self.may_warn_about_drops(device_uuid) {
            return;
        }
        tracing::warn!(
            agent = %self.cfg.id, device = %device_uuid,
            data_lane = counted.dropped_data, critical_lane = counted.dropped_critical,
            "instance queue dropped events; its consumer is lagging"
        );
    }

    /// Whether this instance's drop warning is due again.
    fn may_warn_about_drops(&self, device_uuid: &str) -> bool {
        let now = Instant::now();
        let mut warned = self.drop_warned.lock().expect("drop warnings");
        if let Some(last) = warned.get(device_uuid) {
            if now.duration_since(*last) < DROP_WARN_INTERVAL {
                return false;
            }
        }
        warned.insert(device_uuid.to_string(), now);
        true
    }

    /// Record that the agent is unreachable: latch the reason, flip `connected`, and tell every
    /// attached instance — once per transition, so a down agent does not flood the queues. The
    /// latch is what makes the transition-only broadcast safe: an instance that attaches later
    /// reads the reason out of [`Self::attach`]'s seed, and a session's every drain re-consults
    /// `info().connected` rather than remembering an event it may never have seen.
    async fn mark_down(&self, error: &MtcError) {
        let reason = error.to_string();
        let was_connected = self.info().connected;
        self.last_down
            .lock()
            .expect("down reason")
            .clone_from(&reason);
        self.update_info(|info| {
            info.connected = false;
        });
        if was_connected {
            self.broadcast(&InstanceEvent::AgentDown(reason)).await;
        }
    }

    fn update_info(&self, f: impl FnOnce(&mut AgentInfo)) {
        let mut next = (**self.info.load()).clone();
        f(&mut next);
        self.info.store(Arc::new(next));
    }
}

/// What servicing the control channel during a wait concluded.
enum CtlFlow {
    /// The window elapsed; carry on.
    Elapsed,
    /// A `reconnect` control request was served; models are refreshed and floors reset, so the
    /// state machine should go back to Connecting.
    Reconnected,
    /// Shutdown was requested.
    Shutdown,
}

/// Milliseconds elapsed since `started`, saturating (a latency measure never wraps).
fn elapsed_ms(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// The dedupe key: data item ids are unique **per device**, so the floor is keyed by both.
fn dedupe_key(device_uuid: &str, data_item_id: &str) -> String {
    format!("{device_uuid}\u{1f}{data_item_id}")
}

/// A jitter source with no new dependency: a fresh `RandomState` hash is random per call.
fn rand01() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let n = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    (n % 1_000_000) as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn agent_cfg(url: &str) -> AgentConfig {
        config::parse_agents(&json!({ "agents": [{ "id": "line-a-agent", "url": url }] }))
            .unwrap()
            .remove(0)
    }

    /// A pinned clock: nothing under test reads the wall clock.
    fn clock() -> ClockFn {
        Arc::new(|| "2026-01-01T00:00:00Z".to_string())
    }

    fn runtime() -> Arc<AgentRuntime> {
        AgentRuntime::new(
            agent_cfg("http://agent:5000"),
            &AgentCredentials::default(),
            clock(),
        )
        .unwrap()
    }

    const CURRENT_2_7: &str = include_str!("../../tests/fixtures/current_2.7.xml");
    const HEARTBEAT_2_7: &str = include_str!("../../tests/fixtures/heartbeat_2.7.xml");
    const DEVICES_2_7: &str = include_str!("../../tests/fixtures/devices_2.7.xml");

    /// The documents the stand-in agent is currently serving. A test restages an agent restart by
    /// installing a new `/current` (and, when the machine was reconfigured too, a new `/probe`).
    #[derive(Clone)]
    struct AgentDocs {
        probe: Arc<Mutex<String>>,
        current: Arc<Mutex<String>>,
    }

    impl AgentDocs {
        fn set_probe(&self, doc: &str) {
            *self.probe.lock().expect("probe doc") = doc.to_string();
        }
        fn set_current(&self, doc: &str) {
            *self.current.lock().expect("current doc") = doc.to_string();
        }
    }

    /// A runtime backed by a minimal HTTP agent stand-in: `/probe` answers the devices fixture,
    /// every other path the `/current` one. Enough to drive the cycles that do real I/O
    /// (`snapshot_cycle`, `service_attach_snapshots`) rather than only the pure ingest path.
    async fn agent_backed_runtime() -> Arc<AgentRuntime> {
        agent_backed_runtime_with_docs().await.0
    }

    /// [`agent_backed_runtime`] plus the handle that restages what it serves.
    async fn agent_backed_runtime_with_docs() -> (Arc<AgentRuntime>, AgentDocs) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let docs = AgentDocs {
            probe: Arc::new(Mutex::new(DEVICES_2_7.to_string())),
            current: Arc::new(Mutex::new(CURRENT_2_7.to_string())),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = docs.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let served = served.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match sock.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => head.push(byte[0]),
                        }
                    }
                    let body = if String::from_utf8_lossy(&head).contains("/probe") {
                        served.probe.lock().expect("probe doc").clone()
                    } else {
                        served.current.lock().expect("current doc").clone()
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(response.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        let runtime = AgentRuntime::new(
            agent_cfg(&format!("http://{addr}")),
            &AgentCredentials::default(),
            clock(),
        )
        .unwrap();
        (runtime, docs)
    }

    /// The `/current` fixture as a restarted agent would serve it: a new incarnation, and sequence
    /// numbering that restarted with it.
    fn restarted_current(instance_id: u64, sequence: u64) -> String {
        CURRENT_2_7
            .replace(
                "instanceId=\"1749000000\"",
                &format!("instanceId=\"{instance_id}\""),
            )
            .replace(
                "nextSequence=\"42\"",
                &format!("nextSequence=\"{}\"", sequence + 1),
            )
            .replace("sequence=\"37\"", &format!("sequence=\"{sequence}\""))
    }

    /// One observation, shaped for the queue tests.
    fn obs(data_item_id: &str, sequence: u64, category: Category) -> Box<Observation> {
        Box::new(Observation {
            data_item_id: data_item_id.to_string(),
            sequence,
            timestamp: "2026-07-27T10:00:00Z".into(),
            name: None,
            value: ObsValue::Scalar(json!(sequence)),
            extras: smallvec::smallvec![],
            element: "Position".into(),
            sub_type: None,
            category,
        })
    }

    fn cancelled() -> CancellationToken {
        let token = CancellationToken::new();
        token.cancel();
        token
    }

    // =============================================================================================
    // Attach / detach / demultiplexing
    // =============================================================================================

    #[tokio::test]
    async fn attaching_gives_one_instance_its_own_queue_and_detaching_takes_it_away() {
        let rt = runtime();
        assert!(rt.attached().is_empty());
        let mut handle = rt.attach("OKUMA.123456");
        let _second = rt.attach("MAZAK.999");
        assert_eq!(
            rt.attached(),
            vec!["MAZAK.999".to_string(), "OKUMA.123456".to_string()]
        );

        // One agent, many devices: an event for one device reaches only that device's queue.
        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        let events = handle.rx.drain();
        let delivered: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                InstanceEvent::Obs(o) => Some(o.data_item_id.as_str()),
                _ => None,
            })
            .collect();
        assert!(!delivered.is_empty(), "the CNC's observations: {events:?}");
        assert!(
            !delivered.contains(&"m-avail"),
            "no other device's data: {delivered:?}"
        );

        rt.detach("OKUMA.123456");
        assert_eq!(rt.attached(), vec!["MAZAK.999".to_string()]);
    }

    #[tokio::test]
    async fn only_attached_devices_are_decoded_at_all() {
        let rt = runtime();
        let _h = rt.attach("MAZAK.999");
        let report = rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        assert_eq!(report.device_streams, 2, "the document had two");
        assert_eq!(
            report.observations, 1,
            "only the attached device's observation was decoded"
        );
        assert_eq!(report.published, 1);
        assert!(!report.deferred);
    }

    #[tokio::test]
    async fn a_repeated_snapshot_publishes_nothing_new() {
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        let first = rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        assert!(first.published > 0);
        assert_eq!(first.unknown_elements, 0, "the fixture is fully understood");

        // `/current` returns the same observations until something changes: publishing them again
        // would be a duplicate, not an update.
        let second = rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        assert_eq!(second.observations, first.observations);
        assert_eq!(second.published, 0);

        // The first cycle dispatched the observations AND the agent-up announcement; the second
        // dispatched nothing at all.
        let first_events = handle.rx.drain();
        assert_eq!(
            first_events
                .iter()
                .filter(|e| matches!(e, InstanceEvent::Obs(_)))
                .count(),
            first.published,
            "ordinary flow is one event per observation: {first_events:?}"
        );
        assert!(
            handle.rx.is_empty(),
            "nothing was dispatched the second time"
        );

        // A forced republish (a resume, a repoll) deliberately says the same thing again — and
        // says it as a RE-BASELINE, so the session re-arms its deadband for the whole batch.
        let third = rt.ingest_streams(CURRENT_2_7, true).await.unwrap();
        assert_eq!(third.published, first.published);
        let third_events = handle.rx.drain();
        assert!(
            matches!(third_events.as_slice(), [InstanceEvent::Snapshot(batch)]
                     if batch.len() == third.published),
            "{third_events:?}"
        );
    }

    // =============================================================================================
    // The connectivity authority (P1-1)
    // =============================================================================================

    #[tokio::test]
    async fn the_agent_up_transition_is_announced_once() {
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");

        // Attaching seeds the queue with the CURRENT truth, and the runtime has not delivered yet.
        let seed = handle.rx.drain();
        assert!(
            matches!(seed.as_slice(), [InstanceEvent::AgentDown(r)] if r == NOT_YET_REACHABLE),
            "{seed:?}"
        );

        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        let events = handle.rx.drain();
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, InstanceEvent::AgentUp(_)))
                .count(),
            1,
            "the first document proves the agent is up - once: {events:?}"
        );
        assert!(rt.info().connected);

        rt.ingest_streams(HEARTBEAT_2_7, false).await.unwrap();
        assert!(
            handle.rx.is_empty(),
            "an already-up agent is not announced again"
        );
    }

    #[tokio::test]
    async fn a_heartbeat_document_updates_the_window_without_publishing() {
        let rt = runtime();
        let _h = rt.attach("OKUMA.123456");
        let report = rt.ingest_streams(HEARTBEAT_2_7, false).await.unwrap();
        assert_eq!(report.observations, 0);
        assert_eq!(report.published, 0);
        let info = rt.info();
        assert_eq!(
            info.next_sequence,
            Some(42),
            "liveness moved the cursor, not the data"
        );
        assert_eq!(info.instance_id, Some(1_749_000_000));
        assert_eq!(info.mode, "poll");
    }

    #[tokio::test]
    async fn an_agent_restart_resequences_before_anything_is_published() {
        // P1-4: the name is the rule. A document from a new incarnation was decoded against the
        // model of the dead one, so LLD §5 ladder 3 — re-probe, recompile, THEN snapshot — must
        // complete before ANY of it reaches an instance. It used to be published on the spot and
        // re-probed afterwards, which is one update decoded by one model generation and routed by
        // the next.
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        handle.rx.drain();

        // Same observations, new incarnation, sequences restarted from 3.
        let restarted = restarted_current(1_749_999_999, 3);
        let report = rt.ingest_streams(&restarted, false).await.unwrap();
        assert_eq!(
            report.published, 0,
            "nothing may be published against a model generation that just went void"
        );
        assert!(report.deferred, "and the report says why");
        assert!(
            report.observations > 0,
            "the document was still decoded and counted"
        );
        assert!(
            handle.rx.drain().is_empty(),
            "no observation, and no re-baseline either, reached the instance"
        );
        assert!(
            rt.needs_resync(),
            "a restarted agent's model is re-probed before it is trusted"
        );

        // Nothing claimed a floor: the post-resync snapshot must be free to say all of it again.
        assert_eq!(
            rt.seq
                .lock()
                .expect("sequence state")
                .floor(&dedupe_key("OKUMA.123456", "Xabs")),
            None,
            "the restart cleared every floor, and the deferred document set none"
        );

        // The document still proved the NEW incarnation alive: liveness and the header facts are
        // exactly what a `/current` in the next cycle needs.
        assert_eq!(rt.info().instance_id, Some(1_749_999_999));
        assert!(rt.info().connected, "the restarted agent IS delivering");
    }

    #[tokio::test]
    async fn every_document_under_a_pending_resync_is_deferred_not_only_the_one_that_revealed_it() {
        // The gate is the pending resync, not the header transition: a stream that keeps delivering
        // from the new incarnation while the re-probe is still owed is still decoding against a
        // void model, so its parts wait for the recovery snapshot too.
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        handle.rx.drain();

        rt.ingest_streams(&restarted_current(1_749_999_999, 3), false)
            .await
            .unwrap();
        handle.rx.drain();
        assert!(rt.needs_resync());

        // A later document from the SAME new incarnation: the header says nothing changed.
        let report = rt
            .ingest_streams(&restarted_current(1_749_999_999, 9), false)
            .await
            .unwrap();
        assert_eq!(report.published, 0, "still no verified model to route by");
        assert!(report.deferred);
        assert!(handle.rx.drain().is_empty());
        assert_eq!(
            rt.seq
                .lock()
                .expect("sequence state")
                .floor(&dedupe_key("OKUMA.123456", "Xabs")),
            None,
            "and it claimed no floor either"
        );
    }

    #[tokio::test]
    async fn a_malformed_document_is_counted_and_marks_the_agent_down() {
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        handle.rx.drain();

        assert!(matches!(
            rt.ingest_streams("<MTConnectStreams>", false).await,
            Err(MtcError::Xml(_))
        ));
        let counters = rt.parse_counters();
        assert_eq!(counters.parse_errors, 1);
        assert_eq!(counters.documents_parsed, 1);
        assert!(!rt.info().connected);
        let events = handle.rx.drain();
        assert!(
            matches!(events.as_slice(), [InstanceEvent::AgentDown(_)]),
            "{events:?}"
        );
        // ...and the reason is LATCHED, so a session that attaches later can still read it.
        assert!(
            rt.last_down_reason().contains("xml error"),
            "{}",
            rt.last_down_reason()
        );
    }

    #[tokio::test]
    async fn the_down_reason_is_latched_and_a_late_attach_is_seeded_with_it() {
        let rt = runtime();
        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        assert!(rt.info().connected);

        // Two consecutive failures: only the TRANSITION is broadcast...
        let mut early = rt.attach("OKUMA.123456");
        early.rx.drain();
        assert!(rt
            .ingest_streams("<MTConnectStreams>", false)
            .await
            .is_err());

        // ...and a session attaching BETWEEN them - after the one broadcast it will never see -
        // still learns, from the seed.
        let mut late = rt.attach("MAZAK.999");

        assert!(rt
            .ingest_streams("<MTConnectStreams>", false)
            .await
            .is_err());

        let events = early.rx.drain();
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, InstanceEvent::AgentDown(_)))
                .count(),
            1,
            "a down agent must not flood the queues: {events:?}"
        );
        let seed = late.rx.drain();
        match seed.as_slice() {
            [InstanceEvent::AgentDown(reason)] => {
                assert_eq!(reason, &rt.last_down_reason());
                assert!(reason.contains("xml error"), "{reason}");
            }
            other => panic!("a late attach must be seeded with the current truth: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_connected_runtime_seeds_a_newborn_queue_with_agent_up() {
        let rt = runtime();
        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        let mut handle = rt.attach("OKUMA.123456");
        let seed = handle.rx.drain();
        assert!(matches!(seed.as_slice(), [InstanceEvent::AgentUp(info)] if info.connected));
    }

    // =============================================================================================
    // Liveness (the link half of passive quality)
    // =============================================================================================

    #[tokio::test]
    async fn liveness_starts_unknown_and_a_document_vouches_for_currency() {
        let rt = runtime();
        assert_eq!(
            rt.liveness_age(Instant::now()),
            None,
            "nothing has vouched yet"
        );

        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        let now = Instant::now();
        let age = rt.liveness_age(now).expect("the document vouched");
        assert!(age < Duration::from_secs(1), "{age:?}");
        // A moment later the same document is that much older.
        assert!(rt.liveness_age(now + Duration::from_secs(30)).unwrap() >= Duration::from_secs(30));
    }

    #[test]
    fn the_liveness_window_is_one_missed_heartbeat_or_two_missed_polls() {
        let cfg = config::parse_agents(&json!({ "agents": [{
            "id": "a", "url": "http://agent:5000", "heartbeatMs": 8_000, "pollIntervalMs": 500
        }] }))
        .unwrap()
        .remove(0);
        let rt = AgentRuntime::new(cfg, &AgentCredentials::default(), clock()).unwrap();

        // Polling: two missed polls.
        assert_eq!(rt.liveness_window(), Duration::from_millis(1_000));
        // Streaming: one missed heartbeat.
        rt.set_streaming(true);
        assert_eq!(rt.liveness_window(), Duration::from_millis(8_000));
        rt.set_streaming(false);
        assert_eq!(rt.liveness_window(), Duration::from_millis(1_000));
    }

    #[test]
    fn the_injected_clock_is_what_the_runtime_stamps_with() {
        let rt = AgentRuntime::new(
            agent_cfg("http://agent:5000"),
            &AgentCredentials::default(),
            Arc::new(|| "2031-02-03T04:05:06Z".to_string()),
        )
        .unwrap();
        assert_eq!(
            rt.now(),
            "2031-02-03T04:05:06Z",
            "no wall clock is read below the seam"
        );
    }

    // =============================================================================================
    // The two-lane instance queue
    // =============================================================================================

    #[test]
    fn the_classification_rule_splits_loss_intolerant_events_from_resamplable_values() {
        assert!(is_loss_intolerant(&InstanceEvent::AgentUp(Arc::new(
            AgentInfo::default()
        ))));
        assert!(is_loss_intolerant(&InstanceEvent::AgentDown("gone".into())));
        assert!(is_loss_intolerant(&InstanceEvent::DataLoss { skipped: 1 }));
        assert!(is_loss_intolerant(&InstanceEvent::ModelDrift {
            old: "a".into(),
            new: "b".into()
        }));
        assert!(is_loss_intolerant(&InstanceEvent::StreamDegraded {
            failures: 3
        }));
        assert!(is_loss_intolerant(&InstanceEvent::Snapshot(Vec::new())));
        // A condition transition is a state machine's input, not a resamplable value.
        assert!(is_loss_intolerant(&InstanceEvent::Obs(obs(
            "c",
            1,
            Category::Condition
        ))));
        assert!(!is_loss_intolerant(&InstanceEvent::Obs(obs(
            "x",
            1,
            Category::Sample
        ))));
        assert!(!is_loss_intolerant(&InstanceEvent::Obs(obs(
            "e",
            1,
            Category::Event
        ))));
    }

    #[tokio::test]
    async fn a_drain_yields_the_loss_intolerant_lane_first_then_the_data_lane_in_order() {
        let (tx, mut rx) = instance_queue();
        let cancel = CancellationToken::new();

        tx.send_data(obs("x", 1, Category::Sample));
        tx.send_critical(
            InstanceEvent::Obs(obs("c", 2, Category::Condition)),
            &cancel,
        )
        .await;
        tx.send_data(obs("x", 3, Category::Sample));
        tx.send_critical(InstanceEvent::DataLoss { skipped: 7 }, &cancel)
            .await;

        assert!(!rx.is_empty());
        let drained = rx.drain();
        let ordered: Vec<u64> = drained
            .iter()
            .map(|e| match e {
                InstanceEvent::Obs(o) => o.sequence,
                InstanceEvent::DataLoss { skipped } => *skipped,
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            ordered,
            vec![2, 7, 1, 3],
            "critical lane first, FIFO within each lane"
        );
        assert!(rx.is_empty(), "a drain empties both lanes");
        assert_eq!(
            tx.take_counters(),
            QueueCounters::default(),
            "nothing was lost"
        );
    }

    #[tokio::test]
    async fn a_detached_receiver_makes_every_send_a_counted_no_op() {
        let (tx, rx) = instance_queue();
        drop(rx);
        tx.send_data(obs("x", 1, Category::Sample));
        tx.send_critical(
            InstanceEvent::AgentDown("gone".into()),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(
            tx.take_counters(),
            QueueCounters {
                dropped_data: 1,
                dropped_critical: 1,
                coalesced: 0
            }
        );
        assert_eq!(
            tx.take_counters(),
            QueueCounters::default(),
            "draining resets them"
        );
    }

    #[tokio::test]
    async fn a_cancelled_loss_intolerant_send_is_dropped_and_counted_rather_than_waiting() {
        let (tx, rx) = instance_queue();
        tx.send_critical(InstanceEvent::DataLoss { skipped: 1 }, &cancelled())
            .await;
        assert!(rx.is_empty(), "shutdown is never delayed by a queue");
        assert_eq!(tx.take_counters().dropped_critical, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_send_already_blocked_on_room_gives_way_to_shutdown_at_once() {
        // The other half of the cancellation rule: the bounded wait must be PREEMPTED, not merely
        // bounded. A consumer that has stopped consuming can hold a critical send for five seconds;
        // shutdown cannot be asked to queue behind that (D-R2).
        let (tx, _rx) = instance_queue();
        fill_critical_lane(&tx).await;
        let cancel = CancellationToken::new();

        let sender = tx.clone();
        let token = cancel.clone();
        let waiting = tokio::spawn(async move {
            sender
                .send_critical(InstanceEvent::AgentDown("gone".into()), &token)
                .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "it is waiting for room");

        let started = tokio::time::Instant::now();
        cancel.cancel();
        waiting.await.unwrap();
        assert!(
            started.elapsed() < CRITICAL_SEND_BUDGET,
            "shutdown preempted the wait: {:?}",
            started.elapsed()
        );
        assert_eq!(tx.take_counters().dropped_critical, 1);
    }

    /// Fill the data lane to its depth with one entry per DISTINCT signal, so nothing in it can be
    /// coalesced onto and the next send has to choose.
    fn fill_data_lane(tx: &InstanceSender) {
        for sequence in 0..u64::try_from(INSTANCE_QUEUE_DEPTH).unwrap() {
            tx.send_data(obs(&format!("x{sequence}"), sequence, Category::Sample));
        }
        assert_eq!(
            tx.take_counters(),
            QueueCounters::default(),
            "a lane filled exactly to its depth lost nothing"
        );
    }

    /// Fill the loss-intolerant lane to its depth.
    async fn fill_critical_lane(tx: &InstanceSender) {
        let cancel = CancellationToken::new();
        for skipped in 0..u64::try_from(CRITICAL_QUEUE_DEPTH).unwrap() {
            tx.send_critical(InstanceEvent::DataLoss { skipped }, &cancel)
                .await;
        }
        assert_eq!(
            tx.take_counters(),
            QueueCounters::default(),
            "a lane filled exactly to its depth lost nothing"
        );
    }

    #[test]
    fn a_full_data_lane_supersedes_a_signals_stale_reading_rather_than_losing_the_new_one() {
        let (tx, mut rx) = instance_queue();
        fill_data_lane(&tx);

        // A newer reading of a signal that is ALREADY queued replaces it where it stands: the
        // consumer was going to act on the newer number anyway, so this is not a loss.
        tx.send_data(obs("x7", 9_001, Category::Sample));
        assert_eq!(
            tx.take_counters(),
            QueueCounters {
                coalesced: 1,
                ..QueueCounters::default()
            }
        );

        // A signal with nothing to supersede evicts the OLDEST entry instead — and THAT is a loss.
        tx.send_data(obs("newcomer", 9_002, Category::Sample));
        assert_eq!(
            tx.take_counters(),
            QueueCounters {
                dropped_data: 1,
                ..QueueCounters::default()
            }
        );

        let queued: Vec<(String, u64)> = rx
            .drain()
            .iter()
            .map(|e| match e {
                InstanceEvent::Obs(o) => (o.data_item_id.clone(), o.sequence),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            queued.len(),
            INSTANCE_QUEUE_DEPTH,
            "the lane kept its depth"
        );
        assert!(
            !queued.iter().any(|(id, _)| id == "x0"),
            "the OLDEST entry went, not the newest"
        );
        assert_eq!(
            queued[6],
            ("x7".to_string(), 9_001),
            "the coalesced signal kept its place in line, carrying its LATEST value"
        );
        assert_eq!(queued.last().unwrap().0, "newcomer");
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_loss_intolerant_lane_waits_for_room_and_is_delivered_when_the_consumer_drains()
    {
        let (tx, mut rx) = instance_queue();
        fill_critical_lane(&tx).await;

        // The lane is full: rather than discard a lifecycle event, the send WAITS.
        let sender = tx.clone();
        let waiting = tokio::spawn(async move {
            sender
                .send_critical(
                    InstanceEvent::AgentDown("gone".into()),
                    &CancellationToken::new(),
                )
                .await;
        });
        tokio::time::sleep(CRITICAL_SEND_BUDGET / 2).await;
        assert!(
            !waiting.is_finished(),
            "real consumer lag backpressures acquisition; it does not silently lose the event"
        );

        // A consumer that drains inside the budget gets it.
        assert_eq!(rx.drain().len(), CRITICAL_QUEUE_DEPTH);
        waiting.await.unwrap();
        assert_eq!(
            tx.take_counters(),
            QueueCounters::default(),
            "nothing was lost"
        );
        assert!(matches!(
            rx.drain().as_slice(),
            [InstanceEvent::AgentDown(_)]
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_loss_intolerant_send_past_its_budget_is_dropped_and_counted() {
        let (tx, mut rx) = instance_queue();
        fill_critical_lane(&tx).await;

        // Nobody drains. The wait is BOUNDED (D-R2): acquisition is never frozen by a consumer
        // that has stopped consuming, and what the bound costs is counted rather than hidden.
        let started = tokio::time::Instant::now();
        tx.send_critical(
            InstanceEvent::AgentDown("gone".into()),
            &CancellationToken::new(),
        )
        .await;
        assert!(
            started.elapsed() >= CRITICAL_SEND_BUDGET,
            "it waited its whole budget first: {:?}",
            started.elapsed()
        );
        assert_eq!(tx.take_counters().dropped_critical, 1);
        assert_eq!(
            rx.drain().len(),
            CRITICAL_QUEUE_DEPTH,
            "the lane kept what it had"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_loss_intolerant_send_gives_up_the_moment_its_consumer_goes_away() {
        let (tx, rx) = instance_queue();
        fill_critical_lane(&tx).await;

        let sender = tx.clone();
        let waiting = tokio::spawn(async move {
            sender
                .send_critical(
                    InstanceEvent::AgentDown("gone".into()),
                    &CancellationToken::new(),
                )
                .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());

        // The session closed. No drain will ever come, so waiting out the budget would be a lie.
        let started = tokio::time::Instant::now();
        drop(rx);
        waiting.await.unwrap();
        assert!(
            started.elapsed() < CRITICAL_SEND_BUDGET,
            "a detached queue is answered at once: {:?}",
            started.elapsed()
        );
        assert_eq!(tx.take_counters().dropped_critical, 1);
    }

    #[tokio::test]
    async fn a_lost_consumer_is_counted_never_a_stalled_acquisition() {
        let rt = runtime();
        let handle = rt.attach("OKUMA.123456");
        drop(handle); // the receiver is gone: every send is a counted no-op
        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        assert!(
            rt.dropped_events() > 0,
            "a lost consumer is counted, never a stalled poll"
        );
        let counted = rt.queue_counters();
        assert_eq!(
            rt.dropped_events(),
            counted.dropped_data + counted.dropped_critical,
            "`dropped_events` is both lanes; `queue_counters` is which"
        );
        assert!(
            counted.dropped_data > 0 && counted.dropped_critical > 0,
            "the fixture carries both values and a condition: {counted:?}"
        );
    }

    #[tokio::test]
    async fn coalescing_is_counted_and_reported_without_widening_a_metric_family() {
        // D-R6: `coalesced` is not a loss, so it is not `dropped_events` and it is NOT a new
        // `MtconnectStream` measure - it surfaces here and in a debug log.
        let rt = runtime();
        let handle = rt.attach("OKUMA.123456");
        let tx = rt
            .sinks
            .read()
            .expect("sinks")
            .get("OKUMA.123456")
            .cloned()
            .expect("the instance's sink");
        fill_data_lane(&tx);
        // Every one of these supersedes a queued reading of the same signal.
        for round in 0..3 {
            tx.send_data(obs("x7", 9_000 + round, Category::Sample));
        }
        rt.fold_queue_counters("OKUMA.123456", tx.take_counters());

        assert_eq!(
            rt.queue_counters(),
            QueueCounters {
                coalesced: 3,
                ..QueueCounters::default()
            }
        );
        assert_eq!(
            rt.dropped_events(),
            0,
            "superseding a stale value is no loss"
        );
        drop(handle);
    }

    #[test]
    fn one_lagging_instance_is_warned_about_once_a_window_not_once_an_event() {
        let rt = runtime();
        let lost = QueueCounters {
            dropped_data: 4,
            ..QueueCounters::default()
        };
        assert!(
            rt.may_warn_about_drops("OKUMA.123456"),
            "the first one talks"
        );
        assert!(
            !rt.may_warn_about_drops("OKUMA.123456"),
            "and the burst behind it does not"
        );
        // Another instance keeps its own window: one machine's lag never mutes another's.
        assert!(rt.may_warn_about_drops("MAZAK.999"));

        // Folding still counts every event, warned about or not.
        rt.fold_queue_counters("OKUMA.123456", lost);
        rt.fold_queue_counters("OKUMA.123456", lost);
        assert_eq!(rt.dropped_events(), 8);
    }

    // =============================================================================================
    // Delivery classes (F-N1): ordinary flow vs a re-baseline
    // =============================================================================================

    #[tokio::test]
    async fn ordinary_flow_is_dispatched_per_observation_onto_the_lane_its_class_earns() {
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        handle.rx.drain(); // the attach seed

        let report = rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        let events = handle.rx.drain();
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, InstanceEvent::Snapshot(_))),
            "an ordinary cycle is on-change flow, never a re-baseline: {events:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, InstanceEvent::Obs(_)))
                .count(),
            report.published
        );

        // The condition transitions rode the loss-intolerant lane, so they drained FIRST - which is
        // the condition-before-value order `map_batch` is written against.
        let categories: Vec<Category> = events
            .iter()
            .filter_map(|e| match e {
                InstanceEvent::Obs(o) => Some(o.category),
                _ => None,
            })
            .collect();
        let conditions = categories
            .iter()
            .filter(|c| **c == Category::Condition)
            .count();
        assert!(conditions > 0, "the fixture carries conditions");
        assert!(
            categories[..conditions]
                .iter()
                .all(|c| *c == Category::Condition),
            "conditions first: {categories:?}"
        );
    }

    #[tokio::test]
    async fn a_forced_republish_is_one_re_baseline_event_and_an_ordinary_cycle_is_not() {
        // The F-N1 rule at the runtime's own boundary: `Snapshot` means "rebuild your view", and
        // only a genuine re-baseline may say it. When ordinary flow said it every cycle, every
        // session re-armed its deadband every cycle and the deadband could never suppress anything.
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        handle.rx.drain();

        rt.ingest_streams(CURRENT_2_7, false).await.unwrap();
        assert!(
            handle
                .rx
                .drain()
                .iter()
                .all(|e| !matches!(e, InstanceEvent::Snapshot(_))),
            "ordinary flow does not re-baseline"
        );

        let report = rt.ingest_streams(CURRENT_2_7, true).await.unwrap();
        let events = handle.rx.drain();
        assert!(
            matches!(events.as_slice(), [InstanceEvent::Snapshot(batch)]
                     if batch.len() == report.published),
            "a resume/repoll republish is ONE re-baseline: {events:?}"
        );
    }

    // =============================================================================================
    // Generation safety (P1-4): re-probe → recompile → THEN snapshot
    // =============================================================================================

    #[tokio::test]
    async fn the_recovery_cycle_re_probes_before_it_publishes_the_restarted_agents_view() {
        // The whole ladder-3 order in one place. Cycle N meets a restarted agent and publishes
        // NOTHING; cycle N+1 re-probes first — surfacing the drift the restart brought with it —
        // and only then hands the instance a re-baseline built with the model it just verified.
        let (rt, docs) = agent_backed_runtime_with_docs().await;
        let mut handle = rt.attach("OKUMA.123456");
        handle.rx.drain();

        // The connect phase probes, then a cold cycle: ordinary flow against the model as probed.
        rt.ensure_model("OKUMA.123456").await.unwrap();
        let digest_before = rt.model("OKUMA.123456").unwrap().digest_hex();
        rt.snapshot_cycle(false).await.unwrap();
        assert!(handle
            .rx
            .drain()
            .iter()
            .any(|e| matches!(e, InstanceEvent::Obs(_))));

        // The agent restarts, and comes back describing a machine that was reconfigured while it
        // was down — the exact case a silent remap would corrupt.
        docs.set_current(&restarted_current(1_753_000_000, 3));
        docs.set_probe(&DEVICES_2_7.replace("name=\"OKUMA-CNC\"", "name=\"OKUMA-CNC-REFITTED\""));

        let report = rt.snapshot_cycle(false).await.unwrap();
        assert_eq!(report.published, 0, "cycle N publishes nothing");
        assert!(report.deferred);
        assert!(handle.rx.drain().is_empty(), "and dispatches nothing");
        assert!(rt.needs_resync(), "the re-probe is still owed");
        assert_eq!(
            rt.model("OKUMA.123456").unwrap().digest_hex(),
            digest_before,
            "the model is still the old one: the deferral is what protects the readings"
        );

        // Cycle N+1: re-probe, drift, then the fresh view.
        let report = rt.snapshot_cycle(false).await.unwrap();
        assert!(!report.deferred);
        assert!(report.published > 0, "everything republishes as fresh");
        assert!(!rt.needs_resync(), "the re-probe completed");
        assert_ne!(
            rt.model("OKUMA.123456").unwrap().digest_hex(),
            digest_before,
            "the model was re-verified before anything was routed by it"
        );

        let events = handle.rx.drain();
        let drift_at = events
            .iter()
            .position(|e| matches!(e, InstanceEvent::ModelDrift { .. }))
            .expect("a changed digest is drift, never a silent remap");
        let baseline_at = events
            .iter()
            .position(|e| {
                matches!(e, InstanceEvent::Snapshot(batch)
                                   if batch.iter().any(|o| o.sequence == 3))
            })
            .unwrap_or_else(|| {
                panic!("the post-restart view arrives as a re-baseline: {events:?}")
            });
        assert!(
            drift_at < baseline_at,
            "the recompile reaches the session BEFORE the readings it governs: {events:?}"
        );
    }

    #[tokio::test]
    async fn a_second_restart_during_recovery_defers_again_and_publishes_neither_document() {
        // The agent restart-loops. Each recovery cycle re-probes, meets a document from a NEWER
        // incarnation, and defers again — so no observation from any interim document is ever
        // dispatched, and the floors stay clear for whichever incarnation finally settles.
        let (rt, docs) = agent_backed_runtime_with_docs().await;
        let mut handle = rt.attach("OKUMA.123456");
        handle.rx.drain();
        rt.snapshot_cycle(false).await.unwrap();
        handle.rx.drain();

        docs.set_current(&restarted_current(1_753_000_000, 3));
        let first = rt.snapshot_cycle(false).await.unwrap();
        assert!(first.deferred && first.published == 0);

        // It restarted AGAIN before the recovery snapshot could be taken.
        docs.set_current(&restarted_current(1_755_000_000, 5));
        let second = rt.snapshot_cycle(false).await.unwrap();
        assert!(
            second.deferred && second.published == 0,
            "the recovery snapshot itself revealed a newer incarnation: {second:?}"
        );
        assert!(
            rt.needs_resync(),
            "so the next cycle re-enters resync-first"
        );
        assert!(
            handle.rx.drain().is_empty(),
            "neither interim document reached the instance"
        );

        // It settles: the next cycle re-probes and publishes the surviving incarnation's view.
        let third = rt.snapshot_cycle(false).await.unwrap();
        assert!(!third.deferred);
        assert!(third.published > 0);
        let events = handle.rx.drain();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, InstanceEvent::Snapshot(batch)
                                           if batch.iter().any(|o| o.sequence == 5))),
            "only the incarnation that survived is published: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, InstanceEvent::Obs(o) if o.sequence == 3))
                && !events
                    .iter()
                    .any(|e| matches!(e, InstanceEvent::Snapshot(batch)
                                                   if batch.iter().any(|o| o.sequence == 3))),
            "nothing from the incarnation that came and went: {events:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_re_probe_keeps_the_resync_pending_and_publishes_nothing() {
        // The re-probe is the gate, not a formality afterwards. When it cannot be taken, the cycle
        // fails there — it does not fall through to a `/current` it would have to decode against
        // the model of the incarnation that died.
        let (rt, docs) = agent_backed_runtime_with_docs().await;
        let mut handle = rt.attach("OKUMA.123456");
        rt.ensure_model("OKUMA.123456").await.unwrap();
        rt.snapshot_cycle(false).await.unwrap();
        handle.rx.drain();

        // The agent restarts, and its probe stops answering with anything usable.
        docs.set_current(&restarted_current(1_753_000_000, 3));
        rt.snapshot_cycle(false).await.unwrap();
        handle.rx.drain();
        assert!(rt.needs_resync());
        docs.set_probe("<not-a-devices-document/>");

        let err = rt
            .snapshot_cycle(false)
            .await
            .expect_err("the model cannot be re-verified, so the cycle cannot complete");
        assert!(
            matches!(err, MtcError::Xml(_) | MtcError::NoSuchDevice(_)),
            "{err:?}"
        );
        assert!(rt.needs_resync(), "the resync is still owed");
        assert!(
            !handle
                .rx
                .drain()
                .iter()
                .any(|e| matches!(e, InstanceEvent::Obs(_) | InstanceEvent::Snapshot(_))),
            "and nothing was published behind the failed re-probe"
        );

        // The probe recovers: the same cycle, taken again, completes the ladder.
        docs.set_probe(DEVICES_2_7);
        let report = rt.snapshot_cycle(false).await.unwrap();
        assert!(report.published > 0 && !report.deferred);
        assert!(!rt.needs_resync());
    }

    #[tokio::test]
    async fn the_ladder_three_recovery_cycle_is_marked_a_re_baseline_for_the_sessions() {
        // The recovery `/current` republishes off floors that `reset_for_new_instance` already
        // cleared, so it never needs `republish_all` - but the instances still have to be TOLD
        // their whole view is being rebuilt. `snapshot_cycle` reads the pending resync to say so.
        let rt = agent_backed_runtime().await;
        let mut handle = rt.attach("OKUMA.123456");
        handle.rx.drain();

        // A cold cycle is ordinary flow.
        rt.snapshot_cycle(false).await.unwrap();
        let events = handle.rx.drain();
        assert!(events.iter().any(|e| matches!(e, InstanceEvent::Obs(_))));
        assert!(!events
            .iter()
            .any(|e| matches!(e, InstanceEvent::Snapshot(_))));

        // The stream saw the agent restart: every floor went with the old incarnation and a
        // re-probe is pending. What follows is a re-baseline, and it says so.
        rt.seq
            .lock()
            .expect("sequence state")
            .reset_for_new_instance();
        rt.resync_needed.store(true, Ordering::Relaxed);
        let report = rt.snapshot_cycle(false).await.unwrap();
        assert!(
            report.published > 0,
            "everything republishes off the cleared floors"
        );
        let events = handle.rx.drain();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, InstanceEvent::Snapshot(_))),
            "the recovery cycle is a re-baseline: {events:?}"
        );
        assert!(!rt.needs_resync(), "and the re-probe completed");
    }

    // =============================================================================================
    // The attach snapshot's dedupe floors (F-N2)
    // =============================================================================================

    #[tokio::test]
    async fn a_delivered_attach_snapshot_records_the_floors_the_stream_must_not_repeat() {
        let rt = agent_backed_runtime().await;
        let mut handle = rt.attach("OKUMA.123456");
        handle.rx.drain();

        rt.service_attach_snapshots().await;
        let events = handle.rx.drain();
        assert!(
            matches!(events.as_slice(), [InstanceEvent::Snapshot(_)]),
            "a change-only stream owes a newly attached instance one full view: {events:?}"
        );
        assert_eq!(
            rt.seq
                .lock()
                .expect("sequence state")
                .floor(&dedupe_key("OKUMA.123456", "Xabs")),
            Some(37)
        );
        let report = rt.snapshot_cycle(false).await.unwrap();
        assert_eq!(report.published, 0, "the instance already has them");
    }

    #[tokio::test]
    async fn a_dropped_attach_snapshot_leaves_its_floors_unset_so_the_next_cycle_says_it_again() {
        // F-N2: the floors used to be recorded BEFORE the dispatch was known to have landed, so a
        // dropped attach snapshot took its observations with it - permanently, because the floors
        // then suppressed every repeat.
        let rt = agent_backed_runtime().await;
        let handle = rt.attach("OKUMA.123456");
        drop(handle.rx); // the sink is registered; its consumer is not there

        rt.service_attach_snapshots().await;
        assert!(
            rt.dropped_events() > 0,
            "the snapshot did not land, and that was counted"
        );
        assert_eq!(
            rt.seq
                .lock()
                .expect("sequence state")
                .floor(&dedupe_key("OKUMA.123456", "Xabs")),
            None,
            "nothing may claim the instance has observations it never received"
        );

        // The cost of the drop is one repeat, which is exactly what the floors exist to permit.
        let report = rt.snapshot_cycle(false).await.unwrap();
        assert!(report.published > 0);
    }

    #[test]
    fn the_send_budget_and_the_lane_depths_are_the_documented_ones() {
        assert_eq!(INSTANCE_QUEUE_DEPTH, 1024);
        assert_eq!(CRITICAL_QUEUE_DEPTH, 256);
        assert_eq!(CRITICAL_SEND_BUDGET, Duration::from_secs(5));
    }

    // =============================================================================================
    // The published status view
    // =============================================================================================

    #[test]
    fn the_published_status_view_is_non_secret_and_closed() {
        let rt = runtime();
        let v = rt.info().to_json();
        assert_eq!(v["capability"], "MTCONNECT_CLIENT");
        assert_eq!(v["agentId"], "line-a-agent");
        assert_eq!(v["endpoint"], "http://agent:5000/");
        assert_eq!(v["connected"], false);
        assert_eq!(v["mode"], "poll");
        assert_eq!(v["heartbeatMs"], 10_000);
        assert_eq!(
            v["limitations"],
            json!(["READ_ONLY", "XML_ONLY", "NO_ASSETS"])
        );
        assert!(v["instanceId"].is_null());
        assert!(
            !v.to_string().contains("password"),
            "nothing secret is published"
        );
    }

    #[test]
    fn a_runtime_renders_as_the_agent_it_is_without_leaking_its_internals() {
        let rendered = format!("{:?}", runtime());
        assert!(rendered.contains("line-a-agent"), "{rendered}");
        assert!(rendered.contains("connected: false"), "{rendered}");
    }

    #[test]
    fn the_dedupe_key_is_scoped_to_the_device() {
        // dataItemIds are unique per device, not per agent: two devices may both have `avail`.
        assert_ne!(dedupe_key("A", "avail"), dedupe_key("B", "avail"));
        assert_eq!(dedupe_key("A", "avail"), dedupe_key("A", "avail"));
    }

    // =============================================================================================
    // Structured lifecycle: cancellation reaches every await point (P1-7)
    // =============================================================================================

    /// An agent that accepts the connection and then **never answers**. Every request against it
    /// costs the full `requestTimeoutMs` — the state a dead-but-listening agent, a wedged proxy, or
    /// a paused container puts a client in.
    async fn silent_agent() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // The accepted sockets are held, not dropped: dropping them would answer with a reset.
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        format!("http://{addr}")
    }

    fn poll_only_runtime(url: &str, request_timeout_ms: u64) -> Arc<AgentRuntime> {
        let cfg = config::parse_agents(&json!({ "agents": [{
            "id": "line-a-agent", "url": url, "streaming": "poll-only",
            "pollIntervalMs": 10, "requestTimeoutMs": request_timeout_ms
        }] }))
        .unwrap()
        .remove(0);
        AgentRuntime::new(cfg, &AgentCredentials::default(), clock()).unwrap()
    }

    #[tokio::test]
    async fn a_blocked_acquisition_task_stops_on_its_token_not_on_the_message_behind_the_queue() {
        // THE P1-7 failure, exactly: the task is inside a request a silent agent will never answer,
        // and the control channel it would read a `Shutdown` from is full of work it would have to
        // do FIRST — each item another request to the same silent agent. A shutdown that depended
        // on that message would wait out `requestTimeoutMs` times the queue depth; the task would
        // still be mid-flight when the runtime tore it down.
        let rt = poll_only_runtime(&silent_agent().await, 30_000);
        let task = rt.spawn(CancellationToken::new()).expect("the task starts");

        // Let it get inside the request that will never be answered.
        tokio::time::sleep(Duration::from_millis(100)).await;

        for _ in 0..64 {
            let (reply, _rx) = oneshot::channel();
            let _ = rt.ctl_tx.try_send(AgentCtl::Snapshot {
                device_uuid: "OKUMA.123456".to_string(),
                data_item_ids: Vec::new(),
                reply,
            });
        }
        assert!(
            rt.ctl_tx.try_send(AgentCtl::Shutdown).is_err(),
            "the control channel is saturated: a shutdown MESSAGE has nowhere to go"
        );

        rt.shutdown().await;
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the token stopped the task; it did not wait out the request timeout")
            .expect("and it returned rather than panicking");
        assert!(
            !rt.task_started.load(Ordering::Relaxed),
            "the runtime knows its task is gone"
        );
    }

    #[tokio::test]
    async fn an_idle_polling_task_stops_promptly_and_shutdown_is_idempotent() {
        let rt = poll_only_runtime("http://127.0.0.1:9", 200);
        let task = rt.spawn(CancellationToken::new()).expect("the task starts");
        tokio::time::sleep(Duration::from_millis(50)).await;

        rt.shutdown().await;
        // A second stop is a no-op, and a runtime whose task already returned still accepts one.
        rt.shutdown().await;
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the polling loop returned")
            .expect("cleanly");
        rt.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_backoff_wait_gives_way_to_shutdown_instead_of_running_to_its_deadline() {
        let rt = runtime();
        let (_tx, mut ctl) = mpsc::channel::<AgentCtl>(4);
        let stopper = Arc::clone(&rt);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            stopper.shutdown().await;
        });
        let started = tokio::time::Instant::now();
        let flow = rt
            .wait_serving_ctl(&mut ctl, Duration::from_secs(600), false)
            .await;
        assert!(matches!(flow, CtlFlow::Shutdown));
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(250),
            "a ten-minute backoff window ends the moment the token is cancelled"
        );
    }

    #[tokio::test]
    async fn the_streaming_state_machine_unwinds_from_wherever_the_cancellation_finds_it() {
        // The stand-in answers `/probe` and `/current` but nothing it says is a multipart stream,
        // so the machine loops: connect → snapshot → open → establish failure → backoff. Whichever
        // of those the cancellation lands in, the loop returns.
        let (rt, _docs) = agent_backed_runtime_with_docs().await;
        let stopper = Arc::clone(&rt);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            stopper.shutdown().await;
        });
        let (_tx, mut ctl) = mpsc::channel::<AgentCtl>(4);
        tokio::time::timeout(Duration::from_secs(5), rt.run_streaming(&mut ctl))
            .await
            .expect("the streaming state machine returned on cancellation");
    }

    #[tokio::test]
    async fn a_cancelled_runtime_starts_no_further_acquisition_round() {
        let rt = poll_only_runtime("http://127.0.0.1:9", 200);
        rt.shutdown().await; // cancelled before the machine ever ran
        let (_tx, mut ctl) = mpsc::channel::<AgentCtl>(4);
        tokio::time::timeout(Duration::from_secs(2), rt.run_streaming(&mut ctl))
            .await
            .expect("it refuses to probe for a component that is going away");
        assert!(
            rt.attached().is_empty() && !rt.info().connected,
            "nothing was probed or published"
        );
    }

    #[tokio::test]
    async fn an_unreachable_agent_falls_back_to_a_direct_read_when_no_task_is_running() {
        // No acquisition task has been spawned, so the ctl path degrades to a direct read - which
        // then fails honestly against an unroutable address rather than hanging.
        let rt = AgentRuntime::new(
            config::parse_agents(&json!({ "agents": [{
                "id": "a", "url": "http://127.0.0.1:9", "requestTimeoutMs": 200
            }] }))
            .unwrap()
            .remove(0),
            &AgentCredentials::default(),
            clock(),
        )
        .unwrap();
        let err = rt.request_snapshot("U", &[]).await.unwrap_err();
        assert!(
            matches!(err, MtcError::Transport(_) | MtcError::Timeout { .. }),
            "{err:?}"
        );
    }
}
