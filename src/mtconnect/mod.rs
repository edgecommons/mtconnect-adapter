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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::{mpsc, oneshot, Notify};

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

/// How many events one instance may fall behind before the runtime starts counting drops.
pub const INSTANCE_QUEUE_DEPTH: usize = 1024;

/// The `interval=` a streaming request asks the agent for. The LLD (§14 Q2) floors the interval at
/// 250 ms; per-signal publish cadences below that are shaped client-side by the publish policy.
pub const STREAM_INTERVAL_MS: u32 = 250;

/// Consecutive stream-establish failures before acquisition degrades to `/current` polling
/// (LLD §5). The stream keeps being retried on the reconnect backoff while degraded.
pub const STREAM_ESTABLISH_FAILURE_LIMIT: u32 = 3;

/// Consecutive undecodable parts before the stream is dropped and re-established (LLD §9: a doc
/// that cannot be parsed is dropped and counted; three in a row means the stream itself is bad).
pub const MAX_CONSECUTIVE_UNDECODABLE: u32 = 3;

/// What the runtime tells one device instance.
#[derive(Debug, Clone)]
pub enum InstanceEvent {
    /// One new observation for this device. Boxed: a single observation is by far the largest
    /// thing this enum carries, and every other variant would pay for it in the queue.
    Obs(Box<Observation>),
    /// A whole `/current` snapshot, published together (a resume, a forced repoll).
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

/// One device instance's attachment to a shared agent runtime: it owns no socket, only a queue.
#[derive(Debug)]
pub struct AgentHandle {
    pub agent: Arc<AgentRuntime>,
    pub device_uuid: String,
    pub rx: mpsc::Receiver<InstanceEvent>,
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
}

/// One MTConnect agent, shared by every device instance configured against it.
#[derive(Debug)]
pub struct AgentRuntime {
    cfg: AgentConfig,
    client: MtcClient,
    models: RwLock<HashMap<String, Arc<ProbeModel>>>,
    sinks: RwLock<HashMap<String, mpsc::Sender<InstanceEvent>>>,
    info: ArcSwap<AgentInfo>,
    seq: Mutex<SequenceState>,
    parse: Mutex<ParseCounters>,
    /// The acquisition counters the `MtconnectStream`/`MtconnectProbe` families report (HLD §9).
    stats: AgentStats,
    ctl_tx: mpsc::Sender<AgentCtl>,
    ctl_rx: Mutex<Option<mpsc::Receiver<AgentCtl>>>,
    dropped_events: AtomicU64,
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

impl AgentRuntime {
    /// Build the runtime for one agent. Credentials are already resolved — this constructor cannot
    /// reach a vault, which is the point.
    ///
    /// # Errors
    /// [`MtcError::Tls`]/[`MtcError::Transport`] when the HTTP client cannot be built.
    pub fn new(cfg: AgentConfig, creds: &AgentCredentials) -> Result<Arc<Self>, MtcError> {
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
            models: RwLock::new(HashMap::new()),
            sinks: RwLock::new(HashMap::new()),
            info: ArcSwap::from_pointee(info),
            seq: Mutex::new(SequenceState::new()),
            parse: Mutex::new(ParseCounters::default()),
            stats: AgentStats::default(),
            ctl_tx,
            ctl_rx: Mutex::new(Some(ctl_rx)),
            dropped_events: AtomicU64::new(0),
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

    /// Events dropped because an instance's queue was full.
    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
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
    pub fn attach(self: &Arc<Self>, device_uuid: &str) -> AgentHandle {
        let (tx, rx) = mpsc::channel(INSTANCE_QUEUE_DEPTH);
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
            );
        }
        Ok((model, changed))
    }

    async fn probe_text(&self) -> Result<String, MtcError> {
        let started = std::time::Instant::now();
        let result = self.client.probe().await;
        self.stats.record_probe(elapsed_ms(started), result.is_ok());
        match result {
            Ok(text) => Ok(text),
            Err(e) => {
                self.mark_down(&e);
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
    /// # Errors
    /// Any client or parse error; the runtime marks itself down and tells every attached instance
    /// before returning.
    pub async fn snapshot_cycle(&self, republish_all: bool) -> Result<PollReport, MtcError> {
        let started = std::time::Instant::now();
        let fetched = self.client.current(None).await;
        self.stats
            .record_latency(elapsed_ms(started), fetched.is_ok());
        let text = match fetched {
            Ok(text) => text,
            Err(e) => {
                self.mark_down(&e);
                return Err(e);
            }
        };
        let report = self.ingest_streams(&text, republish_all)?;
        // Ladder 3 completes here, where a re-probe can actually be awaited: a restarted agent may
        // have come back with a different device model, and drift is surfaced, never remapped.
        if self.resync_needed.swap(false, Ordering::Relaxed) {
            for uuid in self.attached() {
                if let Err(e) = self.refresh_model(&uuid).await {
                    tracing::warn!(agent = %self.cfg.id, device = %uuid, error = %e, "re-probe failed");
                }
            }
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
    pub fn ingest_streams(&self, text: &str, republish_all: bool) -> Result<PollReport, MtcError> {
        let doc = match xml::parse_streams(text) {
            Ok(doc) => doc,
            Err(e) => {
                self.parse.lock().expect("parse counters").record_err();
                self.stats.record_document_failed();
                self.mark_down(&e);
                return Err(e);
            }
        };
        self.parse
            .lock()
            .expect("parse counters")
            .record_ok(doc.unknown_elements);
        Ok(self.ingest_streams_doc(&doc, republish_all))
    }

    /// Fold one already-parsed Streams document into the runtime: sequence header, dedupe,
    /// dispatch, published state. Infallible — parsing (and its failure policy) is the caller's.
    fn ingest_streams_doc(&self, doc: &xml::StreamsDoc, republish_all: bool) -> PollReport {
        // The header first: an agent restart voids every sequence number we hold, and it must do so
        // BEFORE anything from this document is measured against a floor.
        let outcome = {
            let mut seq = self.seq.lock().expect("sequence state");
            if republish_all {
                seq.reset_dedupe();
            }
            seq.observe_header(&doc.header)
        };
        if let HeaderOutcome::InstanceChanged { old, new } = outcome {
            // Ladder 3: the numbers are already void (the state reset itself). The MODEL is now
            // suspect too, so a re-probe is scheduled rather than assumed unnecessary.
            tracing::warn!(agent = %self.cfg.id, old, new, "agent restarted; resequencing");
            self.resync_needed.store(true, Ordering::Relaxed);
        }

        let mut report = PollReport {
            device_streams: doc.device_streams.len(),
            unknown_elements: doc.unknown_elements,
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
                let is_new = {
                    let mut seq = self.seq.lock().expect("sequence state");
                    seq.should_publish(&dedupe_key(&ds.uuid, &obs.data_item_id), obs.sequence)
                };
                if is_new {
                    fresh.push(obs);
                }
            }
            report.published += fresh.len();
            if !fresh.is_empty() {
                self.dispatch(&ds.uuid, InstanceEvent::Snapshot(fresh));
            }
        }

        self.stats.record_document(report.observations as u64);

        let was_down = !self.info().connected;
        let mode = if self.streaming_active.load(Ordering::Relaxed) {
            AcqState::Streaming { next: 0 }.mode()
        } else {
            AcqState::Polling.mode()
        };
        self.update_info(|info| {
            info.connected = true;
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
        });
        if was_down {
            let info = self.info();
            self.broadcast(&InstanceEvent::AgentUp(info));
        }
        report
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
        let started = std::time::Instant::now();
        let fetched = self.client.current(None).await;
        self.stats
            .record_latency(elapsed_ms(started), fetched.is_ok());
        let text = match fetched {
            Ok(text) => text,
            Err(e) => {
                self.mark_down(&e);
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

    /// Stop the acquisition task (idempotent; a runtime with no task is already stopped).
    pub async fn shutdown(&self) {
        let _ = self.ctl_tx.send(AgentCtl::Shutdown).await;
    }

    /// Start the acquisition task. Calling it twice is a no-op: the receiver is taken once.
    ///
    /// **Acquisition mode:** under [`StreamPolicy::Prefer`] the task runs the LLD §5 streaming
    /// state machine with polling as its degradation floor; under [`StreamPolicy::PollOnly`] it
    /// only ever polls `/current`.
    pub fn spawn(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let ctl = self.ctl_rx.lock().expect("ctl receiver").take()?;
        self.task_started.store(true, Ordering::Relaxed);
        let me = Arc::clone(self);
        Some(tokio::spawn(async move { me.run(ctl).await }))
    }

    async fn run(self: Arc<Self>, mut ctl: mpsc::Receiver<AgentCtl>) {
        match self.cfg.streaming {
            StreamPolicy::PollOnly => self.run_poll_only(&mut ctl).await,
            StreamPolicy::Prefer => self.run_streaming(&mut ctl).await,
        }
        self.task_started.store(false, Ordering::Relaxed);
    }

    /// The `poll-only` acquisition loop: `/current` on the configured cadence, forever.
    async fn run_poll_only(&self, ctl: &mut mpsc::Receiver<AgentCtl>) {
        let mut ticker =
            tokio::time::interval(Duration::from_millis(u64::from(self.cfg.poll_interval_ms)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
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
        // Consecutive failures to *establish* the stream (the degradation counter, LLD §5).
        let mut establish_failures: u32 = 0;
        // Whether acquisition has degraded to `/current` polling between stream attempts.
        let mut degraded = false;
        // Consecutive connect/probe failures (the plain reconnect backoff).
        let mut connect_failures: u32 = 0;
        // Ladder 2 wants the next snapshot to bypass the dedupe floors.
        let mut republish_next_snapshot = false;

        'connect: loop {
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
                let open_started = std::time::Instant::now();
                let opened = match tokio::time::timeout(
                    open_timeout,
                    self.client.open_sample_stream(&request),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(MtcError::Timeout {
                        ms: open_timeout.as_millis() as u64,
                    }),
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
                        if !degraded && establish_failures >= STREAM_ESTABLISH_FAILURE_LIMIT {
                            degraded = true;
                            tracing::warn!(
                                agent = %self.cfg.id,
                                "degrading to /current polling; streaming retried on backoff"
                            );
                            self.broadcast(&InstanceEvent::StreamDegraded {
                                failures: establish_failures,
                            });
                        }
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

                establish_failures = 0;
                if degraded {
                    degraded = false;
                    tracing::info!(agent = %self.cfg.id, "streaming re-established");
                }
                self.set_streaming(true);
                let exit = self.drive_stream(&mut response, &mut reader, ctl).await;
                self.set_streaming(false);

                match exit {
                    StreamExit::Shutdown => return,
                    StreamExit::CtlReconnect => continue 'connect,
                    // Ladder 1: drop and re-establish from the SAME nextSequence — the buffer
                    // still holds our position, so nothing is lost and nothing needs a snapshot.
                    StreamExit::HeartbeatMissed => {
                        tracing::warn!(
                            agent = %self.cfg.id, window_ms = u64::from(self.cfg.heartbeat_ms) * 2,
                            "heartbeat missed; re-establishing the stream"
                        );
                        continue 'stream;
                    }
                    StreamExit::TransportLost(e) | StreamExit::Malformed(e) => {
                        tracing::warn!(agent = %self.cfg.id, error = %e, "stream dropped; re-establishing");
                        continue 'stream;
                    }
                    StreamExit::EndOfStream => {
                        tracing::info!(agent = %self.cfg.id, "agent closed the stream; re-establishing");
                        continue 'stream;
                    }
                    // Ladder 2: the buffer provably ran past us. Say how much was lost, then
                    // snapshot-republish and resume from the snapshot's position.
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
                        self.broadcast(&InstanceEvent::DataLoss { skipped });
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
            }
        }
    }

    /// Read one established stream until it ends ([`StreamExit`]), servicing the control channel
    /// and the heartbeat window as it goes. Public so the virtual-clock sequence tests can drive
    /// every ladder with a scripted [`ChunkSource`] instead of a socket.
    pub async fn drive_stream(
        &self,
        source: &mut impl ChunkSource,
        reader: &mut MultipartReader,
        ctl: &mut mpsc::Receiver<AgentCtl>,
    ) -> StreamExit {
        let mut watch = HeartbeatWatch::new(
            self.cfg.heartbeat_ms,
            tokio::time::Instant::now().into_std(),
        );
        let mut undecodable: u32 = 0;
        loop {
            let now = tokio::time::Instant::now();
            tokio::select! {
                msg = ctl.recv() => match msg {
                    None | Some(AgentCtl::Shutdown) => return StreamExit::Shutdown,
                    Some(AgentCtl::Snapshot { device_uuid, data_item_ids, reply }) => {
                        let result = self.snapshot(&device_uuid, &data_item_ids).await;
                        let _ = reply.send(result);
                    }
                    Some(AgentCtl::Reconnect { reply }) => {
                        let _ = reply.send(self.reconnect().await);
                        return StreamExit::CtlReconnect;
                    }
                },
                () = self.attach_notify.notified() => {
                    self.service_attach_snapshots().await;
                }
                () = tokio::time::sleep(watch.remaining(now.into_std())) => {
                    if watch.is_expired(tokio::time::Instant::now().into_std()) {
                        return StreamExit::HeartbeatMissed;
                    }
                }
                chunk = source.next_chunk() => {
                    let bytes = match chunk {
                        Err(e) => return StreamExit::TransportLost(e),
                        Ok(None) => return StreamExit::EndOfStream,
                        Ok(Some(bytes)) => bytes,
                    };
                    if let Err(e) = reader.push(&bytes) {
                        return StreamExit::Malformed(e);
                    }
                    loop {
                        let part = match reader.next_part() {
                            Err(e) => return StreamExit::Malformed(e),
                            Ok(None) => break,
                            Ok(Some(part)) => part,
                        };
                        let outcome = self.handle_part(&part);
                        if outcome.is_liveness() {
                            undecodable = 0;
                            watch.touch(tokio::time::Instant::now().into_std());
                        }
                        // Ladder 3 supersedes everything a part could also say.
                        if self.needs_resync() {
                            return StreamExit::InstanceChanged;
                        }
                        match outcome {
                            PartOutcome::OutOfRange { first_sequence } => {
                                return StreamExit::OutOfRange { first_sequence };
                            }
                            PartOutcome::Undecodable => {
                                undecodable += 1;
                                if undecodable >= MAX_CONSECUTIVE_UNDECODABLE {
                                    return StreamExit::Malformed(MtcError::Xml(format!(
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
                        return StreamExit::EndOfStream;
                    }
                }
            }
        }
    }

    /// Classify and fold one multipart part into the runtime. A Streams document (observations or
    /// heartbeat) is ingested through the same dedupe/dispatch path as a poll; an Errors document
    /// is inspected for `OUT_OF_RANGE` and an agent restart; anything else is counted, never
    /// guessed at.
    fn handle_part(&self, part: &multipart::Part) -> PartOutcome {
        match classify_part(part) {
            PartDoc::Streams(doc) => {
                self.parse
                    .lock()
                    .expect("parse counters")
                    .record_ok(doc.unknown_elements);
                let report = self.ingest_streams_doc(&doc, false);
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
                    // Record the floors so the live stream does not immediately repeat these.
                    {
                        let mut seq = self.seq.lock().expect("sequence state");
                        for obs in &observations {
                            seq.should_publish(&dedupe_key(&uuid, &obs.data_item_id), obs.sequence);
                        }
                    }
                    self.dispatch(&uuid, InstanceEvent::Snapshot(observations));
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
    async fn wait_serving_ctl(
        &self,
        ctl: &mut mpsc::Receiver<AgentCtl>,
        wait: Duration,
        poll_while_waiting: bool,
    ) -> CtlFlow {
        let deadline = tokio::time::Instant::now() + wait;
        let mut ticker =
            tokio::time::interval(Duration::from_millis(u64::from(self.cfg.poll_interval_ms)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
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

    fn dispatch(&self, device_uuid: &str, event: InstanceEvent) {
        let sink = self.sinks.read().expect("sinks").get(device_uuid).cloned();
        if let Some(tx) = sink {
            if tx.try_send(event).is_err() {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn broadcast(&self, event: &InstanceEvent) {
        let sinks: Vec<mpsc::Sender<InstanceEvent>> = self
            .sinks
            .read()
            .expect("sinks")
            .values()
            .cloned()
            .collect();
        for tx in sinks {
            if tx.try_send(event.clone()).is_err() {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record that the agent is unreachable and tell every attached instance — once per transition,
    /// so a down agent does not flood the queues.
    fn mark_down(&self, error: &MtcError) {
        let was_connected = self.info().connected;
        self.update_info(|info| {
            info.connected = false;
        });
        if was_connected {
            self.broadcast(&InstanceEvent::AgentDown(error.to_string()));
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

    fn runtime() -> Arc<AgentRuntime> {
        AgentRuntime::new(agent_cfg("http://agent:5000"), &AgentCredentials::default()).unwrap()
    }

    const CURRENT_2_7: &str = include_str!("../../tests/fixtures/current_2.7.xml");
    const HEARTBEAT_2_7: &str = include_str!("../../tests/fixtures/heartbeat_2.7.xml");

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
        rt.ingest_streams(CURRENT_2_7, false).unwrap();
        let event = handle.rx.try_recv().expect("the CNC's observations");
        match event {
            InstanceEvent::Snapshot(obs) => {
                assert!(
                    obs.iter().all(|o| o.data_item_id != "m-avail"),
                    "no other device's data"
                );
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }

        rt.detach("OKUMA.123456");
        assert_eq!(rt.attached(), vec!["MAZAK.999".to_string()]);
    }

    #[tokio::test]
    async fn only_attached_devices_are_decoded_at_all() {
        let rt = runtime();
        let _h = rt.attach("MAZAK.999");
        let report = rt.ingest_streams(CURRENT_2_7, false).unwrap();
        assert_eq!(report.device_streams, 2, "the document had two");
        assert_eq!(
            report.observations, 1,
            "only the attached device's observation was decoded"
        );
        assert_eq!(report.published, 1);
    }

    #[tokio::test]
    async fn a_repeated_snapshot_publishes_nothing_new() {
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        let first = rt.ingest_streams(CURRENT_2_7, false).unwrap();
        assert!(first.published > 0);
        assert_eq!(first.unknown_elements, 0, "the fixture is fully understood");

        // `/current` returns the same observations until something changes: publishing them again
        // would be a duplicate, not an update.
        let second = rt.ingest_streams(CURRENT_2_7, false).unwrap();
        assert_eq!(second.observations, first.observations);
        assert_eq!(second.published, 0);

        // The first cycle dispatched the snapshot AND the agent-up announcement; the second
        // dispatched nothing at all.
        let first_events: Vec<InstanceEvent> =
            std::iter::from_fn(|| handle.rx.try_recv().ok()).collect();
        assert!(first_events
            .iter()
            .any(|e| matches!(e, InstanceEvent::Snapshot(_))));
        assert!(
            handle.rx.try_recv().is_err(),
            "nothing was dispatched the second time"
        );

        // A forced republish (a resume, a repoll) deliberately says the same thing again.
        let third = rt.ingest_streams(CURRENT_2_7, true).unwrap();
        assert_eq!(third.published, first.published);
    }

    #[tokio::test]
    async fn the_agent_up_transition_is_announced_once() {
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        rt.ingest_streams(CURRENT_2_7, false).unwrap();

        let mut events = Vec::new();
        while let Ok(e) = handle.rx.try_recv() {
            events.push(e);
        }
        assert!(
            events
                .iter()
                .any(|e| matches!(e, InstanceEvent::AgentUp(_))),
            "the first document proves the agent is up"
        );
        assert!(rt.info().connected);

        rt.ingest_streams(HEARTBEAT_2_7, false).unwrap();
        assert!(
            handle.rx.try_recv().is_err(),
            "an already-up agent is not announced again"
        );
    }

    #[tokio::test]
    async fn a_heartbeat_document_updates_the_window_without_publishing() {
        let rt = runtime();
        let _h = rt.attach("OKUMA.123456");
        let report = rt.ingest_streams(HEARTBEAT_2_7, false).unwrap();
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
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        rt.ingest_streams(CURRENT_2_7, false).unwrap();
        while handle.rx.try_recv().is_ok() {}

        // Same observations, new incarnation, sequences restarted from 1.
        let restarted = CURRENT_2_7
            .replace("instanceId=\"1749000000\"", "instanceId=\"1749999999\"")
            .replace("sequence=\"37\"", "sequence=\"3\"");
        let report = rt.ingest_streams(&restarted, false).unwrap();
        assert!(
            report.published > 0,
            "a restarted agent's low sequences are not stale"
        );
        assert!(
            rt.needs_resync(),
            "a restarted agent's model is re-probed before it is trusted"
        );
        assert_eq!(rt.info().instance_id, Some(1_749_999_999));
    }

    #[tokio::test]
    async fn a_malformed_document_is_counted_and_marks_the_agent_down() {
        let rt = runtime();
        let mut handle = rt.attach("OKUMA.123456");
        rt.ingest_streams(CURRENT_2_7, false).unwrap();
        while handle.rx.try_recv().is_ok() {}

        assert!(matches!(
            rt.ingest_streams("<MTConnectStreams>", false),
            Err(MtcError::Xml(_))
        ));
        let counters = rt.parse_counters();
        assert_eq!(counters.parse_errors, 1);
        assert_eq!(counters.documents_parsed, 1);
        assert!(!rt.info().connected);
        let events: Vec<InstanceEvent> = std::iter::from_fn(|| handle.rx.try_recv().ok()).collect();
        assert!(matches!(events.as_slice(), [InstanceEvent::AgentDown(_)]));
    }

    #[tokio::test]
    async fn a_full_instance_queue_drops_and_counts_rather_than_blocking_acquisition() {
        let rt = runtime();
        let handle = rt.attach("OKUMA.123456");
        drop(handle); // the receiver is gone: every send fails
        rt.ingest_streams(CURRENT_2_7, false).unwrap();
        assert!(
            rt.dropped_events() > 0,
            "a lost consumer is counted, never a stalled poll"
        );
    }

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
    fn the_dedupe_key_is_scoped_to_the_device() {
        // dataItemIds are unique per device, not per agent: two devices may both have `avail`.
        assert_ne!(dedupe_key("A", "avail"), dedupe_key("B", "avail"));
        assert_eq!(dedupe_key("A", "avail"), dedupe_key("A", "avail"));
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
        )
        .unwrap();
        let err = rt.request_snapshot("U", &[]).await.unwrap_err();
        assert!(
            matches!(err, MtcError::Transport(_) | MtcError::Timeout { .. }),
            "{err:?}"
        );
    }
}
