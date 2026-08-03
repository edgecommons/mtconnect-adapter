# REMEDIATION-SPEC — MtconnectAdapter adversarial-review remediation

Branch: `fix/adversarial-review-remediation`. Baseline: `e6928b7` (tests green, clippy green,
96.75% configured coverage, **`cargo fmt --check` failing**).

This document is **binding** for the implementer agents. Signatures given here are normative —
do not invent alternatives. Where a phase says "decision D-Rn", the register in §10 is the
rationale and must not be re-litigated inside an implementation session. Every phase lands as
one or more commits that leave the tree green: `cargo fmt --check`, `cargo test --all-targets`,
`cargo clippy --all-targets -- -D warnings` all pass at every phase boundary. The coverage gate
(`cargo llvm-cov --fail-under-lines 90` with the workflow's ignore regex) must hold from Phase 2
onward (Phase 1 may transiently dip only if Phase 2 lands in the same PR).

Authoritative design artifacts, re-read before implementing (do not work from this file's
summaries of them):

- `core/docs/adapters/mtconnect-adapter.md` (HLD; D-MTC-1..10 binding; §6 quality table is the
  Phase 6 contract).
- `core/docs/adapters/mtconnect-adapter-implementation.md` (LLD; §3 topology, §5 ladders, §9
  taxonomy).
- `./DESIGN.md` (local register D-MtconnectAdapter-L1..L13), `./AGENTS.md` ("Non-negotiable
  invariants" — all preserved by this spec; the "one state model per agent" invariant is the
  spine of Phase 1).

Non-negotiables restated for the implementers:

- **No `edgecommons` import anywhere under `src/mtconnect/**`** (`tests/isolation.rs` enforces).
  Everything this spec adds under `mtconnect/` uses only std/tokio/tokio-util/smallvec/serde_json.
- **Per-device dedupe floor stays per data item** — nothing here collapses it.
- **`sb/write` stays registered and always refused**; both multipart content-types stay accepted;
  instance addressing stays the library's.
- **No core-library change.** Phases 5/6 are adapter-level mapping only (verified: `Quality::
  Uncertain` ships, `quality`/`quality_raw`/`extra` are free-form on the wire). If an implementer
  believes a core change is needed, STOP and report — do not design one.

---

## 0. Phase order, file ownership, and how churn is avoided

Implementers work **sequentially** in this worktree: 1 → 2 → 3 → 4 → 5 → 6 → 7.

| Phase | Files it may touch |
|---|---|
| 1 Connectivity authority | `mtconnect/mod.rs`, `mtconnect/stream.rs`, `mtconnect/stats.rs`, `device.rs`, `supervisor.rs` (call sites only), tests |
| 2 Delivery classes | `mtconnect/mod.rs` (queue), `device.rs` (drain), tests |
| 3 Generation safety | `mtconnect/mod.rs` (ingest ordering), `shaping.rs`, `device.rs` (policies), `supervisor.rs` (call sites), tests |
| 4 Structured lifecycle | `supervisor.rs`, `mtconnect/mod.rs` (cancel arms), `main.rs` if needed, `Cargo.toml` (tokio-util), tests |
| 5 MTConnect semantics | `mtconnect/observations.rs`, `mtconnect/xml.rs`, `mtconnect/mod.rs` (`/current` dispatch), `mtconnect/error.rs` (counter field), `device.rs` (condition ledger), `commands.rs` (verify only), tests |
| 6 Passive quality | new `src/staleness.rs`, `device.rs` (seam method), `supervisor.rs`/`driver.rs` (drive it), `metrics.rs` (none required), tests |
| 7 Gates and docs | `supervisor.rs`→`driver.rs` split, `Cargo.toml` (edition), `.github/workflows/ci.yml`, `tests/agent_integration.rs`, `tests/live_sim.rs`, all docs |

**The shared interfaces in §1 are final from Phase 1 onward.** Phase 1 lands every §1 signature
(plumbing parameters it does not yet use, with semantics documented as "wired in phase N"), so
later phases change behavior *inside* those types, never the signatures. This is what keeps
`mod.rs` (touched by 1,2,3,4,5) and `device.rs` (1,2,5,6) from churning.

---

## 1. Shared interfaces — defined once, landed in Phase 1

### 1.1 New dependency

```toml
# Cargo.toml [dependencies]
tokio-util = { version = "0.7", default-features = false }
```

`tokio_util::sync::CancellationToken` is used both above and below the seam. It is not an
`edgecommons` import; `tests/isolation.rs` stays satisfied.

### 1.2 The clock seam (isolation-safe wall-clock)

```rust
// src/mtconnect/mod.rs
/// An ISO-8601 UTC "now" supplier. The runtime stamps observation arrival with it (C-6) without
/// importing edgecommons; production passes the library's own clock from app/supervisor.
pub type ClockFn = std::sync::Arc<dyn Fn() -> String + Send + Sync>;
```

Constructor change (landed Phase 1, used for stamping from Phase 7):

```rust
impl AgentRuntime {
    pub fn new(cfg: AgentConfig, creds: &AgentCredentials, clock: ClockFn)
        -> Result<Arc<Self>, MtcError>;
}
```

Production call site (`supervisor.rs::App::new`):
`AgentRuntime::new(cfg, &creds, std::sync::Arc::new(edgecommons::facades::system_clock()))`.
Tests pass `Arc::new(|| "2026-01-01T00:00:00Z".to_string())` or a counter clock.

### 1.3 The two-lane instance queue (replaces `mpsc<InstanceEvent>`)

```rust
// src/mtconnect/mod.rs
/// Data-lane capacity (coalescible Sample/Event observations). Unchanged from LLD §3.
pub const INSTANCE_QUEUE_DEPTH: usize = 1024;
/// Loss-intolerant lane capacity (Condition observations, lifecycle events, snapshots).
pub const CRITICAL_QUEUE_DEPTH: usize = 256;
/// How long a loss-intolerant send may wait for room before it is dropped and counted (D-R2).
pub const CRITICAL_SEND_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

pub fn instance_queue() -> (InstanceSender, InstanceReceiver);

#[derive(Clone, Debug)]
pub struct InstanceSender { /* Arc<Mutex<QueueState>> + Arc<Notify> (room signal) */ }

#[derive(Debug)]
pub struct InstanceReceiver { /* shares QueueState; Drop marks the queue detached */ }

/// Counters the runtime folds into `dropped_events()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueCounters {
    pub dropped_data: u64,
    pub dropped_critical: u64,
    pub coalesced: u64,
}

impl InstanceSender {
    /// Data lane. Never blocks. On overflow: latest-value coalescing per `data_item_id`
    /// (replace-in-place, counted `coalesced`); if no same-id entry exists, the OLDEST data-lane
    /// entry is evicted (counted `dropped_data`) and the new one enqueued. Fail-fast no-op when
    /// the receiver is detached (counted `dropped_data`).
    pub fn send_data(&self, obs: Box<Observation>);

    /// Loss-intolerant lane. Enqueues immediately when there is room; when full, WAITS for room
    /// up to `CRITICAL_SEND_BUDGET`, preempted by `cancel`. Past the budget or on cancellation or
    /// a detached receiver: dropped and counted `dropped_critical` (never an error the caller
    /// must handle — the counter and a rate-limited warn are the surface).
    pub async fn send_critical(&self, event: InstanceEvent, cancel: &tokio_util::sync::CancellationToken);

    /// Drain-and-reset the counters (the runtime aggregates them).
    pub fn take_counters(&self) -> QueueCounters;
}

impl InstanceReceiver {
    /// Everything queued, loss-intolerant lane FIRST (FIFO within each lane), then the data lane
    /// (FIFO, coalesced entries in their original positions). Draining signals room to blocked
    /// critical senders. Non-blocking — the session polls on its own cadence, exactly as the old
    /// `try_recv` loop did.
    pub fn drain(&mut self) -> Vec<InstanceEvent>;
    /// Whether anything is queued (used by tests; cheap).
    pub fn is_empty(&self) -> bool;
}

/// The classification rule, unit-tested on its own:
/// loss-intolerant ⇔ `AgentUp | AgentDown | DataLoss | ModelDrift | StreamDegraded | Snapshot(_)`
/// or `Obs(o)` where `o.category == Category::Condition`. Everything else is data-lane.
pub fn is_loss_intolerant(event: &InstanceEvent) -> bool;
```

Phase 1 lands the type with **interim semantics** (both lanes fail-fast try-push + count — i.e.
today's behavior, in the new shape) so `AgentHandle`, `MtcSession`, and every test compile once.
Phase 2 completes coalescing and the bounded critical wait *inside* the type.

`AgentHandle` becomes:

```rust
pub struct AgentHandle {
    pub agent: Arc<AgentRuntime>,
    pub device_uuid: String,
    pub rx: InstanceReceiver,
}
```

### 1.4 `AgentRuntime` — final public surface deltas

```rust
impl AgentRuntime {
    // Phase 1 — connectivity authority
    /// Record unreachability: stores the latched reason, flips `info.connected=false`, and
    /// broadcasts `AgentDown` on the transition. Async because the broadcast is loss-intolerant.
    async fn mark_down(&self, error: &MtcError);
    /// The latched last-down reason ("not yet reachable" before the first contact).
    pub fn last_down_reason(&self) -> String;

    // Phase 1/6 — liveness for staleness
    /// Time since the agent last VOUCHED for data currency (a Streams document ingested — data
    /// or heartbeat — or a successful `/current` cycle). `None` before first contact.
    pub fn liveness_age(&self, now: std::time::Instant) -> Option<std::time::Duration>;
    /// "One missed heartbeat/poll": `heartbeat_ms` while a stream is established, else
    /// `2 × poll_interval_ms` (D-R12).
    pub fn liveness_window(&self) -> std::time::Duration;

    // Phase 1 — async ingest (dispatch is now async on the critical lane)
    pub async fn ingest_streams(&self, text: &str, republish_all: bool) -> Result<PollReport, MtcError>;
    // (`ingest_streams_doc`, `handle_part`, `dispatch`, `broadcast` become `async fn` internally.)

    // Phase 1 — stream loop accounting
    pub async fn drive_stream(
        &self,
        source: &mut impl ChunkSource,
        reader: &mut MultipartReader,
        ctl: &mut mpsc::Receiver<AgentCtl>,
    ) -> StreamRun;

    // Phase 4 — structured lifecycle
    /// Spawns the acquisition task; `cancel` preempts every await point. Phase 1 lands the
    /// parameter (plumbed into `run`), Phase 4 adds the select arms.
    pub fn spawn(self: &Arc<Self>, cancel: tokio_util::sync::CancellationToken)
        -> Option<tokio::task::JoinHandle<()>>;
    /// Cancels the task's token AND best-effort sends `AgentCtl::Shutdown` (belt and braces).
    pub async fn shutdown(&self);
}

/// What one established stream did before it ended.
#[derive(Debug)]
pub struct StreamRun {
    pub exit: StreamExit,
    /// Liveness-proving parts ingested (observations, heartbeats, agent-error docs). ZERO means
    /// the stream died before proving anything — the headers-then-EOF case — and counts as an
    /// establish failure (D-R4).
    pub liveness_parts: u64,
}
```

`PollReport` gains one field:

```rust
pub struct PollReport {
    pub device_streams: usize,
    pub observations: usize,
    pub published: usize,
    pub unknown_elements: u64,
    /// Phase 3: observations were decoded but NOT dispatched because the document revealed (or
    /// arrived under) a pending instanceId resync — they will be covered by the post-resync
    /// snapshot.
    pub deferred: bool,
}
```

### 1.5 Shaping — route-aware policy (Phase 3 semantics, type lands Phase 1)

```rust
// src/shaping.rs
/// Where a signal's readings publish. Part of the policy identity: a route change flushes the
/// signal's open window so one flushed update can never mix routing generations (P1-8).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SignalRoute {
    pub channel: Option<String>,
    pub component_path: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishPolicy {
    pub batch_ms: u32,
    pub latest_only: bool,
    pub deadband: Option<f64>,
    pub route: SignalRoute,
}
```

`PublishPolicy::is_trivial()` is unchanged (`batch_ms == 0 && deadband.is_none()`); route alone
never forces a table entry, because a trivial policy never buffers and therefore cannot mix
generations. `Shaper::set_policies` keeps its exact signature and its L11 semantics ("flush only
signals whose policy changed") — the fix is that the policy now *includes* the route, so a
route-only change is a policy change.

### 1.6 Device seam deltas (`src/device.rs`)

```rust
#[async_trait]
pub trait DeviceSession: Send + Sync {
    // ... existing methods unchanged ...

    /// Phase 6: the link facts passive-quality evaluation needs. `None` (default) = this backend
    /// has no mediated liveness (the simulator; its read IS its liveness) and the watchdog is
    /// inert for it.
    fn passive_input(&self) -> Option<crate::staleness::PassiveLink> { None }
}
```

`Reading` is unchanged. `MtcSession::shaping_policies()` fills `PublishPolicy::route` from the
served signal (`channel`) + model (`component_path_of`) + name; `policies_from_signals` fills it
from static `SignalConfig` (`component_path: None`).

### 1.7 `run_device` / `run_polling` signature (Phase 1 lands, Phase 4/6/7 use)

```rust
async fn run_device(
    cfg: DeviceConfig,
    backend: Arc<dyn DeviceBackend>,
    data: DataFacade,
    events: EventsFacade,
    dm: Arc<DeviceMetrics>,
    health: Arc<Health>,
    control: mpsc::Receiver<DeviceControl>,
    stale_signal_secs: u64,                          // Phase 6
    cancel: tokio_util::sync::CancellationToken,      // Phase 4
);
```

`run_polling` receives `backend: &Arc<dyn DeviceBackend>` instead of the frozen
`inventory: &[String]` (C-1), plus `stale_signal_secs` and `cancel`.

---

## 2. Phase 1 — One connectivity authority (P1-1, P1-2, the link half of P1-5)

**Principle (AGENTS.md invariant):** `AgentRuntime.info().connected` is the ONLY connectivity
truth for an MTConnect instance. It is written exclusively by the acquisition path (`mark_up` on
ingest, `mark_down` on failure), read by everything else, and mirrored — never re-derived — by
sessions and the supervisor.

### 2.1 Behavioral contract

1. **`connected` means delivering.** `info.connected = true` is set only when a Streams document
   is ingested (poll cycle, stream part, heartbeat part) — exactly the current `ingest_streams_doc`
   rule, now named `mark_up` and additionally refreshing `last_liveness`. A successful `/probe`
   alone never sets it (HLD §5.1: state 1 = reachable + probe verified + **delivering**).
2. **`MtcBackend::connect` refuses a down agent.** Before `ensure_model`:

   ```rust
   if !agent.info().connected {
       return Err(DeviceError::Transient(anyhow::anyhow!(
           "agent `{}` is not delivering ({})", device.agent_id, agent.last_down_reason())));
   }
   ```

   Consequences, accepted and documented (D-R1): at cold start an instance stays
   `CONNECTING`/`BACKOFF` until the shared acquisition task has its first successful cycle; the
   cached probe model is never proof of liveness; a `device-connected` event can no longer fire
   against a dead agent. `ensure_model`'s cache behavior itself is unchanged — it is now only
   reachable behind a live gate.
3. **`MtcSession::read_signals` consults the authority every drain.** After draining and mapping
   notices, and regardless of whether an `AgentDown` event was in the drain:

   ```rust
   if !self.agent.info().connected {
       return Err(DeviceError::Transient(anyhow::anyhow!(
           "agent unreachable: {}", self.agent.last_down_reason())));
   }
   ```

   This kills stickiness structurally: even a session that missed every event cannot stay ONLINE
   against a down runtime. (The drained `AgentDown` still produces its notice; the error return
   is what moves the supervisor.)
4. **`attach` seeds the newborn queue with the current truth** (fixes "reconnected session never
   learns"): after installing the sink, synchronously enqueue on the critical lane
   `AgentUp(info)` if `info.connected`, else `AgentDown(last_down_reason)`. The seeded event is a
   lane push into an empty queue — always room, no await needed (use the internal sync push).
5. **`mark_down` latches.** It stores the reason string (`Mutex<String>`, initial value
   `"not yet reachable"`), flips `connected=false`, and broadcasts `AgentDown` only on the
   transition (unchanged — flooding stays impossible; latch + rule 3 make the transition-only
   broadcast safe).
6. **Every ladder-1 stream exit marks down and backs off.** In `run_streaming`, after
   `drive_stream` returns `StreamRun { exit, liveness_parts }`:

   - `HeartbeatMissed` → `mark_down(&MtcError::Timeout { ms: 2×heartbeat })`
   - `TransportLost(e)` / `Malformed(e)` → `mark_down(&e)`
   - `EndOfStream` → `mark_down(&MtcError::Transport("agent closed the stream".into()))`

   then: if `liveness_parts == 0` the attempt counts as an establish failure
   (`establish_failures += 1`) and the task waits `backoff_delay(establish_failures - 1)` via
   `wait_serving_ctl(ctl, wait, degraded)` before `continue 'stream`; if `liveness_parts > 0`
   the stream was real — `establish_failures = 0` and ONE immediate re-establish is allowed
   (ladder 1's prompt resume from `nextSequence`). **The unconditional
   `establish_failures = 0` after a successful open (mod.rs:855) is deleted** — the reset now
   happens only on evidence of a working stream (`liveness_parts > 0`). The degradation check
   (`>= STREAM_ESTABLISH_FAILURE_LIMIT` → `degraded = true` + `StreamDegraded` broadcast +
   polling during waits) applies on this path exactly as on the failed-open path.
7. **Ladder-2/3 exits (`OutOfRange`, `InstanceChanged`) do NOT mark down** — deviation from the
   phase brief's literal "every stream exit", recorded as D-R3: those documents prove the agent
   is alive and answering; the recovery path re-enters `snapshot_cycle`/`refresh_model`
   immediately, and each of those already calls `mark_down` itself if the recovery I/O fails.
   Marking down on an in-protocol recovery would emit a false `AgentDown`/`AgentUp` pair per
   buffer-wrap on a healthy agent. `CtlReconnect`/`Shutdown` also do not mark down (deliberate
   actions).
8. **`drive_stream` counts liveness parts**: increment `liveness_parts` exactly where
   `outcome.is_liveness()` currently resets `undecodable` and touches the watch.
9. **Poll-only path** is already correct (`snapshot_cycle` marks down on fetch/parse failure) and
   inherits `mark_up`/`last_liveness` via `ingest_streams_doc`.
10. **`last_liveness`** (`AtomicU64` millis since a runtime-construction `Instant` epoch;
    `u64::MAX` = never): refreshed by `mark_up` (any ingested Streams document) and by a
    successful `/current` fetch in `snapshot()` (a served command read also vouches for
    currency; it does NOT flip `connected` — one writer family for `connected`, D-R5).

### 2.2 Existing tests that assert wrong behavior — correct in this phase

- `src/mtconnect/mod.rs::tests::the_agent_up_transition_is_announced_once` — attach now seeds
  one `AgentDown("not yet reachable")` before the first ingest; the assertion set must account
  for the seed (drain it first, then assert the single `AgentUp` on transition).
- `src/mtconnect/mod.rs::tests::a_malformed_document_is_counted_and_marks_the_agent_down` — the
  `matches!(events.as_slice(), [InstanceEvent::AgentDown(_)])` exact-shape assertion must skip
  the seed event.
- `tests/poll_acquisition.rs` / `tests/stream_acquisition.rs` — any case that treats a
  cached-model `connect` as ONLINE against a dead agent must be inverted (connect now fails
  Transient until the runtime delivers).

### 2.3 New tests required

1. **Sticky-ONLINE regression (the P1-1 scenario, end to end at the seam):** runtime ingests one
   good document (connected), then a poll failure marks it down; a session created *after* the
   down (fresh `attach`) must (a) receive the seeded `AgentDown`, (b) return `Err(Transient)`
   from `read_signals`, and `MtcBackend::connect` against the down runtime must fail Transient —
   no path yields an ONLINE-capable session.
2. **Latch:** two consecutive failures broadcast one `AgentDown`; a session attaching between
   them still learns (seed).
3. **Headers-then-EOF loop guard (virtual clock, `tokio::time::pause`):** a scripted client
   whose stream opens successfully then EOFs before any part: assert (a) each cycle waits a
   growing backoff (measure virtual elapsed), (b) after `STREAM_ESTABLISH_FAILURE_LIMIT` cycles
   `StreamDegraded` is broadcast once and `/current` polls run during the waits, (c) `mark_down`
   happened (info().connected false between attempts, when polls also fail). This is the
   mandated loop-guard test.
4. **Healthy-stream exit resumes promptly:** a stream that delivered parts then EOFs re-opens
   without backoff (one immediate retry) and does not increment the degradation counter.
5. **`connect` gate:** `MtcBackend::connect` with a cached model but `connected=false` fails
   Transient; with `connected=true` succeeds without a network probe (cache hit).
6. `liveness_age`/`liveness_window` unit tests (poll vs stream mode).

---

## 3. Phase 2 — Delivery classes (P1-6, and new finding F-N1)

### 3.1 Behavioral contract

1. **Queue semantics** exactly as §1.3: coalescing data lane, bounded-wait critical lane,
   drop-and-count only past `CRITICAL_SEND_BUDGET` or on cancellation/detach. This is the
   recorded deviation from LLD §3's literal unbounded `send().await` — see D-R2. The cancel
   token passed to `send_critical` is the acquisition task's own (Phase 4 wires real
   cancellation; until then the runtime holds a never-cancelled token — semantics identical).
2. **Per-observation dispatch replaces per-batch `Snapshot` for ordinary flow (F-N1 fix).**
   `ingest_streams_doc` dispatches each fresh observation as `InstanceEvent::Obs(Box<Observation>)`
   — Condition-category via `send_critical`, Sample/Event via `send_data`. The
   `InstanceEvent::Snapshot(Vec<Observation>)` variant is **reserved for true re-baselines**:
   `republish_all == true` cycles (resume, ladder-2 recovery, post-resync snapshot) and
   `service_attach_snapshots`. Consequence: `MtcSession::read_signals` sets `resynced = true`
   only on genuine re-baselines, so the shaper's deadband entry state is no longer wiped on
   every batch — the deadband becomes effective for the first time (F-N1). Snapshots ride the
   critical lane (losing a re-baseline breaks resync guarantees).
3. **Drain order** (critical lane first) is the documented condition-before-value ordering
   `map_batch` already relies on; within one drain a condition transition is applied before the
   data-lane values that accompanied it.
4. **Counters:** `AgentRuntime::dropped_events()` now reports
   `dropped_data + dropped_critical` (same public meaning: "events lost because a consumer
   lagged"), and the runtime logs a rate-limited (once per 30 s per instance) `warn!` naming the
   lane. `coalesced` is kept in `QueueCounters`, surfaced via a new
   `AgentRuntime::queue_counters()` accessor and a debug log — it is **not** added to the
   `MtconnectStream` metric family, whose measure set is closed by HLD §9 (D-R6).
5. **`service_attach_snapshots` floor-recording bug is healed structurally:** floors are recorded
   only AFTER `send_critical` returns without dropping. If the send was dropped (budget/detach),
   do not record the floors, so the observations republish on the next cycle instead of
   vanishing.

### 3.2 Existing test that asserts wrong behavior — correct in this phase

- `src/mtconnect/mod.rs:1349 a_full_instance_queue_drops_and_counts_rather_than_blocking_acquisition`
  — rewrite as three tests:
  (a) *detached receiver*: sends are fail-fast counted, never block (the old scenario, now with
  the honest name),
  (b) *data-lane overflow coalesces latest-value per data item* and evicts oldest otherwise, all
  counted,
  (c) *critical send waits for room and is delivered when the consumer drains within the budget*;
  past the budget it is dropped and counted (virtual clock).

### 3.3 New tests required

1. Classification table test for `is_loss_intolerant` (every variant + both `Obs` categories).
2. Drain-order test: condition obs enqueued after scalar obs is drained first.
3. F-N1 regression: two consecutive ordinary poll cycles do NOT set `take_resync()`; a
   `republish_all` snapshot does. Deadband integration: with a 0.5 deadband policy, a
   sub-deadband change arriving in the *next* poll cycle is suppressed (fails on baseline).
4. Backpressure/cancellation: a blocked `send_critical` returns promptly when the token cancels
   (drop counted), and shutdown is never delayed by a full queue.
5. Attach-snapshot floors: a dropped attach snapshot leaves floors unset (observations arrive on
   the following cycle).

---

## 4. Phase 3 — Generation safety (P1-4, P1-8)

### 4.1 Behavioral contract — model generation (P1-4)

The rule: **no observation is dispatched after the runtime learns the model generation it was
decoded against is void.** LLD §5 ladder 3: re-probe → recompile → THEN snapshot.

1. `ingest_streams_doc` (now async): after `observe_header`,

   ```text
   let instance_changed = matches!(outcome, HeaderOutcome::InstanceChanged { .. });
   if instance_changed { resync_needed.store(true); }
   let deferred = instance_changed || self.resync_needed.load();
   if deferred:
       - still mark_up + update AgentInfo header facts (the document proves the NEW incarnation
         is alive; sequence state already reset by observe_header)
       - still record stats/parse counters
       - dispatch NOTHING (no Obs, no Snapshot)
       - return PollReport { published: 0, deferred: true, .. }
   ```

2. `snapshot_cycle` re-orders to resync-first:

   ```text
   if resync_needed.load():
       for uuid in attached(): refresh_model(uuid)?        // error → early return; flag STAYS set
       resync_needed.store(false)
       // ModelDrift events fire from refresh_model as today; sessions recompile on drain
   fetch /current → parse (Phase 5's parse_current) → ingest_streams_doc(...)
   if report.deferred:   // the agent restarted AGAIN mid-recovery
       return Ok(report) // the next cycle re-enters resync-first; nothing was published
   ```

   The **post-ingest re-probe block (mod.rs:449-455) is deleted** — it is the premature-publish
   half of the defect. The `attach_pending.clear()` stays where it is but only on a
   non-deferred report.
3. Streaming: `drive_stream`'s existing `needs_resync() → StreamExit::InstanceChanged` check now
   fires on a document whose observations were **not** dispatched (rule 1), and `run_streaming`'s
   `continue 'connect` lands in the resync-first `snapshot_cycle`. The 'connect phase's
   `ensure_model` loop is unchanged (cold start); resync re-probing belongs to `snapshot_cycle`
   alone so poll-only and streaming share it.
4. Ladder-3 republish: `reset_for_new_instance` already cleared every floor, so the post-resync
   snapshot republishes everything without `republish_all`. Do not add a flag.

### 4.2 Behavioral contract — shaper routing generations (P1-8)

1. `PublishPolicy` gains `route: SignalRoute` (§1.5). `MtcSession::shaping_policies()` populates
   it: `channel` = served signal's effective channel (explicit or derived), `component_path` =
   `model.component_path_of(data_item_id)`, `name` = the served signal's effective name.
   `policies_from_signals` populates `channel`/`name` from `SignalConfig`, `component_path: None`.
2. `Shaper::set_policies` is textually unchanged — `self.policies.get(id) != policies.get(id)`
   now compares routes too, so a reload that changes only a signal's route flushes its open
   window with the OLD readings (which carry the old channel on each `Reading`, so the flushed
   update publishes on the old route — no mixing), and post-swap readings open a fresh window on
   the new route. L11's "unchanged signals keep their windows running" is preserved exactly.
3. `shaping_generation()` is unchanged (`probeDigest.signalGeneration` already moves on both
   drift and reload — verified; the budget is hashed into the generation per L12).

### 4.3 Existing tests that assert wrong behavior — correct in this phase

- `tests/stream_sequence.rs:227 an_instance_change_in_a_streams_part_exits_ladder_three` — the
  assertion block at lines 238-243 (a `Snapshot` carrying post-restart sequence 3 was dispatched
  before any re-probe) asserts the defect. Correct it to: exit is `InstanceChanged`,
  `needs_resync()` true, and **nothing new was dispatched to the instance** from the restart
  document. Add a follow-on assertion (or a second test) that after `refresh_model` +
  `snapshot_cycle` the post-restart observations arrive as a re-baseline `Snapshot`.
- `src/mtconnect/mod.rs:1316 an_agent_restart_resequences_before_anything_is_published` — the
  name states the right rule and the body asserts the wrong one (`report.published > 0`).
  Correct to `published == 0`, `deferred == true`, `needs_resync()`, nothing dispatched, and the
  sequence state reset (fresh low sequences will publish AFTER resync — assert via a subsequent
  `snapshot_cycle` with a scripted probe, or at minimum via floors being empty).

### 4.4 New tests required

1. Poll-mode restart: cycle N returns a restart document → `published == 0`; cycle N+1 (after
   `refresh_model` against a scripted probe) publishes everything as fresh and fires
   `ModelDrift` when the digest changed.
2. Double restart during recovery: a second `instanceId` change in the recovery snapshot defers
   again; no observation from either interim document is ever dispatched.
3. Shaper route flush: open a window under `{batch_ms: 500, route A}`; `set_policies` with the
   identical batching but route B → the window flushes NOW with the old readings; a new reading
   buffers under route B; `due()` publishes them separately. Also: unchanged policy+route keeps
   the window open (L11 preserved).
4. Mixed-generation regression at the supervisor level (drive `run_polling`/driver with a fake
   session whose `shaping_generation` flips mid-window) — may land with Phase 7's driver tests
   if the harness is not yet available here; if so, mark it explicitly in the phase-3 commit
   message as deferred to Phase 7.

---

## 5. Phase 4 — Structured lifecycle (P1-7)

### 5.1 Behavioral contract

1. **Every spawned task's handle is retained.** `App::run` keeps:
   `agent_tasks: Vec<JoinHandle<()>>`, `agent_metric_tasks: Vec<JoinHandle<()>>`,
   `device_tasks: Vec<JoinHandle<()>>`.
2. **Token tree:** one root `CancellationToken`; children `devices_cancel` and `agents_cancel`
   (`root.child_token()`); each device task gets `devices_cancel.child_token()`, each agent task
   `agents_cancel.child_token()` (passed to `AgentRuntime::spawn`). Metric tickers select on
   `agents_cancel`.
3. **Cancel-awareness:**
   - `run_polling`'s `select!` gains `() = cancel.cancelled() => { flush + close + return
     PollExit::Closed }` where "flush" = `publish_shaped(cfg, shaper.flush_all(), ...)` +
     `drain_shaping` (identical to today's channel-closed arm — factor the shared block).
   - `serve_while_down` gains the same arm → `DownOutcome::Closed`.
   - `AgentRuntime::run_poll_only`, `run_streaming` (both `select!`s in `wait_serving_ctl` and
     the connect loop), and `drive_stream` gain `() = cancel.cancelled()` arms returning /
     yielding `StreamExit::Shutdown`. `send_critical` uses the same token (a full queue can
     never stall shutdown — decision 2's cancellation-aware requirement).
4. **Shutdown sequence** (in `App::run` after `gg.shutdown_signal().await`), with constants:

   ```rust
   const DEVICE_SHUTDOWN_BUDGET: Duration = Duration::from_secs(8);
   const AGENT_SHUTDOWN_BUDGET: Duration = Duration::from_secs(4);
   ```

   1. `devices_cancel.cancel()` — device tasks flush shapers (buffered readings are data),
      publish, `session.close()` (detaches from the runtime), return.
   2. Join all device tasks under one shared `DEVICE_SHUTDOWN_BUDGET`
      (`tokio::time::timeout(budget, join_all)`); on timeout, `abort()` the stragglers and
      `warn!` each by instance id.
   3. For each agent: `agent.shutdown().await` (cancels its token + best-effort ctl message);
      join agent + metric tasks under `AGENT_SHUTDOWN_BUDGET`; abort + warn on timeout.
   4. `self.metrics.flush_metrics().await.ok();`

   Ordering rationale: devices first because their flush needs the messaging facade alive and
   their `close()` detaches cleanly from still-running runtimes; agents second; metrics flush
   last so the final counters include shutdown work. Total worst-case shutdown ≤ ~12 s, inside
   typical Greengrass/systemd stop windows.
5. **Sequencing helper is unit-testable:** the join-with-budget-then-abort step is factored as

   ```rust
   // src/app.rs (coverage denominator)
   pub async fn join_all_within(
       tasks: Vec<(String, tokio::task::JoinHandle<()>)>,
       budget: Duration,
   ) -> Vec<String> /* names of aborted stragglers */;
   ```

   tested with dummy tasks on the paused clock.
6. `AgentRuntime::shutdown()` remains callable without a token consumer (idempotent).

### 5.2 New tests required

1. `join_all_within`: all-finish-in-time → empty; one hung task → aborted + named, elapsed ==
   budget (virtual clock).
2. Acquisition-task cancellation: a runtime whose ctl channel is saturated (32 queued snapshots
   never serviced by a hung fake) still exits promptly on token cancel (this is the exact P1-7
   failure — a `Shutdown` message that would sit behind a blocked task).
3. `drive_stream` exits `Shutdown` on cancel while `Hang`ing (extend the stream_sequence script
   harness).
4. Device-task flush-on-cancel: with the Phase 7 driver harness (fake session + recording
   facades), cancel mid-window → the buffered readings were published before exit. If the
   harness is not yet extracted, land the test in Phase 7 and say so in the Phase 4 commit.

---

## 6. Phase 5 — MTConnect semantics (P1-3, C-2, C-3, C-4, C-5)

### 6.1 P1-3 — Condition state keyed by activation identity

**Types (`src/device.rs`, above the seam — protocol-only, no edgecommons):**

```rust
/// One active condition activation on a data item.
#[derive(Debug, Clone, PartialEq)]
pub struct Activation {
    pub state: CondState,                 // Warning | Fault (Normal/Unavailable never stored)
    pub native_code: Option<String>,
    pub condition_id: Option<String>,
}

/// The concurrent activations of ONE condition data item (MTConnect permits several).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConditionLedger {
    /// Keyed by activation identity: `conditionId`, else `nativeCode`, else `""` — the
    /// documented single-activation fallback for agents that send neither (D-R7).
    active: std::collections::BTreeMap<String, Activation>,
    /// The data item reported UNAVAILABLE and nothing has superseded it.
    unavailable: bool,
}

impl ConditionLedger {
    /// Fold one condition observation in. Rules:
    /// * key = conditionId ▷ nativeCode ▷ "".
    /// * Warning/Fault  → upsert `active[key]`; clears `unavailable`.
    /// * Normal with key == ""            → clear ALL activations (the standard's normal sweep).
    /// * Normal with a real key           → remove that activation only.
    /// * Unavailable                      → clear all activations, set `unavailable`.
    pub fn apply(&mut self, obs: &Observation);
    /// Worst state across active activations (CondState::severity: Fault > Unavailable >
    /// Warning > Normal), with the worst activation's native code. `(Normal, None)` when empty
    /// and not unavailable; `(Unavailable, None)` when `unavailable`.
    pub fn aggregate(&self) -> (CondState, Option<String>);
    pub fn active_count(&self) -> usize;
}
```

`MtcSession.conditions` becomes `HashMap<String /*dataItemId*/, ConditionLedger>`.

**`observations.rs`:** add `("conditionId", "conditionId")` to the condition-extras capture list
(the LLD's extras list "nativeCode, nativeSeverity, qualifier, conditionText" grows by one
additive key — wire-visible, D-R8).

**Mapping (`map_batch_at` two-pass, order-independent outcome):**

- Pass 1, in document order: for each condition observation, `ledger.apply(obs)` then snapshot
  `(aggregate, active_count)` **for that observation** (store beside it).
- Pass 2: map every observation.
  - Condition observation → `Reading` whose **value is the aggregate state's name** (not the
    observation's own transition), quality/`qualityRaw` from `condition_quality(aggregate,
    worst_native)`; extras = the observation's own extras (its transition: `nativeCode`,
    `conditionId`, `conditionText`, …) **plus** `activeConditions: <count>` (D-R8). The
    element-level transition is thereby preserved per sample while the signal's value/quality
    reflect the concurrent truth.
  - Non-condition observations consult `worst_bound_condition`, which now takes the ledgers and
    compares `aggregate()`s across the bound ids:

    ```rust
    pub fn worst_bound_condition(
        sig: &SignalConfig,
        conditions: &HashMap<String, ConditionLedger>,
    ) -> Option<(CondState, Option<String>)>;
    ```

- `reading_from_observation` signature changes accordingly (takes the per-observation
  precomputed aggregate for condition obs, ledgers for bindings — implementer may split it into
  `condition_reading(...)` + `value_reading(...)` if that reads better; the published shapes
  above are what is normative).
- Condition **events** (`MtconnectConditionEvent`): raised on a transition of the AGGREGATE into
  `Fault` (was not Fault, now Fault), rate-limit unchanged (1/min per dataItemId); context gains
  `conditionId` and `activeConditions`.

**Consequences fixed:** clearing one of two activations no longer promotes the signal to GOOD
(the other Fault still aggregates); mixed Fault+Warning no longer depends on document order.

### 6.2 C-2 — `MTConnectErrors` on `/current`

New private helper on `AgentRuntime` used by `snapshot_cycle` AND `snapshot`:

```rust
fn parse_current(&self, text: &str) -> Result<xml::StreamsDoc, MtcError> {
    let doc = xml::parse_document(text)?;                       // parse errors counted by caller
    match xml::document_kind(&doc.root.name) {
        xml::DocKind::Streams => xml::streams_from_doc(doc),
        xml::DocKind::Errors => {
            let errs = xml::errors_from_doc(doc)?;
            let first = errs.errors.first();
            Err(MtcError::AgentError {
                code: first.map_or_else(|| "UNKNOWN".into(), |e| e.code.clone()),
                message: first.map_or_else(String::new, |e| e.message.clone()),
            })
        }
        _ => Err(MtcError::Xml(format!("expected MTConnectStreams from /current, got `{}`",
                                       doc.root.name))),
    }
}
```

Policy: an Errors document counts as **parsed OK** (`record_ok`) — the parse succeeded, the agent
answered; it does NOT refresh `last_liveness` (it vouches for nothing) and does NOT mark down by
itself. Track `consecutive_current_errors: AtomicU32` (reset on any successful cycle); on the
**3rd** consecutive `AgentError` cycle, `mark_down(&err)` — a persistently erroring agent is not
delivering (D-R9). The streaming path (`classify_part`) is already correct and unchanged.

`sb/read` surfacing: `MtcError::AgentError` renders `"agent error CODE: msg"`, which
`commands.rs::agent_failure_code` already maps to `MTC_AGENT_ERROR:<CODE>` (verified at
commands.rs:1314) — implementer adds the test, no mapping change expected.

### 6.3 C-3 — Required observation fields

```rust
// src/mtconnect/observations.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeReject {
    MissingDataItemId,
    MissingSequence,     // absent, unparsable, or zero (MTConnect sequences start at 1)
    MissingTimestamp,    // absent or empty
}

pub fn decode(entry: &StreamEntry, meta: Option<&DataItemMeta>)
    -> Result<Observation, DecodeReject>;
```

Timestamp is validated for **presence only** — the string still rides verbatim (validating
RFC3339 shape risks refusing real agents; D-R10). Rejects are counted:
`ParseCounters` gains `pub rejected_observations: u64` + `record_rejected()`; every decode call
site (`ingest_streams_doc`, `snapshot`, tests' helpers) counts a reject and logs at `debug!`
(rate limiting unnecessary at debug). The `MtconnectParse` family gains a `rejectedObservations`
measure `(total, interval)` — an HLD §9 additive extension, updated in the HLD in Phase 7
(D-R11). A rejected observation is never published — it can no longer print as GOOD `sequence: 0`
and then be duplicate-suppressed.

### 6.4 C-4 — Prefixed namespace declarations and the version floor

In `element_from_start` / `parse_document`:

- Capture **every** `xmlns` and `xmlns:*` attribute value seen while `ns_uri` is undecided; the
  captured `ns_uri` becomes the **first declaration whose value starts with
  `"urn:mtconnect.org:"`** (default and prefixed declarations alike). A document declaring only
  foreign namespaces keeps `ns_version = None` as today.
- `parse_ns_version` additionally requires the `urn:mtconnect.org:` prefix (kills false
  positives from arbitrary colon-bearing URIs).
- The floor check is unchanged — it now fires for prefixed sub-1.3 documents.

### 6.5 C-5 — Node-count cap

```rust
// src/mtconnect/xml.rs
/// Maximum elements in one document. A 16 MiB document of REAL MTConnect content stays well
/// under this; millions of empty elements (25-50× heap amplification per byte) do not.
pub const MAX_NODES: usize = 250_000;
```

Counted in `push_element` (a running total across the document, not per level); breach →
`MtcError::Xml("element count exceeds 250000")`. Doc-comment the amplification rationale.

### 6.6 Existing tests that assert wrong behavior — correct in this phase

- `src/mtconnect/xml.rs:732 a_prefixed_document_parses_by_local_name` — the assertion
  `assert_eq!(doc.ns_version, None)` (line ~741) codifies the bypass. Correct to
  `Some(NsVersion { major: 2, minor: 7 })`.
- `src/mtconnect/observations.rs:505
  a_missing_sequence_or_timestamp_degrades_rather_than_dropping_the_value` — asserts the silent
  defaulting (sequence 0, empty timestamp). Replace with reject assertions per §6.3.
- Sweep: any fixture-driven test whose inline XML omits `sequence`/`timestamp` on observation
  elements must be fixed to carry them (e.g. the DATA_SET/TABLE inline fixtures already carry
  both — verify).

### 6.7 New tests required

1. **Concurrent conditions (the mandated cases):**
   - Fault(`c1`) then Warning(`c2`) on one dataItemId (distinct `conditionId`s): value FAULT,
     BAD, `activeConditions: 2`; then Normal(`c1`): value stays… **WARNING** (aggregate), quality
     UNCERTAIN — never GOOD while `c2` is active; then Normal(`c2`): NORMAL/GOOD.
   - Same sequence keyed by `nativeCode` only (no conditionId) — same outcomes (fallback).
   - Order-independence: `[Fault(c1), Warning(c2)]` and `[Warning(c2), Fault(c1)]` in one batch
     yield the same final aggregate.
   - Normal sweep: Normal with neither key clears both activations.
   - Unavailable clears all and aggregates UNAVAILABLE/BAD.
   - `conditionBinding` against a ledger: bound signal degrades by the aggregate, and clearing
     one of two activations does not un-degrade it.
   - A condition signal itself: clear-one-of-two keeps BAD (the P1-3 headline case).
2. **C-2:** `/current` returning an HTTP-200 `MTConnectErrors` document → `MtcError::AgentError`;
   `sb/read` per-entry `MTC_AGENT_ERROR:<code>` (extend the existing commands.rs fake-runtime
   test set); third consecutive error cycle marks down; a success resets the streak.
3. **C-4:** prefixed 2.7 doc → version captured; prefixed 1.1 doc → `UnsupportedVersion`;
   `xmlns:xsi` first + default MTConnect ns second → MTConnect version captured.
4. **C-5:** flat document with `MAX_NODES + 1` empty elements refused; a realistic large fixture
   parses.
5. **C-3:** each reject variant; rejected observations counted in `ParseCounters` and absent
   from dispatch.

---

## 7. Phase 6 — Passive quality (P1-5)

### 7.1 New module `src/staleness.rs` (pure, virtual-clock, in the coverage denominator)

```rust
/// qualityRaw prefix for a value held beyond the liveness window: "MTC_STALE:<ageMs>".
pub const QUALITY_STALE_PREFIX: &str = "MTC_STALE";
/// qualityRaw for readings synthetically degraded because the agent is unreachable.
pub const QUALITY_AGENT_UNREACHABLE: &str = "MTC_AGENT_UNREACHABLE";
/// The update-shape marker extra on every synthetic reading (D-R14).
pub const PASSIVE_EXTRA_KEY: &str = "passive";   // values: "stale" | "expired" | "unreachable" | "recovered"

/// The link facts the watchdog evaluates against (from `DeviceSession::passive_input`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PassiveLink {
    pub unreachable: bool,                       // !agent.info().connected
    pub liveness_age: Option<std::time::Duration>,  // AgentRuntime::liveness_age
    pub liveness_window: std::time::Duration,       // AgentRuntime::liveness_window
}

#[derive(Debug, Default)]
pub struct QualityWatchdog { /* HashMap<String, SignalRecord>, phase: PassivePhase */ }

impl QualityWatchdog {
    /// Record every reading that reached the wire (called from the publish path beside
    /// `dm.on_signal_update`). Clears any synthetic degradation for that signal.
    pub fn on_published(&mut self, reading: &crate::device::Reading, now: std::time::Instant);
    /// Evaluate transitions. Returns synthetic readings to publish NOW (transitions only —
    /// never a steady-state republish). `stale_after` = healthThresholds.staleSignalSecs.
    pub fn evaluate(
        &mut self,
        link: PassiveLink,
        stale_after: std::time::Duration,
        now: std::time::Instant,
    ) -> Vec<crate::device::Reading>;
    /// A re-baseline is coming (resync/resume snapshot): forget held records so the snapshot
    /// rebuilds them; emits nothing.
    pub fn on_rebaseline(&mut self);
}
```

### 7.2 Behavioral contract (realizes HLD §6 rows 2–3; gaps filled by D-R12/13/14)

State ladder per instance (not per signal — the trigger is the link; the per-signal records
carry what is held):

1. **FRESH → STALE** when `!unreachable && liveness_age > liveness_window` ("one missed
   heartbeat/poll"): for every recorded signal whose held quality is `Good` (and `Uncertain`
   readings hold their existing raw — never overwrite a condition-Warning with a stale marker;
   `Bad` holds), emit one synthetic reading: **held value**, `Quality::Uncertain`,
   `quality_raw = "MTC_STALE:<ageMs>"`, extras = held extras (including the held `sequence`,
   per D-MTC-6) + `passive: "stale"`; `capture_ts` = held capture stamp (the value's truth);
   `received_ts` left `None` (worker stamps the emission moment). `ageMs` = milliseconds since
   the agent last vouched (`liveness_age`), the same for every signal in the transition —
   D-R12 records why this clock and not per-signal change-age (MTConnect is on-change: an
   unchanged value under a live heartbeat is *current*, so per-signal age is not staleness).
2. **STALE → EXPIRED** when `liveness_age > stale_after`: synthetic reading per non-Bad-held
   signal: held value, `Quality::Bad`, `quality_raw = "MTC_STALE:<ageMs>"`, `passive: "expired"`.
   (If `stale_after < liveness_window`, EXPIRED wins directly — evaluate() handles the order.)
3. **→ UNREACHABLE** when `link.unreachable` (the runtime marked down): synthetic reading per
   non-Bad-held signal: held value, `Quality::Bad`, `quality_raw = "MTC_AGENT_UNREACHABLE"`,
   `passive: "unreachable"`. This is also emitted by the device task on `PollExit::LinkLost`
   (one last evaluate with `unreachable: true`) **before** returning, so the fleet sees BAD
   before the session dies.
4. **Recovery** (any degraded phase → liveness restored, i.e. `!unreachable &&
   liveness_age <= liveness_window`): synthetic reading per synthetically-degraded signal
   restoring the **held** quality/`quality_raw` (`passive: "recovered"`). Rationale: ladder-1
   re-establishment does not snapshot, so without this a held signal would stay UNCERTAIN until
   its value next changed. When recovery arrives via a re-baseline snapshot instead
   (`on_rebaseline` + fresh publishes), no synthetic recovery fires — the real data already did.
5. Synthetic readings **bypass the shaper** (published via `publish_readings`) — a quality
   transition never sits in a window, and the deadband is irrelevant to it (quality changes pass
   by rule anyway). They **do** feed `dm.on_signal_update`? **No** (D-R13): they must not reset
   the `staleSignals` metric's age — the metric counts genuine value silence and stays as-is.
   They also do not feed `watchdog.on_published` (obviously — they are its output; guard by the
   `passive` extra).
6. UNAVAILABLE-held signals are `Bad` already and never transition passively. Condition signals
   participate by their published quality like any other.

### 7.3 Wiring

- `MtcSession::passive_input()` (§1.6) returns
  `Some(PassiveLink { unreachable: !info.connected, liveness_age, liveness_window })`.
  `SimSession` keeps the default `None` → watchdog inert for the simulator.
- `run_polling` owns one `QualityWatchdog` per session (rebuilt with the session, like the
  shaper). On every tick (after `poll_once` and shaped publishing): if
  `session.passive_input()` is `Some(link)`, `let synthetic = watchdog.evaluate(link,
  stale_after, now)` → `stamp_received` → `publish_readings`. `on_published` is called from the
  same two call sites that call `dm.on_signal_update` (in `publish_shaped`/`publish_readings`,
  threading the watchdog — implementer may instead return published readings and feed the
  watchdog in the caller; the invariant is: **every reading that reached the wire, except
  synthetic ones, is recorded**).
- `session.take_resync()` true → `watchdog.on_rebaseline()` (alongside the existing
  `shaper.reset_deadband()`).
- `stale_after` = the existing `stale_signal_secs` config, threaded per §1.7.

### 7.4 New tests required

1. Watchdog unit table (virtual instants): fresh→stale exact threshold (age == window is not
   stale; age > window is), stale→expired at `stale_after`, direct fresh→expired when
   `stale_after < window`, unreachable from each phase, recovery restores held quality/raw
   verbatim, transitions emit exactly once (steady state emits nothing), Bad-held signals never
   emit, Uncertain-held (condition Warning) keeps its raw on stale entry, `on_rebaseline` +
   fresh publish emits no "recovered".
2. `MTC_STALE:<ageMs>` format test (numeric ms, no padding).
3. Seam test: `MtcSession::passive_input` reflects `info().connected` and mode-dependent window.
4. Driver-level test (Phase 7 harness): a fake session with `passive_input` scripted → synthetic
   UNCERTAIN then BAD readings appear on the wire with held value + `passive` extras, bypassing
   an open batch window.
5. Wire-gate addition (LLD §12 row "wire"): exact envelope assertions for a stale transition
   (quality `UNCERTAIN`, `qualityRaw` `MTC_STALE:…`, `passive` extra, held `sequence`).

---

## 8. Phase 7 — Gates, drivers, docs (C-1, C-6, fmt, coverage, edition, docs)

### 8.1 Mechanical gates

1. **`cargo fmt`** — run once, commit the diff first (isolated commit so review stays readable).
2. **Edition 2024** (D-R15): set `edition = "2024"` (rust-version 1.85 already satisfies it —
   the LLD names edition 2024; the crate's 2021 is the drift). Run `cargo fix --edition`,
   re-run fmt/clippy/tests. If migration surfaces a non-mechanical breakage the implementer
   cannot resolve in ≤1 hour, STOP and report rather than papering over.

### 8.2 C-1 — Unfreeze the resume inventory

`run_polling` receives `backend` (§1.7); the `DeviceControl::Resume` arm computes the inventory
**at resume time**:

```rust
let inventory: Vec<String> =
    backend.inventory(&cfg.connection).into_iter().map(|s| s.id).collect();
```

(`MtcBackend::inventory` already reads the live slot + cached model, so reload-added signals are
included.) The frozen `let inventory = …` before `run_polling` (supervisor.rs:349) is deleted.
Test: reload adds a signal while paused → resume snapshot includes it (extend the existing
poll_acquisition reload leg).

### 8.3 C-6 — `received_ts` stamped at arrival

- `Observation` gains `pub received: Option<String>` (default `None`; decode leaves it `None`).
- `AgentRuntime` stamps it: in `ingest_streams_doc` and `snapshot`, compute `let stamp =
  (self.clock)();` **once per document** and set `obs.received = Some(stamp.clone())` on every
  decoded observation. One document = one arrival moment — correct, and cheap.
- `MtcSession::map_batch` copies it: `reading.received_ts = obs.received.clone()`.
- The worker's `stamp_received` fallback fill is unchanged (sim path; any backend that does not
  stamp). Doc-comment on `Reading::received_ts` updated: for MTConnect it is the **agent-payload
  arrival** moment, not the drain moment.
- Test: enqueue observations, advance the (test) clock, drain later → `received_ts` carries the
  ingest-time stamp, and `build_sample` publishes it as the `receivedTs` extra distinct from
  `serverTs`.

### 8.4 Env-gated suites must fail loudly when infra is expected

- `tests/agent_integration.rs` / `tests/live_sim.rs`: **unset** env → self-skip with the one-line
  notice (unchanged). **Set** env → any failure to reach the peer (connect refused, timeout,
  wrong fixture shape) is a `panic!` naming the URL and the failure — never a skip, never a
  silent pass. Audit every early-`return`/`Ok(())` path in both files under the env-set branch
  and convert to assertions.

### 8.5 Cover supervisor orchestration — the `driver.rs` split

Split `src/supervisor.rs`:

- **`src/driver.rs` (new, IN the coverage denominator):** `run_device`, `run_polling`,
  `poll_once`, `publish_shaped`, `publish_readings`, `publish_with_component_path`,
  `emit_notices`, `serve_while_down`, `sync_served_signals`, `sleep_until_deadline`,
  `drain_shaping`, `severity_of`, `PollExit`/`DownOutcome` — everything driveable with a fake
  `DeviceBackend`/`DeviceSession` plus the recording messaging seam that
  `tests/publish_shaping.rs` / `tests/scoped_delivery.rs` already use.
- **`src/supervisor.rs` (stays excluded):** `App::{new,run}` only — construction, spawning,
  the shutdown sequence's *invocation* (its `join_all_within` logic is already unit-tested in
  app.rs per Phase 4).
- CI ignore regex narrows to `(supervisor\.rs|main\.rs|tests[/\\](live_.*|agent_integration)\.rs)`
  (it already reads exactly this — the change is that `supervisor.rs` shrinks to the genuinely
  live-only shell, and the moved code enters the denominator). Each exclusion keeps its pinned
  reason comment in the workflow.
- New driver tests (fake session + recording facades), minimum set: connect→publish→link-lost→
  alarm-raise cycle; pause clears windows / resume snapshots fresh inventory (C-1); reconnect
  flushes; cancel flushes (Phase 4's deferred test); passive-quality emission (Phase 6's
  deferred test); shaping-generation swap mid-window (Phase 3's deferred test).
- The 90% gate must pass with the enlarged denominator. Do not exclude anything new.

### 8.6 Documentation (same-change rule — no stale status left behind)

1. `./DESIGN.md`: new local decisions appended (mirror D-R1..D-R16 that are local policy), and
   the **Decisions / Validation / Metrics sections re-written where behavior changed** —
   connectivity authority, delivery lanes, resync-before-publish, shutdown order, condition
   ledger, passive quality, received-at-arrival. Replace stale text wholesale.
2. `./AGENTS.md`: "Non-negotiable invariants" gains: *"`AgentRuntime.info().connected` is the
   only connectivity truth — sessions and the supervisor mirror it, never re-derive it"* and
   *"Loss-intolerant events (condition observations, lifecycle, snapshots) ride the bounded
   critical lane; only the coalescible data lane may drop, and every drop is counted."*
   Validation section: driver.rs coverage note.
3. `docs/` (user-facing, present tense, no history): quality mapping page/README table gains the
   `MTC_STALE:<ageMs>`, `MTC_AGENT_UNREACHABLE`, `passive` extra, `conditionId` /
   `activeConditions` extras rows; `docs/reference/configuration.md` `staleSignalSecs` row states
   the BAD-expiry behavior; `docs/reference/metrics.md` gains `rejectedObservations`.
4. Core repo (docs only, no code): `core/docs/adapters/mtconnect-adapter-implementation.md` §3 —
   update the channel-topology paragraph to the two-lane bounded design and reference the
   decision register entry (D-R2) recording the deviation and its rationale;
   `mtconnect-adapter.md` §9 `MtconnectParse` row gains `rejectedObservations`; §6 table gains
   the two BAD-row tokens Phase 6 defined (`MTC_STALE` on expiry, `MTC_AGENT_UNREACHABLE`).
   These are the HLD/LLD catching up to decided design — flag them in the PR description.
5. Memory/status docs are the orchestrator's concern, not this repo's.

### 8.7 The adapter wire gate + consumer spot-check (decision 4)

- Extend the wire suite (local MQTT, exact envelope + extras assertions) with: the `passive`
  stale/expired shapes, `conditionId`/`activeConditions` extras, `componentPath` unchanged,
  `sequence` on synthetic readings.
- Consumer spot-check (manual, recorded in the PR): `edge-console` renders UNCERTAIN/BAD +
  `qualityRaw` strings without a schema change; `telemetry-processor` passes the new extras
  through untouched (they ride `extra`, verified free-form). NO cross-language interop matrix,
  NO Greengrass four-language matrix (settled decision 4).

---

## 9. Consolidated list — existing tests that assert wrong behavior

| # | Test | Wrong assertion | Corrected in |
|---|---|---|---|
| 1 | `tests/stream_sequence.rs:227 an_instance_change_in_a_streams_part_exits_ladder_three` | Asserts post-restart observations dispatched BEFORE re-probe | Phase 3 |
| 2 | `src/mtconnect/mod.rs:1349 a_full_instance_queue_drops_and_counts_rather_than_blocking_acquisition` | Asserts uniform drop for all event classes | Phase 2 |
| 3 | `src/mtconnect/xml.rs:732 a_prefixed_document_parses_by_local_name` | Asserts `ns_version == None` for a prefixed doc (floor bypass) | Phase 5 |
| 4 | `src/mtconnect/mod.rs:1316 an_agent_restart_resequences_before_anything_is_published` | Asserts `published > 0` from the restart document itself | Phase 3 |
| 5 | `src/mtconnect/observations.rs:505 a_missing_sequence_or_timestamp_degrades_rather_than_dropping_the_value` | Asserts silent `sequence: 0` / `""` defaulting | Phase 5 |
| 6 | `src/mtconnect/mod.rs::the_agent_up_transition_is_announced_once` and `::a_malformed_document_is_counted_and_marks_the_agent_down` | Not wrong per se — exact-event-shape assertions that must absorb the Phase 1 attach seed | Phase 1 |

Plus a sweep of `tests/poll_acquisition.rs` / `tests/stream_acquisition.rs` for
connect-means-online assumptions (Phase 1) and `Snapshot`-for-ordinary-delivery assumptions
(Phase 2).

---

## 10. Decision register

- **D-R1 (connectivity gate).** `MtcBackend::connect` and `MtcSession::read_signals` mirror
  `AgentRuntime.info().connected`; a cached model is never liveness. Cold-start cost: an
  instance reports CONNECTING/BACKOFF until the shared acquisition delivers — faithful to HLD
  §5.1's definition of state 1, and the price of never lying ONLINE.
- **D-R2 (bounded loss-intolerant lane — explicit, justified deviation from LLD §3).** LLD §3
  mandates unbounded `send().await` backpressure for Condition/lifecycle events. Settled with
  the user: the shared publish path (`publish_shaped` over the MQTT/GG-IPC facade) is the one
  true cross-agent coupling — a dead broker/nucleus would freeze ALL acquisition indefinitely
  under unbounded backpressure while the backpressured events could not be published anyway.
  Design: reserved critical lane (cap 256) + bounded wait (`CRITICAL_SEND_BUDGET` 5 s — real
  consumer lag backpressures properly) + drop-and-count past the bound + cancellation-aware so
  shutdown always preempts. LLD §3's text is updated to match (Phase 7). The coalescing half of
  LLD §3 is implemented as written.
- **D-R3 (mark-down scope).** Ladder-1 exits (`HeartbeatMissed`, `TransportLost`, `Malformed`,
  `EndOfStream`) mark down; ladder-2/3 exits do not — those documents prove the agent alive and
  their recovery I/O marks down itself on failure. Deviation from the phase brief's literal
  "every stream exit", justified above (false down/up flapping per buffer-wrap otherwise).
- **D-R4 (establish-failure accounting).** A stream counts as established only after its first
  liveness part. `liveness_parts == 0` at exit increments the degradation counter and waits the
  backoff; `> 0` resets it and permits one immediate ladder-1 resume. Kills the tight
  headers-then-EOF loop without penalizing healthy long streams.
- **D-R5 (one writer family for `connected`).** Only acquisition-path ingest/mark_down writes
  `connected`. A command-path `snapshot()` success refreshes `last_liveness` (it vouches for
  currency) but never flips `connected` — no second bookkeeping path.
- **D-R6 (closed metric families).** Queue drop/coalesce counts surface via the existing
  `dropped_events` counter, `queue_counters()`, and logs — not as new `MtconnectStream`
  measures (HLD §9's measure set stays closed; `MtconnectParse` is the one family extended, see
  D-R11).
- **D-R7 (activation identity fallback).** Activation key = `conditionId` ▷ `nativeCode` ▷ `""`
  (single-activation fallback for agents that send neither — pre-2.x behavior preserved
  exactly). A keyless `Normal` is the standard's normal-sweep and clears all activations.
- **D-R8 (condition wire shape).** A condition signal's published value/quality is the
  **aggregate** across concurrent activations; the per-sample extras carry the triggering
  transition (`conditionId`, `nativeCode`, `conditionText`, …) plus `activeConditions`. Two
  additive, wire-visible extras (`conditionId`, `activeConditions`) — free-form `extra` map, no
  core change.
- **D-R9 (`/current` Errors policy).** An HTTP-200 `MTConnectErrors` on `/current` is
  `MtcError::AgentError` (→ `MTC_AGENT_ERROR:<code>`), parsed-OK in the counters, refreshes no
  liveness, and marks down only on the 3rd consecutive erroring cycle — reachable-but-useless
  degrades through staleness and then connectivity, in that order.
- **D-R10 (required-field strictness).** `sequence` must parse as u64 ≥ 1 and `timestamp` must
  be non-empty; the timestamp's *format* is not validated (verbatim pass-through, tolerant of
  real agents' RFC3339 variants). Rejects are dropped + counted, never defaulted.
- **D-R11 (`MtconnectParse` extension).** The family gains `rejectedObservations` — the only
  family extension in this program; HLD §9 updated in the same change (docs-sync rule).
- **D-R12 (staleness clock).** MTConnect is on-change: an unchanged value under live liveness is
  *current*, so passive staleness is driven by the **liveness clock** (time since the agent last
  vouched — Streams doc or successful `/current`), not per-signal change age. `ageMs` in
  `MTC_STALE:<ageMs>` is that liveness age. The liveness window is `heartbeat_ms` (stream) /
  `2×poll_interval_ms` (poll); expiry is `staleSignalSecs` on the same clock. The per-signal
  instants (metrics.rs tracker) continue to drive the `staleSignals` *metric*, whose meaning
  (value silence) is intentionally different and unchanged.
- **D-R13 (two trackers, one feed).** `QualityWatchdog` holds held-reading records;
  `DeviceMetrics` keeps its `last_update` map. Both are fed from the same publish call sites;
  synthetic readings feed neither. Merging them would couple the metrics emitter to the publish
  pipeline's held state for no observable gain.
- **D-R14 (synthetic reading shape).** Passive transitions publish the **held value** with the
  held `sequence` extra (D-MTC-6: sequence on every sample; the synthetic sample describes the
  same observation) plus a `passive` extra naming the transition
  (`stale|expired|unreachable|recovered`), so consumers can distinguish synthetic quality motion
  from agent data. Recovery restores the held quality/raw verbatim.
- **D-R15 (edition).** The crate moves to edition 2024 — the LLD is the contract and
  `rust-version 1.85` already permits it; migration is `cargo fix --edition` + gate re-runs.
  Cost: one mechanical commit; risk: edition-2024 capture/`unsafe`-attr lints, all
  clippy-visible.
- **D-R16 (shaper route identity).** Routing (`channel`, `component_path`, `name`) is part of
  `PublishPolicy` identity; `set_policies` therefore flushes route-changed signals with their
  old readings on their old route. L11's flush-only-changed rule is preserved, not widened.

---

## 11. Call-outs — findings beyond the brief, and where this spec disagrees with it

1. **NEW DEFECT F-N1 — the deadband is currently inert for the MTConnect backend.** Every
   ordinary delivery is dispatched as `InstanceEvent::Snapshot` (mod.rs:537 — `Obs` is never
   constructed in production), `MtcSession::read_signals` sets `resynced = true` on every
   `Snapshot`, and `run_polling` calls `shaper.reset_deadband()` whenever `take_resync()` is
   true — i.e. on every batch. The L11 deadband ("against the last accepted value") is reset
   before it can ever suppress anything across cycles. Fixed structurally in Phase 2 (ordinary
   flow = `Obs`; `Snapshot` = re-baseline only) with a regression test. This was not in the
   verified defect list.
2. **F-N2 — attach-snapshot floors recorded before delivery is known** (mod.rs:1067-1073): if
   the attach snapshot's dispatch drops, the floors are already recorded and those observations
   are never republished. Healed in Phase 2 §3.1(5).
3. **Direction nuance on P1-8:** the buffered readings carry their own (old) `channel`, and
   `publish_shaped` publishes on `first.channel` — so the observed failure mode is old readings
   *plus post-reload readings appended to the same buffer* publishing together on the **old**
   route (or, if the buffer opened post-swap, old-window remnants on the new route). Either way
   one update mixes generations; the fix (D-R16) is as the brief prescribes. Noted only so the
   regression test asserts the real shape.
4. **Deviation from the phase-1 brief:** `mark_down` is NOT called on `OutOfRange` /
   `InstanceChanged` stream exits (D-R3). If you want the literal "every exit", say so and
   Phase 1 adds it — the cost is a false AgentDown/AgentUp event pair on every buffer-wrap
   recovery against a healthy agent.
5. **HLD §6 gaps this spec had to fill** (all recorded as decisions, all adapter-local):
   the BAD-row `qualityRaw` token for "agent unreachable" (`MTC_AGENT_UNREACHABLE`) and for
   staleness expiry (`MTC_STALE:<ageMs>`), the staleness clock semantics (D-R12), and the
   `passive` marker extra (D-R14). The HLD table names the rows but not these tokens.
6. **No core change needed anywhere** — re-verified during design: `Quality::Uncertain`,
   free-form `quality_raw`, and the `extra` maps carry everything Phases 5/6 emit.

## 12. Items needing a user decision BEFORE the affected phase starts

| # | Decision | Affects | Recommendation |
|---|---|---|---|
| 1 | Edition 2021 → 2024 (D-R15) | Phase 7 | Migrate (LLD is the contract; MSRV already 1.85) |
| 2 | D-R3 mark-down scope (ladder-2/3 exits excluded) | Phase 1 | Accept the deviation |
| 3 | Wire-visible additions: `MTC_AGENT_UNREACHABLE`, `MTC_STALE` on BAD, `passive`, `conditionId`, `activeConditions` extras | Phases 5–6 | Accept (additive, free-form extras; documented in docs + wire gate) |
| 4 | `MtconnectParse.rejectedObservations` measure (D-R11, extends an HLD §9 family) | Phase 5 | Accept + update HLD |
| 5 | `driver.rs` split to bring orchestration into coverage | Phase 7 | Accept (mechanical move; the alternative leaves "cover supervisor orchestration" unmet) |

---

## 13. Coordinator sign-off on §12

All five items in §12 are **ACCEPTED as recommended**. Implementers treat these as settled.

| # | Decision | Ruling | Rationale |
|---|---|---|---|
| 1 | Edition 2021 → 2024 (D-R15) | **Accepted** | The LLD is the binding contract and specifies edition 2024; `rust-version = "1.85"` already permits it. Aligning code to the design beats amending the design. If migration surfaces non-mechanical breakage, stop and report rather than widening the change. |
| 2 | D-R3 mark-down scope | **Accepted** | Ladder-2/3 exit documents prove the agent alive; marking down there would emit a false `AgentDown`/`AgentUp` pair on every buffer-wrap against a healthy agent. Strictly more correct than the literal brief. Surfaced to the user as a deviation from the stated plan. |
| 3 | Wire-visible additive tokens/extras | **Accepted** | Verified against the core contract: `quality` is a proto `string`, `quality_raw` a free-form `EcValue`, `extra` a `map<string, EcValue>` on Sample/Signal/SouthboundSignalUpdate. Additive only — no core change, no four-way parity work, no cross-language interop matrix. Must be covered by the adapter wire gate and the consumer spot-check (§8.7). |
| 4 | `MtconnectParse.rejectedObservations` (D-R11) | **Accepted** | Additive measure on an existing family. HLD §9 updated in the same change per the docs-sync rule. |
| 5 | `driver.rs` split (§8.5) | **Accepted** | Consistent with the discipline already documented in `AGENTS.md`: the untestable live seam stays thin and excluded, every pure decision it composes lives in the coverage denominator. Update `AGENTS.md` and the CI ignore regex in the same change. |

### Standing rules for every implementer

1. **The spec is binding.** Signatures in §1 are normative. Do not invent alternatives; if the
   spec is wrong or impossible, STOP and report rather than substituting your own design.
2. **Never reduce the design to make something pass.** A missing capability is either built or
   escalated — never silently dropped, narrowed, or stubbed.
3. **Correct the wrong tests, do not preserve them.** §9 lists five tests that assert defects.
   Deleting an inconvenient assertion without replacing its coverage is a regression.
4. **Green is not done.** `cargo test`/`clippy` passing says nothing about design fidelity.
   Verify against this spec and the HLD/LLD, and report the two claims separately.
5. **Leave the tree green.** Every phase ends with `cargo fmt`, `cargo clippy --all-targets --
   -D warnings`, and `cargo test --all-targets` passing before you report completion.
