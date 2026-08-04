# DESIGN — MtconnectAdapter

> Treat this document as the **design-fidelity contract** for this component: before changing
> behavior, update the relevant section here in the same change, and review new work against what
> is written here — not against a summary of it.
>
> The full design lives in the core repo: `docs/adapters/mtconnect-adapter.md` (HLD, decision
> register D-MTC-1..10) and `docs/adapters/mtconnect-adapter-implementation.md` (LLD). This file
> records what is built here and what is not yet.

## What it is

`com.mbreissi.edgecommons.MtconnectAdapter` is a southbound **MTConnect client**. It connects to one
or more running MTConnect **Agents** over HTTP, reads each agent's device model (`/probe`), acquires
observations by streaming (a multipart `/sample?interval=…` stream, with `/current` polling as the
fallback), and publishes normalized EdgeCommons signals.

It is **not** an MTConnect Agent (it serves no HTTP endpoints, keeps no sequence buffer) and **not**
an MTConnect Adapter in the standard's sense (it ingests no SHDR). A deployment with machine tools
and no agent installs the canonical `mtconnect/agent` next to them; this component consumes it, the
way the OPC UA adapter consumes a Kepware server.

One `component.instances[]` entry is one MTConnect **device** (`Device/@uuid`) served by a
configured agent. Several devices on one agent share one runtime and one acquisition.

## Decisions

The HLD's register is binding; these are this repository's own notes against it.

- **D-MTC-1.** Client role only; agent and SHDR-adapter roles are permanently out of scope.
- **D-MTC-2.** Rust, an owned thin client on stock crates (`reqwest`/rustls, `quick-xml`, `sha2`);
  no third-party MTConnect runtime dependency exists in any EdgeCommons language.
- **D-MTC-3.** `component.global.agents[]` declares each agent once. An instance owns **no socket**:
  it attaches to the agent's shared runtime and drains a bounded queue. One device's failure cannot
  tear down another's session.
- **D-MTC-4.** Streaming is the primary acquisition, with polling as the fallback and the three-step
  resync ladder between them: a missed heartbeat or framing failure re-establishes from the same
  `nextSequence`; an `OUT_OF_RANGE` reports the provably-lost count and republishes a `/current`
  snapshot as fresh; a changed `instanceId` re-probes, surfaces model drift, and resyncs. After three
  consecutive stream-establish failures acquisition degrades to `/current` polling and keeps retrying
  the stream on the reconnect backoff. `streaming: poll-only` never streams.
- **D-MTC-5.** Signals bind by `dataItemId`; the probe tree backs `sb/browse` from cache; probe-model
  drift is surfaced (a digest change raises `ModelDrift`, signals recompile, and a signal whose data
  item disappeared is published BAD `MTC_NO_SUCH_DATAITEM`), never silently remapped.
- **D-MTC-6.** `UNAVAILABLE` is a BAD explicit null; the observation timestamp is the agent's capture
  stamp (published as `serverTs`), `sourceTs` is absent, the adapter's receive moment rides the
  `receivedTs` extra — stamped when the runtime ingests the agent's document, so it means payload
  arrival, never queue drain — and `sequence` rides every sample's extras.
- **D-MTC-7.** MTConnect is read-only (Part 1 Fundamentals §5.1): the schema pins
  `writes.allow: {maxItems: 0}` and the device seam refuses every write permanently. No
  `sb/discover`.
- **D-MTC-8.** Conditions are signals (state as value); `conditionBinding` degrades bound signals —
  `Warning` → UNCERTAIN, `Fault` → BAD, with the alarm's native code in `qualityRaw`.
- **D-MTC-9.** cppagent is the canonical free test peer; a third-party-agent qualification is a
  recorded deferred gate, not a release blocker.
- **D-MTC-10.** Assets, the JSON representation, and MQTT-sink consumption are follow-on
  capabilities with their own designs.

Local decisions, recorded so later sessions do not re-litigate them:

- **D-MtconnectAdapter-L1.** The owned client is an in-crate module tree (`src/mtconnect/`), not a
  workspace crate. `src/mtconnect/**` imports nothing from `edgecommons`, enforced by
  `tests/isolation.rs`.
- **D-MtconnectAdapter-L2.** A namespace version below 1.3 is refused; a version **above** 2.7
  parses (local-name matching is forward-compatible) and is flagged, because refusing a newer agent
  would be a worse failure than reading it.
- **D-MtconnectAdapter-L3.** The published endpoint is derived
  (`mtconnect://<host>[:<port>]/<uuid>`), never configured, so the agent binding and the endpoint
  cannot disagree.
- **D-MtconnectAdapter-L4.** The simulator (`adapter: "sim"`) stays in the component: it needs no
  agent and keeps the scaffold runnable on a laptop.
- **D-MtconnectAdapter-L5.** Adding, removing or reordering `component.instances[]` is
  `RESTART_REQUIRED`, alongside the LLD §8 rule for `agents[]`. An instance owns a supervisor task
  and a session; there is nothing to hand a new one to, and silently ignoring it would be worse than
  saying so. Editing an existing instance's `signals[]` reloads live, as LLD §8 requires.
- **D-MtconnectAdapter-L6.** The browse `viewGeneration` is `"<probeDigest>.<signalGeneration>"`,
  extending LLD §7's probe digest. The digest alone cannot invalidate a cursor when a reload changes
  which entries are flagged `configured`, which would let a consumer page through a view that no
  longer exists.
- **D-MtconnectAdapter-L7.** `MtconnectStream`/`MtconnectProbe` are emitted once per **agent**, not
  once per attached instance. HLD §9 dimensions them `agentId`; emitting them per instance would
  multiply one shared document stream by its device count on every fleet dashboard.
  `MtconnectParse` keeps its HLD `instance` dimension, and because parsing happens at the document
  level — above the per-device split — instances sharing an agent report that agent's counters.
- **D-MtconnectAdapter-L8 (R1.1 probe-derived selection).** An instance's published set is the
  **served union**: the explicit `signals[]` plus the set derived from its `selection` block
  against the cached probe model, computed by ONE pure function
  (`mtconnect::selection::served_set`) that the session, `sb/signals`, `sb/browse` and the
  inventory all consume — so acquisition and every view of it cannot disagree. Explicit entries
  override derived ones **field-by-field**, matched by `dataItemId`; to realize that,
  `SignalConfig.condition_binding`/`publish` are `Option` — absence inherits the derived value,
  `[]`/a policy object overrides it. Explicit entries keep the permanent-BAD missing-item contract
  (D-MTC-5); **derived entries follow the model** — on drift they are recomputed inside the same
  generation bump, removed items stop publishing (no lingering BAD) and the change is announced as
  `MtconnectSignalSetEvent` with counts. `maxSignals` (default 500) caps the derived half only,
  truncating in browse-tree order with a warning event + log — never silently.
- **D-MtconnectAdapter-L9.** The selection rides `InstanceSignals` (the reloadable slot) beside the
  explicit signals, so a selection edit is a live, atomic signals-level swap — never
  `RESTART_REQUIRED` — and the signal-set generation hashes the whole block (and, from R1.1 on, the
  explicit `publish` policies and the Option-presence of `conditionBinding`/`publish`), which keeps
  `viewGeneration = probeDigest.signalGeneration` sufficient: a model change moves the left half,
  any selection/signals change moves the right. Selection patterns are validated side-effect-free
  at config load (`validate_selection`, called from `validate_bindings`): a bad regex, an unknown
  category, `maxSignals: 0`, an inert combination (matchers under `mode: "explicit"`, an empty
  `include` under `mode: "include"`, `include` under `mode: "all"`), or a `selection` on a `sim`
  instance is refused before anything commits.
- **D-MtconnectAdapter-L10.** Derivation choices the R1.1 contract left open, decided here: matcher
  regexes are **anchored** (whole-field; `POSITION` cannot creep into `PATH_POSITION`); the path
  glob matches the raw probe component path (`Axes/Linear[X]`), case-sensitively; a nameless
  item's derived name is `type` + `" "` + `subType`; the derived SAMPLE batch window comes from
  `component.global.defaults.batchMs` (stamped into the compiled selection — there is no
  instance-level batch default to prefer); **no units-aware deadband default is derived**, because
  none is cleanly derivable from units alone (a millimeter on a micro-positioner and on a gantry
  are different facts) — derived signals get no deadband and the docs say so; the served order is
  explicit entries (configuration order) then discovered entries (browse-tree order); id
  collisions get deterministic `-2`, `-3`, … suffixes in browse-tree order; provenance is
  surfaced as `provenance: "configured" | "discovered"` on `sb/signals` rows and browse data-item
  entries, with the browse `configured` flag covering the served union.
- **D-MtconnectAdapter-L11 (publish shaping).** A signal's `publish` policy (`mode`, `batchMs`,
  `deadband`) is enforced by a per-signal shaping engine (`src/shaping.rs`) that sits **above**
  the device session (ADP-5), so the `mtconnect` and `sim` backends are shaped identically.
  `batchMs: 0` — the default — publishes each reading immediately as its own update, so
  unconfigured signals are untouched; `batchMs > 0` buffers GOOD readings and flushes on window
  expiry as ONE `SouthboundSignalUpdate` whose `samples[]` carries the window's readings in
  arrival order, each keeping its own timestamps and extras. `mode: "interval"` keeps only the
  **latest** reading per window (one sample per flush; an empty window publishes nothing) —
  `on-change` + `batchMs` is the keep-everything coalescer, `interval` + `batchMs` the
  latest-value cadence. A BAD/UNCERTAIN reading flushes its window immediately — a quality
  transition never sits in a window. The deadband applies on **entry**, against the last
  **accepted** value; quality/`qualityRaw` changes, non-numeric/array values, and the first
  reading after connect/resync/resume always pass. The session compiles the policy table (so a
  `deadband` is granted only to SAMPLE-category items, per the documented contract); a backend
  with no compile step (the sim) is shaped from its static signal configuration, where no
  category exists and a configured deadband applies to any numeric value. Lifecycle, decided
  here: **pause CLEARS open windows** (nothing may reach the wire while paused, and the resume
  snapshot republishes the current truth — flushing pre-pause readings after it would publish
  stale data out of order); **shutdown, reconnect and link loss FLUSH them** (buffered readings
  are data); `repoll` and the resume snapshot **bypass** shaping (a forced snapshot is a fresh
  full publish); a reload or model drift swaps the policy table atomically with the signal-set
  swap (its generation is `probeDigest.signalGeneration`), and changed signals' windows flush
  with the readings their old policy collected. Timing is ONE deadline per instance task — the
  earliest open window — never per-signal timers, and the engine takes explicit `Instant`s so it
  is virtual-clock testable. Observability is a new `MtconnectAdapterShaping` family
  (`instance`-dimensioned `published`/`coalesced`/`deadbandDropped` pairs) rather than an
  extension of `MtconnectStream`, whose `agentId` dimension and shared-acquisition scope cannot
  carry a per-instance publication fact. `defaults.publishMode` resolves at compile exactly like
  `defaults.batchMs` (L10) and scopes to derived SAMPLE signals only — explicit signals declare
  their own `publish` block, and an absent block outside a selection stays the immediate default.
- **D-MtconnectAdapter-L12 (depth-aware channel derivation).** A derived channel is the **last k**
  UNS-sanitized component-path segments plus the signal id, where `k` is the largest value that
  fits the instance's real UNS topic budget. This supersedes L10's "the whole component path, then
  the id": MTConnect component paths go deeper than a UNS topic can carry — the demo Mazak's
  `stock` sits on `Resources[resources]/Materials[materials]/Stock[stock]`, four channel tokens
  where an instance-scoped topic has room for three — so under `mode: "all"` the library refused
  that topic with `DEPTH_EXCEEDED` and the signal never published at all. Decided here:
  - **The budget is resolved, not assumed.** `app::channel_budget_of` mints a probe topic through
    the library's own `Uns` builder and measures what the prefix did not spend, so the token and
    byte limits (`Uns::MAX_TOPIC_SLASHES` = 7 separators, `Uns::MAX_TOPIC_UTF8_BYTES` = 256) are
    never copied into this repository. It is **per instance** and includes the device, component
    and instance token lengths, so a verbose identity buys no extra channel — it costs one.
    `ChannelBudgets` resolves once at startup (identity is fixed for the process, and adding an
    instance is `RESTART_REQUIRED` — L5) and both `compile_mtconnect` and `SignalRegistry::new`
    stamp it, so the startup compile and every reload derive identical channels.
  - **Root-side segments drop first**, because the leaf-most ones are the informative ones
    (`Materials[materials]/Stock[stock]` says what the signal is; `Resources[resources]` above it
    barely narrows anything). `k` is computed per signal against both limits at once — a longer
    path or a longer id simply keeps fewer segments.
  - **The id is terminal and never dropped.** That is what makes truncation safe: signal ids are
    unique per instance (`validate_bindings` enforces it for the explicit half, the `-2`/`-3`
    suffix chain for the derived half), so however much path is dropped, two derived channels
    cannot collide. No additional suffix rule is needed and none is added.
  - **Nothing is lost.** The full, untruncated component path stays in the `ProbeModel`, is served
    as `signal.address.componentPath` on `sb/signals` and on `sb/browse`, and rides every published
    update as `componentPath` (L13). Only the topic is shaped.
  - **Ordinary truncation is not an event.** It is normal derivation on a deep machine: counted in
    `ServedSet::channel_truncated` and logged once per recompile at DEBUG. The **only** warning is
    the pathological floor — `k = 0` (the id alone) still does not fit, i.e. the instance's own
    identity has consumed the topic — which raises the existing `MtconnectSignalSetEvent` with
    `reason: "channelBudget"`, once per distinct count. The channel stays the bare id in that case,
    so the library's own validation is what refuses it, loudly, on publish.
  - **A hand-set `channel` is untouched**, however deep: it is the operator's statement and already
    fails loudly if it does not fit. An explicit entry that *omits* `channel` takes the derived one
    and is shaped like any other.
  - The budget is hashed into the signal-set generation (L9), because it changes what every derived
    channel is and therefore what a browse cursor was minted against.

- **D-MtconnectAdapter-L13 (the canonical `componentPath`, unconditionally, at update level).**
  Every published `SouthboundSignalUpdate` carries the signal's full, untruncated MTConnect
  component path as `componentPath` on the **update-level** extra map. Decided by the user; this
  records the shape and the two alternatives that were weighed and rejected.
  - **Unconditional.** The key is stamped on *every* update — a truncated derived channel (L12), an
    untruncated one, and an explicitly configured signal alike — so an MTConnect-aware consumer has
    one place to read the canonical value and no branch to write. Rejected alternative **(a):
    stamp it only when the channel was truncated.** It is cheaper on the wire and strictly worse to
    consume: presence becomes a function of the instance's topic budget, so every reader needs a
    conditional and the field's meaning changes with configuration. A field that is sometimes there
    is a field nobody trusts.
  - **Update level, not per sample.** The path is per-signal-static and one update is one signal's
    readings, so a batched window (L11) carries exactly one `componentPath` rather than repeating it
    on every sample. The library's `SouthboundSignalUpdate` reserves only `signal` and `samples` at
    that level and round-trips every other body key through the protobuf `extra` map
    (`map<string, EcValue> extra = 100`), so the placement is a first-class round-trip, not a
    tolerated unknown. `componentPath` collides with nothing: the seven reserved *sample* keys are
    `value`, `quality`, `qualityRaw`, `sourceTs`, `serverTs`, `sourceTsMs`, `serverTsMs`.
  - **One source of truth for the value.** `ProbeModel::component_path_of` is the only formatter;
    `address_of` (what `sb/signals` and `sb/browse` serve) and the publish path both read it, so the
    update and the control plane cannot drift apart. Three values are possible and all three are
    *present*: the slash-joined path; `""` for a device-level data item that hangs off no component;
    and `null` for a signal no device model describes — the permanent-BAD case of an explicit
    `signals[]` entry whose `dataItemId` is absent from the probe, where `sb/signals` reports
    `address.componentPath: null` from `unlearned_address` and the update says exactly the same
    thing. There is no case in which the key is omitted.
  - **Confined to this adapter — no core change.** Rejected alternative **(b): promote the
    component path to a first-class `address` block on the southbound contract.** That is a
    four-language wire-contract change (`docs/SOUTHBOUND.md`, the protobuf schema, the interop
    matrix) to serve one protocol's shape. Reconsider only if a second deep-path protocol appears
    and wants the same thing; until then the additive extra costs the ecosystem nothing.
  - **How it is reached without hand-building the body.** The `SignalUpdate` builder has no
    update-level `extra` setter, so `supervisor.rs` uses the facade's own two-step form:
    `DataFacade::build_body` applies the whole §2.1 contract (quality defaulting, the `qualityRaw`
    marker, the `serverTs` fill, the samples wrapper), `app::stamp_component_path` adds the one
    additive key, and `DataFacade::publish_body_via` mints the `data/{channel}` topic and stamps
    identity from the same `effective_signal_path`. The adapter still formats no body and no topic.

### Remediation decisions (D-R1..D-R16)

The adversarial-review remediation program's register (`REMEDIATION-SPEC.md` §10), mirrored here so
it binds future sessions the way the L-series does:

- **D-R1 (one connectivity gate).** `MtcBackend::connect` and `MtcSession::read_signals` mirror
  `AgentRuntime.info().connected`; a cached probe model is never liveness. An instance therefore
  reports `CONNECTING`/`BACKOFF` — not `ONLINE` — until the shared acquisition ingests its first
  Streams document: faithful to HLD §5.1's state definition, and the price of never lying ONLINE.
- **D-R2 (bounded loss-intolerant lane).** The reserved critical lane (condition observations,
  lifecycle events, snapshots) is **bounded**: cap `CRITICAL_QUEUE_DEPTH` (256), a bounded
  `CRITICAL_SEND_BUDGET` (5 s) wait when full, drop-and-count past the bound, cancellation-aware so
  shutdown always preempts. An explicit, user-settled deviation from the LLD's original unbounded
  `send().await`: the shared publish path is the one true cross-agent coupling, and unbounded
  backpressure against a dead broker/nucleus would freeze ALL acquisition indefinitely while the
  backpressured events could not be published anyway. LLD §3 records the same design.
- **D-R3 (mark-down scope).** Ladder-1 stream exits (heartbeat missed, transport lost, malformed,
  end-of-stream) mark the agent down; ladder-2/3 exits (`OUT_OF_RANGE`, `instanceId` change) do
  not — those documents prove the agent alive, and their recovery I/O marks down by itself on
  failure. Marking down on every exit would emit a false down/up pair per buffer wrap.
- **D-R4 (establish-failure accounting).** A stream counts as established only after its first
  liveness part. A zero-part exit increments the degradation counter and waits the backoff; a
  delivering stream resets the counter and permits one immediate ladder-1 resume. Kills the tight
  headers-then-EOF loop without penalizing healthy long streams.
- **D-R5 (one writer family for `connected`).** Only the acquisition path's ingest/mark-down pair
  writes `connected`. A command-path snapshot success refreshes `last_liveness` (the agent vouched
  for currency) but never flips `connected` — no second bookkeeping path.
- **D-R6 (closed metric families).** Queue drop/coalesce counts surface through the runtime's
  `dropped_events`/`queue_counters()` and logs — never as new `MtconnectStream` measures. HLD §9's
  measure set stays closed; `MtconnectParse` is the one family extended (D-R11).
- **D-R7 (activation identity).** A condition activation is keyed `conditionId` ▷ `nativeCode` ▷
  `""` — the empty key is the single slot for agents that identify nothing, preserving
  one-condition-per-item behavior exactly. A keyless `Normal` is the standard's normal sweep and
  clears all activations.
- **D-R8 (condition wire shape).** A condition signal publishes the **aggregate** across its
  concurrent activations (value and quality); the triggering transition rides the sample extras
  (`conditionId`, `nativeCode`, `conditionText`) beside `activeConditions`. Two additive
  wire-visible extras on the free-form `extra` map — no core change.
- **D-R9 (`/current` Errors policy).** An HTTP-200 `MTConnectErrors` answer to `/current` is
  `MtcError::AgentError` (→ `MTC_AGENT_ERROR:<code>` on `sb/read`), counts as parsed-OK, refreshes
  no liveness, and marks the agent down only on the third consecutive erroring cycle
  (`CURRENT_ERROR_DOWN_STREAK`) — reachable-but-useless degrades through staleness first,
  connectivity second.
- **D-R10 (required-field strictness).** An observation needs a `dataItemId`, a `sequence` parsing
  as u64 ≥ 1, and a non-empty `timestamp`; the timestamp's *format* is not validated (verbatim
  pass-through, tolerant of real agents' RFC3339 variants). Rejects are dropped and counted, never
  defaulted.
- **D-R11 (`MtconnectParse` extension).** The family carries `rejectedObservations` — the only
  family extension in the program; HLD §9 lists it.
- **D-R12 (staleness clock).** MTConnect is on-change: an unchanged value under live liveness is
  *current*, so passive staleness runs on the **liveness clock** — time since the agent last
  vouched (Streams document or successful `/current`) — never per-signal change age. The window is
  `heartbeatMs` (stream) / `2 × pollIntervalMs` (poll); expiry is `staleSignalSecs` on the same
  clock; `<ageMs>` in `MTC_STALE:<ageMs>` is that liveness age. The `staleSignals` *metric* keeps
  its per-signal value-silence meaning, unchanged.
- **D-R13 (two trackers, one feed).** `staleness::QualityWatchdog` holds held-reading records;
  `DeviceMetrics` keeps its `last_update` map. Both are fed from the same publish call sites, and
  synthetic readings feed neither.
- **D-R14 (synthetic reading shape).** A passive transition republishes the **held value** with the
  held `sequence` (the sample still names the observation it describes) plus a `passive` extra
  naming the transition (`stale`|`expired`|`unreachable`|`recovered`). Recovery restores the held
  quality and `qualityRaw` verbatim.
- **D-R15 (edition).** The crate is edition 2024 (`rust-version = "1.85"`), matching the LLD.
- **D-R16 (shaper route identity).** Routing (`channel`, `component_path`, `name`) is part of
  `PublishPolicy` identity: `set_policies` flushes a route-changed signal's open window with its
  old readings on its old route, so one update never mixes routing generations. L11's
  flush-only-changed rule is preserved, not widened.

## Module layout

```text
src/
  app.rs         config types, backoff, health, connectivity, the publish mapping (`build_sample`,
                 `stamp_component_path`), and the structured-shutdown helpers (`join_all_within`
                 and the 6 s / 4 s / 2 s budgets)
  driver.rs      the device drivers: the per-instance connect/poll/publish/reconnect orchestration,
                 the control-channel service, the shaping and passive-quality wiring — written
                 against the `Wire` publish/emit seam so it is driven end to end by fakes, and
                 fully inside the coverage denominator
  supervisor.rs  the thin live shell: construction, agent-runtime and device-task spawning, the
                 shutdown invocation, and `FacadeWire` (the facade-backed `Wire`)
  device.rs      the device seam + the MTConnect backend/session + the condition ledger +
                 credential resolution
  commands.rs    the `sb/*` verbs and the panel descriptors
  metrics.rs     `southbound_health` + the operational families + the HLD §9 families
  reload.rs      the pre-commit reload verdict + the live, swappable per-instance signal set
  shaping.rs     the per-signal publish-shaping engine (batch windows + deadband), pure and
                 virtual-clock tested; `driver.rs` drives it above the session (L11)
  staleness.rs   the passive-quality state machine (`PassiveLink`, `QualityWatchdog`) — pure,
                 virtual-clock tested; `driver.rs` evaluates it every tick (D-R12..D-R14)
  mtconnect/     the owned client — NO edgecommons imports
    mod.rs config.rs client.rs xml.rs model.rs observations.rs sequence.rs stream.rs
    multipart.rs error.rs stats.rs selection.rs
```

## Config

`config.schema.json` is the source of truth. Why the keys exist:

- `agents[]` — an agent is shared infrastructure, so it is declared once and referenced by id.
  Credentials are **references** (`auth.secretRef`, `tls.*SecretRef`) resolved through the
  EdgeCommons vault at the `device.rs` boundary; the protocol client only ever sees values.
- `instances[].connection` — `{agentId, deviceUuid}` and nothing else: the endpoint is derived.
  (`adapter: "sim"` uses `endpoint` instead, having no agent.)
- `instances[].signals[]` — the explicit signals: without a `selection`, a data item that is not
  configured is browsable but not published. `conditionBinding` is what makes a value's quality
  reflect the machine's own alarms.
- `instances[].selection` — probe-derived selection (R1.1, D-MtconnectAdapter-L8..L10): publish
  data items by matcher (`mode: "include"`) or wholesale (`mode: "all"`) instead of naming each
  one; matchers AND within, OR across, `exclude` wins; explicit entries override derived ones
  field-by-field.
- `writes.allow` — pinned empty; the protocol has no write path.

Cross-object invariants JSON Schema cannot express (every `agentId` resolves, uuids unique per
agent, `conditionBinding` never naming the signal's own data item) are enforced at startup by
`mtconnect::config::validate_bindings`.

## Command surface

The generic `sb/*` family ships from the scaffold; `src/commands.rs` maps it onto MTConnect
(LLD §7, HLD §7):

- `sb/status` carries the closed `protocol` object (capability, standard/schema version, agent
  version, `instanceId`, buffer/sequence window, mode, heartbeat, probe digest, limitations),
  nullable until the agent has taught us and read from the runtime's `ArcSwap<AgentInfo>` — never a
  control-channel round-trip.
- `sb/signals` returns the configured inventory with the §5.3 `address`, enriched from the cached
  model and honestly null before the first probe.
- `sb/browse` serves the probe tree in both modes (paged and hierarchical) straight from the cached
  `ProbeModel`: ids `mtc:/component/<path>` / `mtc:/item/<dataItemId>`, entries flagged `Configured`,
  the model digest as `viewGeneration` (a cursor from a superseded model is refused with
  `MTC_VIEW_CHANGED`), working while the agent is unreachable and `BROWSE_FAILED`/`MTC_NO_PROBE`
  before the first probe.
- `sb/read` takes a scoped `/current` snapshot through the agent's control channel (`mode:
  "current"`) with per-entry codes (`MTC_UNAVAILABLE`, `MTC_NO_SUCH_DATAITEM`,
  `MTC_AGENT_ERROR:<code>`, `MTC_PARSE`); an unreachable agent degrades the entries, not the command.
- `sb/write` is registered and refused unconditionally before entry processing, and advertised
  `unsupported` through command availability. Registration order is verbs → availability → panels.
- Five panel descriptors (HLD §8) with their `rendererRequirements`; no view advertises a write
  surface.

`repoll` forces a **fresh** `/current` scoped to the instance's configured data items, serialized
with acquisition through the agent's control channel — not a drain of whatever happened to have
arrived, so an idle machine still answers. `polled` counts the results published, `BAD` ones
(`UNAVAILABLE`, `MTC_NO_SUCH_DATAITEM`) included.

## Metrics and events

`southbound_health` carries the exact SOUTHBOUND §5 set. `signalsSubscribed` reports what the session
is really **serving** — the configured set minus signals whose `dataItemId` the current device model
does not have — which is the same number in stream mode and poll mode, because the compiled set is
what is served and the mode only decides how it arrives.

Beside it: the three generic families (`MtconnectAdapterConnection`, `MtconnectAdapterCommand`, and
`MtconnectAdapterShaping` — the publish-shaping engine's `published`/`coalesced`/`deadbandDropped`
pairs, dimensioned `instance` per L11) and the three of HLD §9. `MtconnectStream` and `MtconnectProbe` are dimensioned `agentId` and emitted **once
per agent** — an agent is shared infrastructure and its document flow exists once however many
devices attach to it — while `MtconnectParse` is dimensioned `instance` and carries
`documentsParsed`, `parseErrors`, and `rejectedObservations` (D-R10/D-R11 — observations refused
for a missing required field). The runtime accumulates
monotonic totals (`src/mtconnect/stats.rs`, EdgeCommons-free like everything under `mtconnect/`) and
`src/metrics.rs` diffs successive snapshots into the `(Total, Interval)` pairs. Dimensions are
`instance`, `agentId`, `verb`, `result` and nothing else. Queue drops and coalesces are runtime
counters (`dropped_events`/`queue_counters()`) and log lines, not a metric family (D-R6).

Events go out through the `events()` facade: `MtconnectAgentEvent` (up / down / degraded-to-polling),
`MtconnectDataLossEvent` (skipped count and sequence window), `MtconnectModelDriftEvent` (old and new
digest), `MtconnectConditionEvent` on a transition of the **aggregate** into `Fault` (a second
concurrent Fault on an already-faulted item is not a new alarm — D-R8; context carries
`conditionId` and `activeConditions`), rate-limited to one per
data item per minute, and `MtconnectSignalSetEvent` (the selection-derived set changed shape — info
with added/removed counts — or `maxSignals` truncated it — warning). Sequence numbers, device uuids and data-item ids are event fields, never metric
dimensions. The mapping from runtime event to operator event lives in `device.rs` (`Notice`), where
it is unit-tested; `driver.rs` only carries it to the wire (`emit_notices`).

## Configuration reload

`component.global.agents[]` owns live sockets, an open stream and a position in the agent's buffer,
and an instance owns a supervisor task and a session — so a candidate that changes either is refused
**before it commits** with `RESTART_REQUIRED`, and the component keeps running on its last-good
configuration. A candidate that does not compile is refused with `INVALID_MTCONNECT_CONFIG`. Both
verdicts come from the pure `reload::classify`, registered as the library's configuration validator
in `main.rs`.

An existing instance's `signals[]` — and its `selection` block, which rides the same
`InstanceSignals` slot — reloads live: `reload::SignalRegistry::apply` compiles every instance
first and only then swaps the slots, so one bad instance cannot leave the others on a new
generation. A session recompiles against the **cached** probe model — no agent round-trip, so a
reload lands while the agent is unreachable. Because an entry's `configured` flag comes from the
signal configuration (explicit + selection), the browse `viewGeneration` composes the probe digest
**and** the signal-set generation, and a cursor minted before either changed is refused with
`MTC_VIEW_CHANGED`.

## Lifecycle

Shutdown is a staged, bounded drain, and every spawned task's handle is retained and joined:

1. device tasks flush open shaping windows, publish, and detach — joined under
   `DEVICE_SHUTDOWN_BUDGET` (6 s);
2. agent runtimes and the metric tickers stop — `AGENT_SHUTDOWN_BUDGET` (4 s);
3. the final metric flush runs — `METRICS_FLUSH_BUDGET` (2 s).

Worst case is the sum (12 s). Whatever overruns its budget is aborted **and named** in the log
(`app::join_all_within`), so a wedged task is a diagnosis, not a hang. A clean stop is not an
incident: it raises no `device-unreachable` and counts no reconnect.

## Validation

- `cargo test` — the unit suite (including `mtconnect::selection`'s matcher/derivation/precedence
  tables, `shaping`'s window/deadband/lifecycle tables and `staleness`'s passive-quality ladder on
  a virtual clock, the two-lane queue and condition-ledger tables, and `driver.rs`'s orchestration
  suite over fake sessions and a recording `Wire`, plus
  `tests/publish_shaping.rs` — the batched `samples[]` wire shape and the sim/mtconnect shaping
  parity — and `tests/passive_quality.rs` — the passive transitions on the wire) plus
  `tests/poll_acquisition.rs` (poll acquisition end to end against a fake agent
  serving canned documents, including the R1.1 legs: the minimal `selection: {mode: "all"}`
  instance, matcher scoping with explicit overrides, `maxSignals` truncation, drift add/remove of
  derived signals, and the live selection reload), `tests/config_schema.rs` (every shipped
  configuration validated against `config.schema.json` and through the semantic validator) and
  `tests/isolation.rs` (the seam rule), `tests/stream_acquisition.rs` + `tests/stream_sequence.rs`
  (the streaming state machine and its three ladders on a virtual clock), `tests/fuzz_style.rs` (the multipart/XML
  hardening cases), `tests/component_path.rs` (the L13 update-level extra), and `tests/tls_auth.rs`
  (rustls trust, mutual TLS, and Basic/bearer
  authentication against a TLS agent minted in-process from a throwaway private CA).
- `cargo clippy --all-targets -- -D warnings`.
- `cargo llvm-cov --fail-under-lines 90` — the coverage gate. `driver.rs` is inside the
  denominator; only `supervisor.rs` (the live shell), `main.rs`, and the env-gated live suites are
  excluded, each pinned to a reason in the CI workflow.
- `tests/live_sim.rs` (`EC_LIVE_SIM=<endpoint>`) — the live simulator path — and
  `tests/agent_integration.rs` (`EC_MTC_AGENT=<url>`, via `tests/compose.mtconnect-agent.yaml`) —
  the canonical cppagent harness: agent restart, `instanceId` resync, buffer wrap/`OUT_OF_RANGE`
  recovery, multi-device demultiplexing. Both self-skip without their variable; `EC_REQUIRE_LIVE=1`
  turns the self-skip into a hard failure for runs that are supposed to reach live infrastructure.
- `tests/wire_gate.rs` (`EC_MTC_AGENT=<url>` **and** `EC_MQTT_BROKER=<host:port>`) — the local-MQTT
  wire gate. A genuine `EdgeCommons` runtime built through `EdgeCommonsBuilder::build()` against the
  real broker (the only construction path for `DataFacade`) drives `driver::run_device` over the
  live cppagent, and a **raw** MQTT subscriber decodes the bytes that landed with `prost`, straight
  against the generated `edgecommons.v1` schema. It pins the topic grammar, the stamped top-level
  `identity`, the update-level `componentPath`/`device` extras, the `sequence`/`receivedTs` sample
  extras, the whole quality vocabulary (`MTC_OK`, `MTC_OK:NORMAL`, `MTC_CONDITION:FAULT|WARNING:<code>`,
  `UNAVAILABLE` as a BAD explicit null, `MTC_STALE:<ageMs>`, `MTC_AGENT_UNREACHABLE`), and the two
  behaviors only a live bus can show: concurrent condition activations (`conditionId` +
  `activeConditions`, D-R7/D-R8) and the passive `stale → expired → unreachable` ladder with its
  `passive` marker (D-R14). The `d1-travel` CONDITION data item in `tests/fixtures/agent-e2e/devices.xml`
  exists for it, and the fixture speaks MTConnect 2.3 so cppagent emits `conditionId`.
- `host-leg/`, `lab-leg/`, `k8s-leg/` — the three platform legs, each a harness crate outside the
  component workspace so none of them enters its build, test or coverage gates. HOST runs the real
  binary as an OS process so `SIGTERM` is a real signal; `lab-leg/` deploys to a Greengrass nucleus
  and reads the bus through a second component over IPC; `k8s-leg/` deploys to a cluster and proves
  the ConfigMap source, both Downward-API identity tiers, the health endpoints and the Prometheus
  endpoint. Each exercises the connectivity ladder, the condition aggregate, the passive ladder and
  the bounded teardown on its own platform.
- A cppagent-over-TLS compose variant is not in the harness; the transport-security row is covered
  by `tests/tls_auth.rs` against a local TLS server instead.
- Two limits the platform legs established, both outside this component's control. On Greengrass the
  nucleus revokes IPC authorization several hundred milliseconds *before* it signals a component
  being removed, so a `--remove` teardown drops the handful of updates still in flight — the publish
  fails, is logged, and is not retried, because the channel it would retry on is already gone.
  Stopping a component does not do this. And a `--remove` teardown carrying a genuinely full batch
  window has not been exercised, so whether a loaded flush completes inside the nucleus's kill
  window is not established.

## Appendix — revision history

| Date | Change |
|---|---|
| 2026-08-04 | Release legs recorded: the three platform harnesses, the Greengrass IPC-revocation and loaded-flush limits, and the packaging repairs they found (container toolchain floor, Kubernetes Prometheus feature, Greengrass `exec` lifecycle form). |
| 2026-08-03 | Adversarial-review remediation folded in: the D-R1..D-R16 register; the `driver.rs`/`staleness.rs` module split; the lifecycle section; validation/coverage discipline and the metrics/events sections brought to current state. |
| 2026-07-28 | Initial version (R1 + selection/shaping/channel-depth decisions). |
