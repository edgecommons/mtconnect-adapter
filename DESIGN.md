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
observations (`/current` today, `/sample?interval=…` streaming next), and publishes normalized
EdgeCommons signals.

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
  `receivedTs` extra, and `sequence` rides every sample's extras.
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

## Module layout

```text
src/
  app.rs         config types, backoff, health, connectivity, the publish mapping (`build_sample`)
  supervisor.rs  the live drivers: agent runtimes, per-instance connect/poll/publish/reconnect
  device.rs      the device seam + the MTConnect backend/session + credential resolution
  commands.rs    the `sb/*` verbs and the panel descriptors
  metrics.rs     `southbound_health` + the operational families + the HLD §9 families
  reload.rs      the pre-commit reload verdict + the live, swappable per-instance signal set
  mtconnect/     the owned client — NO edgecommons imports
    mod.rs config.rs client.rs xml.rs model.rs observations.rs sequence.rs stream.rs
    multipart.rs error.rs stats.rs
```

## Config

`config.schema.json` is the source of truth. Why the keys exist:

- `agents[]` — an agent is shared infrastructure, so it is declared once and referenced by id.
  Credentials are **references** (`auth.secretRef`, `tls.*SecretRef`) resolved through the
  EdgeCommons vault at the `device.rs` boundary; the protocol client only ever sees values.
- `instances[].connection` — `{agentId, deviceUuid}` and nothing else: the endpoint is derived.
  (`adapter: "sim"` uses `endpoint` instead, having no agent.)
- `instances[].signals[]` — signals are explicit: a data item that is not configured is browsable
  but not published. `conditionBinding` is what makes a value's quality reflect the machine's own
  alarms.
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

Beside it: the two generic families (`MtconnectAdapterConnection`, `MtconnectAdapterCommand`) and the
three of HLD §9. `MtconnectStream` and `MtconnectProbe` are dimensioned `agentId` and emitted **once
per agent** — an agent is shared infrastructure and its document flow exists once however many
devices attach to it — while `MtconnectParse` is dimensioned `instance`. The runtime accumulates
monotonic totals (`src/mtconnect/stats.rs`, EdgeCommons-free like everything under `mtconnect/`) and
`src/metrics.rs` diffs successive snapshots into the `(Total, Interval)` pairs. Dimensions are
`instance`, `agentId`, `verb`, `result` and nothing else.

Events go out through the `events()` facade: `MtconnectAgentEvent` (up / down / degraded-to-polling),
`MtconnectDataLossEvent` (skipped count and sequence window), `MtconnectModelDriftEvent` (old and new
digest), and `MtconnectConditionEvent` on a **transition into** `Fault`, rate-limited to one per
data item per minute. Sequence numbers, device uuids and data-item ids are event fields, never metric
dimensions. The mapping from runtime event to operator event lives in `device.rs` (`Notice`), where
it is unit-tested; `supervisor.rs` only carries it to the wire.

## Configuration reload

`component.global.agents[]` owns live sockets, an open stream and a position in the agent's buffer,
and an instance owns a supervisor task and a session — so a candidate that changes either is refused
**before it commits** with `RESTART_REQUIRED`, and the component keeps running on its last-good
configuration. A candidate that does not compile is refused with `INVALID_MTCONNECT_CONFIG`. Both
verdicts come from the pure `reload::classify`, registered as the library's configuration validator
in `main.rs`.

An existing instance's `signals[]` reloads live: `reload::SignalRegistry::apply` compiles every
instance first and only then swaps the slots, so one bad instance cannot leave the others on a new
generation. A session recompiles against the **cached** probe model — no agent round-trip, so a
reload lands while the agent is unreachable. Because an entry's `configured` flag comes from the
signal set, the browse `viewGeneration` composes the probe digest **and** the signal-set generation,
and a cursor minted before either changed is refused with `MTC_VIEW_CHANGED`.

## Validation

- `cargo test` — the unit suite plus `tests/poll_acquisition.rs` (poll acquisition end to end
  against a fake agent serving canned documents), `tests/config_schema.rs` (every shipped
  configuration validated against `config.schema.json` and through the semantic validator) and
  `tests/isolation.rs` (the seam rule), `tests/stream_acquisition.rs` + `tests/stream_sequence.rs`
  (the streaming state machine and its three ladders on a virtual clock), `tests/fuzz_style.rs` (the multipart/XML
  hardening cases), and `tests/tls_auth.rs` (rustls trust, mutual TLS, and Basic/bearer
  authentication against a TLS agent minted in-process from a throwaway private CA).
- `cargo clippy --all-targets -- -D warnings`.
- `cargo llvm-cov --fail-under-lines 90` — the coverage gate.
- `tests/live_sim.rs` (`EC_LIVE_SIM=<endpoint>`) — the live simulator path, skipped otherwise.
- Still to run for a release: the canonical cppagent container (agent restart, buffer wrap,
  multi-device streams), the wire gate over local MQTT, and the HOST/Greengrass/Kubernetes platform
  gates. A cppagent-over-TLS compose variant is not in the harness; the transport-security row is
  covered by `tests/tls_auth.rs` against a local TLS server instead.
