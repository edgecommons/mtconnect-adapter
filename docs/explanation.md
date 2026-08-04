# Explanation — How this adapter is shaped, and why

This page is the mental model behind the code. For exact options see [reference/](reference/); for
tasks, the [how-to guides](how-to-guides.md).

## The southbound contract

An **adapter** connects to devices, reads signals, and publishes them onto the UNS in the shape the
rest of the fleet expects — the same shape the Java (OPC UA) and Python (Modbus) reference adapters
implement. A consumer sees `SouthboundSignalUpdate` on the `data` class, `southbound_health` on
`metric`, and the generic `sb/*` command family on `cmd`, regardless of which protocol produced it.
This component is a **client**, not an agent: it serves no HTTP endpoints and keeps no sequence
buffer of its own. It ingests no SHDR either, so it is not an MTConnect Adapter in the standard's own
sense — a site with machine tools and no agent installs the canonical `mtconnect/agent` next to them,
and this component consumes it, the way the OPC UA adapter consumes a Kepware server.

## The device seam, and why the MTConnect client sits below it

[`crate::device::DeviceSession`] is one live connection to one device; [`crate::device::DeviceBackend`]
opens sessions. Everything above the seam — `src/app.rs`, `src/driver.rs`, `src/supervisor.rs`,
`src/commands.rs`,
`src/metrics.rs` — is written against the trait pair and calls `read_signals`/`write_signal`/`browse`,
never a protocol-specific API. Two backends implement it: the built-in `SimBackend` (kept so the
component runs on a laptop with no agent) and `MtcBackend`, which is a thin adapter over the owned
MTConnect client in `src/mtconnect/**` — a module tree that imports nothing from `edgecommons`
(enforced by `tests/isolation.rs`), so the HTTP/XML protocol logic is fully independent of the UNS,
the envelope, and the command surface.

`MtcSession::write_signal` always refuses (MTConnect's API is read-only by specification — see
below), and `read_named` issues a **live** scoped `/current` request rather than filtering a cached
read, because an on-demand `sb/read` should reflect the agent's current answer, not a value that may
be several poll cycles stale.

## One runtime per agent, one task per device

An **agent** is shared infrastructure: `component.global.agents[]` declares each one once, and every
device instance that names it (`connection.agentId`) attaches to the **same** running acquisition
rather than opening its own socket. Concretely, two things start:

- One `AgentRuntime` per configured agent (`src/mtconnect/mod.rs`), spawned once at startup before
  any device task runs. It owns the one HTTP client, the one cached `AgentInfo` (published
  lock-free through an `ArcSwap`, read directly by `sb/status` — never a control-channel
  round-trip), one cached `ProbeModel` per attached device uuid, and the one `SequenceState` that
  tracks the agent's `instanceId` and sequence bookkeeping for its whole connection.
- One `tokio` task per **device** (`run_device` in `src/driver.rs`), which does not talk to the
  network itself — it drains the `InstanceEvent` queue the agent runtime fans out to it, publishes
  what it reads, and owns that device's own control channel for commands.

This split is what makes "a hundred devices, one agent" cheap and correct: the agent's HTTP
connection and its stream/poll cycle exist exactly once, no matter how many devices attach to it, and
one device's failure — a bad `dataItemId`, a paused publish — can never tear down another device's
session, because they only ever share *read* access to the agent's published state.

## Two delivery lanes: what may be dropped, and what may not

The per-instance queue between the agent runtime and a device task carries two kinds of traffic
with opposite loss tolerances, so it is two lanes, not one. **Coalescible data** — ordinary
observations — rides a lane where a full queue coalesces to the latest value per `dataItemId` and,
past capacity, drops the oldest: for on-change telemetry the newest value *is* the current truth,
and a slow consumer should cost freshness, not correctness. **Loss-intolerant events** — condition
observations, agent lifecycle transitions, model drift, data-loss reports, and snapshots — ride a
reserved lane that data volume can never squeeze: a burst of samples cannot evict the Fault that
explains them. Only the data lane drops, and every drop is counted and logged — never silent.

The critical lane is bounded too, deliberately: past its capacity the sender waits a bounded few
seconds and then drops-and-counts rather than blocking forever, because the shared publish path is
the one coupling every device task has in common — an unbounded wait against a dead broker would
freeze all acquisition indefinitely while the waiting events could not be published anyway.
Real consumer lag backpressures properly inside the bound; only a genuinely wedged consumer is cut
loose, loudly.

Ordinary flow is delivered **per observation**; a snapshot means a genuine re-baseline (connect,
resync, resume). The distinction is what makes `publish.deadband` meaningful: the deadband anchor
survives across cycles and only re-arms on a true re-baseline, so a noisy signal is suppressed
across polls, not merely within one.

## Streaming, polling, and the resync ladder

An agent's `streaming` setting (`prefer`, the default, or `poll-only`) picks how its `AgentRuntime`
acquires observations. `prefer` opens a long-lived `/sample?interval=...&heartbeat=...` multipart
stream, decoding each part as it arrives; `poll-only` — and a `prefer` agent that fails to establish a
stream repeatedly — reads `/current` on a fixed cadence instead. Either way, the same three-step
ladder keeps the published data honest whenever the agent's own state moves out from under the
client:

1. **A heartbeat is missed.** The stream socket may still be open, but no document — not even an
   empty heartbeat one — has arrived within twice the configured `heartbeatMs`. The client drops the
   connection and re-establishes it from the same sequence position: no data has been lost, only the
   transport needs re-dialing.
2. **The agent reports `OUT_OF_RANGE`.** The position this client asked to resume from has already
   fallen out of the agent's own observation buffer — the client was too slow, or was disconnected
   too long. The client takes a fresh `/current` snapshot (publishing it as new values, bypassing the
   normal per-item dedupe so nothing already-known suppresses it), and resumes streaming from that
   snapshot's own sequence position. The number of provably-skipped observations is reported as a
   data-loss event.
3. **The agent's `instanceId` changes.** This means the agent itself restarted — its sequence
   numbering has reset from zero, and its device model *might* have changed too (a configuration
   reload, a different Devices.xml). **Nothing from the restarted agent's document is published**
   until the client has re-probed every device attached to that agent and recompiled the signal
   sets — publishing first would route observations through a model the restart may have
   invalidated. The re-probe compares the new content digest against the cached one, and only if it
   actually changed does it surface a model-drift event — a data item that disappeared publishes as
   a permanent `BAD`/`MTC_NO_SUCH_DATAITEM` rather than being silently dropped or remapped to
   something else. A probe that fails during this recovery fails the whole cycle rather than
   letting stale bindings serve new data. Acquisition then resumes with a fresh snapshot, which
   republishes everything as current.

Deduplication is **per data item**, not one global counter: `SequenceState` tracks the last-published
sequence number for each `dataItemId` independently, because a `/current` snapshot taken to recover
from step 2 or 3 overlaps the stream's own window, and a single global floor would let one data
item's higher sequence number silently suppress a different, still-unpublished, lower-sequence item.

Two accounting rules keep the ladder honest. **Only ladder-1 exits mark the agent down** — a missed
heartbeat, a lost transport, malformed framing, or a closed stream mean the agent stopped
delivering, while the documents that trigger steps 2 and 3 prove the agent alive (their recovery
I/O marks down by itself if it fails), so a buffer wrap against a healthy agent never emits a false
down/up pair. And **a stream only counts as established once it has delivered a first liveness
part**: one that opens and immediately ends counts as an establish failure and waits out the
backoff, so a flapping agent cannot trap acquisition in a tight open/close loop. After three
consecutive establish failures, acquisition degrades to `/current` polling — announced as an
`MtconnectAgentEvent` (`state: "degraded"`) — and keeps retrying the stream on the reconnect
backoff.

A reachable agent can still be useless: an HTTP-200 `MTConnectErrors` answer to `/current` surfaces
on `sb/read` as `MTC_AGENT_ERROR:<code>`, refreshes no liveness, and the **third consecutive**
erroring poll cycle marks the agent down — reachable-but-erroring degrades through staleness first
and connectivity second, in that order.

## The probe model and `sb/browse`

Every agent's `/probe` response projects into a `ProbeModel`: a pre-ordered tree of the device, its
components, and its data items, with stable, round-trippable ids (`mtc:/component/<path>` for the
device and its components, `mtc:/item/<dataItemId>` for data items — the same id a signal binds to).
A content digest of just that device's subtree (`sha256:...`) is published as `sb/status`'s
`probeDigest` and doubles as `sb/browse`'s `viewGeneration` — a cursor minted against one probe
generation is refused (`MTC_VIEW_CHANGED`) rather than silently paging through a model that changed
underneath it. `sb/signals` and `sb/browse` are both served straight from this cached model, so
neither ever queues behind acquisition, and both keep answering while the agent link itself is down —
the address space came from the last successful probe, not from a live round-trip.

## Explicit signals, derived signals, and the served union

An instance's published set has two halves. The **explicit** half is `signals[]`: hand-written
entries, each pinning a stable EdgeCommons identity to one `dataItemId`. The **derived** half is
the `selection` block: a description of *which* data items to publish (`mode: "include"` with
matchers, or `mode: "all"`), from which the adapter derives one signal per selected item out of
the cached probe model — id, name, channel, publish policy, and (by default) a `conditionBinding`
to the CONDITION items of the signal's own component. The two merge into one **served union**:
an explicit entry whose `dataItemId` the selection also matches overrides the derived entry, field
by field — its set fields win, its unset fields take the derived values. One pure function
computes this union, and the session, `sb/signals`, `sb/browse`, and `signalsSubscribed` all read
it, so acquisition and every view of it always agree; `sb/signals` rows and browse entries carry
a `provenance` field (`configured` vs `discovered`) saying which half serves each binding.

### Derived identity is a trade

A derived signal's id is the lower-kebab sanitization of the machine's own `dataItemId`, and its
channel follows the machine's component path. That is what makes `selection: { "mode": "all" }` a
one-line configuration — and it is also the trade: those identities are **protocol-derived**, so
they follow the machine. Rewire the device, rename a component, replace the Devices.xml, and a
derived signal's identity can change with it. The derived set also *follows the model* by design:
when a re-probe shows a data item gone, its derived signal simply stops publishing (announced with
counts as an `MtconnectSignalSetEvent`) rather than lingering as a permanent BAD — discovered
signals track the machine, they do not hold a contract. For any signal whose history must survive
a machine reconfiguration — the one the historian keys on, the one an SPC chart trends — pin an
explicit `signals[]` entry: explicit identities never move, and an explicit binding whose data
item disappears publishes a permanent BAD `MTC_NO_SUCH_DATAITEM`, because a configured promise
that can no longer be kept must be *visible*, not absent. Use the derived set for breadth, and
explicit entries for the identities that matter.

### A machine model is deeper than a topic

An MTConnect device model is a tree with no practical depth limit — a coolant sensor can sit five
or six components below the device — while a UNS topic is a flat address with a hard ceiling: eight
levels and 256 bytes, of which `ecv1/{device}/{component}/{instance}/data/` has already spent five
levels. Mapping one onto the other is not a formatting detail; it is where an unbounded structure
meets a bounded one, and something has to give.

What gives is the **root** of the path, never the leaf and never the id. A derived channel is the
last few component-path segments plus the signal id, taking as many segments as the topic has room
for. `Resources[resources]/Materials[materials]/Stock[stock]` becomes
`materials-materials/stock-stock/stock`: the segments that survive are the ones that say what the
signal *is*, and the one that drops is the container that barely narrowed anything. The room is
measured against the instance's real identity, so a long device or instance name costs channel
depth rather than quietly breaking the topic.

Two properties make this safe to rely on. The signal id is always the terminal segment, and signal
ids are unique within an instance — so however much path is dropped, two signals can never land on
the same channel. And the machine's full component path is not discarded: every published
`SouthboundSignalUpdate` carries it as `componentPath`, the same string `sb/signals` serves in
`signal.address.componentPath` and every browse entry shows. A consumer that needs the exact
position in the machine tree reads it off the message it already has; the topic is an address on
the bus, not a copy of the machine.

That field is on **every** update, not only the ones whose channel was shortened. A path that
appears when it was truncated and vanishes when it was not is worse than useless: it makes the
common case the exception, and every reader has to write the branch. Presence costs one small
string per message; absence costs a conditional in every consumer, forever.

The alternative — publishing the whole path and letting the topic be refused — is what the ceiling
does on its own, and it is the worst outcome: the signal simply never appears, which is
indistinguishable from a machine that has nothing to say. Shortening a channel is visible in the
topic; silence is not.

## Quality is structural, not adapter discipline

Every `Reading` carries a `quality` normalized to `GOOD | BAD | UNCERTAIN`, plus the protocol's
native code in `quality_raw` for diagnosis. MTConnect's own `UNAVAILABLE` is never coerced to a
zero or an empty string — it publishes as an explicit `null` with `BAD` quality, because a signal
that just stops appearing on the bus is indistinguishable from one that has simply not changed,
while a signal published `BAD` is unambiguous. A `conditionBinding` folds a machine's own alarm state
into a *different* signal's quality without touching its value — `Warning` degrades it to
`UNCERTAIN`, `Fault` to `BAD`, and when more than one bound condition is active the worst one always
wins.

Condition state itself is kept per **activation**, not per data item: one condition may be asserted
several times concurrently, and what publishes — as the condition signal's value and through every
binding — is the aggregate across the set of activations. That is why clearing one of two faults
does not promote anything to `GOOD`, and why the outcome of a mixed batch cannot depend on the
order the agent wrote the XML. The details and the extras that ride along (`conditionId`,
`activeConditions`) are in [reference/data-types.md](reference/data-types.md#conditions-state-as-value-and-as-a-quality-modifier).

## Passive quality: a silent agent cannot leave GOOD values behind

MTConnect is on-change, so a value's own age proves nothing — an unchanged reading under a live
agent is simply a machine that is not moving. What a consumer must never be left with is a `GOOD`
value whose agent has stopped vouching for it. The adapter therefore judges held values against the
**liveness clock** — time since the agent last delivered a Streams document or answered a
`/current` — and republishes them with degraded verdicts as that clock crosses thresholds:
`UNCERTAIN` past one missed heartbeat or two missed polls, `BAD` past
`healthThresholds.staleSignalSecs`, `BAD` immediately when the connectivity authority says the link
is gone, and a verbatim restoration of the held verdict when liveness returns. Each synthetic
reading carries a `passive` marker so it can never be mistaken for agent data, and the transitions
bypass batch windows — quality motion is news. The full ladder and its tokens
(`MTC_STALE:<ageMs>`, `MTC_AGENT_UNREACHABLE`) are specified in
[reference/data-types.md](reference/data-types.md#passive-quality--held-values-under-a-silent-agent).

## Metrics: one canonical floor, the worked families on top

Every adapter — whatever the protocol — emits `southbound_health` with the **exact** canonical
measure set (`connectionState`, `publishLatencyMs`, `pollLatencyMs`, `readErrors`, `staleSignals`,
`reconnects`, `writeErrors`, `signalsSubscribed`), so a fleet dashboard has one health metric that
means the same thing everywhere; `writeErrors` is structurally always `0` here, since there is no
device write path, and stays only for cross-adapter dashboard uniformity. On top of that floor,
`src/metrics.rs` ships the connect/reconnect lifecycle (`MtconnectAdapterConnection`), the `sb/*`
command surface (`MtconnectAdapterCommand`), and the publish-shaping engine's activity
(`MtconnectAdapterShaping`), plus three MTConnect-specific families (HLD §9):
`MtconnectStream` and `MtconnectProbe`, each emitted **once per configured agent** rather than once
per device (an agent's own connection and document flow exist exactly once no matter how many
devices attach to it), and `MtconnectParse`, emitted per device instance. See
[reference/metrics.md](reference/metrics.md) for the exact measures. Every dimension across every
family is deliberately low-cardinality: `instance`, `agentId`, `verb`, `result` — never a
`dataItemId`, a sequence number, or an agent URL, which would be unbounded and shred a dashboard.

## Instance connectivity: one provider, two surfaces

`App::run` registers **one** instance-connectivity provider per device. The library reads it twice:
it pushes the same sample into every `state` keepalive's `instances[]` array (push), and it returns
the very same sample from the built-in `sb/status` verb (pull). A console that watches the keepalive
and a console that asks `sb/status` cannot get different answers, because there is only one source.
The `connected` field is the **normalized** flag every console renders a health dot from; `state` is
this adapter's own richer vocabulary (`CONNECTING` / `ONLINE` / `BACKOFF`, and `PAUSED` when paused
while online) — a boolean alone cannot distinguish "still trying" from "administratively paused".

Behind both surfaces sits **one connectivity authority**: the agent runtime's own connected flag,
written only by the acquisition path when it ingests documents or marks the agent down. An
`mtconnect`-adapter instance reports `CONNECTING` (or `BACKOFF`) — not `ONLINE` — until the shared
acquisition has ingested its first Streams document, because a cached probe model proves the agent
answered once, not that it is delivering now. `sb/status`, the keepalive's `instances[]`, and the
`device-connected` event all mirror that one flag; nothing above the runtime re-derives liveness
from its own bookkeeping, so the surfaces cannot disagree. A command-path snapshot that succeeds
refreshes the liveness clock — the agent vouched for currency — but never flips `connected` on its
own.

## Lifecycle: shutdown is structured and bounded

Stopping the component is a staged drain, not an abort: device tasks flush their open shaping
windows, publish, and detach first; then the agent runtimes and the metric tickers stop; then the
final metric flush runs. Each stage has its own budget (6 s + 4 s + 2 s — 12 s worst case), every
spawned task's handle is joined under it, and whatever overruns is aborted **and named** in the
log, so a wedged task is a diagnosis rather than a hang. A clean stop is not an incident: it raises
no `device-unreachable` event and counts no reconnect.

## Command routing, and why nothing is writable

The command surface rides the library's inbox, which subscribes both cmd wildcards
(`ecv1/{device}/mtconnect-adapter/cmd/#` and `ecv1/{device}/mtconnect-adapter/+/cmd/#`). Every `sb/*`
verb declares scope `instance`, so the **library** resolves the addressing before a handler runs: the
topic's instance token is authoritative, a component-addressed request may name the device in the
body instead, and a body `instance` disagreeing with the topic token is `BAD_ARGS`. `commands.rs`
receives the resolved instance and only does the part that needs this component's own configuration —
an instance it has no device for is `NO_SUCH_INSTANCE`, and an unnamed one is the sole configured
device, or `BAD_ARGS` once there are two or more.

`sb/write` is registered and permanently refused. MTConnect's API is read-only by specification
(Part 1 Fundamentals §5.1): an agent serves observations, and there is no write path to allow-list.
The verb stays on the surface so a caller gets a standard, explanatory `WRITE_NOT_ALLOWED` instead
of "unknown verb", the refusal precedes any inspection of the request, and the same fact is
advertised through command availability (`unsupported`) so a console never renders a write control.
`writes.allow` is pinned to the empty array by the configuration schema, so no configuration can
disagree with the protocol. There is no `sb/discover` either — the address space is read from the
cached probe model above, never mutated.

## Appendix — revision history

| Date | Change |
|---|---|
| 2026-08-03 | The delivery-lanes, connectivity-authority, passive-quality, and bounded-shutdown models; resync-before-publish, mark-down scope, and establish-failure accounting in the ladder; condition-activation aggregation. |
| 2026-07-28 | Initial version. |
