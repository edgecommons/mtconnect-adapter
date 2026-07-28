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
opens sessions. Everything above the seam — `src/app.rs`, `src/supervisor.rs`, `src/commands.rs`,
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
- One `tokio` task per **device** (`run_device` in `src/supervisor.rs`), which does not talk to the
  network itself — it drains the `InstanceEvent` queue the agent runtime fans out to it, publishes
  what it reads, and owns that device's own control channel for commands.

This split is what makes "a hundred devices, one agent" cheap and correct: the agent's HTTP
connection and its stream/poll cycle exist exactly once, no matter how many devices attach to it, and
one device's failure — a bad `dataItemId`, a paused publish — can never tear down another device's
session, because they only ever share *read* access to the agent's published state.

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
   reload, a different Devices.xml). The client re-probes every device attached to that agent,
   compares the new probe's content digest against the cached one, and only if it actually changed
   does it surface a model-drift event and recompile which signals bind — a data item that
   disappeared publishes as a permanent `BAD`/`MTC_NO_SUCH_DATAITEM` rather than being silently
   dropped or remapped to something else. Either way, acquisition then resumes with a fresh snapshot.

Deduplication is **per data item**, not one global counter: `SequenceState` tracks the last-published
sequence number for each `dataItemId` independently, because a `/current` snapshot taken to recover
from step 2 or 3 overlaps the stream's own window, and a single global floor would let one data
item's higher sequence number silently suppress a different, still-unpublished, lower-sequence item.

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

## Quality is structural, not adapter discipline

Every `Reading` carries a `quality` normalized to `GOOD | BAD | UNCERTAIN`, plus the protocol's
native code in `quality_raw` for diagnosis. MTConnect's own `UNAVAILABLE` is never coerced to a
zero or an empty string — it publishes as an explicit `null` with `BAD` quality, because a signal
that just stops appearing on the bus is indistinguishable from one that has simply not changed,
while a signal published `BAD` is unambiguous. A `conditionBinding` folds a machine's own alarm state
into a *different* signal's quality without touching its value — `Warning` degrades it to
`UNCERTAIN`, `Fault` to `BAD`, and when more than one bound condition is active the worst one always
wins.

## Metrics: one canonical floor, two worked families

Every adapter — whatever the protocol — emits `southbound_health` with the **exact** canonical
measure set (`connectionState`, `publishLatencyMs`, `pollLatencyMs`, `readErrors`, `staleSignals`,
`reconnects`, `writeErrors`, `signalsSubscribed`), so a fleet dashboard has one health metric that
means the same thing everywhere; `writeErrors` is structurally always `0` here, since there is no
device write path, and stays only for cross-adapter dashboard uniformity. On top of that floor,
`src/metrics.rs` ships the connect/reconnect lifecycle (`MtconnectAdapterConnection`) and the `sb/*`
command surface (`MtconnectAdapterCommand`), plus three MTConnect-specific families (HLD §9):
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
