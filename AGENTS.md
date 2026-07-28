# MtconnectAdapter — component notes

EdgeCommons **southbound protocol adapter** (Rust). Full name `com.mbreissi.edgecommons.MtconnectAdapter`, crate/binary
`mtconnect-adapter`. Depends on the `edgecommons` Rust library. If this repo lives inside the EdgeCommons
org umbrella workspace, read its root `AGENTS.md` first (org repo map, design-fidelity contract,
validation matrix, platform/transport model); everything below is this component's own detail.

## What it is

Connects to devices, reads signals, and publishes them onto the Unified Namespace (UNS) in the
shape the rest of the fleet expects: `SouthboundSignalUpdate` on the `data` class, the canonical
`southbound_health` metric plus two worked operational families, and the generic `sb/*` command
family (SOUTHBOUND.md §2.2 equivalent) on the command inbox. Ships with a simulated device backend
(`src/device.rs`'s `SimBackend`) so it runs with no hardware. Runs on `GREENGRASS` / `HOST` /
`KUBERNETES` via `edgecommons` — no platform branching in this component's own code.

## The seam

`src/device.rs`'s `DeviceSession`/`DeviceBackend` trait pair is the one place protocol knowledge
lives. Everything above it (`src/supervisor.rs`'s connect/poll/backoff supervisor, `src/commands.rs`'s
`sb/*` verbs, `src/metrics.rs`'s families) is written against the trait and does not change when a
new protocol is added. **The boundary rule:** a backend knows protocols; it does not know
EdgeCommons topics, the UNS, envelopes, or metrics.

`DeviceSession` is one live connection to one device. Two methods are required — `read_signals`
(the acquisition cycle) and `write_signal` — and the rest carry defaults, so a new backend
implements only what its protocol actually has: `read_named` (default: read all, filter) serves
`sb/read`, `browse` (default: `Unsupported`) serves `sb/browse`, `snapshot_now` (default:
`read_signals`) serves `repoll`, `take_notices` (default: none) drains the runtime facts that
become `evt` messages, `served_signals` (default: the inventory size) feeds
`southbound_health.signalsSubscribed`, and `close` releases the connection. `DeviceBackend` opens
sessions: `kind` names the `adapter` token, `inventory` (default: empty) reports a device's
configured signals before any connection exists, and `connect` returns a session. Two backends
implement the pair: `MtcBackend`/`MtcSession` (`adapter: "mtconnect"`, the default) over the owned
client in `src/mtconnect/**`, and `SimBackend`/`SimSession` (`adapter: "sim"`).
`MtcSession::write_signal` always refuses.

## Config location

This component's own settings live under `component.global` / `component.instances[]` in the
EdgeCommons config document (`config.schema.json` is the contract); the sibling sections (`tags`,
`hierarchy`, `identity`, `messaging`, `metricEmission`, `logging`, `heartbeat`) are the standard
`edgecommons` envelope, owned by the canonical schema and not redeclared here. `test-configs/`
carries a runnable example.

## Validation expectations

- `cargo test` covers every module against the simulator, a mocked device-control channel, and a
  fake in-process agent (canned XML documents) — no network, no broker, no live device required.
  `tests/poll_acquisition.rs` drives the polling path end to end; `tests/stream_acquisition.rs` and
  `tests/stream_sequence.rs` drive the streaming path and the resync ladder; `tests/config_schema.rs`
  validates every shipped configuration against `config.schema.json` and through
  `mtconnect::config::validate_bindings`; `tests/fuzz_style.rs` exercises the XML parser against
  malformed/hostile input; `tests/isolation.rs` enforces the seam rule below.
- `cargo llvm-cov --fail-under-lines 90` is the coverage gate (`.github/workflows/ci.yml`'s
  `coverage` job) — the org rule is 90% line coverage per language. The `ethernet-ip-adapter`
  discipline is followed: the untestable live drivers are isolated in a thin `src/supervisor.rs`
  seam (the connect/poll/reconnect loop that `.await`s a live session), and the coverage job passes
  `--ignore-filename-regex '(supervisor\.rs|main\.rs|tests[/\\]live_.*\.rs)'` so ONLY that seam plus
  the binary shim and the self-skipping live suite are excluded — each pinned to a reason in the
  workflow. Every pure decision they compose (backoff, the write allow-list, connectivity, the
  metric-family math, XML parsing, the probe model, the sequence/resync state machine) stays in
  `app.rs`/`commands.rs`/`device.rs`/`metrics.rs`/`mtconnect/**`, in the denominator, and is
  unit-tested. Do not lower the gate or exclude testable code to pass it — add tests.
- `tests/scoped_delivery.rs` drives the `sb/*` surface end to end through a **real** `CommandInbox`
  over a recording messaging seam: both cmd wildcards, the topic instance token selecting a device
  among several, the library refusing a conflicting body `instance` before dispatch (handler not
  invoked), and `describe` advertising every verb as `"scope": "instance"`. It is the guard on the
  addressing invariant above.
- `tests/live_sim.rs` is a **self-skipping** live suite, gated on `EC_LIVE_SIM` — it must show as
  skipped in a normal `cargo test` and pass when pointed at a real simulator/device.
- `tests/agent_integration.rs` is a **second, separately env-gated** live suite — set `EC_MTC_AGENT`
  (and optionally `EC_MTC_AGENT_TINY`) after starting `docker compose -f
  tests/compose.mtconnect-agent.yaml up -d`, which brings up the pinned canonical test peer
  (`mtconnect/agent`, i.e. cppagent 2.7.0.12 — D-MTC-9) with two fixtures: a two-device agent for
  probe/stream/restart/`instanceId`-resync/demultiplexing coverage, and a tiny-buffer agent
  (`BufferSize = 7`, 128 observations) for buffer-wrap/`OUT_OF_RANGE` recovery coverage. The SHDR
  feed both containers dial into is served in-process by the test binary itself (fixed host ports,
  reached via `host.docker.internal`), so no separate simulator process is needed. Without
  `EC_MTC_AGENT` every test in this file self-skips, so `cargo test` stays green with no Docker.
- `edgecommons component validate` checks this repo's config against `config.schema.json` and warns
  if `Cargo.lock` is not committed.

## Non-negotiable invariants (do not remove)

- **Instance addressing belongs to the library; this component only looks the device up.** All nine
  `sb/*` verbs register with `CommandScope::Instance`, and the inbox resolves the delivery's
  addressing before a handler runs (topic instance token authoritative, body `instance` folded in,
  a conflict between them refused with `BAD_ARGS`). `Commander::resolve` therefore takes the
  resolved `addressed_instance` and does only the configuration-dependent half: unknown instance ->
  `NO_SUCH_INSTANCE`, `None` -> the sole configured device, or `BAD_ARGS` with two or more. Do not
  reintroduce a `body.get("instance")` read in `commands.rs` — two addressing rules is one too many.
- **The keepalive reports instance state** (D-SC-7): `connectivity_of` stamps the same
  `CONNECTING`/`ONLINE`/`BACKOFF`/`PAUSED` token that answers `sb/status` onto the `state` keepalive's
  `instances[]` entries via `with_state`. One state model, every surface — never a second
  bookkeeping path.
- **The write allow-list is checked BEFORE any device I/O** — moot for the shipping protocol
  (`writes.allow` is schema-pinned to `maxItems: 0`, and `sb/write` refuses unconditionally before
  any entry is inspected — D-MTC-7), but the check still runs in that order so a future write-capable
  backend inherits the discipline rather than a special case.
- **The device seam stays protocol-only.** No EdgeCommons types in `src/device.rs`, and no
  `edgecommons` import anywhere under `src/mtconnect/**` — `tests/isolation.rs` enforces the latter
  mechanically (a source-text scan, not a trust exercise).
- **One state model per agent, shared by every device it serves.** `AgentRuntime` (`src/mtconnect/mod.rs`)
  owns exactly one `ArcSwap<AgentInfo>`, one cached `ProbeModel` per attached device uuid, and one
  `SequenceState`/`ParseCounters` pair for the whole agent connection. `sb/status` and `sb/browse`
  read this published state directly and never round-trip through a device's control channel — a
  status call cannot queue behind acquisition, and browsing keeps working while the agent link is
  down, because the address space came from the last successful probe, not from the wire.
- **Both MTConnect streaming content-types are accepted.** The multipart reader
  (`src/mtconnect/multipart.rs`) is built from the response's own `Content-Type` header and handles
  both `multipart/x-mixed-replace` (cppagent's historical framing) and
  `multipart/mixed` — an agent is free to send either, and the client does not assume one.
- **Every published observation carries a dedupe key, not a chronological guess.** The dedupe floor
  in `mtconnect::sequence::SequenceState` (`last_published`, keyed by `dataItemId`) is per data item,
  not a single global sequence cursor — a `/current` snapshot taken to recover from `OUT_OF_RANGE` or
  an `instanceId` resync overlaps the stream's own window, and a global floor would let one data
  item's higher sequence number silently suppress a different, still-unpublished, lower-sequence
  item. Do not collapse this back to one counter.
- **MTConnect's whole surface is read-only** (Part 1 Fundamentals §5.1, D-MTC-7): `sb/write` stays
  registered (so a caller gets `WRITE_NOT_ALLOWED`, not "unknown verb") but is refused before any
  entry is inspected, is advertised `unsupported` via command availability, and no panel names a
  `writeVerb`. There is no `sb/discover` either — the address space is read from the cached probe
  model, never mutated.

## Org conventions this scaffold inherits

- Southbound contract: a data point is a **signal**, never a "tag" (EdgeCommons envelope `tags` is
  unrelated business metadata).
- Writes are allow-listed by stable `signal.id`, checked before any device I/O; the default is
  read-only.
- Four-way parity: if this repo's Java/Python/TypeScript siblings exist, observable command/metric
  behavior should match — same verbs, same error codes, same measure names.
- Builders/facades are the construction path (`data()`, `events()`, `commands()`, `MetricBuilder`) —
  never hand-built topics or envelopes.
- Runtime artifacts (vaults, parameter caches, generated streams, TLS certs, logs, build output,
  local broker state) stay out of Git.
