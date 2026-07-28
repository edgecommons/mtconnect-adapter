# MtconnectAdapter

A **southbound MTConnect client**: it connects to one or more running MTConnect **Agents** over
HTTP, reads each agent's device model and observations, and publishes normalized EdgeCommons
signals onto the UNS in the shape the rest of the fleet expects — so a consumer can chart an
MTConnect data item next to a Modbus register or an OPC UA node without knowing any of the three
protocols. It is a client, not an agent: it serves no HTTP endpoints and keeps no sequence buffer of
its own. A deployment with machine tools and no agent installs the canonical `mtconnect/agent` next
to them; this component consumes it, the way the OPC UA adapter consumes a Kepware server.

```text
  connect (probe) ──► acquire (stream or poll) ──► publish SouthboundSignalUpdate ──► report health
     ▲                                                                                    │
     └───────────────────────── reconnect / resync with backoff ◄─────────────────────────┘
```

> Full docs: [`docs/README.md`](docs/README.md). Design contract: [`DESIGN.md`](DESIGN.md).

## Run it

Against the built-in simulator — no agent, no hardware:

```bash
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

It publishes a moving `temperature-1` and a deliberately faulted `pressure-1` on
`ecv1/{device}/mtconnect-adapter/device-1/data/{signal}`.

Against a real MTConnect agent, point `-c FILE` at `test-configs/mtconnect.json` instead (an
`agents[]` entry plus one `mtconnect`-adapter instance bound to it — see
[docs/sample-configurations.md](docs/sample-configurations.md)). `docker compose -f
tests/compose.mtconnect-agent.yaml up -d` stands up the pinned reference agent (`mtconnect/agent`,
the cppagent implementation) this repo's own test suite validates against.

## The device seam

`src/device.rs` defines the protocol boundary every backend implements:

```rust
#[async_trait]
pub trait DeviceSession: Send + Sync {
    async fn read_signals(&mut self) -> Result<Vec<Reading>>;
    async fn read_named(&mut self, ids: &[String]) -> Result<Vec<Reading>>; // default: read-all, filter
    async fn write_signal(&mut self, signal_id: &str, value: &Value) -> Result<()>;
    async fn browse(&mut self, cursor: Option<String>, max: usize) -> Result<BrowsePage, BrowseError>; // default: Unsupported
    async fn snapshot_now(&mut self) -> Result<Vec<Reading>>;   // default: read_signals  (`repoll`)
    fn take_notices(&mut self) -> Vec<Notice>;                  // default: none          (`evt`)
    fn served_signals(&self) -> Option<u64>;                    // default: the inventory size
    async fn close(&mut self);
}

#[async_trait]
pub trait DeviceBackend: Send + Sync {
    fn kind(&self) -> &'static str;
    fn inventory(&self, cfg: &ConnectionConfig) -> Vec<SignalInfo>; // default: empty
    async fn connect(&self, cfg: &ConnectionConfig) -> Result<Box<dyn DeviceSession>>;
}
```

**The boundary rule, worth enforcing in review:** a backend knows *protocols*. It does not know
EdgeCommons topics, the UNS, message envelopes, or metrics. `src/mtconnect/**` — the owned MTConnect
HTTP/XML client — imports nothing from `edgecommons`, enforced by `tests/isolation.rs`.

Two backends implement the pair today: `MtcBackend`/`MtcSession` (`adapter: "mtconnect"`, the
default) is the real client, built on `src/mtconnect/**`; `SimBackend`/`SimSession`
(`adapter: "sim"`) is the built-in simulator kept so the component runs on a laptop with no agent.
`MtcSession::write_signal` always refuses — see below.

## The contract this implements (`docs/SOUTHBOUND.md`)

**Publish through the `data()` facade, never by hand.** It constructs the `SouthboundSignalUpdate`
body (`{device, signal, samples}`), mints
`ecv1/{device}/{component}/{instance}/data/{signal}`, and stamps identity. A hand-rolled topic is a
topic that will eventually disagree with the envelope.

**Quality on every sample**, normalized to `GOOD | BAD | UNCERTAIN`, with the protocol's own status
code kept in `qualityRaw` for diagnosis. This is what lets a consumer gate on quality without
knowing your protocol — and it is why **a failed read is published as `BAD`, not swallowed**. A
signal that silently stops updating is indistinguishable from one that is simply not changing. The
simulator's `pressure-1` demonstrates exactly this.

**`southbound_health`, dimensioned by instance** — the canonical SOUTHBOUND.md §5 set:
`connectionState`, `publishLatencyMs`, `pollLatencyMs`, `readErrors`, `staleSignals`, `reconnects`,
`writeErrors`, `signalsSubscribed` — so an operator sees a link go down without reading logs. On top
of it, `src/metrics.rs` ships the generic connect/reconnect and command-surface families
(`MtconnectAdapterConnection`, `MtconnectAdapterCommand`) plus three MTConnect-specific families per
HLD §9: `MtconnectStream` and `MtconnectProbe` (dimensioned `agentId`, emitted once per shared agent,
not once per device), and `MtconnectParse` (dimensioned `instance`). See
[reference/metrics.md](docs/reference/metrics.md) for every measure.

**Per-instance connectivity, from one provider.** `App::run` registers an instance-connectivity
provider reporting one entry per configured device. The library reads it twice: it pushes the
sample into every `state` keepalive's `instances[]`, and it returns the same sample from the
built-in `status` command verb when a console asks. A watcher and an asker cannot get different
answers.

```json
{ "instance": "device-1", "connected": true, "state": "ONLINE",
  "detail": "sim://device-1", "attributes": { "adapter": "sim", "paused": false } }
```

`connected` is the **normalized** flag — always present, so a console renders a health dot without
knowing your protocol. `state` is this adapter's **own** vocabulary (`CONNECTING` / `ONLINE` /
`BACKOFF`), because a boolean cannot tell "reconnecting" from "administratively disabled".
`attributes` is an **open** bag for domain data, so what only your adapter understands rides along
without destabilizing the two fields every consumer relies on.

## Nothing is writable — MTConnect is a read-only protocol

```json
{ "id": "cnc-1", "adapter": "mtconnect",
  "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
  "writes": { "allow": [] } }
```

MTConnect's API is read-only by specification (Part 1 Fundamentals §5.1): an agent serves
observations, and there is nothing to write. `sb/write` stays registered — so a caller gets a
standard, explanatory `WRITE_NOT_ALLOWED` instead of "unknown verb" — and the refusal precedes any
inspection of the request, so no entry is ever resolved and nothing reaches a device. The same fact
is advertised through command availability (`unsupported`), so a console disables the surface
instead of offering a control that can never work, and `writes.allow` is pinned to the empty array
by the configuration schema.

`connection` is deliberately **open** — every protocol needs different keys (a unit id, a security
policy, a slave address). Everything else in `config.schema.json` is closed, so a typo is caught.

## The command surface (`src/commands.rs`)

The adapter serves the generic southbound `sb/*` family on its `commands()` inbox (SOUTHBOUND.md
§2.2): `sb/status`, `sb/read`, `sb/write`, `sb/signals`, `sb/browse`, `sb/pause`, `sb/resume`,
`reconnect`, `repoll`. All nine declare scope `instance`, so the library resolves the addressing —
the topic's instance token (`…/{instance}/cmd/{verb}`), else a body `instance`, a conflict between
them refused with `BAD_ARGS` — and this module maps the resolved instance onto a configured device
(unnamed means the sole one; required once more than one is configured). Every session-touching verb
is handed to the device's own task over a control channel and confirmed through the reply that rides
it, so the inbox never touches a live connection — while `sb/status` and `sb/browse` answer from the
agent runtime's *published* state (its document headers and cached probe model), so neither queues
behind acquisition and the address space stays browsable while the agent is unreachable.

The same module registers five edge-console panels — `overview` (a status dashboard, the lifecycle
action bar, and link-health metrics), `device-structure` (the probe tree over `sb/browse`'s `ref`
mode), `signals` (a signal grid bound to `sb/signals`), `conditions` (condition/data-loss/agent
events), and `diagnostics` (sequence and buffer state, agent events, stream metrics). None of them
advertises a write surface. `repoll` is refused with `PAUSED` while the instance is paused.
