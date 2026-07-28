# MtconnectAdapter

A **southbound protocol adapter**: it connects to devices, reads signals, and publishes them onto
the UNS in the shape the rest of the fleet expects — so a consumer can chart a Modbus register and
an OPC UA node without knowing either protocol.

```text
  connect ──► poll ──► publish SouthboundSignalUpdate ──► report health
     ▲                                                         │
     └──────────── reconnect with backoff ◄────────────────────┘
```

> Full docs: [`docs/README.md`](docs/README.md). This template ships without a `Cargo.lock` (the
> scaffold generates offline, without a toolchain or network); commit it after your first build.

## Run it

```bash
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

It ships a **simulated backend**, so it runs with no hardware: it publishes a moving
`temperature-1` and a deliberately faulted `pressure-1` on
`ecv1/{device}/mtconnect-adapter/device-1/data/{signal}`.

## Where your code goes

`src/device.rs`. A protocol implements two traits:

```rust
#[async_trait]
pub trait DeviceSession: Send + Sync {
    async fn read_signals(&mut self) -> Result<Vec<Reading>>;
    async fn write_signal(&mut self, signal_id: &str, value: &Value) -> Result<()>;
    async fn close(&mut self);
}

#[async_trait]
pub trait DeviceBackend: Send + Sync {
    fn kind(&self) -> &'static str;
    async fn connect(&self, cfg: &ConnectionConfig) -> Result<Box<dyn DeviceSession>>;
}
```

**The boundary rule, worth enforcing in review:** a backend knows *protocols*. It does not know
EdgeCommons topics, the UNS, message envelopes, or metrics. If your `impl DeviceSession` imports
`edgecommons::uns`, the seam has leaked.

Replace `SimBackend` with your protocol. Everything above the seam — the connection lifecycle,
backoff, publishing, health, the command surface — is written against the traits and does not change.

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
`writeErrors`, `signalsSubscribed` — so an operator sees a link go down without reading logs. On top of it, `src/metrics.rs` ships the
**operational-family pattern** two families deep (`MtconnectAdapterConnection`,
`MtconnectAdapterCommand`) as worked examples, with a signposted place to add your protocol's own
`Inventory` / `Poll` / `Publish` families.

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

## Writes are allow-listed, and the list is empty by default

```json
{ "id": "device-1", "adapter": "sim",
  "connection": { "endpoint": "sim://device-1" },
  "pollIntervalMs": 5000,
  "writes": { "allow": [] } }
```

Only signal ids in `writes.allow` can be written, matched on the **stable `signal.id`** and checked
before the write ever reaches the device. Anything else is refused, whatever the command asks for.
An adapter that will write any address it is handed is a control-system vulnerability, not a
convenience — so the default is read-only, and opening it is a deliberate act.

A write is **confirmed**: the command's reply is the device's answer, not "we sent it".

`connection` is deliberately **open** — every protocol needs different keys (a unit id, a security
policy, a slave address). Everything else in `config.schema.json` is closed, so a typo is caught.

## The command surface (`src/commands.rs`)

The adapter serves the generic southbound `sb/*` family on its `commands()` inbox (SOUTHBOUND.md
§2.2): `sb/status`, `sb/read`, `sb/write`, `sb/signals`, `sb/browse`, `sb/pause`, `sb/resume`,
`reconnect`, `repoll`. All nine declare scope `instance`, so the library resolves the addressing —
the topic's instance token (`…/{instance}/cmd/{verb}`), else a body `instance`, a conflict between
them refused with `BAD_ARGS` — and this module maps the resolved instance onto a configured device
(unnamed means the sole one; required once more than one is configured). Every session-touching verb
is handed to the device's own task over a
control channel and confirmed through the reply that rides it, so the inbox never touches a live
connection. The same module registers three edge-console panels — `overview`, `signals`,
`diagnostics` — an adapter-overview summary with lifecycle bindings, a signal grid bound to
`sb/signals`, and a hierarchical inventory tree driving `sb/browse`'s `ref` mode. `repoll` is
refused with `PAUSED` while the instance is paused.
