# How-to Guides

*This documents the generated scaffold; rewrite it as you build the component out.*

Recipes for specific tasks. Each assumes the adapter builds and runs (see the
[tutorial](tutorial.md)). For concepts see [explanation.md](explanation.md); for exhaustive options
see [reference/](reference/).

---

## Replace the simulator with a real protocol

Everything lives behind two traits in `src/device.rs` — **this is the whole seam**:

```rust
#[async_trait]
pub trait DeviceSession: Send + Sync {
    async fn read_signals(&mut self) -> Result<Vec<Reading>>;
    async fn read_named(&mut self, ids: &[String]) -> Result<Vec<Reading>> { /* default: filter */ }
    async fn write_signal(&mut self, signal_id: &str, value: &Value) -> Result<()>;
    async fn browse(&mut self, cursor: Option<String>, max: usize) -> Result<BrowsePage, BrowseError> {
        Err(BrowseError::Unsupported)
    }
    async fn close(&mut self);
}

#[async_trait]
pub trait DeviceBackend: Send + Sync {
    fn kind(&self) -> &'static str;
    fn inventory(&self, cfg: &ConnectionConfig) -> Vec<SignalInfo> { Vec::new() }
    async fn connect(&self, cfg: &ConnectionConfig) -> Result<Box<dyn DeviceSession>>;
}
```

1. Implement `DeviceBackend`/`DeviceSession` for your protocol (a new `mod` next to `device.rs`, or
   inline — the scaffold keeps `SimBackend`/`SimSession` there as the worked example).
2. Register it in `src/supervisor.rs`'s `App::backend_for()`, matching on `cfg.adapter` (the config `adapter`
   field, e.g. `"modbus"`, `"opcua"` — whatever string you choose).
3. Decide what a **transient** vs **permanent** [`DeviceError`] is for your protocol —
   `connect` returning `Permanent` (a bad endpoint, a rejected credential) makes the supervisor back
   off to its ceiling immediately instead of hammering a config error every second.
4. If your protocol supports discovery, override `browse`; if it doesn't, leave the default — an
   honest `BROWSE_UNSUPPORTED` beats a fake empty page.
5. Extend `ConnectionConfig`'s config schema (`config.schema.json`'s `connection` object is
   deliberately open — `additionalProperties: true`) with whatever keys your protocol needs (a unit
   id, a security policy, a port).

**The boundary rule, worth enforcing in review:** a backend knows protocols. It does **not** know
EdgeCommons topics, the UNS, message envelopes, or metrics. If your `impl DeviceSession` imports
`edgecommons::uns`, the seam has leaked — everything above `src/device.rs` (`src/app.rs`, `src/supervisor.rs`,
`src/commands.rs`, `src/metrics.rs`) is written against the trait and never changes for a new
protocol.

---

## Add your protocol's metric families

`src/metrics.rs` ships `southbound_health` (the canonical set, do not change it) plus two worked
operational families, `MtconnectAdapterConnection` and `MtconnectAdapterCommand`. Your protocol
almost certainly wants more: an **inventory** (configured signals per table/group), a **poll** family
(read attempts, decode errors, samples good/bad), and a **publish** family (messages published,
publish latency). Add them next to the two worked families in `family_defs()`:

```rust
// out.push(FamilyDef {
//     name: format!("{}Poll", "MtconnectAdapter"),
//     dimensions: dims(&["instance", "result"]),
//     measures: { let mut m = pair_defs("pollCycles"); m.extend(pair_defs("protocolReadErrors")); m },
// });
```

Pre-define the new family in `DeviceMetrics::define_all`, call its `on_*` recording method from
wherever the event happens (a poll, a decode), and drain it in `emit_periodic`. `modbus-adapter`'s
`ModbusPoll`/`ModbusInventory`/`ModbusPublish` and `ethernet-ip-adapter`'s equivalents are the fully
worked reference — copy their shape, rename the dimensions to your protocol's vocabulary.

**Keep every dimension low-cardinality.** `instance`, `verb`, `result`, a poll-group id, a table
name — never a signal name, an address, an endpoint URL, or error text. Those belong in `data`,
`evt`, logs, or command replies, not in a CloudWatch/Prometheus dimension set (an unbounded
dimension shreds a fleet dashboard).

---

## Tune polling

| You want… | Change |
|-----------|--------|
| Faster/slower reads | `component.instances[].pollIntervalMs` (or `component.global.defaults.pollIntervalMs`) |
| Faster/slower reconnects | Backoff is currently a fixed `Backoff::default()` (`base_ms: 1000, max_ms: 60000`) in `src/app.rs` — expose it as config if your deployments need different windows per device |
| A signal to count as stale sooner/later | `component.global.healthThresholds.staleSignalSecs` (feeds `southbound_health.staleSignals`) |

---

## Read signals from a client

Reads ride the library **command inbox** (`ecv1/{device}/mtconnect-adapter/cmd/{verb}`). See
[reference/messaging-interface.md](reference/messaging-interface.md) for every payload shape; the
short version:

```text
publish ecv1/<device>/mtconnect-adapter/cmd/sb/read
  {"header":{"name":"sb/read","reply_to":"app/r","correlation_id":"1"},
   "body":{"instance":"cnc-1","signals":[{"signalId":"x-position"}]}}
```

The reply is a scoped `/current` snapshot (`"mode": "current"`) with one entry per requested signal.
An entry the agent cannot serve comes back `BAD` with its own code (`MTC_UNAVAILABLE`,
`MTC_NO_SUCH_DATAITEM`, `MTC_AGENT_ERROR:<code>`) while the command itself stays `ok`.

Writing is not possible: MTConnect's API is read-only by specification, so `sb/write` answers
`WRITE_NOT_ALLOWED` for every request and advertises itself as `unsupported` on `describe`.

---

## Bridge several devices from one adapter

Add another entry to `component.instances[]` — each device gets its own task, its own connection
lifecycle, and its own entry in `state.instances[]`:

```jsonc
"instances": [
  { "id": "device-1", "adapter": "sim", "connection": { "endpoint": "sim://device-1" } },
  { "id": "device-2", "adapter": "sim", "connection": { "endpoint": "sim://device-2" } }
]
```

With two or more devices, `instance` becomes **required** in every command body (`BAD_ARGS` if
missing, `NO_SUCH_INSTANCE` if unrecognized) — the single-device convenience only applies when
exactly one is configured.

---

## Deploy to a platform

**HOST:**
```bash
cargo run -- --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json -t my-thing
```

**Greengrass:** the on-device build uses the GDK custom build system
(`gdk-config.json` → `build.sh`, which compiles with `--features greengrass` — Linux-only, the SDK
is a C-FFI crate needing `libclang`).
```bash
gdk component build
gdk component publish
```
If `gdk-config.json`'s `publish.bucket` still carries the `edgecommons-set-artifact-bucket`
sentinel, set a real S3 bucket first — `edgecommons component validate` errors on the sentinel.

**Kubernetes:** build the image, push or `kind load` it, set `image:` in `k8s/deployment.yaml`,
then `kubectl apply -f k8s/`. With `--platform auto` the library detects Kubernetes from the
ServiceAccount token, so the container needs no CLI args — config comes from the mounted
ConfigMap, identity from the Downward API.

---

## Wire CI

`.github/workflows/ci.yml` calls the org's reusable `component-ci.yml` (build/test/clippy) plus an
in-repo `coverage` job (`cargo llvm-cov --fail-under-lines 90`). Push the generated repo to GitHub
under the `edgecommons` org (or point `uses:` at your own fork of `.github`) and add the
`EDGECOMMONS_READ_TOKEN` secret if your dependency form needs it (a `pinned-rev`/`registry` git
dependency does; `local` does not, since CI would need the sibling checkout too — CI generally
targets `registry`/`pinned-rev`, not `local`).

Commit `Cargo.lock` after your first build — the template ships without one (generating one
requires the toolchain and, for a `registry`/`pinned-rev` dependency, network access, which the
scaffold itself deliberately never uses). `edgecommons component validate` warns if it is missing.

`.github/workflows/deploy-docs.yml` is a no-op until the repo carries the
`CLOUDFLARE_DEPLOY_HOOK` secret and is registered in `registry/components.json` — harmless either
way, and one less file to hand-write later.
