# Tutorial — From scaffold to live values

*This documents the generated scaffold; rewrite it as you build the component out.*

By the end you will have built `com.mbreissi.edgecommons.MtconnectAdapter`, run it against its built-in device
simulator, watched a signal update flow onto the Unified Namespace (UNS), and read a value back
through the command surface. No hardware required — the scaffold ships with a **simulated backend**
(`SimBackend`, in `src/device.rs`) for exactly this reason.

## 1. Prerequisites

- A Rust toolchain (edition 2021, `rust-version = "1.85"` — matches the `edgecommons` library's MSRV).
- A local MQTT broker on `localhost:1883` (`docker run -d -p 1883:1883 emqx/emqx`).

## 2. Build it

```bash
cargo build
```

The `standalone` feature (dual-broker MQTT) is the default — this is the HOST-platform build you
use for local development. The Greengrass IPC build (`--features greengrass`) is Linux-only and is
what `build.sh` uses for the on-device artifact; you do not need it for this tutorial.

## 3. Run it

```bash
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

`test-configs/config.json` configures one device, `device-1`, using the `sim` backend
(`"adapter": "sim"`) at a poll interval of 5 seconds. You should see it connect immediately (the
simulator never fails to connect unless its endpoint is empty) and start polling.

## 4. Watch values flow

Subscribe to the UNS `data` class — one wildcard covers the whole fleet:

```bash
mosquitto_sub -t 'ecv1/+/+/+/data/#' -v
```

Every ~5 seconds you should see two `SouthboundSignalUpdate` messages on
`ecv1/my-thing/mtconnect-adapter/device-1/data/temperature-1` and
`ecv1/my-thing/mtconnect-adapter/device-1/data/pressure-1`:

- `temperature-1` carries a moving sine-wave value with quality `GOOD`.
- `pressure-1` is **deliberately faulted** — the simulator always reports it with quality `BAD` and
  `qualityRaw: "SENSOR_FAULT"`, and a `null` value. This is on purpose: a failed reading is
  published, not swallowed, so a consumer can tell "this signal is bad" from "this signal stopped
  existing".

Also try:

```bash
mosquitto_sub -t 'ecv1/+/+/+/state' -v      # the keepalive, with per-device connectivity
mosquitto_sub -t 'ecv1/+/+/+/metric/#' -v   # southbound_health + the two operational families
```

The `state` keepalive's `instances[]` array carries one entry for `device-1` —
`{ "instance": "device-1", "connected": true, "state": "ONLINE", "detail": "sim://device-1", ... }`
— fed by the same connectivity provider the built-in `status` command verb reads.

## 5. Read a signal on demand

The read/write/status surface rides the library's command inbox
(`ecv1/{device}/mtconnect-adapter/cmd/{verb}`). With a raw MQTT client, set `header.name` to the verb and
`header.reply_to`/`header.correlation_id` for the reply:

```text
publish ecv1/my-thing/mtconnect-adapter/cmd/sb/read
  {"header":{"name":"sb/read","reply_to":"app/r","correlation_id":"1"},
   "body":{"signals":[{"signalId":"temperature-1"}]}}
subscribe app/r  →  {"ok":true,"result":{"id":"device-1","reads":[
  {"signal":{"id":"temperature-1"},"value":21.7,"quality":"GOOD","qualityRaw":"unspecified"}]}}
```

## 6. Check status

```text
publish ecv1/my-thing/mtconnect-adapter/cmd/sb/status
  {"header":{"name":"sb/status","reply_to":"app/r","correlation_id":"2"},"body":{}}
subscribe app/r  →  {"ok":true,"result":{"id":"device-1","adapter":"sim","connected":true,
  "state":"ONLINE","paused":false,"endpoint":"sim://device-1","metrics":{...}}}
```

Only one device is configured, so `instance` is optional in the request body — the command surface
routes to the sole configured device automatically (add a second instance and it becomes required).

## 7. Prove it end-to-end

```bash
cargo test
```

Every module ships its own tests against the simulator and a mocked device-control channel — no
network, no broker, no device required. `src/commands.rs` covers every `sb/*` verb's happy path and
error code; `src/metrics.rs` proves `southbound_health` emits exactly the canonical measure set;
`src/device.rs` proves the simulator's contract (a failed read is `BAD`, not omitted).

Next: the [how-to guides](how-to-guides.md) for replacing the simulator with a real protocol,
tuning polling, and deploying; the [reference](reference/) for every option, topic, and metric; the
[explanation](explanation.md) for why the code is shaped the way it is.
