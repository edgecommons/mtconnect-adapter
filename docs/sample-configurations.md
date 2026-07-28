# Sample Configurations

*This documents the generated scaffold; rewrite it as you build the component out.*

Two configurations: the shipped `test-configs/config.json` explained option-by-option, and a
non-trivial multi-device variant. For the exhaustive option list see
[reference/configuration.md](reference/configuration.md); for message shapes see
[reference/messaging-interface.md](reference/messaging-interface.md).

The adapter loads **one JSON document** from `-c/--config`. The top level carries `component`
(this scaffold's own config) plus the standard `edgecommons` sections: `tags`, `hierarchy`,
`identity`, `messaging`, `metricEmission`, `logging`, `heartbeat`.

---

## 1. The shipped `test-configs/config.json`

```jsonc
{
  "logging": { "level": "DEBUG", "rust_format": "{timestamp} [{level}] [{component}] {target} - {message}" },
  "hierarchy": { "levels": ["site", "device"] },
  "identity": { "site": "factory-1" },
  "heartbeat": { "enabled": true, "intervalSecs": 5, "measures": { "cpu": true, "memory": true }, "destination": "local" },
  "metricEmission": { "target": "log", "namespace": "edgecommons" },
  "tags": { "site": "factory-1" },
  "component": {
    "global": {
      "defaults": { "pollIntervalMs": 5000 },
      "timeouts": { "connectMs": 5000, "reconnectBackoffMinMs": 1000, "reconnectBackoffMaxMs": 60000 },
      "healthThresholds": { "staleSignalSecs": 30 }
    },
    "instances": [
      { "id": "device-1", "adapter": "sim", "connection": { "endpoint": "sim://device-1" },
        "pollIntervalMs": 5000, "writes": { "allow": [] } }
    ]
  }
}
```

**What each option does at runtime**

| Option | Effect |
|--------|--------|
| `logging.level` / `rust_format` | Standard edgecommons log level + format string. `DEBUG` here is intentionally verbose for local dev. |
| `hierarchy.levels` / `identity` | Places the component in the UNS enterprise tree: `identity.path = "factory-1/<thing>"`. The last hierarchy level's value is always the resolved Thing name (`-t`). |
| `heartbeat.*` | The `state` keepalive cadence and its `cpu`/`memory` system measures — independent of device polling. |
| `metricEmission.target: log` | Routes `southbound_health` and the two operational families to a local log file. Set `target: "messaging"` to see them on the UNS `metric` class instead. |
| `component.global.defaults.pollIntervalMs` | Fallback read cadence for any device that does not set its own. |
| `component.global.timeouts.*` | Connect timeout and reconnect backoff window (currently informational in the scaffold's fixed `Backoff::default()` — wire them through if you make backoff configurable). |
| `component.global.healthThresholds.staleSignalSecs` | A signal with no update for longer than this counts toward `southbound_health.staleSignals`. |
| `instances[].id` | The `{instance}` token of every UNS topic for this device, and the `instance` metric dimension. |
| `instances[].adapter` | Which `DeviceBackend` to use (`"sim"` ships; add your protocol's string when you register a real backend in `src/supervisor.rs::make_backend`). |
| `instances[].connection.endpoint` | Opaque to the framework; the simulator only checks it is non-empty. A real protocol reads whatever else it needs from this **open** object. |
| `instances[].pollIntervalMs` | Per-device override of the global default. |
| `instances[].writes.allow` | Empty — read-only by default. See variant 2 for opening it up. |

Run it:

```bash
cargo run -- --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json -t my-thing
```

---

## 2. A non-trivial variant: two devices, one writable

Two devices behind one adapter process, one of them with a writable signal and a faster poll rate:

```jsonc
{
  "tags": { "line": "5" },
  "hierarchy": { "levels": ["site", "area", "device"] },
  "identity": { "site": "plant1", "area": "pumphouse" },
  "logging": { "level": "INFO" },
  "messaging": { "local": { "type": "mqtt", "host": "localhost", "port": 1883, "clientId": "mtconnect-adapter-pumphouse" } },
  "metricEmission": { "target": "messaging" },
  "component": {
    "global": {
      "defaults": { "pollIntervalMs": 5000 },
      "healthThresholds": { "staleSignalSecs": 15 }
    },
    "instances": [
      {
        "id": "skid-1",
        "adapter": "sim",
        "connection": { "endpoint": "sim://skid-1" },
        "pollIntervalMs": 1000,
        "writes": { "allow": ["temperature-1"] }
      },
      {
        "id": "skid-2",
        "adapter": "sim",
        "connection": { "endpoint": "sim://skid-2" },
        "writes": { "allow": [] }
      }
    ]
  }
}
```

**How this behaves differently from the shipped config**

- **Two devices, two tasks.** `skid-1` and `skid-2` each get their own connect/poll loop and their
  own entry in the `state` keepalive's `instances[]` array. One going down does not affect the
  other.
- **`instance` becomes required.** With only one device configured, a command body may omit
  `instance`; with two, `sb/status`/`sb/read`/`sb/write`/etc. **must** name one (`BAD_ARGS` if
  missing, `NO_SUCH_INSTANCE` if the name is not configured).
- **`skid-1` polls 5× faster** (`pollIntervalMs: 1000` overrides the `global.defaults` value of
  `5000`), independent of `skid-2`, which inherits the default.
- **`skid-1.temperature-1` is writable.** A `sb/write` naming `temperature-1` on `skid-1` is
  allow-listed and reaches the simulator's `write_signal` (which just logs and accepts it); the same
  write addressed at `skid-2` — or at any other signal id on `skid-1` — is refused with
  `WRITE_NOT_ALLOWED` before any device I/O happens.
- **A shorter staleness window** (`staleSignalSecs: 15`) means `southbound_health.staleSignals`
  trips sooner if a device stops updating — useful when the deployment expects tighter freshness
  than the 30-second scaffold default.
- **`metricEmission.target: "messaging"`** puts `southbound_health` and the operational families on
  the UNS `metric` class instead of a log file, so `mosquitto_sub -t 'ecv1/+/+/+/metric/#' -v` shows
  them directly.

Run it the same way, pointing `-c FILE` at this file instead.

---

## Northbound: getting device data to the cloud

Everything above publishes to the **local bus** (Greengrass IPC, or the local MQTT broker on
HOST/Kubernetes). To also carry the adapter's own operational telemetry (heartbeat,
`southbound_health`, the operational metric families) to AWS IoT Core, add a `messaging.northbound`
block and set `heartbeat.destination`/`metricEmission.targetConfig.destination` to `"northbound"` —
see the core library's platform docs for the dual-MQTT provider shape. The adapter does not push
polled signal data off-box itself; that is a deployment choice for a separate consumer of the local
`data` topics (a bridge, or the library's streaming subsystem for high-volume forwarding).
