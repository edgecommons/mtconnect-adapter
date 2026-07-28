# Sample Configurations

Four configurations: the shipped simulator config, the shipped real-agent config explained
option-by-option, a non-trivial multi-device/secured variant, and the probe-derived selection
shapes. For the exhaustive option list see
[reference/configuration.md](reference/configuration.md); for message shapes see
[reference/messaging-interface.md](reference/messaging-interface.md).

The adapter loads **one JSON document** from `-c/--config`. The top level carries `component`
(this adapter's own config) plus the standard `edgecommons` sections: `tags`, `hierarchy`,
`identity`, `messaging`, `metricEmission`, `logging`, `heartbeat`.

---

## 1. The shipped `test-configs/config.json` (simulator)

The quickest way to see the adapter running with no agent at all:

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

No `component.global.agents[]` here — a pure `sim` deployment needs no agent. Run it:

```bash
cargo run -- --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json -t my-thing
```

---

## 2. The shipped `test-configs/mtconnect.json` (real MTConnect agent)

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
      "agents": [
        {
          "id": "line-a-agent",
          "url": "http://127.0.0.1:5000",
          "streaming": "poll-only",
          "pollIntervalMs": 1000,
          "requestTimeoutMs": 10000,
          "heartbeatMs": 10000
        }
      ],
      "defaults": { "pollIntervalMs": 1000, "publishMode": "on-change", "maxDocumentBytes": 16777216 },
      "healthThresholds": { "staleSignalSecs": 30 }
    },
    "instances": [
      {
        "id": "cnc-1",
        "adapter": "mtconnect",
        "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
        "pollIntervalMs": 1000,
        "signals": [
          { "id": "x-position", "name": "X axis actual position", "dataItemId": "Xabs", "conditionBinding": ["Xtravel"] },
          { "id": "x-load", "name": "X axis load", "dataItemId": "Xload" },
          { "id": "spindle-speed", "name": "Spindle speed", "dataItemId": "Sspeed" },
          { "id": "execution", "name": "Execution state", "dataItemId": "execution" },
          { "id": "x-travel-condition", "name": "X axis travel condition", "dataItemId": "Xtravel" }
        ],
        "writes": { "allow": [] }
      }
    ]
  }
}
```

Point `url` at whatever agent you actually have running (e.g. `http://127.0.0.1:5010` for the
compose harness in the [tutorial](tutorial.md#7-run-it-against-a-real-mtconnect-agent)) before
running it.

**What each option does at runtime**

| Option | Effect |
|--------|--------|
| `agents[0].id` | The stable id `connection.agentId` below references. |
| `agents[0].url` | The agent's base URL — no userinfo, no credentials embedded; see variant 3 for `auth`/`tls`. |
| `agents[0].streaming: "poll-only"` | This particular agent is read on a fixed cadence, never via a multipart stream — a deliberate, simple choice for this fixture. Omit the key (or set `"prefer"`) to stream instead. |
| `agents[0].pollIntervalMs` | How often `line-a-agent`'s `/current` is read on the polling path. |
| `agents[0].heartbeatMs` | The streaming liveness window (unused while this agent is `poll-only`, but validated regardless). |
| `component.global.healthThresholds.staleSignalSecs` | A signal with no update for longer than this counts toward `southbound_health.staleSignals`. |
| `instances[0].connection.agentId` / `.deviceUuid` | Which agent, and which `Device/@uuid` on it, this instance represents. The published endpoint (`mtconnect://127.0.0.1:5000/OKUMA.123456`) is **derived** from these two — never itself configured. |
| `instances[0].pollIntervalMs` | Per-device override of the drain-and-publish cadence (independent of the agent's own `pollIntervalMs` above). |
| `signals[].dataItemId` | The MTConnect `DataItem/@id` each signal reads. `x-position`'s value comes from `Xabs`; its quality also reflects `Xtravel`'s condition state (next row). |
| `signals[].conditionBinding` | `x-position` degrades to `UNCERTAIN`/`BAD` whenever `Xtravel` (a CONDITION data item) reports `Warning`/`Fault` — the value is untouched. `x-travel-condition` publishes that same data item's own state directly, as its own signal — both are valid, and independent of each other. |
| `writes.allow` | Empty, and schema-pinned to stay that way: MTConnect has no write path. |

---

## 3. A non-trivial variant: two devices on a secured agent, streaming

Two devices behind one TLS/authenticated agent, one polling and one streaming:

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
      "agents": [
        {
          "id": "pumphouse-agent",
          "url": "https://agent.pumphouse.example.com",
          "auth": { "type": "basic", "username": "svc-mtconnect", "secretRef": "pumphouse/agent-password" },
          "tls": { "caSecretRef": "pumphouse/agent-ca-bundle" },
          "streaming": "prefer",
          "heartbeatMs": 10000
        }
      ],
      "healthThresholds": { "staleSignalSecs": 15 }
    },
    "instances": [
      {
        "id": "skid-1",
        "adapter": "mtconnect",
        "connection": { "agentId": "pumphouse-agent", "deviceUuid": "PUMP.SKID.001" },
        "signals": [
          { "id": "suction-pressure", "dataItemId": "SuctionPress" },
          { "id": "skid-fault", "dataItemId": "SkidFault" }
        ]
      },
      {
        "id": "skid-2",
        "adapter": "mtconnect",
        "connection": { "agentId": "pumphouse-agent", "deviceUuid": "PUMP.SKID.002" },
        "pollIntervalMs": 2000,
        "signals": [
          { "id": "suction-pressure", "dataItemId": "SuctionPress", "conditionBinding": ["SkidFault"] }
        ]
      }
    ]
  }
}
```

**How this behaves differently from the shipped config**

- **One agent, two devices, three tasks.** `pumphouse-agent`'s HTTP connection and stream/poll cycle
  exist exactly once; `skid-1` and `skid-2` each get their own drain-and-publish task and their own
  entry in the `state` keepalive's `instances[]` array — one going down (a bad `dataItemId`, a pause)
  never affects the other or the shared agent connection.
- **Credentials never appear here.** `auth.secretRef`/`tls.caSecretRef` are vault references, resolved
  once at startup — the password and CA bundle themselves live in the credential vault, not in this
  file, logs, or `sb/status`.
- **`instance` becomes required.** With two devices configured, `sb/status`/`sb/read`/`sb/browse`/etc.
  **must** name one (`BAD_ARGS` if missing, `NO_SUCH_INSTANCE` if the name is not configured).
- **`skid-2` binds its own condition to itself, differently.** `skid-2`'s `suction-pressure` degrades
  quality when *its own* `SkidFault` condition trips; `skid-1` publishes the same-shaped signal with
  no such binding — each instance's `conditionBinding` is independent, even reading the same
  `dataItemId` name across two different physical devices.
- **`skid-2` polls slower** (`pollIntervalMs: 2000` overrides nothing since the agent has no
  `pollIntervalMs` set at the instance level for `skid-1`, which falls back to
  `component.global.defaults.pollIntervalMs`, `5000` by built-in default).
- **A shorter staleness window** (`staleSignalSecs: 15`) trips `southbound_health.staleSignals` sooner
  if a device stops updating.
- **`metricEmission.target: "messaging"`** puts `southbound_health` and the operational families on
  the UNS `metric` class instead of a log file, so `mosquitto_sub -t 'ecv1/+/+/+/metric/#' -v` shows
  them directly.

Run it the same way, pointing `-c FILE` at this file instead.

---

## 4. Probe-derived selection: publish a device without naming its signals

The smallest possible MTConnect instance — an identity, a binding, and "publish it all". Every
data item the agent's `/probe` reports publishes with a derived id, name, and channel; each
non-condition signal is automatically bound to its own component's CONDITION items:

```jsonc
{
  "hierarchy": { "levels": ["site", "device"] },
  "identity": { "site": "factory-1" },
  "messaging": { "local": { "type": "mqtt", "host": "localhost", "port": 1883 } },
  "component": {
    "global": {
      "agents": [ { "id": "line-a-agent", "url": "http://127.0.0.1:5000" } ]
    },
    "instances": [
      {
        "id": "cnc-1",
        "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
        "selection": { "mode": "all" }
      }
    ]
  }
}
```

A data item `Xabs` on `Axes/Linear[X]` publishes as signal `xabs` on the channel
`axes/linear-x/xabs`; the derived set is capped at `maxSignals` (500 by default) and truncation is
announced, never silent.

Matchers scope the selection instead of taking everything, and explicit `signals[]` entries pin
the identities that matter — overriding the derived entry for the same `dataItemId`, field by
field:

```jsonc
"instances": [
  {
    "id": "cnc-1",
    "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
    "signals": [
      // Pinned: a stable id and channel for the signal the historian keys on. Its unset fields
      // (here: conditionBinding) still take the derived values.
      { "id": "x-position", "channel": "machining/x", "dataItemId": "Xabs" }
    ],
    "selection": {
      "mode": "include",
      "include": [
        { "category": "SAMPLE", "path": "Axes/**" },     // every axis sample ...
        { "type": "EXECUTION|PROGRAM" }                  // ... plus the controller state events
      ],
      "exclude": [ { "idMatch": ".*-debug" } ],          // exclude wins over include
      "maxSignals": 200
    }
  }
]
```

Fields within one matcher are ANDed, matchers in the list are ORed, and regexes are anchored
(`POSITION` does not match `PATH_POSITION`). Derived identities follow the machine's own model —
pin an explicit entry for anything whose id must survive a machine reconfiguration; see
[explanation.md](explanation.md#derived-identity-is-a-trade).

---

## Northbound: getting device data to the cloud

Everything above publishes to the **local bus** (Greengrass IPC, or the local MQTT broker on
HOST/Kubernetes). To also carry the adapter's own operational telemetry (heartbeat,
`southbound_health`, the operational metric families) to AWS IoT Core, add a `messaging.northbound`
block and set `heartbeat.destination`/`metricEmission.targetConfig.destination` to `"northbound"` —
see the core library's platform docs for the dual-MQTT provider shape. The adapter does not push
polled signal data off-box itself; that is a deployment choice for a separate consumer of the local
`data` topics (a bridge, or the library's streaming subsystem for high-volume forwarding).
