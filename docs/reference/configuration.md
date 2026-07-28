# Reference — Configuration

*This documents the generated scaffold; rewrite it as you build the component out.*

Every option `MtconnectAdapter` itself understands. For *why*, see
[explanation.md](../explanation.md); for tasks, the [how-to guides](../how-to-guides.md); for
worked examples, [sample-configurations.md](../sample-configurations.md).

## Config source

The adapter reads one JSON document from `-c/--config`, defaulting by platform: `HOST` → `FILE`,
`GREENGRASS` → `GG_CONFIG`, `KUBERNETES` → `CONFIGMAP`. This component's own settings live under
`component`; the sibling sections (`tags`, `hierarchy`, `identity`, `messaging`, `logging`,
`metricEmission`, `heartbeat`) are standard `edgecommons` sections, owned by the canonical schema
(`schema/edgecommons-config-schema.json`) and not redeclared here.

## `component.global`

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `defaults.pollIntervalMs` | integer | `5000` | Fallback read cadence for a device that does not set its own. |
| `timeouts.connectMs` | integer | `5000` | How long a connect attempt may take before it is treated as failed. |
| `timeouts.reconnectBackoffMinMs` | integer | `1000` | The first reconnect window. |
| `timeouts.reconnectBackoffMaxMs` | integer | `60000` | The reconnect ceiling; backoff is jittered within the window. |
| `healthThresholds.staleSignalSecs` | integer | `30` | A signal with no update for longer than this counts toward `southbound_health.staleSignals`. |

## `component.instances[]` (one device each)

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `id` | string | **required** | Unique device id; the `{instance}` token of this device's UNS topics and the `instance` metric dimension. Must be lower-kebab (`^[a-z0-9]+(?:-[a-z0-9]+)*$`). |
| `adapter` | string | `"sim"` | Which `DeviceBackend` to use — matches `DeviceBackend::kind()` in `src/device.rs`. Published in every reading's `device.adapter` field. |
| `connection` | object | **required** | How to reach the device. Deliberately **open** (`additionalProperties: true`) — every protocol needs different keys. |
| `connection.endpoint` | string | **required** | The endpoint, in whatever form the protocol uses. Published in `device.endpoint`. The simulator only checks it is non-empty. |
| `pollIntervalMs` | integer | `component.global.defaults.pollIntervalMs` | Per-device override of the read cadence. |
| `writes.allow` | array of string | `[]` | Signal ids (matched on the **stable** `signal.id`, never a volatile index) this device may write. Empty = read-only. Checked before any device I/O. |

## Identity & the UNS device tree

`hierarchy.levels` names the enterprise tree, deepest (the device) last; `identity` supplies every
level's value **except** the last (which is always the resolved Thing name, `-t`). With the default
(`["device"]`), topics are `ecv1/{thing}/mtconnect-adapter/{instance}/...`.

```jsonc
"hierarchy": { "levels": ["site", "area", "device"] },
"identity":  { "site": "plant1", "area": "pumphouse" }
// -> identity.path = "plant1/pumphouse/<thing>"
```

## Precedence

`pollIntervalMs` resolves **device `pollIntervalMs`** ▸ **`global.defaults.pollIntervalMs`** ▸
built-in (`5000`).

## Complete example

```jsonc
{
  "hierarchy": { "levels": ["site", "device"] },
  "identity": { "site": "factory-1" },
  "messaging": { "local": { "type": "mqtt", "host": "localhost", "port": 1883 } },
  "metricEmission": { "target": "messaging" },
  "component": {
    "global": {
      "defaults": { "pollIntervalMs": 2000 },
      "healthThresholds": { "staleSignalSecs": 20 }
    },
    "instances": [
      {
        "id": "device-1",
        "adapter": "sim",
        "connection": { "endpoint": "sim://device-1" },
        "writes": { "allow": ["temperature-1"] }
      }
    ]
  }
}
```

## Limitations

- This scaffold's `connection` object carries no security/credential handling — add whatever your
  protocol needs (a certificate path, a `$secret` reference) as you extend the schema.
- `browse` (`sb/browse`) is unsupported until you override the seam's default — see
  [reference/data-types.md](data-types.md) and the [how-to guides](../how-to-guides.md).
