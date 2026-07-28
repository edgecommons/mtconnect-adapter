# Reference — Configuration

Every option `MtconnectAdapter` itself understands. `config.schema.json` is the source of truth; this
page documents it option by option. For *why*, see [explanation.md](../explanation.md); for tasks,
the [how-to guides](../how-to-guides.md); for worked examples,
[sample-configurations.md](../sample-configurations.md).

## Config source

The adapter reads one JSON document from `-c/--config`, defaulting by platform: `HOST` → `FILE`,
`GREENGRASS` → `GG_CONFIG`, `KUBERNETES` → `CONFIGMAP`. This component's own settings live under
`component`; the sibling sections (`tags`, `hierarchy`, `identity`, `messaging`, `logging`,
`metricEmission`, `heartbeat`) are standard `edgecommons` sections, owned by the canonical schema and
not redeclared here.

## `component.global.agents[]`

An agent is shared infrastructure — the canonical `mtconnect/agent` running next to the machines,
exactly as a Kepware server is shared infrastructure for the OPC UA adapter. Declare each one **once**
here; every instance that names it (`connection.agentId`) attaches to its one shared acquisition.
**Required** whenever any instance uses the `mtconnect` adapter — an instance whose `connection.agentId`
names no configured agent is refused at startup. Only a pure `sim` deployment may omit `agents[]`
entirely.

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `id` | string | **required** | Stable, unique agent id, lower-kebab (`^[a-z0-9]+(?:-[a-z0-9]+)*$`). Devices reference it through `connection.agentId`. |
| `url` | string | **required** | The agent's base URL, `http://` or `https://` only. Must **not** embed userinfo — credentials come from the vault through `auth`, never the URL. |
| `auth` | object | none (unauthenticated) | `{"type":"basic","username":"...","secretRef":"..."}` or `{"type":"bearer","secretRef":"..."}`. The referenced value is resolved through the EdgeCommons credential vault at startup and never appears in configuration, logs, or `sb/status`. |
| `tls.caSecretRef` | string | none | A vault reference to a PEM CA bundle to trust in addition to the platform roots. |
| `tls.certSecretRef` / `tls.keySecretRef` | string | none | A vault-referenced PEM client certificate + key, for mutual TLS. Set together or not at all — one without the other is a config error. |
| `heartbeatMs` | integer | `10000` | The `heartbeat` a streaming request asks the agent for, and the liveness window: silence for **twice** this long means the stream is dead even if the socket has not closed. |
| `streaming` | `"prefer"` \| `"poll-only"` | `"prefer"` | `prefer` opens a multipart `/sample?interval=...` stream and falls back to polling on repeated failure; `poll-only` never streams — it reads `/current` on `pollIntervalMs`. |
| `pollIntervalMs` | integer | `1000` | How often this agent's `/current` is read on the polling path (used directly in `poll-only` mode, and as the fallback cadence when streaming degrades). |
| `requestTimeoutMs` | integer | `10000` | Timeout for one-shot requests (`/probe`, `/current`, a windowed `/sample`). The long-lived streaming request is bounded by `heartbeatMs` instead — liveness, not a fixed deadline. |
| `maxDocumentBytes` | integer | `16777216` | Response/part size cap for this agent. A document (or one multipart part) larger than this is refused rather than buffered — checked against `Content-Length` up front and enforced again mid-stream for a body with none. |
| `reconnect.initialMs` / `reconnect.maxMs` | integer | `1000` / `60000` | Capped exponential backoff with full jitter for this agent's own connect/stream-reconnect loop — independent of any device's polling cadence. |

An agent is refused at startup (`MtcError::Config`, not silently corrected) for: a non-lower-kebab
`id`; a duplicate `id` or `url` across `agents[]`; `heartbeatMs`/`pollIntervalMs`/`requestTimeoutMs`
of `0`; `maxDocumentBytes` of `0`; `reconnect.maxMs` less than `reconnect.initialMs`; or
`tls.certSecretRef` set without `tls.keySecretRef` (or vice versa).

## `component.global.defaults`, `.timeouts`, `.healthThresholds`

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `defaults.pollIntervalMs` | integer | `5000` | Fallback read cadence for a device that sets no `pollIntervalMs` of its own. |
| `defaults.publishMode` | `"on-change"` \| `"interval"` | `"on-change"` | Default publish policy for signals that declare none. |
| `defaults.batchMs` | integer | `0` | Default coalescing window for `interval` mode. |
| `defaults.maxDocumentBytes` | integer | `16777216` | Default response/part size cap for an agent that sets none of its own. |
| `defaults.reconnect.*` | object | see above | Default reconnect bounds for an agent that sets none of its own. |
| `timeouts.connectMs` | integer | `5000` | How long a connect attempt may take before it is treated as failed. |
| `timeouts.reconnectBackoffMinMs` / `.reconnectBackoffMaxMs` | integer | `1000` / `60000` | The reconnect window; backoff is jittered within it, so a plant full of adapters does not reconnect in lockstep when an agent restarts. |
| `healthThresholds.staleSignalSecs` | integer | `30` | A signal with no update for longer than this counts toward `southbound_health.staleSignals`. A signal that silently stops updating is otherwise indistinguishable from one that is simply not changing. |

## `component.instances[]` (one MTConnect device each)

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `id` | string | **required** | Unique device id, lower-kebab. The `{instance}` token of this device's UNS topics and the `instance` metric dimension. |
| `adapter` | `"mtconnect"` \| `"sim"` | `"mtconnect"` | Which backend serves this instance. `mtconnect` is the real client; `sim` is the built-in simulator, which needs no agent. Published in `device.adapter` on every reading. |
| `connection.agentId` | string | required for `adapter: "mtconnect"` | The `agents[].id` serving this device. |
| `connection.deviceUuid` | string | required for `adapter: "mtconnect"` | The `Device/@uuid` this instance represents — verified against the agent's `/probe` at connect. |
| `connection.endpoint` | string | required for `adapter: "sim"` | Only for the simulator, which has no agent to derive an endpoint from. An `mtconnect`-adapter instance's endpoint is **derived** — `mtconnect://<host>[:<port>]/<uuid>` — from `agentId` + `deviceUuid`, never configured, so the two can never disagree. |
| `pollIntervalMs` | integer | `component.global.defaults.pollIntervalMs` | Per-device override of the read cadence. |
| `signals[]` | array | `[]` | The signals this device publishes — see below. A configured `dataItemId` that is not in the signal set is browsable (`sb/browse`) but never published. |
| `writes.allow` | array | `[]`, **pinned empty** | The schema caps this at `maxItems: 0`: MTConnect has no write path, so there is nothing to allow-list. `sb/write` is registered and always answers `WRITE_NOT_ALLOWED`, advertised `unsupported` on the command-availability surface. |

## `component.instances[].signals[]`

A signal is a stable EdgeCommons identity bound to one MTConnect `dataItemId` within this instance's
device. The `dataItemId` is unique per device by the standard, which is what makes the binding stable
across agent restarts — even though the observation timestamps and sequence numbers reset.

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `id` | string | **required** | The stable `signal.id` on the wire, lower-kebab. It never changes when the machine's data item is renamed. |
| `name` | string | none | A human label. |
| `channel` | string | none | An explicit UNS channel for this signal, instead of publishing on its id. |
| `dataItemId` | string | **required** | The `DataItem/@id` this signal reads. One not present in the device's current model is published as a BAD signal with `qualityRaw: MTC_NO_SUCH_DATAITEM` — named, never silently dropped. |
| `conditionBinding[]` | array of string | `[]` | CONDITION data-item ids whose state degrades this signal's quality: a bound `Warning` makes it `UNCERTAIN` and a bound `Fault` makes it `BAD`, with the alarm's own native code riding in `qualityRaw`. Must **not** name the signal's own `dataItemId` — refused at startup if it does. When more than one bound condition is active at once, the **worst** one wins (`Fault` over `Warning` over `Normal`). |
| `publish.mode` | `"on-change"` \| `"interval"` | `"on-change"` | How this signal is published. |
| `publish.batchMs` | integer | `0` | Coalescing window for `interval` mode. |
| `publish.deadband` | number | none | Absolute deadband, SAMPLE-category signals only: a change smaller than this is not published. |

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
built-in (`5000`). This is the *drain-and-publish* cadence for a device — separate from the agent's
own `pollIntervalMs`, which governs how often the shared acquisition reads `/current` when that
agent is not streaming.

## Complete example

```jsonc
{
  "hierarchy": { "levels": ["site", "device"] },
  "identity": { "site": "factory-1" },
  "messaging": { "local": { "type": "mqtt", "host": "localhost", "port": 1883 } },
  "metricEmission": { "target": "messaging" },
  "component": {
    "global": {
      "agents": [
        {
          "id": "line-a-agent",
          "url": "https://agent.line-a.example.com",
          "auth": { "type": "basic", "username": "svc-mtconnect", "secretRef": "line-a/agent-password" },
          "streaming": "prefer",
          "heartbeatMs": 10000
        }
      ],
      "healthThresholds": { "staleSignalSecs": 20 }
    },
    "instances": [
      {
        "id": "cnc-1",
        "adapter": "mtconnect",
        "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
        "signals": [
          { "id": "x-position", "dataItemId": "Xabs", "conditionBinding": ["Xtravel"] },
          { "id": "x-travel-condition", "dataItemId": "Xtravel" }
        ],
        "writes": { "allow": [] }
      }
    ]
  }
}
```

## Reloading configuration

A watchable config source (`FILE`) and the built-in `reload-config` verb both re-read the document
and re-apply it. What happens then depends on what changed:

| Change | Effect |
|---|---|
| `signals[]` of an existing instance | **Applied live.** The signal set is recompiled against the **cached** probe model and swapped atomically, so it lands even while the agent is unreachable, and no session reconnects or re-probes. `sb/signals` answers from the new set immediately, and `southbound_health.signalsSubscribed` follows on the next read. |
| `component.global.agents[]` — any field, or an entry added or removed | Refused with `RESTART_REQUIRED`. An agent owns live sockets, an open multipart stream, and a position in that agent's ring buffer. |
| `component.instances[]` — an instance added, removed, or reordered | Refused with `RESTART_REQUIRED`. An instance owns a supervisor task and a session. |
| Anything that does not compile — an unresolvable `connection.agentId`, two devices claiming one uuid on an agent, a `conditionBinding` naming its own `dataItemId`, an unknown key | Refused with `INVALID_MTCONNECT_CONFIG`. |

A refused candidate is rejected **before** it commits, so the component keeps running on its
last-good configuration instead of being left half-applied; the code and diagnostic reach the
operator through the reload reply and the component's logs.

A new signal set changes which browse entries are flagged `configured`, so `sb/browse`'s
`viewGeneration` covers both the probe digest and the signal set. A cursor minted before a reload is
refused with `MTC_VIEW_CHANGED` — restart the browse rather than paging through a view that no longer
exists.

## Limitations

- MTConnect Assets (`/asset`) and the optional JSON representation are not read by this client —
  only the XML representation, and only `Devices`/`Streams`/`Errors` documents.
- A namespace version below `1.3` is refused at connect; one above `2.7` still parses (matching is by
  local element name, not namespace URI) but is flagged as an unrecognized version rather than
  silently trusted.
- `browse` (`sb/browse`) works from the last cached probe — before an `mtconnect`-adapter instance's
  first successful probe it answers `BROWSE_FAILED`/`MTC_NO_PROBE`; see
  [reference/data-types.md](data-types.md) and the [how-to guides](../how-to-guides.md).
