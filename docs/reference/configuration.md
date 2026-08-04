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
| `heartbeatMs` | integer | `10000` | The `heartbeat` a streaming request asks the agent for, and the liveness window: silence for **twice** this long means the stream is dead even if the socket has not closed. Silence past **one** `heartbeatMs` already republishes held values `UNCERTAIN` with `qualityRaw: MTC_STALE:<ageMs>` ([data-types.md](data-types.md#passive-quality--held-values-under-a-silent-agent)). |
| `streaming` | `"prefer"` \| `"poll-only"` | `"prefer"` | `prefer` opens a multipart `/sample?interval=...` stream and falls back to polling on repeated failure; `poll-only` never streams — it reads `/current` on `pollIntervalMs`. |
| `pollIntervalMs` | integer | `1000` | How often this agent's `/current` is read on the polling path (used directly in `poll-only` mode, and as the fallback cadence when streaming degrades). |
| `requestTimeoutMs` | integer | `10000` | Timeout for one-shot requests (`/probe`, `/current`, a windowed `/sample`). The long-lived streaming request is bounded by `heartbeatMs` instead — liveness, not a fixed deadline. |
| `maxDocumentBytes` | integer | `16777216` | Response/part size cap for this agent. A document (or one multipart part) larger than this is refused rather than buffered — checked against `Content-Length` up front and enforced again mid-stream for a body with none. Independently of bytes, a document is refused past 250 000 XML elements. |
| `reconnect.initialMs` / `reconnect.maxMs` | integer | `1000` / `60000` | Capped exponential backoff with full jitter for this agent's own connect/stream-reconnect loop — independent of any device's polling cadence. |

An agent is refused at startup (`MtcError::Config`, not silently corrected) for: a non-lower-kebab
`id`; a duplicate `id` or `url` across `agents[]`; `heartbeatMs`/`pollIntervalMs`/`requestTimeoutMs`
of `0`; `maxDocumentBytes` of `0`; `reconnect.maxMs` less than `reconnect.initialMs`; or
`tls.certSecretRef` set without `tls.keySecretRef` (or vice versa).

## `component.global.defaults`, `.timeouts`, `.healthThresholds`

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `defaults.pollIntervalMs` | integer | `5000` | Fallback read cadence for a device that sets no `pollIntervalMs` of its own. |
| `defaults.publishMode` | `"on-change"` \| `"interval"` | `"on-change"` | The publish mode selection-derived SAMPLE signals use (see [publish shaping](#publish-shaping)). Explicit `signals[]` entries set their own `publish.mode`; derived EVENT/CONDITION signals are always `on-change`, immediate. |
| `defaults.batchMs` | integer | `0` | The coalescing window selection-derived SAMPLE signals publish with. Explicit `signals[]` entries set their own `publish.batchMs`. |
| `defaults.maxDocumentBytes` | integer | `16777216` | Default response/part size cap for an agent that sets none of its own. |
| `defaults.reconnect.*` | object | see above | Default reconnect bounds for an agent that sets none of its own. |
| `timeouts.connectMs` | integer | `5000` | How long a connect attempt may take before it is treated as failed. |
| `timeouts.reconnectBackoffMinMs` / `.reconnectBackoffMaxMs` | integer | `1000` / `60000` | The reconnect window; backoff is jittered within it, so a plant full of adapters does not reconnect in lockstep when an agent restarts. |
| `healthThresholds.staleSignalSecs` | integer | `30` | Two uses of one threshold. A signal with no value update for longer than this counts toward `southbound_health.staleSignals` — a signal that silently stops updating is otherwise indistinguishable from one that is simply not changing. And it is the limit on how long a held value may stand in for a silent agent: once the time since the agent last vouched for currency passes it (the **liveness** clock, not per-signal change age), held values republish `BAD` with `qualityRaw: MTC_STALE:<ageMs>` ([data-types.md](data-types.md#passive-quality--held-values-under-a-silent-agent)). On a streaming agent the expiry step is reached only when this is shorter than `2 × heartbeatMs`: a stream that misses two heartbeat windows is declared dead, and every held value goes `BAD` with `MTC_AGENT_UNREACHABLE` at that point. With the default `heartbeatMs` of 10 000 the link is unreachable at 20 s, so a `staleSignalSecs` above 20 never takes effect while streaming. Both outcomes are `BAD`; they differ in which `qualityRaw` names the reason. |

## `component.instances[]` (one MTConnect device each)

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `id` | string | **required** | Unique device id, lower-kebab. The `{instance}` token of this device's UNS topics and the `instance` metric dimension. |
| `adapter` | `"mtconnect"` \| `"sim"` | `"mtconnect"` | Which backend serves this instance. `mtconnect` is the real client; `sim` is the built-in simulator, which needs no agent. Published in `device.adapter` on every reading. |
| `connection.agentId` | string | required for `adapter: "mtconnect"` | The `agents[].id` serving this device. |
| `connection.deviceUuid` | string | required for `adapter: "mtconnect"` | The `Device/@uuid` this instance represents — verified against the agent's `/probe` at connect. |
| `connection.endpoint` | string | required for `adapter: "sim"` | Only for the simulator, which has no agent to derive an endpoint from. An `mtconnect`-adapter instance's endpoint is **derived** — `mtconnect://<host>[:<port>]/<uuid>` — from `agentId` + `deviceUuid`, never configured, so the two can never disagree. |
| `pollIntervalMs` | integer | `component.global.defaults.pollIntervalMs` | Per-device override of the read cadence. |
| `signals[]` | array | `[]` | The explicit signals this device publishes — see below. Without a `selection`, a `dataItemId` that is not in the signal set is browsable (`sb/browse`) but never published. |
| `selection` | object | none | Probe-derived signal selection — publish data items by matcher (or all of them) instead of naming each one; see [`selection`](#componentinstancesselection) below. Only for the `mtconnect` adapter: a `sim` instance carrying a `selection` is refused (the simulator has no probe to derive from). |
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
| `publish.mode` | `"on-change"` \| `"interval"` | `"on-change"` | `on-change` publishes every accepted reading — immediately, or coalesced into the batch window when `batchMs` > 0. `interval` keeps only the **latest** accepted reading per window, publishing one sample per window. With `batchMs: 0` both modes publish immediately. |
| `publish.batchMs` | integer | `0` | The signal's coalescing window, in milliseconds. `0` publishes each reading immediately. Above `0`, GOOD readings buffer and the window flushes on expiry as **one** `SouthboundSignalUpdate` whose `samples[]` carries the window's readings in arrival order — each sample keeping its own timestamps and extras (`sequence`, `receivedTs`, …). A BAD or UNCERTAIN reading flushes the window immediately, itself included: a quality transition never sits in a window. |
| `publish.deadband` | number | none | Absolute deadband, SAMPLE-category signals only, applied when a reading enters the publish pipeline: a numeric value differing from the last accepted value by less than this is not published. Non-numeric and array values always pass; any `quality`/`qualityRaw` change always passes; the first reading after a connect, resync, or resume always passes. |

### Publish shaping

The `publish` policy is enforced per signal, **above** the protocol session — the `mtconnect` and
`sim` backends are shaped by the same engine, so a policy behaves identically whichever backend
serves the instance. The rules around the three keys:

- **Unconfigured signals are untouched.** No `publish` block (or `batchMs: 0`, the default) means
  every reading publishes immediately as its own update.
- **Batching produces one message per window.** The window opens when the first reading buffers and
  closes `batchMs` later; the flush is one `SouthboundSignalUpdate` whose `samples[]` carries the
  buffered readings in arrival order. Under `mode: "interval"` only the latest reading of the
  window is kept; an empty window publishes nothing.
- **Quality outranks the window.** A BAD or UNCERTAIN reading — an `UNAVAILABLE`, a
  `conditionBinding` degradation — flushes its signal's window at once, and the synthetic
  passive-quality transitions (`MTC_STALE`, `MTC_AGENT_UNREACHABLE` —
  [data-types.md](data-types.md#passive-quality--held-values-under-a-silent-agent)) bypass the
  windows entirely.
- **Deadband gates entry, not exit.** A suppressed reading never reaches a window. The comparison
  anchor is the last **accepted** value, so a slow drift still publishes once it accumulates past
  the deadband.
- **`sb/pause` discards open windows.** Nothing reaches the wire while paused, and `sb/resume`
  republishes a fresh snapshot of the whole inventory first — flushing pre-pause readings after
  that snapshot would publish stale data out of order, so they are dropped, with a log line.
  The deadband re-arms on resume: the first reading of every signal passes.
- **`repoll` and the resume snapshot bypass shaping.** A forced snapshot is a fresh full publish
  of the current truth, not on-change flow.
- **Shutdown, reconnect, and link loss flush open windows.** Buffered readings are data; a SIGTERM
  does not lose them.
- **Reloads swap the policy atomically.** Editing a signal's `publish` block rides the same live
  signals-swap as any other signal edit; the open windows of changed signals flush with the
  readings their old policy collected.

The engine's activity is observable as the [`MtconnectAdapterShaping`](metrics.md#mtconnectadaptershaping)
metric family: updates published, readings coalesced, readings deadband-dropped.

## `component.instances[].selection`

A `selection` block describes **which** of the device's data items to publish, and the adapter
derives one signal per selected item from the probe model — beside (not instead of) any explicit
`signals[]`. The derived set is recomputed against the cached model, so it follows the machine: a
data item that appears after a model change starts publishing, one that disappears stops (see
[drift semantics](#how-the-derived-set-behaves) below).

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `mode` | `"explicit"` \| `"include"` \| `"all"` | `"explicit"` | `explicit` publishes only the explicit `signals[]` — the behavior of an absent `selection`. `include` publishes the data items matching any `include` matcher. `all` publishes every data item. `exclude` applies to both selecting modes and always wins. |
| `include[]` | array of matcher | `[]` | The matchers that select data items — **OR across the list**. Required non-empty under `mode: "include"`; refused under the other modes, where it would be inert. |
| `exclude[]` | array of matcher | `[]` | Matchers that remove data items from the selection. Exclude wins over include and over `mode: "all"`. |
| `maxSignals` | integer ≥ 1 | `500` | Caps the **derived** set only — explicit `signals[]` never count against it. Exceeding it truncates deterministically in browse-tree order, with a warning event (`MtconnectSignalSetEvent`) and a log line — never silently. |
| `autoConditionBinding` | boolean | `true` | Each derived non-condition signal binds the CONDITION data items of **its own component**, so a machine alarm degrades its component's values exactly as a hand-written `conditionBinding` would. Set `false` to derive no bindings. |

### Matchers

A matcher is a closed object; every field is optional, and the fields that are present are
**AND**ed. An empty matcher `{}` matches every data item. Regex fields are **anchored**: the
pattern must match the whole field, so `POSITION` does not match `PATH_POSITION`. All patterns are
validated when the configuration loads — a bad regex is refused before anything commits.

| Field | Matches against | Semantics |
|-------|-----------------|-----------|
| `category` | the item's category | Exactly `SAMPLE`, `EVENT`, or `CONDITION`. |
| `type` | `DataItem/@type` | Anchored regex (`POSITION`, `POSITION\|LOAD`, `.*_VELOCITY`). |
| `subType` | `DataItem/@subType` | Anchored regex. An item with **no** subType never matches this field. |
| `idMatch` | `DataItem/@id` | Anchored regex. |
| `path` | the component path (`Axes/Linear[X]`) | Glob, matched segment-wise: `*` and `?` within a segment, `**` spans zero or more whole segments (`Axes/**`, `**/Path[P1]`). The empty string is the device level. |

### What a derived signal looks like

For every selected data item the adapter derives:

| Field | Derivation |
|-------|------------|
| `id` | The lower-kebab sanitization of the `dataItemId` (`Xabs` → `xabs`, `SpindleSpeed` → `spindle-speed`, `t1-Tpos` → `t1-tpos`). A collision with another id gets a deterministic `-2`, `-3`, … suffix in browse-tree order, with a warning log. |
| `name` | The probe's own `DataItem/@name`; an item with none is named by its type plus subType (`POSITION ACTUAL`). |
| `channel` | The UNS-sanitized component path, then the id: `Axes/Linear[X]` + `xabs` → `axes/linear-x/xabs`. A device-level item publishes on its id alone. A path deeper than the UNS topic can carry keeps its leaf-most segments — see [Deep component paths](#deep-component-paths). |
| `publish` | SAMPLE: the mode from `component.global.defaults.publishMode` with `batchMs` from `component.global.defaults.batchMs`, and **no deadband** — a units-aware default is not cleanly derivable (a millimeter on a micro-positioner and on a gantry are different facts), so none is invented; set one on an explicit entry when you want it. EVENT/CONDITION: `on-change`, immediate. |
| `conditionBinding` | Under `autoConditionBinding` (the default), the CONDITION data items of the signal's own component; a CONDITION signal itself binds nothing. |

### Deep component paths

A UNS topic carries at most 8 levels and 256 UTF-8 bytes, and an instance's data topic
(`ecv1/{device}/{component}/{instance}/data/…`) has already spent five of those levels. Machine
tool models nest deeper than that: `Resources[resources]/Materials[materials]/Stock[stock]` plus a
signal id is four channel tokens where three fit.

A derived channel is therefore the **last few** component-path segments plus the id — as many
segments as the topic has room for, computed per signal:

| Component path | Derived channel |
|----------------|-----------------|
| *(device level)* | `stock` |
| `Controller[controller]` | `controller-controller/estop` |
| `Axes/Linear[X]` | `axes/linear-x/xabs` |
| `Resources[resources]/Materials[materials]/Stock[stock]` | `materials-materials/stock-stock/stock` |
| `Systems[systems]/Hydraulic[hydraulic]/Pump[pump]/Motor[motor]/Sensor[sensor]` | `motor-motor/sensor-sensor/ptemp` |

The leaf-most segments are kept because they are the ones that identify the signal; segments nearer
the device drop first. The room is measured against the instance's own identity, so a long device,
component or instance name leaves fewer bytes for the channel.

**Nothing is lost.** The full, untruncated component path is served as
`signal.address.componentPath` on `sb/signals` and on every `sb/browse` entry — only the topic is
shortened. Channels stay unique because the signal id is always the last segment and signal ids are
unique within an instance.

Two limits on this:

- A `channel` you set by hand on an explicit `signals[]` entry is published exactly as written. If
  it does not fit the topic, the publish fails with a `DEPTH_EXCEEDED` or `LENGTH_EXCEEDED`
  validation error rather than being silently rewritten.
- If an identity is long enough that not even a bare signal id fits, those signals cannot publish.
  The adapter raises an `MtconnectSignalSetEvent` warning with `reason: "channelBudget"` naming how
  many signals are affected; shorten the device, component or instance name.

### Precedence: explicit entries win, field by field

An explicit `signals[]` entry whose `dataItemId` the selection also matches **overrides** the
derived entry, field by field: the fields it sets win, the fields it leaves out take the derived
values. Absence and emptiness are different statements — `"conditionBinding": []` clears the auto
binding, while omitting `conditionBinding` inherits it; the same holds for `publish`. Explicit
entries keep the strict missing-item contract: a `dataItemId` the model does not have publishes a
permanent BAD `MTC_NO_SUCH_DATAITEM`, exactly as without a selection.

### How the derived set behaves

- **The derived set follows the model.** On model drift (a re-probe after an agent restart, or a
  `reconnect`), the derived set is recomputed against the new model inside the same generation
  bump: newly matching items start publishing, removed derived items **stop** — they were
  discovered, not configured, so they do not linger as BAD. Any change is announced as an
  `MtconnectSignalSetEvent` naming the added/removed counts.
- **Identity stability is a trade.** Derived ids, names and channels are protocol-derived: they are
  deterministic, but they follow the machine's own `dataItemId`s and component names. Pin an
  explicit `signals[]` entry for any signal whose identity must survive a machine reconfiguration —
  see [explanation.md](../explanation.md#derived-identity-is-a-trade).
- **`signalsSubscribed` counts the served union** — the explicit signals (minus permanently-BAD
  unbound ones) plus the derived set.
- **Before the first probe** there is no model to derive from: `sb/signals` lists only the explicit
  entries, and the derived half appears with the first probe.

## Identity & the UNS device tree

`hierarchy.levels` names the enterprise tree, deepest (the device) last; `identity` supplies every
level's value **except** the last (which is always the resolved Thing name, `-t`). With the default
(`["device"]`), topics are `ecv1/{thing}/mtconnect-adapter/{instance}/...`.

```jsonc
"hierarchy": { "levels": ["site", "area", "device"] },
"identity":  { "site": "plant1", "area": "pumphouse" }
// -> identity.path = "plant1/pumphouse/<thing>"
```

`component.token` supplies the `{component}` segment of those topics and the `identity.component`
field of every message. It is `mtconnect-adapter` in every shipped configuration and in the recipe:
UNS tokens are lower-kebab, while the Greengrass component name is the reverse-DNS
`com.mbreissi.edgecommons.MtconnectAdapter`. Leave it set — dropping it makes the library fall back
to the short form of the component name, `MtconnectAdapter`, and the topics in this documentation
stop matching what the adapter publishes.

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
    "token": "mtconnect-adapter",
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
| `selection` of an existing instance — added, removed, or edited | **Applied live**, riding the same atomic swap as a `signals[]` edit: the served union is recomputed against the cached model, with no restart, reconnect, or re-probe. A selection whose regex does not compile is refused with `INVALID_MTCONNECT_CONFIG` before it commits. |
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

## Appendix — revision history

| Date | Change |
|---|---|
| 2026-08-03 | `staleSignalSecs` documented as both the `staleSignals` threshold and the passive BAD-expiry threshold on the liveness clock; the 250 000-element document cap; the one-missed-heartbeat UNCERTAIN step; passive transitions bypass batch windows. |
| 2026-07-28 | Initial version. |
