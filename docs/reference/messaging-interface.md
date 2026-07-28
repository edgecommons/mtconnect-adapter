# Reference — Messaging Interface & CLI

Every topic and message this adapter publishes or accepts, and its CLI flags. Addressing follows the
**Unified Namespace (UNS)**: `ecv1/{device}/{component}/{instance}/{class}[/channel]`. For the
data/control plane model, see [explanation.md](../explanation.md); for client recipes, the
[how-to guides](../how-to-guides.md).

- `{device}` — the resolved Thing name (`-t`, or the last `hierarchy` level).
- `{component}` — the component UNS token, `mtconnect-adapter`, set by `component.token`. It is a
  separate identifier from the Greengrass component name (`com.mbreissi.edgecommons.MtconnectAdapter`),
  which never appears on the wire.
- `{instance}` — a configured device id (`device-1`, …). It always appears on `data`/`evt` topics,
  and optionally on a `cmd` topic to address one device (`…/{instance}/cmd/{verb}`); the `state`
  keepalive is component-scoped (no instance token in its topic).

## Envelope

The envelope is documented here in its JSON projection — the canonical field names and shapes; the
MQTT/IPC wire encoding is the protobuf envelope (`proto/edgecommons/v1`), which round-trips this
projection exactly.

All messages use the EdgeCommons envelope: `{header, identity, tags, body}`. The library stamps
the top-level `identity` (`{hier, path, component, instance}`) on every message built from a facade.
Request/reply carries `header.reply_to` + `header.correlation_id`; the reply publishes to `reply_to`
with the same `correlation_id`.

## Topics

| Class | Message | Scope | Direction | Topic | Reply |
|-------|---------|-------|-----------|-------|-------|
| `data` | `SouthboundSignalUpdate` | — | adapter → bus | `ecv1/{device}/mtconnect-adapter/{instance}/data/{signal}` | — |
| `evt` | `evt` | — | adapter → bus | `ecv1/{device}/mtconnect-adapter/{instance}/evt/{severity}/{type}` | — |
| `cmd` | `sb/status` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/sb/status` | `{ok,result}` |
| `cmd` | `sb/read` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/sb/read` | `{ok,result}` |
| `cmd` | `sb/write` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/sb/write` | `{ok,result}` |
| `cmd` | `sb/signals` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/sb/signals` | `{ok,result}` |
| `cmd` | `sb/browse` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/sb/browse` | `{ok,result}` |
| `cmd` | `sb/pause` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/sb/pause` | `{ok,result}` |
| `cmd` | `sb/resume` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/sb/resume` | `{ok,result}` |
| `cmd` | `reconnect` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/reconnect` | `{ok,result}` |
| `cmd` | `repoll` | `instance` | bus → adapter | `ecv1/{device}/mtconnect-adapter/[{instance}/]cmd/repoll` | `{ok,result}` |
| `metric` | `southbound_health`, `MtconnectAdapterConnection`, `MtconnectAdapterCommand`, `MtconnectStream`, `MtconnectProbe`, `MtconnectParse` | — | adapter → bus (auto) | `ecv1/{device}/mtconnect-adapter/metric/{metricName}` | — |
| `state` | keepalive | — | adapter → bus (auto) | `ecv1/{device}/mtconnect-adapter/state` | — |

**Scope** is the verb's declared addressing, advertised on its `describe` entry. All nine verbs act
on one device, so all nine are `instance`: a request may be addressed to a device on the topic
(`…/{instance}/cmd/{verb}`) or to the component (`…/cmd/{verb}`) naming the device in the body — see
[Addressing a verb](#addressing-a-verb).

Fleet consumers subscribe the six UNS wildcards — telemetry `ecv1/+/+/+/data/#`, events
`ecv1/+/+/+/evt/#`, metrics `ecv1/+/+/+/metric/#`, state `ecv1/+/+/+/state`. `state`/`metric`/`cfg`
are library-owned **reserved** classes — this adapter only ever mints `data`/`evt` topics via the
`data()`/`events()` facades and `cmd` replies via the command inbox, never a hand-assembled string.

## The command inbox

Served through the library's **command inbox**, which subscribes both cmd wildcards:
`ecv1/{device}/mtconnect-adapter/cmd/#` (component-addressed) and
`ecv1/{device}/mtconnect-adapter/+/cmd/#` (instance-addressed). A request's **verb** is the topic channel
after `cmd/`, matching `header.name`. Built-in verbs (`ping`, `status`, `describe`, `reload-config`,
`get-configuration`) ship automatically; this scaffold registers the `sb/*` + `reconnect`/`repoll`
verbs (`src/commands.rs`).

The reply body is `{"ok": true, "result": <verb result>}` on success, or
`{"ok": false, "error": {"code", "message"}}` on failure — codes: `BAD_ARGS` (a malformed request, a
body `instance` conflicting with the topic's token, or a missing instance with two or more devices),
`NO_SUCH_INSTANCE`, `WRITE_NOT_ALLOWED` (every `sb/write` request, unconditionally),
`DEVICE_UNAVAILABLE` (the device's own task is gone), `RECONNECT_FAILED`, `BROWSE_UNSUPPORTED`,
`BROWSE_FAILED` (carrying `MTC_NO_PROBE` / `MTC_VIEW_CHANGED` / `MTC_BAD_CURSOR` in its message for
an `mtconnect`-adapter instance), `PAUSED` (a `repoll` on a paused instance — resume first). `sb/read`
never fails at the top level for a per-signal problem — see [`sb/read`](#sbread-command-requestreply)
below for its per-entry codes.

### Addressing a verb

Every verb here declares scope `instance`, and the **library** resolves the addressing before the
adapter's handler runs:

1. The topic's instance token is authoritative. `…/device-2/cmd/sb/read` acts on `device-2`.
2. A body `instance` that disagrees with the topic token is `BAD_ARGS` — checked first, before
   anything else about the request.
3. A component-addressed request may name the device in the body instead:
   `…/cmd/sb/read` with `{"instance": "device-2", …}` is equivalent to (1).
4. When neither names one, the adapter resolves it against its own configuration: with exactly one
   device configured that device answers; with two or more it is `BAD_ARGS`. An instance that is not
   configured is `NO_SUCH_INSTANCE`.

Steps 1-3 belong to the library and are identical for every EdgeCommons component; only step 4 needs
this component's configuration.

## Data plane

### `SouthboundSignalUpdate` (adapter → bus, `data` class)

Published through the library's `data()` facade — the adapter never hand-builds the body or the
topic:

```jsonc
"body": {
  "device": { "adapter": "mtconnect", "instance": "cnc-1", "endpoint": "http://agent:5000" },
  "signal": { "id": "x-position", "name": "X position" },
  "componentPath": "Axes/Linear[X]",
  "samples": [ { "value": 123.456, "quality": "GOOD", "qualityRaw": "MTC_OK", "serverTs": "2026-07-19T00:00:00Z" } ]
}
```

An omitted `quality` defaults to `GOOD` with `qualityRaw: "unspecified"` (a synthesized-vs-reported
marker); a failed read publishes an explicit `BAD` with the native fault text as `qualityRaw` and
`value: null`.

The sample's `serverTs` is the **capture** moment: the seam's `capture_ts` when the backend
supplies one, else the worker's read-completion receive stamp (a direct client's receive moment IS
the capture moment). A device-authored `source_ts` rides as `sourceTs` only when present — never
synthesized — and when a mediating server makes the adapter's receive moment differ from the
effective `serverTs`, it rides as a per-sample `receivedTs` extra:

```jsonc
"samples": [ { "value": 21.7, "quality": "GOOD", "qualityRaw": "OK",
               "sourceTs": "2026-07-19T00:00:00.1Z", "serverTs": "2026-07-19T00:00:00.4Z",
               "receivedTs": "2026-07-19T00:00:00.9Z" } ]
```

`samples[]` is an array because a signal whose `publish.batchMs` is above `0` coalesces a whole
batch window into one update ([configuration.md](configuration.md#publish-shaping)): the array then
carries every reading of the window in arrival order, each sample keeping its own `serverTs`,
quality, and extras (`sequence`, `receivedTs`, ...). An unbatched signal publishes one sample per
update.

#### `componentPath` — the canonical address, on every update

Every `SouthboundSignalUpdate` carries a `componentPath` member beside `signal` and `samples`. It
is the signal's **full, untruncated** MTConnect component path — the same string `sb/signals`
serves in `signal.address.componentPath` — so a consumer that needs to know where on the machine a
value came from reads one field and never calls the control plane.

| Value | Meaning |
|---|---|
| `"Axes/Linear[X]"` | The component chain holding the signal's data item, slash-joined, exactly as the probe declares it. |
| `""` | The data item hangs off the device itself and belongs to no component (`avail` and friends). |
| `null` | No device model describes this signal — an explicit `signals[]` entry whose `dataItemId` is not in the probe (published `BAD` with `qualityRaw: "MTC_NO_SUCH_DATAITEM"`), or a backend with no probe model. `sb/signals` reports the same `null`. |

It is **always present**, with no exception: unconditional presence is the point, so reader code
never branches on whether the key is there. It is stamped **once per update**, never per sample —
the path is a property of the signal, and a batched update is one signal's readings:

```jsonc
"body": {
  "signal": { "id": "stock" },
  "componentPath": "Resources[resources]/Materials[materials]/Stock[stock]",
  "samples": [ { "value": "ALUMINUM-6061", "quality": "GOOD", "serverTs": "2026-07-19T00:00:00Z", "sequence": 41 },
               { "value": "ALUMINUM-7075", "quality": "GOOD", "serverTs": "2026-07-19T00:00:01Z", "sequence": 44 } ]
}
```

The topic's channel is a different thing and may be shorter: a component path deeper than the UNS
topic budget is shortened to its leaf-most segments when the channel is derived
([configuration.md](configuration.md#deep-component-paths)). `componentPath` is never shortened, which
is what makes the two safe to have side by side — the topic addresses the signal, this states where
it lives.

### `sb/write` (command)

```jsonc
"body": { "writes": [ { "signalId": "x-position", "value": 42.5 } ] }
// reply: { "ok": false,
//          "error": { "code": "WRITE_NOT_ALLOWED",
//                     "message": "MTConnect is read-only (Part 1 Fundamentals §5.1)" } }
```

The verb is registered and every request is refused. MTConnect's API is read-only by specification,
so the refusal precedes any inspection of the body: no entry is resolved, no allow-list is
consulted, and nothing reaches a device. The refusal is also advertised on the verb's `describe`
entry as `availability: { "state": "unsupported", "reason": "MTConnect is read-only" }`, so a
console disables the surface instead of offering a write that can never work. The instance schema
pins `writes.allow` to the empty array.

### `sb/read` (command, request/reply)

```jsonc
// request: { "signals": [ { "signalId": "x-position" } ] }
// reply:   { "id": "cnc-1", "mode": "current", "reads": [
//   { "signal": { "id": "x-position" }, "value": 123.456, "quality": "GOOD", "qualityRaw": "MTC_OK",
//     "extra": { "sequence": 37 } } ] }
```

A read is answered from a scoped `/current` snapshot taken through the agent's control channel, so
`mode` is always `current`. A signal-ref is `{"signalId": "…"}` / `{"id": "…"}` (the stable id
directly) or `{"name": "…"}` (resolved against the configured signal set).

Failures are reported **per entry**, with `quality: BAD` and one of these `qualityRaw` codes; the
command itself stays `ok`, because one unreadable signal is not a failed session:

| `qualityRaw` | Meaning |
|---|---|
| `MTC_UNAVAILABLE` | The agent has no value for that data item. |
| `MTC_NO_SUCH_DATAITEM` | The configured `dataItemId` is not in the device model. |
| `MTC_AGENT_ERROR:<code>` | The agent could not serve the snapshot — `UNREACHABLE`, `TIMEOUT`, `HTTP`, `TLS`, `AUTH`, or the agent's own error code. |
| `MTC_PARSE` | The agent's answer could not be parsed. |
| `UNRESOLVED_REF` | The request named a signal this instance does not configure. |

`DEVICE_UNAVAILABLE` is reserved for the device task itself being gone.

## Control plane

- **`sb/status`** → `{ id, adapter, connected, state, paused, endpoint, metrics, protocol }`. The
  `protocol` object is the MTConnect capability view (below), assembled from the agent runtime's
  published state — a status call never waits on acquisition.
- **`sb/signals`** → `{ id, signals: [ { id, name, writable, address, units, conditionBinding,
  bound, provenance }, ... ] }` — the **served** inventory (the explicit `signals[]` plus the
  `selection`-derived set) with the round-trippable `address`, no device round-trip. `writable` is
  always `false`. `address` carries `{protocol, agentId, deviceUuid, dataItemId, category, type,
  subType, componentPath}`; everything the probe supplies is `null` until the device model has been
  fetched, and `bound` says whether the `dataItemId` exists in the current model. `provenance` is
  `"configured"` for an explicit entry and `"discovered"` for a selection-derived one; before the
  first probe only the explicit entries are listed (there is no model to derive from).
- **`sb/browse`** → the probe tree, paged by default (below) or hierarchical when the request
  carries `ref`. Mixing `ref`/`depth`/`maxRefs` with `cursor`/`max` is `BAD_ARGS`, as is
  `depth`/`maxRefs` without `ref`.
- **`sb/pause`** / **`sb/resume`** → `{ id, paused, changed }` — idempotent; pausing an
  already-paused device reports `changed: false`.
- **`reconnect`** → `{ id, connected: true }` or a `RECONNECT_FAILED` error.
- **`repoll`** → `{ id, polled: <count> }` — a forced, fresh `/current` scoped to this instance's
  configured data items, not a drain of what happened to have arrived: an idle machine still answers.
  `polled` is the number of signal results published, `BAD` ones (`UNAVAILABLE`,
  `MTC_NO_SUCH_DATAITEM`) included; refused with `PAUSED` while paused.

### `sb/status.result.protocol`

A closed object; every field the agent teaches us is `null` until it has:

```jsonc
{ "capability": "MTCONNECT_CLIENT",
  "standardVersion": "2.7", "schemaNamespace": "urn:mtconnect.org:MTConnectDevices:2.7",
  "agentId": "line-a-agent", "agentVersion": "2.7.0.12", "instanceId": 1749000000,
  "bufferSize": 131072, "firstSequence": 1, "nextSequence": 43,
  "mode": "stream", "heartbeatMs": 10000, "lastHeartbeatAt": "2026-07-27T10:00:00Z",
  "probeDigest": "sha256:…",
  "limitations": [ "READ_ONLY", "XML_ONLY", "NO_ASSETS" ] }
```

`mode` is `stream` or `poll`. Every document an agent sends — including the empty heartbeat
document — proves liveness, so `lastHeartbeatAt` is the last document's stamp. `probeDigest` is the
content digest of this device's probe subtree and is also the browse `viewGeneration`.

### Paged `sb/browse`

```jsonc
// request: { "max": 200, "cursor": "sha256:…#12" }
// reply:   { "id": "cnc-1", "viewGeneration": "sha256:…", "cursor": "sha256:…#212",
//            "entries": [ { "id": "mtc:/item/Xabs", "name": "Xabs", "kind": "DATA_ITEM",
//                           "type": "POSITION", "subType": "ACTUAL", "category": "SAMPLE",
//                           "units": "MILLIMETER", "dataItemId": "Xabs",
//                           "parentId": "mtc:/component/Axes/Linear[X]", "depth": 3,
//                           "configured": true, "provenance": "configured" } ] }
```

Entries are the device's probe projection in pre-order — the device, its own data items, then each
component subtree. Ids are stable and round-trippable: `mtc:/component/<path>` for the device and
its components, `mtc:/item/<dataItemId>` for data items. `configured` flags a data item any served
signal binds — explicit or `selection`-derived — and every component holding one; `provenance`
refines it on data items (`"configured"` for an explicit binding, `"discovered"` for a
selection-derived one, `null` for an unserved item and for component/device nodes). The tree is
served from the cached probe, so browsing keeps working while the agent is unreachable; before the
first probe the answer is `BROWSE_FAILED` with `MTC_NO_PROBE`. A `cursor` carries the
`viewGeneration` it was minted against — paging on through a model that changed underneath is
refused with `MTC_VIEW_CHANGED` rather than mixing two address spaces.

### Hierarchical `sb/browse` (the panel mode)

The `treeBrowser` panel drives `sb/browse` with `{ instance?, ref, depth?, maxRefs? }` instead of a
cursor. `ref` selects the node: `"root"` is an alias of the device node (`mtc:/component/`), and any
`nodeId` a previous reply handed out expands that node. An unknown `ref` is `BAD_ARGS`. `depth` is
bounded 1–4 (default 1) and `maxRefs` 1–1000 (default 200); `maxRefs` bounds the whole reply, not
each level, and `truncated` says whether it cut the expansion short. A data item is a known leaf
(`"refs": []`); a component that may have children omits `refs` until it is expanded.

```jsonc
// request: { "ref": "root", "depth": 1, "maxRefs": 200 }
// reply:   { "id": "cnc-1", "mode": "hierarchical", "viewGeneration": "sha256:…",
//            "root": { "nodeId": "mtc:/component/", "name": "OKUMA-CNC", "nodeClass": "device",
//                      "dataType": null, "kind": "DEVICE", "configured": true,
//                      "refs": [ { "referenceType": "contains",
//                                  "target": { "nodeId": "mtc:/item/avail", "name": "avail",
//                                              "nodeClass": "dataItem", "dataType": "AVAILABILITY",
//                                              "kind": "DATA_ITEM", "category": "EVENT",
//                                              "dataItemId": "avail", "configured": false,
//                                              "refs": [] } } ] },
//            "refCount": 4, "depth": 1, "truncated": false }
```

## Panels

Five edge-console panel descriptors are registered via `register_panel` (`src/commands.rs`),
`scope: "instance"` (repeated on every command-backed widget), order 10/20/30/40/50. Each view
declares the `rendererRequirements` tokens it needs, and edge-console refuses to mount a view whose
requirements it cannot meet:

- **`overview`** (10) — a `statusDashboard` bound to `sb/status` (adapter state, connected, paused,
  endpoint, agent and standard version, mode, instance id, next sequence, heartbeat age, probe
  digest), an `actionBar` for `sb/pause` / `sb/resume` / `reconnect` / `repoll`, and a `metricSeries`
  of `southbound_health`.
- **`device-structure`** (20) — a hierarchical `treeBrowser` (`browseVerb: sb/browse`,
  `rootRef: "root"`, `depth: 1`, `maxRefs: 200`, `readVerb: sb/read`) with the columns
  Name / Kind / Type / SubType / Category / DataItem / Configured.
- **`signals`** (30) — a `signalGrid` bound to `sb/signals` through both `signalsVerb` and the
  renderer-compat `subscriptionsVerb` alias (a descriptor field alias — no `sb/subscriptions` wire
  verb exists), with `readVerb: sb/read` and the columns Signal / Name / DataItem / Category /
  Type / Units / Quality binding.
- **`conditions`** (40) — an `eventFeed` of `MtconnectConditionEvent`, `MtconnectDataLossEvent`, and
  `MtconnectAgentEvent`, plus an observation-flow `metricSeries`.
- **`diagnostics`** (50) — a sequence/buffer `statusDashboard`, an `eventFeed` of the agent and
  model-drift events, and a `metricSeries` of stream gaps, reconnects, heartbeats, and parse errors.

No view names a `writeVerb` or binds `sb/write`: MTConnect has nothing to write, and the permanent
refusal rides the command-availability surface instead.

## Events (`evt` class)

Published through the library's `events()` facade; severity **derives** the channel
(`evt/{severity}/{type}`), so the topic and the body can never disagree.

The lifecycle events every adapter emits: `device-connected` (info), `device-unreachable` (critical,
raised on drop / cleared on restore), `adapter-paused` (warning), `adapter-resumed` (info).

On top of them, five families carry what only MTConnect knows. Sequence numbers, device uuids and
data-item ids belong here, in the event's `context` — never as a metric dimension.

| Type | Severity | Emitted when | `context` |
|---|---|---|---|
| `MtconnectAgentEvent` | info / critical / warning | the agent became reachable (`state: "up"`), unreachable (`"down"`), or streaming could not be established and acquisition degraded to polling (`"degraded"`) | `instance`, `agentId`, `state`; plus `mode`, `instanceId`, `agentVersion`, `standardVersion` when up, `reason` when down, `failures` when degraded |
| `MtconnectDataLossEvent` | warning | the agent's buffer overran the adapter's position, so observations are provably lost (resync ladder step 2) | `instance`, `agentId`, `skipped`, `firstSequence`, `nextSequence`, `bufferSize` |
| `MtconnectModelDriftEvent` | warning | a re-probe returned a different device model: signals recompile and browse cursors are void | `instance`, `agentId`, `deviceUuid`, `oldDigest`, `newDigest` |
| `MtconnectConditionEvent` | critical | a CONDITION data item **transitioned into** `Fault` | `instance`, `dataItemId`, `state`, `previousState`, `nativeCode`, `timestamp` |
| `MtconnectSignalSetEvent` | info / warning | the `selection`-derived signal set changed shape — it followed a model change or a reload (info, with counts), or `maxSignals` truncated the derived set (warning; a cap is never silent) | `instance`, `deviceUuid`; set change: `added`, `removed`, `discovered`, `served`; truncation: `reason: "maxSignals"`, `maxSignals`, `matched`, `truncated` |

A condition that is merely still asserted is not a new event, and a fault that clears and re-latches
raises at most one event per data item per minute. The condition state itself is unaffected by that
limit: it publishes as the signal's value on every observation, and degrades any signal that binds it
through `conditionBinding`.

```jsonc
// ecv1/{device}/mtconnect-adapter/cnc-1/evt/critical/MtconnectConditionEvent
{ "severity": "critical", "type": "MtconnectConditionEvent",
  "message": "condition `Xtravel` went to Fault", "timestamp": "2026-07-27T10:00:05.000Z",
  "context": { "instance": "cnc-1", "dataItemId": "Xtravel", "state": "FAULT",
               "previousState": "NORMAL", "nativeCode": "ALM-2",
               "timestamp": "2026-07-27T10:00:04.900000Z" } }
```

The `conditions` and `diagnostics` panels subscribe to these families by name.

## State keepalive (`state` class, reserved — automatic)

Publishes every ~5 s on `ecv1/{device}/mtconnect-adapter/state`. The RUNNING keepalive carries an
`instances[]` array — one entry per configured device — from the same connectivity provider
`sb/status` reads. `state` is this adapter's own vocabulary
(`CONNECTING`/`ONLINE`/`BACKOFF`/`PAUSED`), so a deliberately paused device is distinguishable from
one that has gone quiet; `connected` stays the normalized flag any consumer can read:

```jsonc
{ "status": "RUNNING", "uptimeSecs": 3600,
  "instances": [ { "instance": "device-1", "connected": true, "state": "ONLINE",
                    "detail": "sim://device-1", "attributes": { "adapter": "sim", "paused": false } } ] }
```

## CLI

| Flag | Values | Notes |
|------|--------|-------|
| `--platform` | `GREENGRASS` \| `HOST` \| `KUBERNETES` \| `auto` | Default `auto`. |
| `--transport` | `MQTT [path]` \| `IPC` | HOST/Kubernetes use MQTT; the path is the messaging config. |
| `-c/--config` | `FILE <path>` \| `ENV` \| `GG_CONFIG` \| `CONFIGMAP` | Default from the platform. |
| `-t/--thing` | `<name>` | Thing name; the `{device}` token of every UNS topic. |
