# Reference — Messaging Interface & CLI

*This documents the generated scaffold; rewrite it as you build the component out.*

Every topic and message this adapter publishes or accepts, and its CLI flags. Addressing follows the
**Unified Namespace (UNS)**: `ecv1/{device}/{component}/{instance}/{class}[/channel]`. For the
data/control plane model, see [explanation.md](../explanation.md); for client recipes, the
[how-to guides](../how-to-guides.md).

- `{device}` — the resolved Thing name (`-t`, or the last `hierarchy` level).
- `{component}` — the component UNS token, `mtconnect-adapter`.
- `{instance}` — a configured device id (`device-1`, …). It always appears on `data`/`evt` topics,
  and optionally on a `cmd` topic to address one device (`…/{instance}/cmd/{verb}`); the `state`
  keepalive is component-scoped (no instance token in its topic).

## Envelope

All messages use the EdgeCommons JSON envelope: `{header, identity, tags, body}`. The library stamps
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
| `metric` | `southbound_health`, `MtconnectAdapterConnection`, `MtconnectAdapterCommand` | — | adapter → bus (auto) | `ecv1/{device}/mtconnect-adapter/metric/{metricName}` | — |
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
`NO_SUCH_INSTANCE`, `WRITE_NOT_ALLOWED`, `WRITE_FAILED`, `DEVICE_UNAVAILABLE`, `READ_FAILED`,
`RECONNECT_FAILED`, `BROWSE_UNSUPPORTED`, `BROWSE_FAILED`, `PAUSED` (a `repoll` on a paused
instance — resume first).

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
  "device": { "adapter": "sim", "instance": "device-1", "endpoint": "sim://device-1" },
  "signal": { "id": "temperature-1", "name": "Ambient temperature" },
  "samples": [ { "value": 21.7, "quality": "GOOD", "qualityRaw": "unspecified", "serverTs": "2026-07-19T00:00:00Z" } ]
}
```

An omitted `quality` defaults to `GOOD` with `qualityRaw: "unspecified"` (a synthesized-vs-reported
marker); a failed read (the simulator's `pressure-1`) publishes an explicit `BAD` with the native
fault text as `qualityRaw` and `value: null`.

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

### `sb/write` (command)

```jsonc
"body": { "writes": [ { "signalId": "temperature-1", "value": 42.5 } ] }
// result: { "id": "device-1", "written": 1,
//           "results": [ { "signal": "temperature-1", "value": 42.5, "ok": true } ] }
```

A single `{signalId/id/name, value}` object (no `writes` array) is also accepted. A signal-ref is
`{"signalId": "…"}` / `{"id": "…"}` (the stable id directly) or `{"name": "…"}` (resolved against
the configured inventory). Every entry is checked against `writes.allow` **before** it reaches the
device; `WRITE_NOT_ALLOWED` when every entry is refused, `WRITE_FAILED` when every attempted write
reaches the device and every one is rejected there.

### `sb/read` (command, request/reply)

```jsonc
// request: { "signals": [ { "signalId": "temperature-1" } ] }
// reply:   { "id": "device-1", "reads": [
//   { "signal": { "id": "temperature-1" }, "value": 21.7, "quality": "GOOD", "qualityRaw": "unspecified" } ] }
```

An unresolvable ref is reported per-entry with `quality: BAD`/`qualityRaw: "UNRESOLVED_REF"`, not
omitted.

## Control plane

- **`sb/status`** → `{ id, adapter, connected, state, paused, endpoint, metrics }`.
- **`sb/signals`** → `{ id, signals: [ { id, name, writable }, ... ] }` — the configured/backend
  inventory, no device round-trip.
- **`sb/browse`** → paged discovery by default: `{ id, entries: [ { id, name, type }, ... ],
  cursor? }`, or `BROWSE_UNSUPPORTED` if the backend has no discovery (the simulator's one-page
  browse is the worked example). A request carrying `ref` selects the hierarchical panel mode
  instead (below); mixing `ref`/`depth`/`maxRefs` with `cursor`/`max` is `BAD_ARGS`, as is
  `depth`/`maxRefs` without `ref`.
- **`sb/pause`** / **`sb/resume`** → `{ id, paused, changed }` — idempotent; pausing an
  already-paused device reports `changed: false`.
- **`reconnect`** → `{ id, connected: true }` or a `RECONNECT_FAILED` error.
- **`repoll`** → `{ id, polled: <count> }`; refused with `PAUSED` while paused.

### Hierarchical `sb/browse` (the panel mode)

The `treeBrowser` panel drives `sb/browse` with `{ instance?, ref, depth?, maxRefs? }` instead of a
cursor. `ref` selects the node: `"root"` is the device itself, whose `contains` refs are the same
inventory the paged mode serves; a signal id selects that node as a known leaf (`"refs": []`). An
unknown `ref` is `BAD_ARGS`, and so is `depth`/`maxRefs` without `ref`. `depth` is bounded 1–4
(default 1) and `maxRefs` 1–1000 (default 200); the adapter's inventory is flat, so a deeper `depth`
finds no grandchildren.

```jsonc
// request: { "ref": "root", "depth": 1, "maxRefs": 200 }
// reply:   { "id": "device-1", "mode": "hierarchical",
//            "root": { "nodeId": "root", "name": "device-1", "nodeClass": "device", "dataType": null,
//                      "refs": [ { "referenceType": "contains",
//                                  "target": { "nodeId": "temperature-1", "name": "Ambient temperature",
//                                              "nodeClass": "signal", "dataType": "REAL" } } ] },
//            "refCount": 1, "depth": 1, "truncated": false }
```

## Panels

Three edge-console panel descriptors are registered via `register_panel` (`src/commands.rs`),
`scope: "instance"` (repeated on every command-backed widget), order 10/20/30:

- **`overview`** — an *Adapter overview* summary (Signals / Lifecycle / Writes rows) plus a
  *Lifecycle bindings* command summary (`sb/status`, `reconnect`, `sb/pause`, `sb/resume`,
  `repoll`).
- **`signals`** — a `signalGrid` bound to `sb/signals` through both `signalsVerb` and the
  renderer-compat `subscriptionsVerb` alias (a descriptor field alias — no `sb/subscriptions` wire
  verb exists), with `readVerb: sb/read`.
- **`diagnostics`** — a hierarchical `treeBrowser` (`browseVerb: sb/browse`, `rootRef: "root"`,
  `depth: 1`, `maxRefs: 200`, `readVerb: sb/read`) plus a *Diagnostic commands* summary
  (`sb/status`, `sb/browse`).

No widget names a `writeVerb` — writes stay on the command surface behind the allow-list.

## Events (`evt` class)

Published through the library's `events()` facade; severity **derives** the channel
(`evt/{severity}/{type}`), so the topic and the body can never disagree. This scaffold emits
`device-connected` (info), `device-unreachable` (critical, raised on drop / cleared on restore),
`adapter-paused` (warning), and `adapter-resumed` (info).

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
