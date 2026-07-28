# Reference — Data Types

How an MTConnect observation becomes a published `Reading`, and how a `Reading` becomes the JSON on
the wire. For the config keys that select what gets read, see
[reference/configuration.md](configuration.md); for the message shapes,
[reference/messaging-interface.md](messaging-interface.md).

## The seam's type: `Reading`

Every backend — the real MTConnect client and the built-in simulator alike — produces
`Vec<Reading>` from `DeviceSession::read_signals`/`read_named`. Its fields:

| Field | Meaning |
|---|---|
| `signal_id` | The canonical, stable id the rest of the fleet keys on — the configured `signal.id`, never the MTConnect `dataItemId` directly (though they are commonly equal). |
| `name` | An optional human label. |
| `value` | The decoded value, as JSON — see "Value typing by MTConnect category" below. |
| `quality` | Normalized `Good` \| `Bad` \| `Uncertain`. |
| `quality_raw` | The protocol-native status/code, kept verbatim for diagnosis — see the table below. |
| `source_ts` | Always absent for MTConnect: there is no device-authored timestamp distinct from the agent's own capture stamp. |
| `capture_ts` | The agent's own observation timestamp (the `timestamp` attribute on the `Streams`/`Current` element) — published as the sample's `serverTs`. |
| `received_ts` | Auto-stamped by the worker at read completion for every reading the backend does not stamp itself. |
| `extra` | Protocol extras that ride every sample: `sequence` (the agent's once-only ordering key) always; `resetTriggered`, `nativeCode`, `sampleRate`, `statistic`, `duration` when the observation carries them. |
| `channel` | An explicit UNS channel override, from `signal.channel`. |

## Value typing by MTConnect category

MTConnect's `SAMPLE`/`EVENT`/`CONDITION` categories decode to different JSON shapes:

- **SAMPLE** (a measured quantity — position, speed, load, temperature): a single JSON number, or a
  JSON array of numbers for a **TimeSeries** representation or a vector unit (`MILLIMETER_3D` and
  similar) — whitespace-separated numeric tokens in the element text become one array, in order.
  Anything that is not a clean numeric vector falls back to a string rather than being coerced.
- **EVENT** (a discrete or enumerated state — `execution`, `mode`, a part count): the element's text,
  verbatim, as a JSON string — **unless** the device model says the type is numeric (it has `units`,
  or the type is one of `PART_COUNT`, `LINE`, `LINE_NUMBER`, `TOOL_NUMBER`, `SEQUENCE_NUMBER`,
  `PATH_FEEDRATE_OVERRIDE`, `ROTARY_VELOCITY_OVERRIDE`), in which case it decodes as a JSON number.
- **CONDITION** (an alarm/diagnostic state — `Xtravel`, a limit switch): the value **is the state
  name itself** — `"NORMAL"`, `"WARNING"`, `"FAULT"`, or `"UNAVAILABLE"` — read off the observation
  element's own tag name (`<Normal>`, `<Warning>`, `<Fault>`, `<Unavailable>`), never guessed.
  Unrecognized condition element names default to `UNAVAILABLE` — a state this client cannot read
  must not look healthy. See [Conditions](#conditions-state-as-value-and-as-a-quality-modifier)
  below for how the same data item can also *degrade another signal's quality*.
- **DataSet / Table representation** (a data item whose `representation` attribute is `DATA_SET` or
  `TABLE`): decodes to a JSON object keyed by each `<Entry key="...">`'s key; `TABLE` entries nest one
  level via `<Cell key="...">` children. An entry marked `removed="true"` becomes an explicit JSON
  `null` — "this key is gone" is not the same as "this key was never present".

A data item this client's cached model does not (yet) know about still decodes — conservatively, from
the observation element itself rather than the model — because a stream can outrun a re-probe by a
moment; it is never silently dropped for lack of metadata.

## `UNAVAILABLE`

MTConnect's own `UNAVAILABLE` state (a device that has not yet reported a value, or a data item that
temporarily cannot be read) is never coerced to `0`, `""`, or omitted. It publishes as an **explicit
JSON `null`** with `quality: BAD` and `qualityRaw: "UNAVAILABLE"` — a legitimate protocol answer,
deliberately published, and structurally different from a signal that has simply not changed.

## Conditions: state-as-value, and as a quality modifier

A CONDITION data item is a signal like any other — configure it directly and its own state
(`NORMAL`/`WARNING`/`FAULT`/`UNAVAILABLE`) publishes as that signal's value. Additionally, any
*other* signal's `conditionBinding[]` can name one or more CONDITION data items to fold their state
into that signal's own quality, without changing its value:

| Condition state | Bound signal's quality | `qualityRaw` |
|---|---|---|
| `Normal` | unaffected (stays whatever the value's own quality was) | — |
| `Warning` | degrades to `Uncertain` (never *improves* a worse quality) | `MTC_CONDITION:WARNING[:<nativeCode>]` |
| `Fault` | degrades to `Bad` | `MTC_CONDITION:FAULT[:<nativeCode>]` |
| `Unavailable` | degrades to `Bad` | `UNAVAILABLE` |

When a signal binds more than one condition, the **worst** currently-observed state wins
(`Fault` > `Unavailable` > `Warning` > `Normal`). A condition's own severity — not its data-item id —
decides which one is reported.

## Quality

`Quality::Good` / `Bad` / `Uncertain`, published on the wire as `"GOOD"` / `"BAD"` / `"UNCERTAIN"`.
For the real MTConnect backend, `quality_raw` follows one fixed vocabulary:

| `qualityRaw` | When |
|---|---|
| `MTC_OK` | A normal, in-range scalar or vector reading. |
| `UNAVAILABLE` | The data item's own value is MTConnect `UNAVAILABLE`. |
| `MTC_NO_SUCH_DATAITEM` | The configured `dataItemId` is not present in the device's current probe model. |
| `MTC_CONDITION:WARNING[:<code>]` / `MTC_CONDITION:FAULT[:<code>]` | A bound condition degraded this signal — see above. |

The built-in simulator only ever produces `Good` (`temperature-1`, `quality_raw: "OK"`) and `Bad`
(`pressure-1`, always faulted, `quality_raw: "SENSOR_FAULT"`, `value: null`) — proof that a failed
reading is **published**, never swallowed, independent of which backend is in use.

## Timestamps — the four-slot model

A `Reading` carries up to three optional ISO-8601 UTC timestamps, never synthesized from one another
(the fourth slot — the publish moment — is the envelope header's, stamped by the library):

- **`capture_ts`** becomes the sample's `serverTs` — for MTConnect, this is always the agent's own
  observation `timestamp` attribute, since the agent is the mediating server between the physical
  controller and this client.
- **`received_ts`** is auto-stamped by the worker at read completion for every reading the backend
  did not stamp itself, and additionally rides as a per-sample `receivedTs` extra only when it
  differs from the effective `serverTs` — i.e., whenever there was a capture stamp to compare it
  against.
- **`source_ts`** would ride as `sourceTs` if the device supplied a machine-authored time distinct
  from the agent's; MTConnect has no such concept, so this slot is always `None` for the real
  backend and `sourceTs` never appears on an MTConnect sample.

```jsonc
{ "value": 123.456, "quality": "GOOD", "qualityRaw": "MTC_OK",
  "serverTs": "2026-07-27T10:00:04.250000Z",
  "receivedTs": "2026-07-27T10:00:04.900000Z",
  "extra": { "sequence": 44821 } }
```

## Published identity

Every `SouthboundSignalUpdate` carries, in `body.signal`: `id` (the stable id above) and `name` (the
optional human label). `device.adapter` (`"mtconnect"` or `"sim"`) and `device.endpoint`
(`mtconnect://<host>[:<port>]/<uuid>` for a real device, `sim://<id>` for the simulator) accompany
every reading, so a consumer can always tell which backend and which physical connection a value
came from, independent of `signal.id`.

## The probe address (`sb/signals[].address`, `sb/browse` entries)

Once an `mtconnect`-adapter instance has been probed at least once, its cached model supplies a
round-trippable, non-secret address for every data item — see
[reference/messaging-interface.md](messaging-interface.md) for the full `sb/signals`/`sb/browse`
shapes:

```jsonc
{ "protocol": "mtconnect", "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456",
  "dataItemId": "Xabs", "category": "SAMPLE", "type": "POSITION", "subType": "ACTUAL",
  "componentPath": "Axes/Linear[X]" }
```

Every field the probe supplies is `null` until the device has been probed — the binding keys
(`protocol`/`agentId`/`deviceUuid`/`dataItemId`) are configuration and always known immediately.
