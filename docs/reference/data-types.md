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
| `received_ts` | The moment the agent's payload reached this adapter — stamped when the runtime ingests the document that carried the observation, not when a device task later drains it. A backend that stamps nothing gets the worker's read-completion fallback. |
| `extra` | Protocol extras that ride every sample: `sequence` (the agent's once-only ordering key) always; `resetTriggered`, `nativeCode`, `sampleRate`, `statistic`, `duration` when the observation carries them; `conditionId`, `nativeSeverity`, `qualifier`, `conditionText`, and `activeConditions` on condition observations; `passive` on the synthetic quality transitions described [below](#passive-quality--held-values-under-a-silent-agent). |
| `channel` | An explicit UNS channel override, from `signal.channel`. |

## Required observation fields

MTConnect makes `dataItemId`, `sequence`, and `timestamp` required on every observation, and this
client refuses rather than invents: an observation missing its `dataItemId`, carrying a `sequence`
that does not parse as an integer ≥ 1, or carrying an empty `timestamp` is **rejected** — dropped
and counted by `MtconnectParse.rejectedObservations` — never defaulted. Only the timestamp's
*presence* is checked; its text rides verbatim, because real agents vary in their RFC3339 spelling
and refusing a variant would lose real data.

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
  name** — `"NORMAL"`, `"WARNING"`, or `"FAULT"` — read off the observation element's own tag name
  (`<Normal>`, `<Warning>`, `<Fault>`), never guessed, and aggregated across the data item's
  concurrent activations (see [Conditions](#conditions-state-as-value-and-as-a-quality-modifier)
  below). An `<Unavailable>` state — and an unrecognized condition element name, since a state this
  client cannot read must not look healthy — publishes as an explicit `null` with `quality: BAD`,
  like any unavailable value. The same data item can also *degrade another signal's quality* — see
  below.
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

A CONDITION data item is a signal like any other — configure it directly and its state publishes as
that signal's value (`NORMAL`/`WARNING`/`FAULT`; MTConnect `UNAVAILABLE` publishes as an explicit
`null`, exactly like an unavailable value). Additionally, any *other* signal's `conditionBinding[]`
can name one or more CONDITION data items to fold their state into that signal's own quality,
without changing its value.

**One condition data item can carry several concurrent activations.** A controller's `system`
condition, say, may be asserted more than once at a time, each activation identified across its own
`Warning`/`Fault`/`Normal` transitions by `conditionId` (or `nativeCode` for agents that predate
it; an agent that sends neither gets a single activation slot, preserving one-condition-per-item
behavior exactly). What a condition signal publishes is the **aggregate** across those activations —
the worst asserted state, with the worst activation's `nativeCode` — so clearing one of two
activations does not promote the signal to `GOOD` while the other still stands, and a mixed batch
publishes the same truth whatever order the agent wrote it in. The triggering transition survives in
the sample's own extras (`conditionId`, `nativeCode`, `conditionText`), and `activeConditions`
counts how many activations stand behind the published state (`0` for a cleared or unavailable data
item). A `Normal` that names an activation clears only that activation; a `Normal` with no identity
is the standard's normal sweep and clears them all.

A condition signal's own quality follows its aggregate state: `NORMAL` is `GOOD`
(`qualityRaw: "MTC_OK:NORMAL"`), `WARNING` is `UNCERTAIN`, `FAULT` is `BAD` — the same
`MTC_CONDITION:*` vocabulary as below. For a **bound** signal, each bound data item contributes its
aggregate:

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
| `MTC_OK:NORMAL` | A condition signal whose aggregate state is `NORMAL`. |
| `UNAVAILABLE` | The data item's own value is MTConnect `UNAVAILABLE`. |
| `MTC_NO_SUCH_DATAITEM` | The configured `dataItemId` is not present in the device's current probe model. |
| `MTC_CONDITION:WARNING[:<code>]` / `MTC_CONDITION:FAULT[:<code>]` | A condition signal's own aggregate state, or a bound condition degrading another signal — see above. |
| `MTC_STALE:<ageMs>` | A held value republished with a degraded verdict because the agent stopped vouching for its currency — `<ageMs>` is the liveness age. `UNCERTAIN` past one missed heartbeat/poll, `BAD` past `staleSignalSecs`. See below. |
| `MTC_AGENT_UNREACHABLE` | A held value republished `BAD` because the agent link is down. See below. |

The built-in simulator only ever produces `Good` (`temperature-1`, `quality_raw: "OK"`) and `Bad`
(`pressure-1`, always faulted, `quality_raw: "SENSOR_FAULT"`, `value: null`) — proof that a failed
reading is **published**, never swallowed, independent of which backend is in use.

## Passive quality — held values under a silent agent

MTConnect is an on-change protocol: an unchanged value is not a silent one, so a signal's own age
says nothing about whether its value is still true. What does say so is the **liveness clock** —
the time since the agent last vouched for currency, either by delivering a Streams document (data
or heartbeat) or by answering a `/current` cycle. When that clock crosses a threshold, every value
the instance holds on the wire is **republished with a degraded verdict**, so a consumer never
keeps a `GOOD` value forever against an agent that has gone quiet:

| Link state | Republished quality | `qualityRaw` | `passive` extra |
|---|---|---|---|
| Liveness age past one missed heartbeat (`heartbeatMs`, streaming) or two missed polls (`2 × pollIntervalMs`, polling) | `GOOD` → `UNCERTAIN`; an already-`UNCERTAIN` value keeps its own reason | `MTC_STALE:<ageMs>` | `"stale"` |
| Liveness age past `healthThresholds.staleSignalSecs` — the limit on how long a held value may stand in | `BAD` | `MTC_STALE:<ageMs>` | `"expired"` |
| The agent link is down | `BAD` | `MTC_AGENT_UNREACHABLE` | `"unreachable"` |
| Liveness returns | the held quality and `qualityRaw`, restored **verbatim** | *(the held one)* | `"recovered"` |

The rules around the table:

- **Only transitions publish.** A steady state — however long it lasts — emits nothing.
- **The clock is the link's, not the signal's.** Every held signal of an instance crosses at the
  same moment and carries the same `<ageMs>`, because the fact being reported is about the agent.
  `healthThresholds.staleSignalSecs` is that expiry threshold on the liveness clock — time since
  the agent last vouched — not a per-signal change age.
- **Every synthetic reading names its observation.** It republishes the held value with the held
  timestamps and extras — the held `sequence` included — plus the `passive` marker, and a fresh
  `receivedTs` for the emission itself. The marker is what tells synthetic quality motion from a
  sample the agent actually delivered.
- **Quality only ever degrades.** A value the agent already called `BAD` never transitions, and an
  `UNCERTAIN` one (a condition `Warning`, say) keeps its own `qualityRaw` at the stale step rather
  than having its reason overwritten. Recovery restores exactly what was held — a value that was
  `UNCERTAIN` for a condition Warning comes back `UNCERTAIN`, not `GOOD`.
- **Transitions bypass batch windows.** A quality transition is news and never sits in a
  [publish-shaping](configuration.md#publish-shaping) window.
- **The `staleSignals` metric is a different question.** It counts genuine per-signal value silence
  ([metrics.md](metrics.md#southbound_health)); synthetic readings do not reset it, and do not
  re-enter the held-value bookkeeping either.
- A fresh snapshot (a resync, `sb/resume`) re-baselines: the held set rebuilds from the snapshot's
  own publishes and the ladder starts over.

The simulator reports no link facts — its read *is* its liveness — so `sim` instances are never
judged passively.

## Timestamps — the four-slot model

A `Reading` carries up to three optional ISO-8601 UTC timestamps, never synthesized from one another
(the fourth slot — the publish moment — is the envelope header's, stamped by the library):

- **`capture_ts`** becomes the sample's `serverTs` — for MTConnect, this is always the agent's own
  observation `timestamp` attribute, since the agent is the mediating server between the physical
  controller and this client.
- **`received_ts`** is the moment the agent's payload reached this adapter — stamped when the
  runtime ingests the document carrying the observation, so it measures agent-to-adapter transit,
  not how long the reading waited in an internal queue. (A reading no backend stamped gets the
  worker's read-completion fallback.) It rides as a per-sample `receivedTs` extra only when it
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

## Appendix — revision history

| Date | Change |
|---|---|
| 2026-08-03 | Condition aggregate semantics and the `conditionId`/`conditionText`/`activeConditions` extras; required-field rejection rules; passive-quality section (`MTC_STALE`/`MTC_AGENT_UNREACHABLE`, the `passive` extra); `received_ts` defined as payload arrival. |
| 2026-07-28 | Initial version. |
