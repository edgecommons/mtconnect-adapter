# Reference — Metrics

`MtconnectAdapter` emits metrics through the EdgeCommons metric service (`src/metrics.rs`). With
`metricEmission.target: messaging`, they publish on the reserved UNS `metric` class:

```text
ecv1/{device}/mtconnect-adapter/metric/{metricName}
```

The adapter never writes reserved `metric` topics directly — it defines metrics through
`MetricBuilder`, so the same names/measures/dimensions reach `log`, `messaging`, `cloudwatch`, and
`prometheus` targets identically.

## Dimension model

Every dimension is deliberately low-cardinality: `instance`, `agentId`, `verb` (the closed set of
registered `sb/*` verbs), and `result` (`success`/`error`). Signal names, `dataItemId`s, addresses,
endpoints, sequence numbers, and raw error text are **never** metric dimensions — an unbounded
dimension shreds a fleet dashboard. Use `data`, `evt`, logs, or command replies for those details.

## `southbound_health`

The metric **every** southbound adapter emits — the canonical floor, unchanged across protocols.

Dimensions: `instance`.

| Measure | Unit | Res (s) | Purpose |
|---|---:|---:|---|
| `connectionState` | Count | 1 | `1` connected, `0` not. Drives simple health alarms. |
| `publishLatencyMs` | Milliseconds | 1 | Time spent publishing the most recent poll's readings. |
| `pollLatencyMs` | Milliseconds | 1 | Time spent draining and reading the device on the most recent cycle. |
| `readErrors` | Count | 60 | Failed reads in the reporting interval — polling failures without reading logs. |
| `staleSignals` | Count | 60 | Signals with no **value** update for longer than `healthThresholds.staleSignalSecs` — genuine value silence. The synthetic passive-quality republishes ([data-types.md](data-types.md#passive-quality--held-values-under-a-silent-agent)) are not value updates and do not reset it. |
| `reconnects` | Count | 60 | Reconnects (link drops that required re-establishing the session). |
| `writeErrors` | Count | 60 | Structurally always `0` for this adapter — MTConnect has no write path — kept for cross-adapter dashboard uniformity. |
| `signalsSubscribed` | Count | 1 | Signals the connected instance currently **serves**: its configured set minus any whose `dataItemId` the current device model does not have (those publish a permanent BAD instead). The same number whether acquisition is streaming or polling — the compiled set is what is served, and the mode only decides how it arrives. `0` while disconnected. A gauge, not a pair. |

## `MtconnectAdapterConnection`

The connect/reconnect lifecycle, per device.

Dimensions: `instance`.

| Measure | Unit | Purpose |
|---|---:|---|
| `connectionState` | Count | `1` connected, `0` not (a gauge, mirrors `southbound_health`). |
| `connectAttemptsTotal` / `connectAttemptsInterval` | Count | Connect attempts, cumulative / since the last emit. |
| `connectFailuresTotal` / `connectFailuresInterval` | Count | Failed connect attempts. |
| `reconnectAttemptsTotal` / `reconnectAttemptsInterval` | Count | Re-establishments after a previous drop (excludes the first connect). |
| `connectionDropsTotal` / `connectionDropsInterval` | Count | Times a live session was lost. |
| `connectedDurationMs` | Milliseconds | Time spent connected since the previous emission. |

## `MtconnectAdapterCommand`

The `sb/*` command surface, per device.

Dimensions: `instance`, `verb`, `result` (`success`/`error`) — the full `(verb, result)` matrix is
pre-defined at startup so the dimension set is fixed and discoverable.

| Measure | Unit | Purpose |
|---|---:|---|
| `commandRequestsTotal` / `commandRequestsInterval` | Count | Invocations of this verb with this result. |
| `commandErrorsTotal` / `commandErrorsInterval` | Count | Invocations that returned a coded error (mirrors the `error`-result rows of `commandRequests`, kept separate for a quick numerator). |
| `commandLatencyMs` | Milliseconds | Accumulated handler latency for this `(verb, result)` combination. |

## `MtconnectAdapterShaping`

What the per-signal publish-shaping engine (the `publish` policy —
[configuration.md](configuration.md#publish-shaping)) did to the instance's flow. Per **instance**,
not per agent: shaping is a property of one device's publication path, above the session — where
`MtconnectStream` measures the shared acquisition below it.

Dimensions: `instance`.

| Measure | Unit | Purpose |
|---|---:|---|
| `publishedTotal` / `publishedInterval` | Count | `SouthboundSignalUpdate`s the engine released to the wire — immediate publishes and window flushes alike. Forced snapshots (`repoll`, the resume snapshot) bypass the engine and are not counted here. |
| `coalescedTotal` / `coalescedInterval` | Count | Readings deferred into a batch window instead of publishing immediately. |
| `deadbandDroppedTotal` / `deadbandDroppedInterval` | Count | Readings a deadband suppressed on entry. |

## `MtconnectStream`

One **agent**'s acquisition — streaming or polling, whichever is currently active — emitted once per
configured agent, not once per device attached to it: an agent's connection and document flow exist
exactly once no matter how many devices share it (`component.global.agents[]`).

Dimensions: `agentId`, `result`.

| Measure | Unit | Purpose |
|---|---:|---|
| `documentsTotal` / `documentsInterval` | Count | `success`: documents decoded. `error`: documents that failed to decode. |
| `observationsTotal` / `observationsInterval` | Count | Observations published from decoded documents (`success` cell only). |
| `heartbeatsTotal` / `heartbeatsInterval` | Count | Heartbeat (empty) documents received while streaming (`success` cell only). |
| `reconnectsTotal` / `reconnectsInterval` | Count | Streams **re**-established after a missed heartbeat, a transport drop, or malformed framing. The first stream a process opens is the initial connect, not a reconnect (`error` cell only). |
| `gapsTotal` / `gapsInterval` | Count | Observations provably lost — the count the agent's `firstSequence` proves was skipped past our position (`error` cell only). |
| `outOfRangeTotal` / `outOfRangeInterval` | Count | `OUT_OF_RANGE` recoveries: the resync-ladder step 2 events those lost observations were discovered through (`error` cell only). |
| `latencyMs` | Milliseconds | Accumulated latency of this cell's acquisition requests (`/current`, opening a stream) since the previous emit. |

## `MtconnectProbe`

One **agent**'s `/probe` traffic, emitted once per configured agent.

Dimensions: `agentId`, `result`.

| Measure | Unit | Purpose |
|---|---:|---|
| `probesTotal` / `probesInterval` | Count | `/probe` requests, split by whether the agent answered. |
| `modelChangesTotal` / `modelChangesInterval` | Count | Probes whose content digest differed from the previously cached model (a `ModelDrift`) — always `0` in the `error` cell, since a failed probe cannot have seen a new model. |
| `latencyMs` | Milliseconds | Accumulated `/probe` latency for this cell since the previous emit. |

## `MtconnectParse`

Document decoding, reported **per device instance** (parsing happens above the per-device split, at
the document level, so every instance attached to one agent reports that agent's own document
counters).

Dimensions: `instance`, `result`.

| Measure | Unit | Purpose |
|---|---:|---|
| `documentsParsedTotal` / `documentsParsedInterval` | Count | Documents this instance's agent decoded successfully. |
| `parseErrorsTotal` / `parseErrorsInterval` | Count | Documents that failed to decode. |
| `rejectedObservationsTotal` / `rejectedObservationsInterval` | Count | Observations the agent sent that this client refused for a missing required field — no `dataItemId`, no `sequence` parsable as an integer ≥ 1, or an empty `timestamp`. A reject is dropped and counted, never defaulted. |

This family is only defined for an `mtconnect`-adapter instance; the built-in simulator parses no
documents and emits nothing under this name.

## The Total/Interval counter convention

Every **counter** measure is emitted as a pair: `<name>Total` (monotonic since the process started)
and `<name>Interval` (since the previous emit of that family — **reset on emit**). Gauges
(`connectionState`, `signalsSubscribed`) and interval sums (the `*Ms` latencies/durations) are single
measures. This is the same convention `modbus-adapter` and `ethernet-ip-adapter` use, so a fleet
dashboard reads every adapter's operational metrics the same way. Within `MtconnectStream` and
`MtconnectProbe`, the `result` dimension is a full outcome split, not just an error tally: every
measure exists in both the `success` and `error` cells, and each counter is non-zero only in the cell
its outcome actually belongs to (a stream gap is by definition a recovery from something that went
wrong, so it only ever appears in the `error` cell).

## Appendix — revision history

| Date | Change |
|---|---|
| 2026-08-03 | `MtconnectParse` gains `rejectedObservations`; `staleSignals` meaning pinned to value silence. |
| 2026-07-28 | Initial version. |
