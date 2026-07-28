# Reference — Metrics

*This documents the generated scaffold; rewrite it as you build the component out.*

`MtconnectAdapter` emits metrics through the EdgeCommons metric service (`src/metrics.rs`). With
`metricEmission.target: messaging`, they publish on the reserved UNS `metric` class:

```text
ecv1/{device}/mtconnect-adapter/metric/{metricName}
```

The adapter never writes reserved `metric` topics directly — it defines metrics through
`MetricBuilder`, so the same names/measures/dimensions reach `log`, `messaging`, `cloudwatch`, and
`prometheus` targets identically.

## Dimension model

Every dimension is deliberately low-cardinality: `instance`, `verb` (the closed set of registered
`sb/*` verbs), and `result` (`success`/`error`). Signal names, addresses, endpoints, and raw error
text are **never** metric dimensions — an unbounded dimension shreds a fleet dashboard. Use `data`,
`evt`, logs, or command replies for those details.

## `southbound_health`

The metric **every** southbound adapter emits — the canonical floor, unchanged across protocols.

Dimensions: `instance`.

| Measure | Unit | Res (s) | Purpose |
|---|---:|---:|---|
| `connectionState` | Count | 1 | `1` connected, `0` not. Drives simple health alarms. |
| `publishLatencyMs` | Milliseconds | 1 | Time spent publishing the most recent poll's readings. |
| `pollLatencyMs` | Milliseconds | 1 | Time spent reading the device on the most recent poll. |
| `readErrors` | Count | 60 | Failed reads in the reporting interval — polling failures without reading logs. |
| `staleSignals` | Count | 60 | Signals with no update for longer than `healthThresholds.staleSignalSecs`. |
| `reconnects` | Count | 60 | Reconnects (link drops that required re-establishing the session). |
| `writeErrors` | Count | 60 | Write entries that failed on the device path in the reporting interval — device rejections and unavailable-device aborts. Entries that never reach the device (unresolved refs, allow-list refusals, missing values) are not counted. |
| `signalsSubscribed` | Count | 1 | Signals the connected session currently serves (the `sb/signals` inventory size); `0` while disconnected. A gauge, not a pair. |

## `MtconnectAdapterConnection`

The worked operational family for the connect/reconnect lifecycle — named from the component so a
fleet view separates one adapter's connection health from another's.

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

The worked operational family for the `sb/*` command surface.

Dimensions: `instance`, `verb`, `result` (`success`/`error`) — the full `(verb, result)` matrix is
pre-defined at startup so the dimension set is fixed and discoverable.

| Measure | Unit | Purpose |
|---|---:|---|
| `commandRequestsTotal` / `commandRequestsInterval` | Count | Invocations of this verb with this result. |
| `commandErrorsTotal` / `commandErrorsInterval` | Count | Invocations that returned a coded error (mirrors the `error`-result rows of `commandRequests`, kept separate for a quick numerator). |
| `commandLatencyMs` | Milliseconds | Accumulated handler latency for this `(verb, result)` combination. |

## The Total/Interval counter convention

Every **counter** measure is emitted as a pair: `<name>Total` (monotonic since the process started)
and `<name>Interval` (since the previous emit of that family — **reset on emit**). Gauges
(`connectionState`, `signalsSubscribed`) and interval sums (the `*Ms` latencies/durations) are
single measures. This is
the same convention `modbus-adapter` and `ethernet-ip-adapter` use, so a fleet dashboard reads every
adapter's operational metrics the same way.

## Adding your protocol's families

Your protocol has its own inventory, poll/subscribe path, and publish path worth measuring — add
`MtconnectAdapterInventory` / `MtconnectAdapterPoll` / `MtconnectAdapterPublish` families next to
the two above. See the [how-to guide](../how-to-guides.md#add-your-protocols-metric-families) and
`modbus-adapter`/`ethernet-ip-adapter`'s fully worked equivalents (poll cycles, samples
good/bad/changed/suppressed, batch flushes, …) for the full pattern.
