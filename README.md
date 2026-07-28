# MTConnect Adapter for EdgeCommons

Connects machine tools sitting behind MTConnect agents to the EdgeCommons Unified Namespace, so a
CNC's axis position charts next to a Modbus register and an OPC UA node without a consumer knowing
any of the three protocols.

Two things to know before you read further:

- **It is a client of agents, never an agent itself.** It serves no HTTP endpoints and keeps no
  sequence buffer; it reads `/probe`, `/current`, and `/sample` from agents you already run. If your
  machines have no agent in front of them, install the canonical free
  [`mtconnect/agent`](https://hub.docker.com/r/mtconnect/agent) Docker image next to them first —
  this component consumes it, the way the OPC UA adapter consumes a Kepware server.
- **It is read-only, by protocol design.** MTConnect agents serve observations and accept no writes
  (Part 1 Fundamentals §5.1), so nothing here can command a machine. The `sb/write` verb exists only
  to answer `WRITE_NOT_ALLOWED` instead of "unknown verb".

## How it fits together

```text
   machine tools        MTConnect agent(s)        MtconnectAdapter          UNS broker
  ──────────────  SHDR  ──────────────────  HTTP  ─────────────────  MQTT  ────────────────
   CNC · robot    ────►  /probe /current    ────►  one instance      ────►  edge-console
   cell · PLC            /sample (stream)          per machine              processors
                                                   signals + health         historians
```

One `component.instances[]` entry is one machine — one MTConnect `Device/@uuid` — and its id becomes
the `{instance}` token of every topic that machine publishes on. An agent, by contrast, is shared
plumbing: you declare it once in `component.global.agents[]`, and every machine it serves attaches to
that single acquisition rather than opening a socket of its own. A hundred machines behind one agent
cost one HTTP connection and one document stream, and one machine's trouble never disturbs another's.

## Quick start

Five minutes, no machine tools, no agent to install: the public MTConnect demo agent stands in for
the plant.

**Prerequisites.** Rust (1.85+), and a local MQTT broker:

```bash
docker run -d --name edgecommons-emqx -p 1883:1883 -p 18083:18083 emqx/emqx:latest
```

**The config.** Save this as `demo-agent.json` — an agent, a machine on it, and "publish everything":

```json
{
  "hierarchy": { "levels": ["site", "device"] },
  "identity": { "site": "factory-1" },
  "component": {
    "token": "mtconnect-adapter",
    "global": {
      "agents": [
        { "id": "demo-agent", "url": "https://demo.mtconnect.org" }
      ]
    },
    "instances": [
      {
        "id": "okuma",
        "connection": { "agentId": "demo-agent", "deviceUuid": "OKUMA.123456" },
        "selection": { "mode": "all" }
      }
    ]
  }
}
```

`https://demo.mtconnect.org` is the MTConnect Institute's public agent; it serves two machines,
`OKUMA.123456` and `Mazak`. Add a second instance pointed at `Mazak` and both stream through the same
agent connection.

**Run it.** The messaging file is separate from the component config — it tells the transport which
broker to use, and the shipped one points at `localhost:1883`:

```bash
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./demo-agent.json \
  -t demo-thing
```

**What you'll see.** Within a couple of seconds the adapter probes the agent, derives a signal for
every data item the machine reports, and starts publishing — 105 topics for this device, updating as
fast as the machine reports them:

```text
ecv1/demo-thing/mtconnect-adapter/okuma/data/axes-axes/linear-x/lx1actm
ecv1/demo-thing/mtconnect-adapter/okuma/data/axes-axes/rotary-c1/ls1-chuck-state
ecv1/demo-thing/mtconnect-adapter/okuma/data/controller-controller/lpexecution
ecv1/demo-thing/mtconnect-adapter/okuma/data/systems-systems1/coolant-coolant-system1/lcoolant-system1-cond
ecv1/demo-thing/mtconnect-adapter/okuma/data/lavail
```

The envelope on the X-axis position topic:

```json
{
  "header": { "name": "SouthboundSignalUpdate", "version": "1.0" },
  "identity": {
    "hier": [ { "level": "site", "value": "factory-1" },
              { "level": "device", "value": "demo-thing" } ],
    "path": "factory-1/demo-thing", "component": "mtconnect-adapter", "instance": "okuma"
  },
  "body": {
    "signal": { "id": "lx1actm", "name": "X1actm" },
    "samples": [
      { "value": 4622.9039, "quality": "GOOD", "qualityRaw": "MTC_OK",
        "serverTs": "2026-07-28T16:40:10.505404Z",
        "receivedTs": "2026-07-28T16:40:17.7568664Z", "sequence": 1651597 }
    ],
    "device": { "adapter": "mtconnect", "instance": "okuma",
                "endpoint": "mtconnect://demo.mtconnect.org/OKUMA.123456" }
  }
}
```

Every field there is derived: the signal id from the machine's own `dataItemId`, the channel from its
place in the component tree, the endpoint from the agent binding. Nothing was named by hand.
[The tutorial](docs/tutorial.md) walks the same path with the built-in simulator and then a real
agent; [the messaging reference](docs/reference/messaging-interface.md) is the full topic and payload
contract.

## Configuration at a glance

The adapter reads one JSON document from `-c/--config`. Its own settings live under `component`;
everything around them (`identity`, `hierarchy`, `messaging`, `logging`, `metricEmission`,
`heartbeat`) is the standard EdgeCommons envelope. One key inside `component` belongs to that
envelope too: `component.token` is the adapter's UNS token, `mtconnect-adapter` — the `{component}`
segment of every topic it publishes and the `identity.component` field of every message. Every
option below is specified in [the configuration reference](docs/reference/configuration.md).

**Agents are declared once and referenced by id.** A real plant agent usually wants TLS and
credentials, and both arrive as *references* resolved through the EdgeCommons vault — a secret never
appears in the configuration, the logs, or a command reply:

```json
{ "id": "line-a-agent", "url": "https://agent.line-a.example.com",
  "auth": { "type": "basic", "username": "svc-mtconnect", "secretRef": "line-a/agent-password" },
  "tls": { "caSecretRef": "line-a/agent-ca" },
  "streaming": "prefer", "heartbeatMs": 10000 }
```

`streaming: "prefer"` (the default) opens a long-lived multipart `/sample` stream and pushes
observations as they happen; `"poll-only"` reads `/current` on `pollIntervalMs` instead, for an agent
or a network that will not hold a stream open.

**Instances name a machine on an agent, and nothing else.** The published endpoint is derived from
those two values rather than configured, so the binding and the endpoint cannot drift apart:

```json
{ "id": "cnc-1", "adapter": "mtconnect",
  "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" } }
```

**Choosing signals: derive them, or pin them.** A `selection` block publishes data items by matcher —
or wholesale with `{ "mode": "all" }` — and follows the machine: an item that appears after a model
change starts publishing, one that disappears stops. An explicit `signals[]` entry does the opposite:
it fixes an identity that must survive a machine reconfiguration, and a `dataItemId` the model no
longer has publishes a permanent BAD rather than vanishing. Use both — an explicit entry overrides
the derived one for the same data item, field by field:

```json
"signals": [ { "id": "x-position", "channel": "machining/x", "dataItemId": "Xabs" } ],
"selection": {
  "mode": "include",
  "include": [ { "category": "SAMPLE", "path": "Axes/**" }, { "type": "EXECUTION|PROGRAM" } ],
  "exclude": [ { "idMatch": ".*-debug" } ]
}
```

Fields within one matcher are ANDed, matchers are ORed, exclude always wins, and regexes are anchored
so `POSITION` cannot creep into `PATH_POSITION`. See
[`selection`](docs/reference/configuration.md#componentinstancesselection) for the derivation rules.

**Publish shaping controls what reaches the bus.** A fast axis does not need every reading as its own
message, and a noisy sensor does not need to publish at all when it has not really moved:

```json
"publish": { "mode": "on-change", "batchMs": 1000, "deadband": 0.5 }
```

`batchMs` coalesces a window's readings into one update whose `samples[]` carries them all;
`mode: "interval"` keeps only the latest per window; `deadband` suppresses numeric changes smaller
than itself. A BAD or UNCERTAIN reading always flushes its window immediately — a quality transition
never sits waiting. See [publish shaping](docs/reference/configuration.md#publish-shaping).

## Operating it

The adapter answers the generic southbound command family on its command inbox, addressed either to
a machine on the topic (`…/{instance}/cmd/{verb}`) or to the component with the machine named in the
body:

| Verb | What it does |
|---|---|
| `sb/status` | The machine's live state plus what the agent has taught us — version, `instanceId`, buffer and sequence window, acquisition mode, heartbeat age, probe digest. |
| `sb/signals` | The inventory this machine is serving, each row carrying its probe address and whether it was configured or discovered. |
| `sb/browse` | The device's structure, paged or hierarchical, straight from the cached probe model — so it keeps working while the agent is unreachable. |
| `sb/read` | A fresh scoped `/current` snapshot of named signals, with a per-entry reason when one cannot be read. |
| `sb/pause` / `sb/resume` | Stop and restart publishing for one machine. Resume republishes the whole inventory first, so nobody consumes a stale picture. |
| `reconnect` / `repoll` | Re-establish the session; force a fresh full read. |
| `sb/write` | Always refuses with `WRITE_NOT_ALLOWED`, before it inspects the request. MTConnect has no write path, so there is nothing to allow-list — the verb stays registered so a caller gets an explanation instead of "unknown verb", and it is advertised `unsupported` so a console never renders a control that cannot work. |

Five [edge-console](https://github.com/edgecommons/edge-console) panels ship with the component and
mount automatically: **Overview** (state, lifecycle actions, link health), **Device Structure** (the
probe tree), **Signals** (the served inventory), **Conditions & Events** (machine alarms, data loss,
agent transitions), and **Diagnostics** (sequence and buffer state, stream metrics). None of them
names a write surface.

What to watch when something looks wrong:

| Signal of trouble | Where it shows | Reading it |
|---|---|---|
| Link down | `southbound_health.connectionState` | `0` means this machine's session is not established. `MtconnectAgentEvent` says whether the agent went away or the stream degraded to polling. |
| Fewer signals than expected | `southbound_health.signalsSubscribed` | What the session is actually serving. A drop means data items the configuration names are missing from the current device model. |
| Values going quiet | `southbound_health.staleSignals` | Signals with no update for longer than `healthThresholds.staleSignalSecs`. |
| Gaps in history | `MtconnectDataLossEvent` | The agent's ring buffer overran our position; the event names how many observations were provably lost and the sequence window they fell in. |
| Ids or structure changed | `MtconnectModelDriftEvent`, `MtconnectSignalSetEvent` | The machine's model changed and the signal set was recomputed; the events name the digests and the added/removed counts. |
| A machine alarm | `MtconnectConditionEvent` | A condition transitioned into `Fault`. Any signal bound to it through `conditionBinding` degrades to BAD at the same moment. |

Every measure is defined in [the metrics reference](docs/reference/metrics.md); every event body in
[the messaging reference](docs/reference/messaging-interface.md).

## Behavior worth knowing

**Acquisition heals itself.** The adapter prefers streaming and falls back to polling: after three
consecutive failures to establish a stream it degrades to `/current` polling and keeps retrying the
stream on a jittered backoff, so a plant full of adapters does not reconnect in lockstep. A missed
heartbeat, or a broken frame, re-establishes the stream from the same position.

**An agent restart or a buffer overrun is recovered and reported — never silent.** If the agent's
buffer has moved past our position, the adapter reports how many observations were provably lost and
republishes a fresh snapshot as current data. If the agent's `instanceId` changed — it restarted — it
re-probes, surfaces any model drift, and resyncs. Each of those raises an event, because data you
never received is a fact an operator needs, not a log line to find later.

**Two timestamps, two meanings.** `serverTs` is when the machine's data was captured, as the agent
reported it. `receivedTs` is when this adapter received it — the two differ by however long the
observation sat in the agent's buffer, which is why both are published.

**What it does not do.** MTConnect Assets (`/asset`) and the optional JSON representation are not
read — only the XML `Devices`, `Streams`, and `Errors` documents. Consuming an agent's MQTT sink
instead of its HTTP interface is not supported. And nothing is writable, ever.

## Deployment

Three packs ship in the repo, all driven by the same CLI contract
(`--platform` / `--transport` / `-c` / `-t`):

- **HOST** — `Dockerfile` plus `compose.yaml`, which brings up a throwaway EMQX broker and the
  component together (`docker compose up --build`). This is the pack the quick start above exercises,
  and it is validated live against a real broker and a real agent.
- **Greengrass v2** — `recipe.yaml`, `gdk-config.json`, and `build.sh` for `gdk component build` /
  `gdk component publish`. The pack is generated from the component's own configuration; it has not
  been run on a Greengrass device.
- **Kubernetes** — `k8s/deployment.yaml` and `k8s/configmap.yaml`; with `--platform auto` the library
  detects Kubernetes and takes its config from the mounted ConfigMap and its identity from the
  Downward API, so the container needs no arguments. The manifests are generated; they have not been
  run on a cluster.

[The deployment how-to](docs/how-to-guides.md) has the commands for each.

## Contributing & development

The repo:

- `src/` — the component above the protocol: the device seam, the connect/acquire/publish supervisor,
  the command surface, the metric families, the reload gate, and the publish-shaping engine.
- `src/mtconnect/` — the owned MTConnect HTTP/XML client: probe model, observations, the sequence and
  resync state machine, multipart streaming, and signal derivation.
- `tests/` — the unit and integration suites, plus `compose.mtconnect-agent.yaml`, which stands up
  the pinned canonical cppagent as a live test peer.
- `docs/` — the full documentation set: tutorial, how-to guides, reference, and explanation.
- `config.schema.json` and `test-configs/` — the configuration contract and runnable examples; the
  deployment packs sit at the repo root and in `k8s/`.

**The one boundary rule:** nothing under `src/mtconnect/` may import `edgecommons`. A backend knows
protocols; it does not know UNS topics, envelopes, or metrics. `tests/isolation.rs` enforces this
mechanically, so crossing the line fails the build rather than a review.

Build and test:

```bash
cargo test                                    # no network, no broker, no agent needed
cargo clippy --all-targets -- -D warnings
cargo llvm-cov --fail-under-lines 90          # the coverage gate CI enforces
```

The live suites self-skip unless you point them at something. For the real-agent harness:

```bash
docker compose -f tests/compose.mtconnect-agent.yaml up -d
EC_MTC_AGENT=http://localhost:5010 cargo test --test agent_integration -- --test-threads=1
```

That harness covers what a fake agent cannot: agent restart, `instanceId` resync, buffer wrap and
`OUT_OF_RANGE` recovery, and multi-device demultiplexing on one stream.

Where to look next: [`DESIGN.md`](DESIGN.md) for why the component is shaped this way and which
decisions are binding, [`AGENTS.md`](AGENTS.md) for the invariants an agent or a contributor must not
remove, and [`docs/`](docs/README.md) for the user-facing documentation set.

## License

Business Source License 1.1 — see [`LICENSE`](LICENSE).
