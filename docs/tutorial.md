# Tutorial — From simulator to a real MTConnect agent

By the end you will have built `com.mbreissi.edgecommons.MtconnectAdapter`, run it first against its
built-in device simulator, watched a signal update flow onto the Unified Namespace (UNS), read a
value back through the command surface, then pointed the same binary at a real MTConnect agent and
watched its probe/observation model come to life — browsing the device structure, reading the
agent's own capability, and seeing a bound condition degrade a signal's quality.

Command examples use [ec-uns-cmd](https://github.com/edgecommons/ec-uns-cmd), installed on `PATH`.
`--body` is a native JSON argument object; the tool constructs protobuf, subscribes before publishing,
and prints the reply `result` or `error` within a deadline. Topic instance addressing selects the device.

## 1. Prerequisites

- A Rust toolchain (edition 2024, `rust-version = "1.85"` — matches the `edgecommons` library's MSRV).
- A local MQTT broker on `localhost:1883` (`docker run -d -p 1883:1883 emqx/emqx`, or
  `docker compose up -d` from this repo's own `compose.yaml`).
- For the second half: Docker, to run a real MTConnect agent.

## 2. Build it

```bash
cargo build
```

## 3. Run it against the simulator

No agent, no hardware:

```bash
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

`test-configs/config.json` configures one device, `device-1`, using the `sim` backend
(`"adapter": "sim"`) at a poll interval of 5 seconds. You should see it connect immediately (the
simulator never fails to connect unless its endpoint is empty) and start polling.

## 4. Watch values flow

Subscribe to the UNS `data` class — this filter covers instance-scope adapter data; component-scope fleet data also requires `ecv1/+/+/data/#`:

Normal messaging carries protobuf bytes. In this organization workspace install the matching
Python decoder with `pip install paho-mqtt -e ../core/libs/python`, then display a human-readable
JSON projection with this subscriber. The here-document uses Bash; in PowerShell save its Python
contents to a `.py` file and run `python <file>.py`.
```bash
python - <<'PY'
import json
import paho.mqtt.client as mqtt
from edgecommons.messaging.message import Message

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
c.on_connect = lambda c, u, f, rc, p: c.subscribe("ecv1/my-thing/mtconnect-adapter/+/data/#", qos=1)
def on_message(c, u, m):
    message = Message.from_bytes(m.payload)
    print(m.topic, json.dumps(message.to_diagnostic_json(), indent=2))
c.on_message = on_message
c.connect("localhost", 1883)
try:
    c.loop_forever()
except KeyboardInterrupt:
    pass
finally:
    c.unsubscribe("ecv1/my-thing/mtconnect-adapter/+/data/#")
    c.disconnect()
PY
```

Every ~5 seconds you should see two `SouthboundSignalUpdate` messages on
`ecv1/my-thing/mtconnect-adapter/device-1/data/temperature-1` and
`ecv1/my-thing/mtconnect-adapter/device-1/data/pressure-1`:

- `temperature-1` carries a moving sine-wave value with quality `GOOD`.
- `pressure-1` is **deliberately faulted** — the simulator always reports it with quality `BAD` and
  `qualityRaw: "SENSOR_FAULT"`, and a `null` value. This is on purpose: a failed reading is
  published, not swallowed, so a consumer can tell "this signal is bad" from "this signal stopped
  existing".

Also try:

```bash
mosquitto_sub -t 'ecv1/+/+/state' -v      # the keepalive, with per-device connectivity
mosquitto_sub -t 'ecv1/+/+/metric/#' -v   # southbound_health + the operational families
```

The `state` keepalive's `instances[]` array carries one entry for `device-1` —
`{ "instance": "device-1", "connected": true, "state": "ONLINE", "detail": "sim://device-1", ... }`
— fed by the same connectivity provider the built-in `status` command verb reads.

## 5. Read a signal on demand

The read/status/browse surface rides the library's command inbox at
`ecv1/{device}/mtconnect-adapter[/{instance}]/cmd/{verb}`. The `--instance` option below selects
the instance topic for `device-1`. With an EdgeCommons SDK, build `header.name` from the full verb
and `header.reply_to`/`header.correlation_id` for the reply:

```bash
ec-uns-cmd --broker localhost:1883 --device my-thing --component mtconnect-adapter --instance device-1 sb/read --body '{"signals":[{"signalId":"temperature-1"}]}'
```

Only one device is configured, so you can also omit `--instance`: the component-scope command
routes to the sole configured device. With multiple devices, use `--instance` to address the
target explicitly. The topic instance is authoritative; a conflicting `body.instance` is rejected.

## 6. Check status

```bash
ec-uns-cmd --broker localhost:1883 --device my-thing --component mtconnect-adapter --instance device-1 sb/status
```

The reply has no `protocol` object here — that field only appears for an `mtconnect`-adapter
instance, since it carries the agent's own capability and standard version.

## 7. Run it against a real MTConnect agent

Stand up the pinned reference agent (cppagent) this repo's own integration suite validates
against:

```bash
docker compose -f tests/compose.mtconnect-agent.yaml up -d
```

This starts an MTConnect agent on `localhost:5010` serving two simulated devices, including one with
uuid `MTC-E2E-001`. Point the adapter at `test-configs/mtconnect.json` instead, which declares one
agent (`line-a-agent`, `http://127.0.0.1:5000` in the shipped file — change its `url` to
`http://127.0.0.1:5010` to reach the compose agent, or edit the file to match whatever agent you have
running) and one instance (`cnc-1`) bound to it, with five signals including a condition binding:

```bash
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/mtconnect.json \
  -t my-thing
```

Watch the same `data` topics as before — now carrying real MTConnect observations, e.g.
`ecv1/my-thing/mtconnect-adapter/cnc-1/data/x-position` with `qualityRaw: "MTC_OK"` and a
`serverTs` taken from the agent's own capture timestamp, plus `extra.sequence` — the agent's
once-only ordering key, carried on every sample.

## 8. Ask the agent what it knows

`sb/status` now returns a `protocol` object once the agent has answered its first `/probe`:

```bash
ec-uns-cmd --broker localhost:1883 --device my-thing --component mtconnect-adapter --instance cnc-1 sb/status
```

`sb/browse` walks the same device's probe tree — try it with no body for the first page, or with
`{"ref":"root","depth":1}` for the hierarchical panel shape (see
[reference/messaging-interface.md](reference/messaging-interface.md) for both response shapes).

## 9. See a bound condition degrade a signal

`test-configs/mtconnect.json`'s `x-position` signal declares `"conditionBinding": ["Xtravel"]`. Drive
the agent's `Xtravel` CONDITION data item into `Warning` or `Fault` (through the agent's own adapter
feed, or `tests/agent_integration.rs` if you are reading the code) and watch `x-position`'s next
published sample carry `quality: "UNCERTAIN"` or `"BAD"` with the alarm's own code in `qualityRaw`
(`MTC_CONDITION:WARNING:<code>` / `MTC_CONDITION:FAULT:<code>`) — the value itself is unchanged; only
the quality reflects the machine's own alarm state. The `x-travel-condition` signal in the same
config publishes the condition's own state (`NORMAL`/`WARNING`/`FAULT`) directly, as its own reading.

## 10. Run the local suites

```bash
cargo test
```

Every module ships its own tests — the XML parser against malformed and hostile input
(`tests/fuzz_style.rs`), the probe model and its digest, the polling path end to end against a fake
agent (`tests/poll_acquisition.rs`), the streaming path and all three resync ladder steps
(`tests/stream_acquisition.rs`, `tests/stream_sequence.rs`), every configuration shipped in
`test-configs/` against `config.schema.json` (`tests/config_schema.rs`), the `src/mtconnect/**`
isolation rule (`tests/isolation.rs`), and the full `sb/*` command surface through a real
`CommandInbox` (`tests/scoped_delivery.rs`) — no network, no broker, no live agent required.
`tests/agent_integration.rs` additionally drives the real cppagent container from step 7 (probe,
stream, restart/`instanceId` resync, buffer-wrap recovery) when `EC_MTC_AGENT` is set; it self-skips
otherwise, so the ordinary gate stays green with no Docker. In a run that is *supposed* to reach the
live agent, set `EC_REQUIRE_LIVE=1` as well — the self-skip then becomes a hard failure instead of a
silently green gate.

Next: the [how-to guides](how-to-guides.md) for tuning streaming/polling, binding conditions, and
deploying; the [reference](reference/) for every option, topic, and metric; the
[explanation](explanation.md) for why the code is shaped the way it is.
