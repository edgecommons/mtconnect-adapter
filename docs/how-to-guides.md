# How-to Guides

Recipes for specific tasks. Each assumes the adapter builds and runs (see the
[tutorial](tutorial.md)). For concepts see [explanation.md](explanation.md); for exhaustive options
see [reference/](reference/).

---

## Point the adapter at a real MTConnect agent

Add one entry to `component.global.agents[]` and one `component.instances[]` entry naming it:

```jsonc
"component": {
  "global": {
    "agents": [
      { "id": "line-a-agent", "url": "http://127.0.0.1:5010" }
    ]
  },
  "instances": [
    {
      "id": "cnc-1",
      "adapter": "mtconnect",
      "connection": { "agentId": "line-a-agent", "deviceUuid": "MTC-E2E-001" },
      "signals": [
        { "id": "x-position", "dataItemId": "Xabs" }
      ]
    }
  ]
}
```

`connection.deviceUuid` must match a `Device/@uuid` the agent's `/probe` actually serves — the
adapter verifies it at connect and refuses the instance (permanently, not with a retry) if it does
not. The endpoint the adapter publishes and reports on `sb/status` is **derived**
(`mtconnect://<host>[:<port>]/<uuid>`) from `agentId` + `deviceUuid`, never configured separately, so
the two can never disagree. `test-configs/mtconnect.json` is a complete worked example; see
[sample-configurations.md](sample-configurations.md).

To test against the canonical reference agent locally: `docker compose -f
tests/compose.mtconnect-agent.yaml up -d` (see the [tutorial](tutorial.md#7-run-it-against-a-real-mtconnect-agent)).

---

## Bind a signal to one of the device's own alarms

Add `conditionBinding` to a signal, naming one or more CONDITION `dataItemId`s. When any of them is
`Warning` or `Fault`, the *bound signal's* quality degrades — its value is untouched:

```jsonc
"signals": [
  { "id": "x-position", "dataItemId": "Xabs", "conditionBinding": ["Xtravel"] },
  { "id": "x-travel-condition", "dataItemId": "Xtravel" }
]
```

`x-position` now publishes `quality: "UNCERTAIN"` (`qualityRaw: "MTC_CONDITION:WARNING:<code>"`)
while `Xtravel` is warning, and `"BAD"` (`"MTC_CONDITION:FAULT:<code>"`) while it is faulted — the
alarm's own native code rides along for diagnosis. A `conditionBinding` must not name the signal's
own `dataItemId` (refused at startup), but nothing stops publishing the *same* condition both ways —
directly, as `x-travel-condition` above, and as a modifier on another signal, as shown. When a signal
binds more than one condition, the worst currently-active one wins. See
[reference/data-types.md](reference/data-types.md#conditions-state-as-value-and-as-a-quality-modifier).

---

## Choose streaming or polling

`component.global.agents[].streaming` is `"prefer"` (the default) or `"poll-only"`, per agent:

| You want… | Setting |
|---|---|
| Lowest latency, event-driven delivery, the full three-step resync ladder | `"prefer"` (default) — opens a multipart `/sample?interval=...` stream, and degrades to polling automatically if the stream cannot be established after repeated attempts (retrying the stream on its own reconnect cadence in the background). |
| A simple, predictable read cadence — or an agent/network path that does not support long-lived multipart responses (some reverse proxies buffer or time these out) | `"poll-only"` — always reads `/current` on `pollIntervalMs`, never opens a stream. |

`heartbeatMs` (per agent) governs a streaming connection's liveness window — silence for twice this
long is treated as a dead stream and triggers ladder step 1 (reconnect from the same position, no
data lost). `pollIntervalMs` (per agent) governs the polling cadence, whether that agent is
`poll-only` or a `prefer` agent that is currently degraded.

---

## Secure the connection to an agent

Configure `auth` and/or `tls` on the agent entry — every value is a **vault reference**, never a
literal secret in configuration:

```jsonc
{
  "id": "line-a-agent",
  "url": "https://agent.line-a.example.com",
  "auth": { "type": "basic", "username": "svc-mtconnect", "secretRef": "line-a/agent-password" },
  "tls": {
    "caSecretRef": "line-a/agent-ca-bundle",
    "certSecretRef": "line-a/client-cert",
    "keySecretRef": "line-a/client-key"
  }
}
```

`auth.type: "bearer"` (with just `secretRef`) is the alternative to `basic`. `tls.certSecretRef` and
`tls.keySecretRef` must be set together (mutual TLS) or not at all — one without the other is refused
at startup. Every reference is resolved through the EdgeCommons credential vault once, at startup,
before that agent's runtime starts; an unresolvable reference is a startup error, never a silent
unauthenticated fallback. Nothing resolved this way ever appears in configuration, logs, or
`sb/status`.

---

## Browse a device's structure

`sb/browse` serves the cached probe tree two ways — pick whichever shape suits the caller:

```text
publish .../cmd/sb/browse   {"body":{}}                              # paged, from the top
publish .../cmd/sb/browse   {"body":{"cursor":"sha256:...#212"}}     # the next page
publish .../cmd/sb/browse   {"body":{"ref":"root","depth":1}}        # hierarchical (edge-console panel mode)
```

Both modes read the same cached model — nothing round-trips to the agent — so browsing keeps working
even while the agent link is down, as long as at least one probe has succeeded since this process
started. Before that first probe, both modes answer `BROWSE_FAILED`/`MTC_NO_PROBE`. See
[reference/messaging-interface.md](reference/messaging-interface.md#paged-sbbrowse) for both full
response shapes.

---

## Read signals from a client

Reads ride the library **command inbox** (`ecv1/{device}/mtconnect-adapter/cmd/{verb}`):

```text
publish ecv1/<device>/mtconnect-adapter/cmd/sb/read
  {"header":{"name":"sb/read","reply_to":"app/r","correlation_id":"1"},
   "body":{"instance":"cnc-1","signals":[{"signalId":"x-position"}]}}
```

The reply is always a scoped `/current` snapshot (`"mode": "current"`) taken through the agent's
control channel — never served from a cache — with one entry per requested signal. An entry the
agent cannot serve comes back `BAD` with its own code (`MTC_UNAVAILABLE`, `MTC_NO_SUCH_DATAITEM`,
`MTC_AGENT_ERROR:<code>`) while the command itself stays `ok`; only an absent device task at all is
`DEVICE_UNAVAILABLE`.

Writing is not possible: MTConnect's API is read-only by specification, so `sb/write` answers
`WRITE_NOT_ALLOWED` for every request and advertises itself as `unsupported` on `describe`.

---

## Bridge several devices from one agent

Add another `component.instances[]` entry naming the **same** `agentId` — the two devices share one
HTTP connection and one acquisition cycle, each with its own task, its own connection lifecycle
health, and its own `state.instances[]` entry:

```jsonc
"instances": [
  { "id": "cnc-1", "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.111" } },
  { "id": "cnc-2", "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.222" } }
]
```

With two or more devices, `instance` becomes **required** in every command body (`BAD_ARGS` if
missing, `NO_SUCH_INSTANCE` if unrecognized) — the single-device convenience only applies when
exactly one instance is configured across the whole component.

---

## Deploy to a platform

**HOST:**
```bash
cargo run -- --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/mtconnect.json -t my-thing
```

**Greengrass:** the on-device build uses the GDK custom build system (`gdk-config.json` →
`build.sh`).
```bash
gdk component build
gdk component publish
```
If `gdk-config.json`'s `publish.bucket` still carries the `edgecommons-set-artifact-bucket`
sentinel, set a real S3 bucket first — `edgecommons component validate` errors on the sentinel.

**Kubernetes:** build the image, push or `kind load` it, set `image:` in `k8s/deployment.yaml`,
then `kubectl apply -f k8s/`. With `--platform auto` the library detects Kubernetes from the
ServiceAccount token, so the container needs no CLI args — config comes from the mounted
ConfigMap, identity from the Downward API.

---

## Wire CI

`.github/workflows/ci.yml` calls the org's reusable `component-ci.yml` (build/test/clippy) plus an
in-repo `coverage` job (`cargo llvm-cov --fail-under-lines 90`, excluding only
`supervisor.rs`/`main.rs` and the env-gated live suites — see `AGENTS.md`). Add the `EDGECOMMONS_READ_TOKEN`
secret if your dependency form needs it (a `pinned-rev`/`registry` git dependency does; `local` does
not). Commit `Cargo.lock` if you have not already — `edgecommons component validate` warns if it is
missing.

`.github/workflows/deploy-docs.yml` is a no-op until the repo carries the `CLOUDFLARE_DEPLOY_HOOK`
secret and is registered in `registry/components.json` — harmless either way.

The env-gated live suites (`tests/live_sim.rs`, `tests/agent_integration.rs`) are not run in the
default CI job — they self-skip without their respective environment variable, so the ordinary gate
never depends on Docker or a live agent being reachable from the runner. A CI leg or lab run that is
*supposed* to have the live infrastructure sets `EC_REQUIRE_LIVE=1` beside it: the self-skip then
becomes a hard failure, so a broken harness cannot masquerade as a green gate.
