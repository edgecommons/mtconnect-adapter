# Explanation — How this adapter is shaped, and why

*This documents the generated scaffold; rewrite it as you build the component out.*

This page is the mental model behind the generated code. For exact options see
[reference/](reference/); for tasks, the [how-to guides](how-to-guides.md).

## The southbound contract

An **adapter** connects to devices, reads signals, and publishes them onto the UNS in the shape the
rest of the fleet expects — the same shape the Java (OPC UA) and Python (Modbus) reference adapters
implement. A consumer sees `SouthboundSignalUpdate` on the `data` class, `southbound_health` on
`metric`, and the generic `sb/*` command family on `cmd`, regardless of which protocol produced it —
only `device.adapter`, the opaque `signal.address`/`signal.id`, and the protocol-named metric
families differ.

## The device seam

[`crate::device::DeviceSession`] is one live connection to one device;
[`crate::device::DeviceBackend`] opens sessions. Implement the pair once per protocol and everything
above it — the connection lifecycle, backoff, publishing, health, the command surface — is written
against the trait and never learns your protocol. This is the boundary that keeps
`src/commands.rs`/`src/metrics.rs`/`src/app.rs`/`src/supervisor.rs` from having to change when the *next* protocol is
added: they call `read_signals`/`write_signal`/`browse`, never a protocol-specific API.

Two consequences worth internalizing:

- **The default `read_named` reads everything and filters.** Correct for any backend; override it
  only when your protocol can read a named subset more cheaply than a full read (a targeted Modbus
  read of one register, say).
- **`browse` defaults to `BROWSE_UNSUPPORTED`.** A protocol with no discovery (a fixed register map)
  stays honest by leaving it unimplemented, rather than faking an empty page that looks like "there
  is nothing to browse" instead of "this protocol cannot browse".

## One task per device, with a control channel

Each configured device runs in its own `tokio` task (`run_device` in `src/supervisor.rs`): its own connect
loop, its own poll loop, its own backoff state. A device going down only takes its own instance's
signals to `BAD` — the others keep streaming. That task also owns a **control channel**
([`DeviceControl`]): every command that must touch the (non-`Sync`) session or serialize with the
poll loop — a write, an on-demand read, a browse, a reconnect, a repoll — is *sent* to the task and
*confirmed* through the reply that rides it. The command surface (`src/commands.rs`) never touches
the session directly; it only ever talks to the control channel.

This is why a write is **confirmed**, not fire-and-forget: the reply the command layer returns is
the device's own answer, arriving back over the same channel the write went out on.

## The supervisor and backoff

The connect loop (`run_device`) retries with exponential backoff and full jitter
([`Backoff::delay`]): `base_ms * 2^attempt`, capped at `max_ms`, then a uniform random point inside
that window. Jitter is not decoration — without it, every adapter that lost the same device (a PLC
power-cycle event affecting a whole line) would reconnect on the exact same tick, hammering the
device the instant it comes back. A **permanent** connect failure (a bad endpoint — see
[`DeviceError::Permanent`]) skips the ramp and backs off straight to the ceiling, because retrying a
misconfiguration on a one-second cadence just floods the log for no benefit.

While the connection is down, the task still services its control channel
(`serve_while_down`): pause/resume take effect immediately (they only need the shared health flag),
and I/O verbs answer honestly with "disconnected" rather than queuing silently — a `reconnect`
command cuts the backoff short and reconnects now.

## Quality is structural, not adapter discipline

Every [`Reading`] carries a `quality` normalized to `GOOD | BAD | UNCERTAIN`, plus the protocol's
native code in `quality_raw` for diagnosis. The simulator's `pressure-1` signal is deliberately
always `BAD` with `quality_raw: "SENSOR_FAULT"` and a `null` value — proof that a failed reading is
**published**, not silently dropped. A signal that just stops appearing on the bus is
indistinguishable from one that has simply not changed; a signal published with `BAD` quality is
unambiguous. When you implement a real backend, a read that fails for *one* signal should still
return that signal (with `Quality::Bad`) rather than failing the whole `read_signals` call — one
dead register must not blind you to the other ninety-nine.

## Verify semantics: this archetype has none of its own

Unlike a *sink* (which must verify a delivery landed before releasing its source), a protocol
adapter's contract ends at "I published what I read, with an honest quality flag." There is no
delivery-confirmation step here — the read itself, or its absence (`Quality::Bad`), is the whole
signal. What a protocol adapter *does* confirm is command execution: a write is not reported `ok`
until the device's own response comes back over the control channel (see above).

## Metrics: one canonical floor, two worked families, and where yours go

Every adapter — whatever the protocol — emits `southbound_health` with the **exact** canonical
measure set (`connectionState`, `publishLatencyMs`, `pollLatencyMs`, `readErrors`, `staleSignals`,
`reconnects`, `writeErrors`, `signalsSubscribed`), so a fleet dashboard has one health metric that
means the same thing everywhere. On
top of that floor, `src/metrics.rs` ships the **operational-family pattern** — a `Total`/`Interval`
counter-pair convention — two families deep (`MtconnectAdapterConnection`,
`MtconnectAdapterCommand`), with a signposted place to add your protocol's own `Inventory` / `Poll`
/ `Publish` families (see the [how-to guide](how-to-guides.md#add-your-protocols-metric-families)).
Every dimension across every family is deliberately low-cardinality: `instance`, `verb`, `result` —
never a signal name, address, or endpoint, which would be unbounded and shred a dashboard.

## Instance connectivity: one provider, two surfaces

`App::run` registers **one** instance-connectivity provider. The library reads it twice: it pushes
the same sample into every `state` keepalive's `instances[]` array (push), and it returns the very
same sample from the built-in `status` command verb (pull). A console that watches the keepalive and
a console that asks `sb/status` cannot get different answers, because there is only one source. The
`connected` field is the **normalized** flag every console renders a health dot from; `state` is this
adapter's own richer vocabulary (`CONNECTING` / `ONLINE` / `BACKOFF`, and `PAUSED` when paused while
online) — a boolean alone cannot distinguish "still trying" from "administratively paused".

## Command routing and the allow-list

The command surface rides the library's inbox, which subscribes both cmd wildcards
(`ecv1/{device}/mtconnect-adapter/cmd/#` and `ecv1/{device}/mtconnect-adapter/+/cmd/#`). Every `sb/*` verb
declares scope `instance`, so the **library** resolves the addressing before a handler runs: the
topic's instance token is authoritative, a component-addressed request may name the device in the
body instead, and a body `instance` disagreeing with the topic token is `BAD_ARGS`. `commands.rs`
receives the resolved instance and only does the part that needs this component's configuration —
an instance it has no device for is `NO_SUCH_INSTANCE`, and an unnamed one is the sole configured
device, or `BAD_ARGS` once there are two or more.
Writes are allow-listed **by stable `signal.id`**, checked before any device I/O — an adapter that
writes whatever address it is asked to is a control-system vulnerability, not a feature, so the
default (`writes.allow: []`) is read-only, and opening it up is a deliberate per-signal act.
