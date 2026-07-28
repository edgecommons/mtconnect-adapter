# Reference — Data Types

*This documents the generated scaffold; rewrite it as you build the component out.*

Unlike a fixed-register protocol (Modbus's bits and 16-bit registers, say), this scaffold's device
seam (`src/device.rs`) does not impose a wire type system of its own — a [`Reading`]'s `value` is an
opaque `serde_json::Value`, and it is **your protocol's job** to define what that means. This page
documents the seam's own types (what every backend must produce and consume) and what a real
protocol typically adds on top.

## The seam's types

| Type | Field | Meaning |
|------|-------|---------|
| [`Reading`] | `signal_id` | The canonical, stable id the rest of the fleet keys on. Never a volatile index — it is what `writes.allow` matches against and what `sb/read`/`sb/write` address. |
| | `name` | An optional human label. |
| | `value` | The decoded value, as JSON: `number`, `boolean`, `string`, or `null` (a failed read — see below). |
| | `quality` | Normalized [`Quality`]: `Good` \| `Bad` \| `Uncertain`. |
| | `quality_raw` | The protocol-native status/code, kept verbatim for diagnosis. |
| | `source_ts` | Optional ISO-8601 UTC. The **machine** timestamp — device/field-authored time, set only when the protocol supplies it. Published as `sourceTs`, never synthesized. |
| | `capture_ts` | Optional ISO-8601 UTC. The **capture** timestamp — the moment the protocol read the value (a mediating server's stamp, e.g. an OPC UA server's). Published as the sample's `serverTs`. |
| | `received_ts` | Optional ISO-8601 UTC. The **adapter receive** timestamp — auto-stamped by the worker at read completion when the backend leaves it `None`. |
| [`SignalInfo`] | `id`, `name` | One entry of the **inventory** (`sb/signals`) — known from config/backend without a device round-trip. |
| [`BrowsedSignal`] | `id`, `name`, `type_name` | One entry discovered by [`browse`] — a signal the device *offers*, whether or not it is configured. `type_name` is the device-native type string, kept verbatim (`"REAL"`, `"holding/uint16"`, whatever your protocol calls it). |

## Quality

`Quality::Good` / `Bad` / `Uncertain`, published on the wire as `"GOOD"` / `"BAD"` / `"UNCERTAIN"`.
The simulator only ever produces `Good` (`temperature-1`) and `Bad` (`pressure-1`, always faulted,
with `quality_raw: "SENSOR_FAULT"` and `value: null`) — `Uncertain` is unused by the simulated
backend but common in a real one: a stale cached read, a value outside its calibrated range, a
sensor that answered but warned. Use it when your protocol has that middle state; if it does not,
`Good`/`Bad` alone is a perfectly honest mapping.

**The rule that matters:** a read that fails for *one* signal should still return that signal (with
`Quality::Bad` and a `null` value) rather than being omitted from the `Vec<Reading>` `read_signals`
returns. A signal that silently stops appearing is indistinguishable from one that has not changed;
a signal published with `BAD` quality is unambiguous.

## What a real protocol typically adds

A fixed-register protocol (Modbus) or a typed-node protocol (OPC UA) usually needs more structure
than "an opaque JSON value" to decode/encode correctly. Two well-worn patterns from the reference
adapters:

- **A per-signal type + layout descriptor**, analogous to Modbus's `table`/`address`/`type`/
  `wordOrder`/`byteOrder`/`scale`/`bit` — carried in your extension of `ConnectionConfig`'s
  (deliberately open) config schema, and used inside your `DeviceSession::read_signals`/
  `write_signal` implementation to convert between the wire representation and the JSON `value`
  this seam expects.
- **A stable, protocol-derived `signal.id`** distinct from the human `name` — Modbus's
  `u<unitId>/<table>/<address>/<type>`, OPC UA's `ns=<n>;i=<id>`. Keep it independent of any
  topic/config ordering, so a redeployment that reorders your config does not change which physical
  point a `signal.id` refers to.

## Published identity

Every `SouthboundSignalUpdate` carries, in `body.signal`:

- `id` — the stable id (see above).
- `name` — the optional human label, when the backend has one (the simulator always sets one).

`device.adapter` (the `DeviceBackend::kind()` string) and `device.endpoint`
(`ConnectionConfig::endpoint`) accompany every reading, so a consumer can always tell which backend
and which physical connection a value came from, independent of `signal.id`.

## Value typing notes

- `bool` is a JSON boolean; numeric readings are JSON numbers (the simulator emits `f64`); a failed
  reading emits JSON `null`.
- There is no scale/offset or byte/word-order layer in the seam itself — build it into your
  `DeviceSession` implementation if your protocol's wire format needs it (see above).
- **Timestamps — the four-slot model.** A [`Reading`] carries up to three optional ISO-8601 UTC
  timestamps, never synthesized from one another (the fourth slot — the publish moment — is the
  envelope header's, stamped by the library): `capture_ts` becomes the sample's `serverTs`, falling
  back to `received_ts` when the backend sets no capture stamp (a direct client's receive moment IS
  the capture moment); `received_ts` is auto-stamped by the worker at read completion for every
  reading the backend did not stamp itself, and additionally rides as a per-sample `receivedTs`
  extra only when a mediating server makes it differ from the effective `serverTs`; `source_ts`
  rides as `sourceTs` only when the device supplied it. The simulator sets none of the three (it
  has no device clock), so its samples' `serverTs` is the worker's receive stamp and nothing else.

[`Reading`]: ../../src/device.rs
[`SignalInfo`]: ../../src/device.rs
[`BrowsedSignal`]: ../../src/device.rs
[`browse`]: ../../src/device.rs
