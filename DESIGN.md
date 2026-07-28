# DESIGN — MtconnectAdapter

> Treat this document as the **design-fidelity contract** for this component: before changing
> behavior, update the relevant section here in the same change, and review new work against what
> is written here — not against a summary of it.

## What it is

`com.mbreissi.edgecommons.MtconnectAdapter` is a southbound protocol adapter: it connects to devices, reads signals, and
publishes them onto the UNS in the shape the rest of the fleet expects. This scaffold ships a
simulated device backend as a worked example; fill in the sections below as you replace it with a
real protocol.

## Decisions

Record each real design decision here as you make it, numbered so later sessions can cite it —
mirror the `D-<PREFIX>-<n>` convention used across the EdgeCommons repos (e.g. `D-MtconnectAdapter-1`).

- **D-MtconnectAdapter-1.** *(example — replace)* Which protocol(s) this adapter speaks, and why.
- **D-MtconnectAdapter-2.** *(example — replace)* What a transient vs. permanent connect failure
  means for this protocol (drives the supervisor's backoff-vs-ceiling choice in `src/supervisor.rs`).

## Config

`config.schema.json` is the source of truth for `component.global`/`component.instances[]`; describe
here *why* each new key exists as you add it, not just its shape (the schema's `description` fields
cover shape). Note any device-specific keys you add to `connection` (deliberately open in the
shipped schema) and what they mean for your protocol.

## Command surface

The generic `sb/*` family (`src/commands.rs`) ships unchanged: `sb/status`, `sb/read`, `sb/write`,
`sb/signals`, `sb/browse`, `sb/pause`, `sb/resume`, `reconnect`, `repoll`. Record here any verb whose
*behavior* becomes protocol-specific (e.g. what `sb/browse` enumerates once you override the
seam's default), and any new verb you add beyond this set.

## Metrics

`southbound_health` (the canonical floor) and the two worked operational families
(`MtconnectAdapterConnection`, `MtconnectAdapterCommand`) ship unchanged. Record here the
protocol-specific families you add (`MtconnectAdapterInventory` / `Poll` / `Publish` or your own
names) — their dimensions, measures, and what each one is *for*, so a later reader does not have to
reverse-engineer intent from `src/metrics.rs` alone.

## Validation

- `cargo test` — unit/integration tests against the simulator and mocked control channel.
- `cargo llvm-cov --fail-under-lines 90` — the coverage gate.
- `tests/live_sim.rs` (`EC_LIVE_SIM=<endpoint>`) — the live path against a real
  simulator/device, skipped otherwise.
- Record here any additional validation this protocol needs (a vendor simulator, a specific lab
  device) and where it runs.
