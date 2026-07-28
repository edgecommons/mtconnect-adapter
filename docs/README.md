# MtconnectAdapter — Documentation

*This documents the generated scaffold; rewrite it as you build the component out.*

`com.mbreissi.edgecommons.MtconnectAdapter` connects to devices, reads signals, and publishes them onto the Unified
Namespace (UNS) in the shape the rest of the fleet expects — so a consumer can chart a value from
this adapter without knowing the protocol behind it. Built on the `edgecommons` Rust library, it
runs wherever you deploy it — a Greengrass v2 component, a standalone HOST process, or a Kubernetes
pod. It ships with a **simulated device backend** so it runs with no hardware; replace it with your
protocol.

| Doc | Start here when you want to… |
|-----|------------------------------|
| **[Tutorial](tutorial.md)** | learn by doing — build the scaffold, run it against the simulator, and watch data cross the bus |
| **[How-to guides](how-to-guides.md)** | accomplish a task — plug in a real protocol, tune polling, deploy, wire CI |
| **[Reference](reference/)** | look up an exact config key, topic, payload, metric, or type |
| **[Explanation](explanation.md)** | understand the shape — the device seam, the supervisor/backoff, quality semantics |

## Quick routing

- **"I'm new here."** → [Tutorial](tutorial.md).
- **"What config option does X?"** → [Reference — Configuration](reference/configuration.md).
- **"What message on which topic?"** → [Reference — Messaging Interface](reference/messaging-interface.md).
- **"What does this metric mean?"** → [Reference — Metrics](reference/metrics.md).
- **"How does a reading become a JSON value?"** → [Reference — Data Types](reference/data-types.md).
- **"Why is the code shaped this way?"** → [Explanation](explanation.md).
- **"Show me a complete config."** → [Sample Configurations](sample-configurations.md).

## Audience

These docs describe the component **as generated** — the simulated backend, the exact `sb/*` verbs
and metrics the scaffold registers. They are for whoever runs `edgecommons component new` next and
needs to know what they got before they start replacing the simulator with a real device.
