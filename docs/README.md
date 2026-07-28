# MtconnectAdapter — Documentation

`com.mbreissi.edgecommons.MtconnectAdapter` is a southbound **MTConnect client**. It connects to one
or more running MTConnect Agents over HTTP, reads each agent's device model and observations, and
publishes normalized EdgeCommons signals onto the Unified Namespace (UNS) — so a consumer can chart
an MTConnect data item without knowing the protocol behind it. Built on the `edgecommons` Rust
library, it runs wherever you deploy it — a Greengrass v2 component, a standalone HOST process, or a
Kubernetes pod. It ships with a built-in **simulated backend** (`adapter: "sim"`) so it runs with no
agent for a quick first look, alongside the real MTConnect client (`adapter: "mtconnect"`, the
default).

| Doc | Start here when you want to… |
|-----|------------------------------|
| **[Tutorial](tutorial.md)** | learn by doing — build it, run it against the simulator and a real agent, watch data cross the bus |
| **[How-to guides](how-to-guides.md)** | accomplish a task — bind conditions, tune streaming/polling, browse the address space, deploy, wire CI |
| **[Reference](reference/)** | look up an exact config key, topic, payload, metric, or type |
| **[Explanation](explanation.md)** | understand the shape — the agent runtime, the resync ladder, quality semantics |

## Quick routing

- **"I'm new here."** → [Tutorial](tutorial.md).
- **"What config option does X?"** → [Reference — Configuration](reference/configuration.md).
- **"What message on which topic?"** → [Reference — Messaging Interface](reference/messaging-interface.md).
- **"What does this metric mean?"** → [Reference — Metrics](reference/metrics.md).
- **"How does an MTConnect observation become a JSON value?"** → [Reference — Data Types](reference/data-types.md).
- **"Why is the code shaped this way?"** → [Explanation](explanation.md).
- **"Show me a complete config."** → [Sample Configurations](sample-configurations.md).

## What this is not

It is not an MTConnect **Agent** — it serves no HTTP endpoints and keeps no sequence buffer — and it
is not an MTConnect **Adapter** in the standard's own sense (it ingests no SHDR). A site with
machine tools and no agent installs the canonical `mtconnect/agent` next to them; this component
consumes it, the way the OPC UA adapter consumes a Kepware server.
