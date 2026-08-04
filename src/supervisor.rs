//! # Runtime supervisor — construction, spawning, shutdown (the live-infra seam)
//!
//! [`App`] wires the `edgecommons` runtime to the component: it resolves credentials and builds one
//! shared [`AgentRuntime`] per configured agent, mints each instance's `data()`/`events()` facades,
//! spawns one task per device and one metrics ticker per agent, registers the command surface and
//! the connectivity provider, and — on SIGTERM — runs the bounded shutdown sequence.
//!
//! It is deliberately as thin as it can be. Every **decision** the component makes lives elsewhere
//! and is unit-tested: the connect/poll/publish/reconnect orchestration in [`crate::driver`],
//! reconnect backoff ([`crate::app::Backoff::delay`]), the write allow-list
//! ([`crate::app::Writes::permits`]), pause gating ([`crate::app::set_paused`]), per-device
//! connectivity ([`connectivity_of`]), the token tree and the join-with-budget shutdown math
//! ([`shutdown_within`]), and the metric-family math ([`crate::metrics`]).
//!
//! What is left needs a live `EdgeCommons` runtime to exist at all — the library's facades have no
//! public constructor, and the agents talk to real endpoints — so this file is validated by the
//! self-skipping `tests/live_sim.rs` / `tests/agent_integration.rs` suites and the scaffold→build
//! gate, and is the ONE module excluded from the unit-coverage denominator
//! (`.github/workflows/ci.yml`), exactly as `ethernet-ip-adapter`'s live seams are.
//!
//! `FacadeWire` is the bridge: it satisfies [`crate::driver::Wire`] with the library's real
//! per-instance facades, so the drivers never name a type they cannot construct.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use edgecommons::prelude::*;

use crate::app::{
    DeviceConfig, DeviceControl, Health, TaskTokens, compile_mtconnect, connectivity_of,
    shutdown_within,
};
use crate::device::{DeviceBackend, MtcBackend, SimBackend, resolve_agent_credentials};
use crate::driver::{METRICS_INTERVAL, Wire, run_device};
use crate::metrics::{AgentMetrics, AgentTelemetry, DeviceMetrics};
use crate::mtconnect::AgentRuntime;
use crate::mtconnect::config::parse_agents;
use crate::reload::SignalRegistry;
use tokio::sync::mpsc;

/// The `component.global.healthThresholds.staleSignalSecs` default (SOUTHBOUND.md §4/§5).
const DEFAULT_STALE_SIGNAL_SECS: u64 = 30;

// =================================================================================================
// The facade-backed wire
// =================================================================================================

/// [`crate::driver::Wire`] over one instance's real `data()`/`events()` facades.
///
/// The publish path is the facade's own two-step, not a hand-built message: `DataFacade::build_body`
/// applies the whole §2.1 contract (quality defaulting, the `qualityRaw` marker, the `serverTs`
/// fill, the samples wrapper), this adapter adds the one additive `componentPath` key the design
/// calls for (D-MtconnectAdapter-L13 — the `SignalUpdate` builder has no update-level `extra`
/// setter), and `publish_body_via` mints the `data/{channel}` topic and stamps identity. Everything
/// `DataFacade::publish` does beyond that is preserved: the channel comes from the same
/// `effective_signal_path`, the per-call channel override rides through unchanged, and the two
/// fail-fast structural checks `publish` makes are re-made here rather than dropped.
struct FacadeWire {
    data: DataFacade,
    events: EventsFacade,
}

#[async_trait]
impl Wire for FacadeWire {
    async fn publish(
        &self,
        update: &SignalUpdate,
        component_path: Option<&str>,
    ) -> edgecommons::Result<()> {
        if update.signal_id.as_deref().unwrap_or_default().is_empty() {
            return Err(EdgeCommonsError::Facade(
                "data publish requires a stable signal.id (the consumer key)".to_string(),
            ));
        }
        if update.samples.is_empty() {
            return Err(EdgeCommonsError::Facade(
                "data publish requires at least one sample".to_string(),
            ));
        }
        let mut body = self.data.build_body(update)?;
        crate::app::stamp_component_path(&mut body, component_path);
        let path = update
            .effective_signal_path()
            .unwrap_or_default()
            .to_string();
        self.data
            .publish_body_via(&path, body, update.via.clone())
            .await
    }

    async fn emit(
        &self,
        severity: Severity,
        event_type: &str,
        message: Option<String>,
        context: Option<serde_json::Value>,
    ) -> edgecommons::Result<()> {
        self.events
            .emit(severity, event_type, message, context)
            .await
    }

    async fn raise_alarm(
        &self,
        severity: Severity,
        event_type: &str,
        message: Option<String>,
        context: Option<serde_json::Value>,
    ) -> edgecommons::Result<()> {
        self.events
            .raise_alarm(severity, event_type, message, context)
            .await
    }

    async fn clear_alarm(
        &self,
        severity: Severity,
        event_type: &str,
        context: Option<serde_json::Value>,
    ) -> edgecommons::Result<()> {
        self.events.clear_alarm(severity, event_type, context).await
    }
}

// =================================================================================================
// App
// =================================================================================================

pub struct App {
    config: Arc<Config>,
    metrics: Arc<dyn MetricService>,
    devices: Vec<DeviceConfig>,
    /// `component.global.healthThresholds.staleSignalSecs`.
    stale_signal_secs: u64,
    /// One shared runtime per `component.global.agents[]` entry (D-MTC-3). Built before any
    /// instance supervisor starts, so an instance's `connect` only has to attach.
    agents: HashMap<String, Arc<AgentRuntime>>,
    /// The MTConnect backend, wired to those runtimes.
    mtconnect: Arc<MtcBackend>,
    /// Every instance's live, reloadable signal set (LLD §8).
    signals: Arc<SignalRegistry>,
}

/// Applies a committed configuration to the live runtime (LLD §8). A candidate that would need a
/// restart — a changed `agents[]`, an added or removed instance — never reaches this listener: the
/// pre-commit validator (`crate::reload::classify`, registered in `main.rs`) refuses it and the
/// component keeps running on its last-good configuration.
struct ConfigListener {
    signals: Arc<SignalRegistry>,
}

#[async_trait::async_trait]
impl ConfigurationChangeListener for ConfigListener {
    async fn on_configuration_change(&self, config: Arc<Config>) -> bool {
        let raw = serde_json::json!({
            "component": { "global": config.global(), "instances": instances_of(&config) }
        });
        match self.signals.apply(&raw) {
            Ok(changed) if changed.is_empty() => {
                tracing::info!("configuration reloaded; no signal set changed");
            }
            Ok(changed) => {
                tracing::info!(instances = ?changed, "signal sets recompiled and swapped");
            }
            Err(e) => {
                // The validator already refused anything that cannot compile; if one still gets
                // here, the live generation is untouched and saying so beats failing quietly.
                tracing::error!(error = %e, "reloaded configuration did not compile; keeping the live signal sets");
                return false;
            }
        }
        true
    }
}

/// The `component.instances[]` array of a configuration snapshot, rebuilt from the accessor pair
/// the library exposes.
fn instances_of(config: &Config) -> Vec<serde_json::Value> {
    config
        .instance_ids()
        .iter()
        .filter_map(|id| config.instance(id).cloned())
        .collect()
}

impl App {
    pub fn new(gg: &EdgeCommons) -> anyhow::Result<Self> {
        let config = gg.config();
        let metrics = gg.metrics();

        let stale_signal_secs = config
            .global()
            .get("healthThresholds")
            .and_then(|h| h.get("staleSignalSecs"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(DEFAULT_STALE_SIGNAL_SECS);

        let mut devices = Vec::new();
        for id in config.instance_ids() {
            match config
                .instance(&id)
                .ok_or_else(|| anyhow::anyhow!("no config"))
                .and_then(|v| Ok(serde_json::from_value::<DeviceConfig>(v.clone())?))
            {
                Ok(d) => devices.push(d),
                Err(e) => tracing::warn!("skipping malformed device `{id}`: {e}"),
            }
        }
        anyhow::ensure!(
            !devices.is_empty(),
            "no valid devices in component.instances[]"
        );

        // The agents come first: an MTConnect instance is one device of an agent that is declared
        // once and shared (D-MTC-3), and its endpoint is derived from that pairing.
        let needs_agents = devices.iter().any(|d| d.adapter == crate::device::KIND);
        let agent_configs = if needs_agents {
            parse_agents(config.global())?
        } else {
            Vec::new()
        };
        let defaults = crate::app::publish_defaults_of(config.global());
        // Every instance's UNS channel budget, resolved once against the live identity: it is what
        // shapes the derived channels of a machine whose component paths run deeper than a topic
        // can carry. Identity is fixed for the process, so this map outlives every reload.
        let budgets =
            crate::app::ChannelBudgets::resolve(gg, devices.iter().map(|d| d.id.as_str()));
        let mtc_devices = compile_mtconnect(&mut devices, &agent_configs, defaults, &budgets)?;

        // Vault references become values exactly once, here — the protocol client never sees a
        // reference, and never learns the credential service exists.
        let credential_service = gg.credentials();
        let mut agents = HashMap::new();
        for cfg in agent_configs {
            let creds = resolve_agent_credentials(&cfg, credential_service.as_deref())
                .map_err(|e| anyhow::anyhow!(e))?;
            let id = cfg.id.clone();
            tracing::info!(agent = %id, url = %cfg.url, mode = ?cfg.streaming, "agent runtime configured");
            // The library's own clock, injected across the isolation seam: `src/mtconnect/**`
            // stamps arrival with it without ever importing `edgecommons`.
            agents.insert(
                id,
                AgentRuntime::new(cfg, &creds, edgecommons::facades::system_clock())?,
            );
        }
        let mtconnect = Arc::new(MtcBackend::new(agents.clone(), mtc_devices, budgets));
        let signals = mtconnect.signals();
        gg.add_config_change_listener(Arc::new(ConfigListener {
            signals: Arc::clone(&signals),
        }));

        Ok(Self {
            config,
            metrics,
            devices,
            stale_signal_secs,
            agents,
            mtconnect,
            signals,
        })
    }

    /// The backend serving one device's `adapter`.
    fn backend_for(&self, cfg: &DeviceConfig) -> Option<Arc<dyn DeviceBackend>> {
        match cfg.adapter.as_str() {
            "sim" => Some(Arc::new(SimBackend)),
            crate::device::KIND => Some(Arc::clone(&self.mtconnect) as Arc<dyn DeviceBackend>),
            other => {
                tracing::error!(instance = %cfg.id, adapter = %other, "unknown adapter");
                None
            }
        }
    }

    pub async fn run(&self, gg: &EdgeCommons) -> anyhow::Result<()> {
        // The structured-lifecycle token tree (P1-7): one root, one child per family, one
        // grandchild per task — so the device tasks can be stopped and drained BEFORE the agent
        // runtimes they detach from. Every handle spawned below is retained and joined; nothing is
        // left to be aborted mid-flush by the runtime's own teardown.
        let tokens = TaskTokens::new();
        let mut agent_tasks: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();
        let mut agent_metric_tasks: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();
        let mut device_tasks: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();

        // The shared acquisition tasks start BEFORE any instance supervisor: an instance attaches
        // to a running agent runtime, it does not start one.
        for (agent_id, agent) in &self.agents {
            // Each acquisition task carries its own token, a child of the agent family's: shutdown
            // cancels the family, and every await point in the task — a hung request, a backoff
            // wait, a blocked loss-intolerant send — gives way to it.
            if let Some(task) = agent.spawn(tokens.agent()) {
                agent_tasks.push((format!("agent `{agent_id}` acquisition"), task));
            }
            // One emitter per AGENT for the acquisition families (HLD §9): the stream is shared by
            // every device on it, so it is measured once rather than once per attached instance.
            let am = Arc::new(AgentMetrics::new(
                Arc::clone(&self.metrics),
                Arc::clone(&self.config),
                Arc::clone(agent) as Arc<dyn AgentTelemetry>,
            ));
            am.define_all();
            let ticker_cancel = tokens.agents();
            let task = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(METRICS_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        () = ticker_cancel.cancelled() => return,
                        _ = ticker.tick() => am.emit_periodic().await,
                    }
                }
            });
            agent_metric_tasks.push((format!("agent `{agent_id}` metrics"), task));
        }

        // Each device's health, shared with its task and read by the connectivity provider.
        let mut reported: Vec<(DeviceConfig, Arc<Health>)> = Vec::new();
        // The per-device handles the command surface routes on.
        let mut handles: Vec<crate::commands::DeviceHandle> = Vec::new();

        for device in &self.devices {
            // Per-instance facades: `data()` mints this device's topics and stamps its identity.
            let instance = gg.instance(&device.id)?;

            let health = Arc::new(Health::default());
            let dm = Arc::new(DeviceMetrics::new(
                Arc::clone(&self.metrics),
                Arc::clone(&self.config),
                device.id.clone(),
                Arc::clone(&health),
                self.stale_signal_secs,
                agent_telemetry(device, &self.agents),
            ));
            // Pre-define the metric set so it is fixed and discoverable at startup.
            dm.define_all();

            // The signal inventory `sb/signals` shows — a config/backend view, no device round-trip.
            // Its size drives the `southbound_health.signalsSubscribed` gauge while the link is up.
            let Some(backend) = self.backend_for(device) else {
                continue;
            };
            let signals = backend.inventory(&device.connection);
            health.set_signal_inventory(signals.len() as u64);

            let (control_tx, control_rx) = mpsc::channel::<DeviceControl>(16);
            reported.push((device.clone(), Arc::clone(&health)));
            handles.push(crate::commands::DeviceHandle {
                cfg: device.clone(),
                control: control_tx,
                health: Arc::clone(&health),
                dm: Arc::clone(&dm),
                signals,
                // M4 wiring (LLD §7): the published protocol view `sb/status`, `sb/signals`, and
                // `sb/browse` answer from — the shared agent runtime's `ArcSwap<AgentInfo>` and its
                // cached probe model, read without ever waiting on acquisition. `None` for the
                // simulator, which has no agent.
                protocol: crate::commands::ProtocolView::of(device, &self.agents, &self.signals),
            });

            let wire: Arc<dyn Wire> = Arc::new(FacadeWire {
                data: instance.data(),
                events: instance.events(),
            });
            let task = tokio::spawn(run_device(
                device.clone(),
                backend,
                wire,
                dm,
                health,
                control_rx,
                self.stale_signal_secs,
                tokens.device(),
            ));
            device_tasks.push((format!("instance `{}`", device.id), task));
        }

        // ONE provider, TWO surfaces: the library pushes this sample into the `state` keepalive's
        // `instances[]` every tick, and returns the very same sample from the built-in `status`
        // command verb. Whoever watches and whoever asks cannot get different answers.
        let provider: Arc<InstanceConnectivityProvider> = Arc::new(move || {
            reported
                .iter()
                .map(|(cfg, health)| connectivity_of(cfg, health))
                .collect()
        });
        gg.set_instance_connectivity_provider(Some(provider));

        // The southbound command surface (`crate::commands`). `ping` / `reload-config` /
        // `get-configuration` are already live — the library registered them before we ran.
        if let Some(commands) = gg.commands() {
            crate::commands::register_all(&commands, handles)?;
        }

        // SIGTERM / Ctrl-C — a *process* signal, which is precisely the control input that still
        // arrives when the broker is gone and every publish is stalled.
        gg.shutdown_signal().await;
        tracing::info!("shutdown signal received");

        // The ordering and the budgets are `crate::app`'s, unit-tested there; this is only the
        // wiring. Agent acquisition tasks and their metric tickers are joined together under the
        // one agent budget.
        let agents: Vec<Arc<AgentRuntime>> = self.agents.values().map(Arc::clone).collect();
        let metrics = Arc::clone(&self.metrics);
        agent_tasks.extend(agent_metric_tasks);
        let report = shutdown_within(
            &tokens,
            device_tasks,
            agent_tasks,
            async move {
                for agent in &agents {
                    agent.shutdown().await;
                }
            },
            async move {
                metrics.flush_metrics().await.ok();
            },
        )
        .await;

        if report.is_clean() {
            tracing::info!("shutdown complete: every task flushed and returned");
        } else {
            tracing::warn!(
                devices = ?report.aborted_devices, agents = ?report.aborted_agents,
                "shutdown aborted tasks that had not returned inside their budget"
            );
        }
        Ok(())
    }
}

/// The agent telemetry behind one device — the source of its `MtconnectParse` family. `None` for
/// the simulator, which has no agent and parses no documents.
fn agent_telemetry(
    cfg: &DeviceConfig,
    agents: &HashMap<String, Arc<AgentRuntime>>,
) -> Option<Arc<dyn AgentTelemetry>> {
    if cfg.adapter != crate::device::KIND {
        return None;
    }
    let (agent_id, _uuid) = crate::device::connection_binding(&cfg.connection).ok()?;
    agents
        .get(&agent_id)
        .map(|a| Arc::clone(a) as Arc<dyn AgentTelemetry>)
}
