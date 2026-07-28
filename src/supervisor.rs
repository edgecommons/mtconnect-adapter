//! # Runtime supervisor — the connect/poll/reconnect drivers (the live-infra seam)
//!
//! This is the async **driver** layer: [`App`] wires the `edgecommons` runtime to one task per
//! configured device, and each task's connect → poll → publish → reconnect loop `.await`s a live
//! [`DeviceBackend`]/[`DeviceSession`], the `data()`/`events()` facades, and the command control
//! channel. It is deliberately kept as thin as possible: every pure decision it composes — reconnect
//! backoff ([`Backoff::delay`]), the write allow-list ([`Writes::permits`]), pause gating
//! ([`set_paused`]), per-device connectivity ([`connectivity_of`]), and the metric-family math
//! ([`crate::metrics`]) — lives in a unit-tested module, not here.
//!
//! Because these functions need a live runtime/session/broker to exercise, they are validated by the
//! self-skipping `tests/live_sim.rs` suite (against a real simulator/device) and the scaffold→build
//! gate, and are excluded from the unit-coverage denominator (`.github/workflows/ci.yml`), exactly as
//! `ethernet-ip-adapter`'s `supervisor.rs`/`poll_driver.rs` seams are. Everything they call stays in
//! the denominator and is tested.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use edgecommons::prelude::*;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::app::{
    build_sample, compile_mtconnect, connectivity_of, set_paused, stamp_received, Backoff,
    DeviceConfig, DeviceControl, Health, LinkState,
};
use crate::device::{
    resolve_agent_credentials, BrowseError, DeviceBackend, DeviceSession, MtcBackend, NoticeLevel,
    SimBackend,
};
use crate::metrics::{AgentMetrics, AgentTelemetry, DeviceMetrics};
use crate::mtconnect::config::parse_agents;
use crate::mtconnect::AgentRuntime;
use crate::reload::SignalRegistry;
use crate::shaping::{policies_from_signals, Shaper};

/// How often the periodic metrics emit runs, in the poll loop.
const METRICS_INTERVAL: Duration = Duration::from_secs(30);
/// The `component.global.healthThresholds.staleSignalSecs` default (SOUTHBOUND.md §4/§5).
const DEFAULT_STALE_SIGNAL_SECS: u64 = 30;

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
    config.instance_ids().iter().filter_map(|id| config.instance(id).cloned()).collect()
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
        anyhow::ensure!(!devices.is_empty(), "no valid devices in component.instances[]");

        // The agents come first: an MTConnect instance is one device of an agent that is declared
        // once and shared (D-MTC-3), and its endpoint is derived from that pairing.
        let needs_agents = devices.iter().any(|d| d.adapter == crate::device::KIND);
        let agent_configs =
            if needs_agents { parse_agents(config.global())? } else { Vec::new() };
        let defaults = crate::app::publish_defaults_of(config.global());
        let mtc_devices = compile_mtconnect(&mut devices, &agent_configs, defaults)?;

        // Vault references become values exactly once, here — the protocol client never sees a
        // reference, and never learns the credential service exists.
        let credential_service = gg.credentials();
        let mut agents = HashMap::new();
        for cfg in agent_configs {
            let creds = resolve_agent_credentials(&cfg, credential_service.as_deref())
                .map_err(|e| anyhow::anyhow!(e))?;
            let id = cfg.id.clone();
            tracing::info!(agent = %id, url = %cfg.url, mode = ?cfg.streaming, "agent runtime configured");
            agents.insert(id, AgentRuntime::new(cfg, &creds)?);
        }
        let mtconnect = Arc::new(MtcBackend::new(agents.clone(), mtc_devices));
        let signals = mtconnect.signals();
        gg.add_config_change_listener(Arc::new(ConfigListener { signals: Arc::clone(&signals) }));

        Ok(Self { config, metrics, devices, stale_signal_secs, agents, mtconnect, signals })
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
        // The shared acquisition tasks start BEFORE any instance supervisor: an instance attaches
        // to a running agent runtime, it does not start one.
        for agent in self.agents.values() {
            agent.spawn();
            // One emitter per AGENT for the acquisition families (HLD §9): the stream is shared by
            // every device on it, so it is measured once rather than once per attached instance.
            let am = Arc::new(AgentMetrics::new(
                Arc::clone(&self.metrics),
                Arc::clone(&self.config),
                Arc::clone(agent) as Arc<dyn AgentTelemetry>,
            ));
            am.define_all();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(METRICS_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    am.emit_periodic().await;
                }
            });
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
            let Some(backend) = self.backend_for(device) else { continue };
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

            tokio::spawn(run_device(
                device.clone(),
                backend,
                instance.data(),
                instance.events(),
                dm,
                health,
                control_rx,
            ));
        }

        // ONE provider, TWO surfaces: the library pushes this sample into the `state` keepalive's
        // `instances[]` every tick, and returns the very same sample from the built-in `status`
        // command verb. Whoever watches and whoever asks cannot get different answers.
        let provider: Arc<InstanceConnectivityProvider> = Arc::new(move || {
            reported.iter().map(|(cfg, health)| connectivity_of(cfg, health)).collect()
        });
        gg.set_instance_connectivity_provider(Some(provider));

        // The southbound command surface (`crate::commands`). `ping` / `reload-config` /
        // `get-configuration` are already live — the library registered them before we ran.
        if let Some(commands) = gg.commands() {
            crate::commands::register_all(&commands, handles)?;
        }

        gg.shutdown_signal().await;
        tracing::info!("shutdown signal received");
        for agent in self.agents.values() {
            agent.shutdown().await;
        }
        self.metrics.flush_metrics().await.ok();
        Ok(())
    }
}

// =================================================================================================
// The device task
// =================================================================================================

/// One device's lifecycle: connect, poll, publish, reconnect — and service its control channel.
///
/// The connect loop and the poll loop are nested on purpose. A read failure that breaks the link
/// drops out of the poll loop and back into connect — the only place that knows how to back off.
async fn run_device(
    cfg: DeviceConfig,
    backend: Arc<dyn DeviceBackend>,
    data: DataFacade,
    events: EventsFacade,
    dm: Arc<DeviceMetrics>,
    health: Arc<Health>,
    mut control: mpsc::Receiver<DeviceControl>,
) {
    let backoff = Backoff::default();
    let mut attempt: u32 = 0;
    // A `reconnect` command's reply, held until the next connect settles it.
    let mut pending_reconnect: Option<oneshot::Sender<std::result::Result<(), String>>> = None;

    loop {
        // --- CONNECT (servicing control while down, so pause/reconnect don't block on backoff) ---
        let session = loop {
            dm.on_connect_attempt();
            health.set_link(if attempt == 0 { LinkState::Connecting } else { LinkState::Backoff });
            let now = Instant::now();
            match backend.connect(&cfg.connection).await {
                Ok(session) => {
                    attempt = 0;
                    dm.on_connected(now);
                    health.set_link(LinkState::Online);
                    dm.emit_now().await;
                    let _ = events
                        .emit(
                            Severity::Info,
                            "device-connected",
                            Some(format!("connected to {}", cfg.connection.endpoint)),
                            Some(json!({ "instance": cfg.id, "adapter": backend.kind() })),
                        )
                        .await;
                    let _ = events.clear_alarm(Severity::Critical, "device-unreachable", None).await;
                    if let Some(reply) = pending_reconnect.take() {
                        let _ = reply.send(Ok(()));
                    }
                    break session;
                }
                Err(e) => {
                    dm.on_connect_failure();
                    if let Some(reply) = pending_reconnect.take() {
                        let _ = reply.send(Err(e.to_string()));
                    }
                    // A permanent failure fails identically forever — back off to the ceiling.
                    let permanent = !e.is_transient();
                    let wait = if permanent {
                        Duration::from_millis(backoff.max_ms)
                    } else {
                        backoff.delay(attempt, rand01())
                    };
                    attempt = attempt.saturating_add(1);
                    tracing::warn!(
                        instance = %cfg.id, error = %e, permanent,
                        wait_ms = wait.as_millis() as u64, "connect failed"
                    );
                    match serve_while_down(&mut control, &events, &health, wait).await {
                        DownOutcome::Reconnect(reply) => {
                            pending_reconnect = Some(reply);
                            attempt = 0;
                        }
                        DownOutcome::Elapsed => {}
                        DownOutcome::Closed => return,
                    }
                }
            }
        };

        // --- POLL (until the link breaks or a reconnect is requested) ---
        // The inventory ids back the resume-time snapshot (HLD §7: resume snapshots first).
        let inventory: Vec<String> =
            backend.inventory(&cfg.connection).into_iter().map(|s| s.id).collect();
        // The session just compiled its signals against the device model: the gauge reports what is
        // really being served, not what was merely configured.
        sync_served_signals(session.as_ref(), &health);
        let exit =
            run_polling(&cfg, session, &data, &events, &dm, &health, &mut control, &inventory)
                .await;

        // The link is down (or a reconnect asked us to drop it).
        health.set_link(LinkState::Backoff);
        health.reconnects.fetch_add(1, Ordering::Relaxed);
        dm.on_connection_dropped(Instant::now());
        dm.emit_now().await;
        let _ = events
            .raise_alarm(
                Severity::Critical,
                "device-unreachable",
                Some(format!("lost the link to {}", cfg.connection.endpoint)),
                Some(json!({ "instance": cfg.id })),
            )
            .await;

        match exit {
            PollExit::LinkLost => {}
            PollExit::Reconnect(reply) => {
                pending_reconnect = Some(reply);
            }
            PollExit::Closed => return,
        }
    }
}

/// What ended the poll loop.
enum PollExit {
    /// A read broke the connection; reconnect via the connect loop.
    LinkLost,
    /// A `reconnect` command asked us to drop + re-establish; settle its reply on the next connect.
    Reconnect(oneshot::Sender<std::result::Result<(), String>>),
    /// The control channel closed (component shutdown).
    Closed,
}

/// Read on the poll interval and publish — through the per-signal shaping engine
/// ([`crate::shaping::Shaper`]) — servicing the control channel, until the link breaks or a
/// reconnect is requested.
///
/// **Pause semantics (HLD §7 / D-MTC-7):** the read keeps running while paused — the backend's
/// stream keeps draining and its latest-value/condition caches keep updating — but nothing is
/// published, and the shaping buffers are **cleared** (the resume-time snapshot republishes the
/// current truth, so flushing pre-pause readings after it would publish stale data out of order).
/// Resuming publishes a fresh snapshot of the whole configured inventory first (`read_named`, a
/// live read, bypassing the shaper), then normal, shaped flow resumes with the deadband re-armed.
///
/// **Shaping lifecycle:** the engine is rebuilt with each session (so the first reading after a
/// connect passes the deadband); its policy table follows the session's shaping generation, so a
/// signal reload or a model drift swaps the policies atomically with the signal-set swap and
/// flushes the changed signals' windows with their old policy; every exit path — shutdown, link
/// loss, reconnect — flushes the open windows so no buffered reading is lost.
#[allow(clippy::too_many_arguments)]
async fn run_polling(
    cfg: &DeviceConfig,
    mut session: Box<dyn crate::device::DeviceSession>,
    data: &DataFacade,
    events: &EventsFacade,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
    control: &mut mpsc::Receiver<DeviceControl>,
    inventory: &[String],
) -> PollExit {
    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.poll_interval_ms));
    let mut since_metrics = Instant::now();

    // The shaping engine, fresh per session. The session's compiled policy table wins (it knows
    // the model — deadband only on numeric SAMPLE items); a backend with no compile step (the
    // simulator) is shaped from its static signal configuration — identically, above the session.
    let mut shaper = Shaper::new();
    let mut shaping_gen = session.shaping_generation();
    let _ = shaper.set_policies(if shaping_gen.is_some() {
        session.shaping_policies()
    } else {
        policies_from_signals(&cfg.signals)
    });

    loop {
        // ONE deadline per instance task: the earliest open batch window.
        let batch_deadline = shaper.next_deadline();
        tokio::select! {
            // Poll and control share this one task, so a write can never race a read on the same
            // connection — most device protocols are a single request/response channel.
            ctrl = control.recv() => {
                let Some(ctrl) = ctrl else {
                    // Component shutdown: flush the open batch windows — a SIGTERM must not lose
                    // the readings a window was still coalescing.
                    publish_shaped(cfg, shaper.flush_all(), data, dm, health).await;
                    drain_shaping(dm, &mut shaper);
                    return PollExit::Closed;
                };
                match ctrl {
                    DeviceControl::Write(req) => {
                        let result = session
                            .write_signal(&req.signal_id, &req.value)
                            .await
                            .map_err(|e| e.to_string());
                        if let Err(e) = &result {
                            tracing::warn!(instance = %cfg.id, signal = %req.signal_id, error = %e, "write failed");
                        }
                        let _ = req.ack.send(result);
                    }
                    DeviceControl::ReadNow { ids, reply } => {
                        let result = session.read_named(&ids).await.map_err(|e| e.to_string());
                        let _ = reply.send(result);
                    }
                    DeviceControl::Browse { cursor, max, reply } => {
                        let _ = reply.send(session.browse(cursor, max).await);
                    }
                    DeviceControl::Pause { reply } => {
                        let changed = set_paused(health, true);
                        if changed {
                            // Pause gates the wire, so the open windows are CLEARED, not flushed:
                            // the resume-time snapshot republishes the current truth, and flushing
                            // pre-pause readings after it would publish stale data out of order.
                            let discarded = shaper.clear_buffers();
                            if discarded > 0 {
                                tracing::info!(
                                    instance = %cfg.id, discarded,
                                    "pause discarded buffered readings; resume snapshots the current truth"
                                );
                            }
                            let _ = events
                                .emit(
                                    Severity::Warning,
                                    "adapter-paused",
                                    Some("telemetry production paused".to_string()),
                                    Some(json!({ "instance": cfg.id })),
                                )
                                .await;
                        }
                        let _ = reply.send(changed);
                    }
                    DeviceControl::Resume { reply } => {
                        let changed = set_paused(health, false);
                        if changed {
                            let _ = events
                                .emit(
                                    Severity::Info,
                                    "adapter-resumed",
                                    Some("telemetry production resumed".to_string()),
                                    Some(json!({ "instance": cfg.id })),
                                )
                                .await;
                        }
                        let _ = reply.send(changed);
                        // Resume snapshots FIRST (HLD §7): while paused, drained updates were
                        // deliberately not published, so the fleet's last view of every signal is
                        // stale. A live read of the whole inventory republishes the current truth
                        // before on-change flow resumes — BYPASSING the shaper (a forced snapshot
                        // is a fresh full publish, not on-change flow), which is then re-armed:
                        // the first shaped reading after a resume always passes the deadband.
                        if changed && !inventory.is_empty() {
                            let outcome = session.read_named(inventory).await;
                            emit_notices(cfg, &mut session, events).await;
                            match outcome {
                                Ok(mut readings) => {
                                    stamp_received(&mut readings, &now_iso());
                                    publish_readings(cfg, &readings, data, dm, health).await;
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        instance = %cfg.id, error = %e,
                                        "resume snapshot failed; on-change flow resumes without it"
                                    );
                                }
                            }
                        }
                        if changed {
                            shaper.reset_deadband();
                        }
                    }
                    DeviceControl::Reconnect { reply } => {
                        // Buffered readings are data: flush them before dropping the session.
                        publish_shaped(cfg, shaper.flush_all(), data, dm, health).await;
                        drain_shaping(dm, &mut shaper);
                        session.close().await;
                        return PollExit::Reconnect(reply);
                    }
                    DeviceControl::Repoll { reply } => {
                        if health.is_paused() {
                            let _ = reply.send(Err("instance is paused - resume first".to_string()));
                        } else {
                            // A forced FRESH snapshot, not a drain of what happened to arrive
                            // (LLD §7): `polled` counts what was published, BAD results included.
                            // It BYPASSES the shaper — a repoll exists precisely to say the whole
                            // current truth again, now.
                            let result = session.snapshot_now().await;
                            emit_notices(cfg, &mut session, events).await;
                            match result {
                                Ok(mut readings) => {
                                    stamp_received(&mut readings, &now_iso());
                                    let n =
                                        publish_readings(cfg, &readings, data, dm, health).await;
                                    let _ = reply.send(Ok(n));
                                }
                                Err(e) => {
                                    tracing::warn!(instance = %cfg.id, error = %e, "repoll failed");
                                    health.read_errors.fetch_add(1, Ordering::Relaxed);
                                    let _ = reply.send(Err(e.to_string()));
                                    publish_shaped(cfg, shaper.flush_all(), data, dm, health).await;
                                    drain_shaping(dm, &mut shaper);
                                    session.close().await;
                                    return PollExit::LinkLost;
                                }
                            }
                        }
                    }
                }
            }

            // The tick keeps running while paused: the backend keeps draining (a paused MTConnect
            // instance's stream cache stays current — HLD §7), and only PUBLICATION is gated.
            _ = ticker.tick() => {
                let publish = !health.is_paused();
                let outcome = poll_once(cfg, &mut session, health, publish).await;
                emit_notices(cfg, &mut session, events).await;
                sync_served_signals(session.as_ref(), health);
                match outcome {
                    Err(()) => {
                        // The link broke: flush the open windows so nothing buffered is lost.
                        publish_shaped(cfg, shaper.flush_all(), data, dm, health).await;
                        drain_shaping(dm, &mut shaper);
                        session.close().await;
                        return PollExit::LinkLost;
                    }
                    Ok(None) => {} // paused: drained, nothing to publish
                    Ok(Some(readings)) => {
                        // A drained snapshot re-baselined the view (attach, resync ladder): the
                        // next reading of every signal must pass the deadband as fresh.
                        if session.take_resync() {
                            shaper.reset_deadband();
                        }
                        // A reload or model drift recompiled the served set inside the read: swap
                        // the policy table with it, flushing changed windows on the OLD policy.
                        if let Some(next) = session.shaping_generation() {
                            if shaping_gen.as_deref() != Some(next.as_str()) {
                                shaping_gen = Some(next);
                                let flushed = shaper.set_policies(session.shaping_policies());
                                publish_shaped(cfg, flushed, data, dm, health).await;
                            }
                        }
                        let now = Instant::now();
                        let mut updates = Vec::new();
                        for reading in readings {
                            updates.extend(shaper.offer(reading, now));
                        }
                        publish_shaped(cfg, updates, data, dm, health).await;
                        drain_shaping(dm, &mut shaper);
                    }
                }
            }

            // A batch window expired: flush it — ONE update whose samples[] carries the window's
            // readings in arrival order. One deadline serves every window (no per-signal timers).
            () = sleep_until_deadline(batch_deadline), if batch_deadline.is_some() => {
                let due = shaper.due(Instant::now());
                publish_shaped(cfg, due, data, dm, health).await;
                drain_shaping(dm, &mut shaper);
            }
        }

        if since_metrics.elapsed() >= METRICS_INTERVAL {
            dm.emit_periodic().await;
            since_metrics = Instant::now();
        }
    }
}

/// One poll: read and hand the readings back for shaping. `Ok(Some(readings))` = publish these
/// (through the shaper); `Ok(None)` = the instance is paused (the read still ran — the backend
/// drains and its caches update — but nothing may reach the wire); `Err(())` = the *connection*
/// broke (caller reconnects).
async fn poll_once(
    cfg: &DeviceConfig,
    session: &mut Box<dyn crate::device::DeviceSession>,
    health: &Arc<Health>,
    publish: bool,
) -> std::result::Result<Option<Vec<crate::device::Reading>>, ()> {
    let started = Instant::now();
    let mut readings = match session.read_signals().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(instance = %cfg.id, error = %e, "read failed; reconnecting");
            health.read_errors.fetch_add(1, Ordering::Relaxed);
            return Err(());
        }
    };
    let latency = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    health.poll_latency_ms.store(latency, Ordering::Relaxed);
    if !publish {
        // Paused: the drain above kept the caches current; publication stays gated (HLD §7).
        return Ok(None);
    }
    // The read just completed: this is the adapter's receive moment for the whole batch
    // (docs/SOUTHBOUND.md §2) — stamped here, not at publish, so batching cannot skew it.
    stamp_received(&mut readings, &now_iso());
    Ok(Some(readings))
}

/// Sleep until a batch window's deadline. Only ever awaited behind an `is_some()` select guard.
async fn sleep_until_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
        None => std::future::pending().await,
    }
}

/// Publish the shaper's released updates: each is ONE `SouthboundSignalUpdate` whose `samples[]`
/// carries one signal's readings in arrival order — the wire's batching shape
/// (docs/SOUTHBOUND.md §2). Records publish latency and feeds the staleness tracker, exactly as
/// the unshaped path does.
async fn publish_shaped(
    cfg: &DeviceConfig,
    updates: Vec<crate::shaping::Update>,
    data: &DataFacade,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
) -> u64 {
    if updates.is_empty() {
        return 0;
    }
    let publish_started = Instant::now();
    let mut published = 0u64;
    for readings in &updates {
        let Some(first) = readings.first() else { continue };
        // The data() facade builds the SouthboundSignalUpdate body, mints the topic, and stamps
        // identity. Every reading becomes one sample via the unit-tested `build_sample`.
        let mut signal = data.signal(&first.signal_id);
        if let Some(name) = &first.name {
            signal = signal.name(name);
        }
        if let Some(channel) = &first.channel {
            signal = signal.signal_path(channel);
        }
        let update = signal
            .device_parts(&cfg.adapter, &cfg.id, &cfg.connection.endpoint)
            .samples(readings.iter().map(build_sample))
            .build();

        if let Err(e) = data.publish(update).await {
            tracing::warn!(instance = %cfg.id, signal = %first.signal_id, error = %e, "publish failed");
        } else {
            published += 1;
            dm.on_signal_update(&first.signal_id, Instant::now());
        }
    }
    let publish_latency = u64::try_from(publish_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    health.publish_latency_ms.store(publish_latency, Ordering::Relaxed);
    published
}

/// Move the shaper's counters into the `MtconnectAdapterShaping` family's feed.
fn drain_shaping(dm: &Arc<DeviceMetrics>, shaper: &mut Shaper) {
    if let Some(counters) = shaper.take_counters() {
        dm.on_shaping(counters);
    }
}

/// Publish a batch of readings through the `data()` facade, recording publish latency and feeding
/// the staleness tracker. Shared by the poll tick, `repoll`, and the resume-time snapshot.
async fn publish_readings(
    cfg: &DeviceConfig,
    readings: &[crate::device::Reading],
    data: &DataFacade,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
) -> u64 {
    let publish_started = Instant::now();
    let mut published = 0u64;
    for r in readings {
        // The data() facade builds the SouthboundSignalUpdate body, mints the topic, and stamps
        // identity. Do not hand-build any of the three. The whole value/quality/timestamp/extras
        // mapping lives in the unit-tested `build_sample` (docs/SOUTHBOUND.md §2).
        let sample = build_sample(r);

        let mut signal = data.signal(&r.signal_id);
        if let Some(name) = &r.name {
            signal = signal.name(name);
        }
        if let Some(channel) = &r.channel {
            signal = signal.signal_path(channel);
        }
        let update = signal
            .device_parts(&cfg.adapter, &cfg.id, &cfg.connection.endpoint)
            .sample(sample)
            .build();

        if let Err(e) = data.publish(update).await {
            tracing::warn!(instance = %cfg.id, signal = %r.signal_id, error = %e, "publish failed");
        } else {
            published += 1;
            // Feed the staleness tracker — a signal that keeps updating is not stale.
            dm.on_signal_update(&r.signal_id, Instant::now());
        }
    }
    let publish_latency = u64::try_from(publish_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    health.publish_latency_ms.store(publish_latency, Ordering::Relaxed);
    published
}

/// Drain the session's protocol notices and publish each as a UNS event through the `events()`
/// facade — the HLD §9 event surface (`MtconnectAgentEvent`, `MtconnectDataLossEvent`,
/// `MtconnectModelDriftEvent`, `MtconnectConditionEvent`). The *mapping* lives in `device.rs`; this
/// only carries it to the wire.
async fn emit_notices(
    cfg: &DeviceConfig,
    session: &mut Box<dyn DeviceSession>,
    events: &EventsFacade,
) {
    for notice in session.take_notices() {
        if let Err(e) = events
            .emit(
                severity_of(notice.level),
                notice.event_type,
                Some(notice.message.clone()),
                Some(notice.context.clone()),
            )
            .await
        {
            tracing::warn!(instance = %cfg.id, event = notice.event_type, error = %e, "event emit failed");
        }
    }
}

/// The library severity one notice level publishes under.
fn severity_of(level: NoticeLevel) -> Severity {
    match level {
        NoticeLevel::Info => Severity::Info,
        NoticeLevel::Warning => Severity::Warning,
        NoticeLevel::Critical => Severity::Critical,
    }
}

/// Report what the session is actually delivering as `southbound_health.signalsSubscribed`. A
/// backend that compiles its signals against a device model (MTConnect) knows better than the
/// configuration does; one that does not keeps the configured inventory size.
fn sync_served_signals(session: &dyn DeviceSession, health: &Arc<Health>) {
    if let Some(count) = session.served_signals() {
        health.set_signal_inventory(count);
    }
}

/// What servicing the control channel while the session is down concluded.
enum DownOutcome {
    /// A `reconnect` command wants us to connect *now* (cut the backoff short); settle its reply on
    /// the next connect.
    Reconnect(oneshot::Sender<std::result::Result<(), String>>),
    /// The backoff window elapsed — retry the connect.
    Elapsed,
    /// The control channel closed (component shutdown).
    Closed,
}

/// Service the control channel while the session is **down**, for up to `wait`. Pause/resume take
/// effect (they only need the shared flag + event); the I/O verbs answer "disconnected" (the command
/// layer maps that to `DEVICE_UNAVAILABLE` / `BROWSE_FAILED`); a `reconnect` returns its reply so the
/// caller connects now.
async fn serve_while_down(
    control: &mut mpsc::Receiver<DeviceControl>,
    events: &EventsFacade,
    health: &Arc<Health>,
    wait: Duration,
) -> DownOutcome {
    let deadline = Instant::now() + wait;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return DownOutcome::Elapsed;
        }
        tokio::select! {
            biased;
            ctrl = control.recv() => {
                match ctrl {
                    None => return DownOutcome::Closed,
                    Some(DeviceControl::Reconnect { reply }) => return DownOutcome::Reconnect(reply),
                    Some(DeviceControl::Pause { reply }) => {
                        let changed = set_paused(health, true);
                        if changed {
                            let _ = events.emit(Severity::Warning, "adapter-paused", None, None).await;
                        }
                        let _ = reply.send(changed);
                    }
                    Some(DeviceControl::Resume { reply }) => {
                        let changed = set_paused(health, false);
                        if changed {
                            let _ = events.emit(Severity::Info, "adapter-resumed", None, None).await;
                        }
                        let _ = reply.send(changed);
                    }
                    Some(DeviceControl::Write(req)) => {
                        let _ = req.ack.send(Err("device is disconnected".to_string()));
                    }
                    Some(DeviceControl::ReadNow { reply, .. }) => {
                        let _ = reply.send(Err("device is disconnected".to_string()));
                    }
                    Some(DeviceControl::Repoll { reply }) => {
                        let _ = reply.send(Err("device is disconnected".to_string()));
                    }
                    Some(DeviceControl::Browse { reply, .. }) => {
                        let _ = reply.send(Err(BrowseError::Failed("device is disconnected".to_string())));
                    }
                }
            }
            _ = tokio::time::sleep(remaining) => return DownOutcome::Elapsed,
        }
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
    agents.get(&agent_id).map(|a| Arc::clone(a) as Arc<dyn AgentTelemetry>)
}

/// The adapter's receive-moment stamp: ISO-8601 UTC "now", from the library's own clock (the same
/// one the facades use to default `serverTs`).
fn now_iso() -> String {
    (edgecommons::facades::system_clock())()
}

fn rand01() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let n = std::collections::hash_map::RandomState::new().build_hasher().finish();
    (n % 1_000_000) as f64 / 1_000_000.0
}
