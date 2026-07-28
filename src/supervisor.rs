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

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use edgecommons::prelude::*;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::app::{
    connectivity_of, sample_timestamps, set_paused, stamp_received, Backoff, DeviceConfig,
    DeviceControl, Health, LinkState,
};
use crate::device::{BrowseError, DeviceBackend, Quality, SimBackend};
use crate::metrics::DeviceMetrics;

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
}

struct ConfigListener;

#[async_trait::async_trait]
impl ConfigurationChangeListener for ConfigListener {
    async fn on_configuration_change(&self, config: Arc<Config>) -> bool {
        tracing::info!(identity = %config.identity().path(), "configuration changed");
        true
    }
}

impl App {
    pub fn new(gg: &EdgeCommons) -> anyhow::Result<Self> {
        gg.add_config_change_listener(Arc::new(ConfigListener));

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

        Ok(Self { config, metrics, devices, stale_signal_secs })
    }

    pub async fn run(&self, gg: &EdgeCommons) -> anyhow::Result<()> {
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
            ));
            // Pre-define the metric set so it is fixed and discoverable at startup.
            dm.define_all();

            // The signal inventory `sb/signals` shows — a config/backend view, no device round-trip.
            // Its size drives the `southbound_health.signalsSubscribed` gauge while the link is up.
            let signals = make_backend(device)
                .map(|b| b.inventory(&device.connection))
                .unwrap_or_default();
            health.set_signal_inventory(signals.len() as u64);

            let (control_tx, control_rx) = mpsc::channel::<DeviceControl>(16);
            reported.push((device.clone(), Arc::clone(&health)));
            handles.push(crate::commands::DeviceHandle {
                cfg: device.clone(),
                control: control_tx,
                health: Arc::clone(&health),
                dm: Arc::clone(&dm),
                signals,
            });

            tokio::spawn(run_device(
                device.clone(),
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
        self.metrics.flush_metrics().await.ok();
        Ok(())
    }
}

/// Instantiate the backend for a device's `adapter`. A real adapter matches its protocol(s) here.
fn make_backend(cfg: &DeviceConfig) -> Option<Box<dyn DeviceBackend>> {
    match cfg.adapter.as_str() {
        "sim" => Some(Box::new(SimBackend)),
        other => {
            tracing::error!(instance = %cfg.id, adapter = %other, "unknown adapter");
            None
        }
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
    data: DataFacade,
    events: EventsFacade,
    dm: Arc<DeviceMetrics>,
    health: Arc<Health>,
    mut control: mpsc::Receiver<DeviceControl>,
) {
    let Some(backend) = make_backend(&cfg) else {
        return;
    };
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
        let exit = run_polling(&cfg, session, &data, &events, &dm, &health, &mut control).await;

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

/// Read on the poll interval and publish, servicing the control channel, until the link breaks or a
/// reconnect is requested.
async fn run_polling(
    cfg: &DeviceConfig,
    mut session: Box<dyn crate::device::DeviceSession>,
    data: &DataFacade,
    events: &EventsFacade,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
    control: &mut mpsc::Receiver<DeviceControl>,
) -> PollExit {
    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.poll_interval_ms));
    let mut since_metrics = Instant::now();

    loop {
        tokio::select! {
            // Poll and control share this one task, so a write can never race a read on the same
            // connection — most device protocols are a single request/response channel.
            ctrl = control.recv() => {
                let Some(ctrl) = ctrl else { return PollExit::Closed; };
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
                    }
                    DeviceControl::Reconnect { reply } => {
                        session.close().await;
                        return PollExit::Reconnect(reply);
                    }
                    DeviceControl::Repoll { reply } => {
                        if health.is_paused() {
                            let _ = reply.send(Err("instance is paused - resume first".to_string()));
                        } else {
                            match poll_once(cfg, &mut session, data, dm, health).await {
                                Ok(n) => {
                                    let _ = reply.send(Ok(n));
                                }
                                Err(()) => {
                                    let _ = reply.send(Err("link error".to_string()));
                                    session.close().await;
                                    return PollExit::LinkLost;
                                }
                            }
                        }
                    }
                }
            }

            _ = ticker.tick(), if !health.is_paused() => {
                if poll_once(cfg, &mut session, data, dm, health).await.is_err() {
                    session.close().await;
                    return PollExit::LinkLost;
                }
            }
        }

        if since_metrics.elapsed() >= METRICS_INTERVAL {
            dm.emit_periodic().await;
            since_metrics = Instant::now();
        }
    }
}

/// One poll: read, publish each reading, record latencies + staleness. `Ok(n)` = signals published;
/// `Err(())` = the *connection* broke (caller reconnects).
async fn poll_once(
    cfg: &DeviceConfig,
    session: &mut Box<dyn crate::device::DeviceSession>,
    data: &DataFacade,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
) -> std::result::Result<u64, ()> {
    let backend_adapter = cfg.adapter.clone();
    let started = Instant::now();
    let mut readings = match session.read_signals().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(instance = %cfg.id, error = %e, "read failed; reconnecting");
            health.read_errors.fetch_add(1, Ordering::Relaxed);
            return Err(());
        }
    };
    // The read just completed: this is the adapter's receive moment for the whole batch
    // (docs/SOUTHBOUND.md §2) — stamped here, not at publish, so batching cannot skew it.
    stamp_received(&mut readings, &now_iso());
    let latency = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    health.poll_latency_ms.store(latency, Ordering::Relaxed);

    let publish_started = Instant::now();
    let mut published = 0u64;
    for r in readings {
        // The data() facade builds the SouthboundSignalUpdate body, mints the topic, and stamps
        // identity. Do not hand-build any of the three.
        let quality = match r.quality {
            Quality::Good => edgecommons::facades::Quality::Good,
            Quality::Bad => edgecommons::facades::Quality::Bad,
            Quality::Uncertain => edgecommons::facades::Quality::Uncertain,
        };
        let mut sample = Sample::with_quality(r.value.clone(), quality);
        if let Some(raw) = &r.quality_raw {
            sample = sample.quality_raw(raw);
        }
        // The four-slot mapping (docs/SOUTHBOUND.md §2): capture -> serverTs (falling back to the
        // receive moment — a direct client's receive IS capture); sourceTs only when the device
        // supplied it (never synthesized); receivedTs as a per-sample extra only when a mediating
        // server makes it differ from the effective serverTs.
        let (server_ts, received_extra) = sample_timestamps(&r);
        sample.source_ts = r.source_ts.clone();
        if let Some(ts) = server_ts {
            sample = sample.server_ts(ts);
        }
        if let Some(received) = received_extra {
            sample = sample.extra("receivedTs", received);
        }

        let mut signal = data.signal(&r.signal_id);
        if let Some(name) = &r.name {
            signal = signal.name(name);
        }
        let update = signal
            .device_parts(&backend_adapter, &cfg.id, &cfg.connection.endpoint)
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
    Ok(published)
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
