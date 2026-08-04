//! # Device drivers — the connect / poll / publish / reconnect orchestration
//!
//! One task per configured device lives here: [`run_device`]'s connect+backoff loop, `run_polling`'s
//! read/shape/publish loop, the control-channel service (`serve_while_down`), the publish paths
//! (`publish_shaped`, `publish_readings`), the notice→event carrier (`emit_notices`), and the
//! passive-quality evaluation (`publish_passive`).
//!
//! **Why this is a module of its own.** The `ethernet-ip-adapter` discipline this component follows
//! keeps the untestable live seam as thin as it can be and puts *every decision it composes* in the
//! coverage denominator. The orchestration above is decision-dense — pause clears windows while
//! resume snapshots, a link loss flushes before it degrades, a shaping-generation swap flushes on the
//! OLD policy, a cancellation flushes before it detaches — and none of it needs a broker or a device:
//! it is driven end to end by a fake [`DeviceBackend`]/[`DeviceSession`] over the recording [`Wire`]
//! below. What is left in [`crate::supervisor`] is construction, spawning, and the shutdown
//! invocation, which genuinely do need a live `EdgeCommons` runtime.
//!
//! **The [`Wire`] seam.** The library's `DataFacade`/`EventsFacade` are minted by a live runtime and
//! have no public constructor, so a driver that named them directly could not be driven without a
//! broker. The drivers therefore publish and emit through [`Wire`]; `supervisor.rs` supplies the
//! facade-backed implementation (which still does exactly what it always did: `build_body` ▸
//! `stamp_component_path` ▸ `publish_body_via`), and the tests here supply a recording one.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use edgecommons::prelude::*;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::app::{
    Backoff, DeviceConfig, DeviceControl, Health, LinkState, build_sample, set_paused,
    stamp_received,
};
use crate::device::{BrowseError, DeviceBackend, DeviceSession, NoticeLevel};
use crate::metrics::DeviceMetrics;
use crate::shaping::{Shaper, policies_from_signals};
use crate::staleness::{PassiveLink, QualityWatchdog};

/// How often the periodic metrics emit runs, in the poll loop.
pub const METRICS_INTERVAL: Duration = Duration::from_secs(30);

// =================================================================================================
// The publish/emit seam
// =================================================================================================

/// Everything a device task puts on the UNS: signal updates on the `data` class and operator
/// events/alarms on the `evt` class.
///
/// Production is `supervisor::FacadeWire` over the library's per-instance `data()`/`events()`
/// facades — the only construction path the library offers for them, and one that needs a live
/// runtime. Naming the concrete facades here would make the whole poll loop unreachable without a
/// broker, so the drivers name this instead and the live shell supplies it.
#[async_trait]
pub trait Wire: Send + Sync {
    /// Publish one built `SouthboundSignalUpdate`, stamping the signal's canonical component path
    /// on the update-level extra (D-MtconnectAdapter-L13).
    ///
    /// # Errors
    /// Whatever the underlying facade reports — a malformed update, an unroutable channel, or a
    /// messaging failure.
    async fn publish(
        &self,
        update: &SignalUpdate,
        component_path: Option<&str>,
    ) -> edgecommons::Result<()>;

    /// Emit one operator event.
    ///
    /// # Errors
    /// Whatever the underlying facade reports.
    async fn emit(
        &self,
        severity: Severity,
        event_type: &str,
        message: Option<String>,
        context: Option<Value>,
    ) -> edgecommons::Result<()>;

    /// Raise a stateful alarm.
    ///
    /// # Errors
    /// Whatever the underlying facade reports.
    async fn raise_alarm(
        &self,
        severity: Severity,
        event_type: &str,
        message: Option<String>,
        context: Option<Value>,
    ) -> edgecommons::Result<()>;

    /// Clear a stateful alarm (same severity as the raise, so it rides the same channel).
    ///
    /// # Errors
    /// Whatever the underlying facade reports.
    async fn clear_alarm(
        &self,
        severity: Severity,
        event_type: &str,
        context: Option<Value>,
    ) -> edgecommons::Result<()>;
}

// =================================================================================================
// The device task
// =================================================================================================

/// One device's lifecycle: connect, poll, publish, reconnect — and service its control channel.
///
/// The connect loop and the poll loop are nested on purpose. A read failure that breaks the link
/// drops out of the poll loop and back into connect — the only place that knows how to back off.
///
/// **Shutdown (P1-7):** `cancel` preempts every await point in both loops. Cancelled while polling,
/// the task flushes its open batch windows, publishes them, and closes (detaches) the session before
/// returning; cancelled while connecting or backing off there is nothing buffered, so it returns at
/// once. Either way it returns *itself* — the supervisor joins it rather than letting the runtime's
/// teardown abort it mid-flush.
#[allow(clippy::too_many_arguments)]
pub async fn run_device(
    cfg: DeviceConfig,
    backend: Arc<dyn DeviceBackend>,
    wire: Arc<dyn Wire>,
    dm: Arc<DeviceMetrics>,
    health: Arc<Health>,
    mut control: mpsc::Receiver<DeviceControl>,
    stale_signal_secs: u64,
    cancel: CancellationToken,
) {
    let backoff = Backoff::default();
    let mut attempt: u32 = 0;
    // A `reconnect` command's reply, held until the next connect settles it.
    let mut pending_reconnect: Option<oneshot::Sender<std::result::Result<(), String>>> = None;

    loop {
        // --- CONNECT (servicing control while down, so pause/reconnect don't block on backoff) ---
        let session = loop {
            dm.on_connect_attempt();
            health.set_link(if attempt == 0 {
                LinkState::Connecting
            } else {
                LinkState::Backoff
            });
            let now = Instant::now();
            // Nothing is buffered before a session exists, so a cancelled connect has nothing to
            // flush — it just stops, promptly, however long the connect itself would have taken.
            let attempt_result = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    tracing::info!(instance = %cfg.id, "shutdown while connecting");
                    return;
                }
                result = backend.connect(&cfg.connection) => result,
            };
            match attempt_result {
                Ok(session) => {
                    attempt = 0;
                    dm.on_connected(now);
                    health.set_link(LinkState::Online);
                    dm.emit_now().await;
                    let _ = wire
                        .emit(
                            Severity::Info,
                            "device-connected",
                            Some(format!("connected to {}", cfg.connection.endpoint)),
                            Some(json!({ "instance": cfg.id, "adapter": backend.kind() })),
                        )
                        .await;
                    let _ = wire
                        .clear_alarm(Severity::Critical, "device-unreachable", None)
                        .await;
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
                    match serve_while_down(&mut control, wire.as_ref(), &health, wait, &cancel)
                        .await
                    {
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
        // The session just compiled its signals against the device model: the gauge reports what is
        // really being served, not what was merely configured.
        sync_served_signals(session.as_ref(), &health);
        let exit = run_polling(
            &cfg,
            session,
            &backend,
            wire.as_ref(),
            &dm,
            &health,
            &mut control,
            stale_signal_secs,
            cancel.clone(),
        )
        .await;

        // A deliberate stop is not a link failure: the poll loop already flushed its windows and
        // detached, so returning here keeps shutdown off the alarm surface. Raising
        // `device-unreachable` on every clean stop would alarm the whole fleet at each deployment.
        if matches!(exit, PollExit::Closed) {
            tracing::info!(instance = %cfg.id, "device task stopped");
            return;
        }

        // The link is down (or a reconnect asked us to drop it).
        health.set_link(LinkState::Backoff);
        health.reconnects.fetch_add(1, Ordering::Relaxed);
        dm.on_connection_dropped(Instant::now());
        dm.emit_now().await;
        let _ = wire
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
            // Handled above, before the alarm: a stop is not an outage.
            PollExit::Closed => return,
        }
    }
}

/// What ended the poll loop.
#[derive(Debug)]
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
///
/// **Shutdown (P1-7):** `cancel` is an arm of the same `select!` as the tick and the control
/// channel, so a SIGTERM lands between two awaits rather than aborting one: the open windows are
/// flushed and published, the session is closed (detaching it from the shared agent runtime), and
/// the loop returns [`PollExit::Closed`] for the supervisor to join.
#[allow(clippy::too_many_arguments)]
async fn run_polling(
    cfg: &DeviceConfig,
    mut session: Box<dyn DeviceSession>,
    backend: &Arc<dyn DeviceBackend>,
    wire: &dyn Wire,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
    control: &mut mpsc::Receiver<DeviceControl>,
    stale_signal_secs: u64,
    cancel: CancellationToken,
) -> PollExit {
    // Passive quality (P1-5, HLD §6 rows 2-3): how long a held value may stand in before it is
    // BAD. The watchdog is rebuilt with the session, like the shaper — the readings it holds are
    // this session's, and a reconnect re-baselines them from the attach snapshot.
    let stale_after = Duration::from_secs(stale_signal_secs);
    let mut watchdog = QualityWatchdog::default();
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
        // Checked here as well as in the `select!` below, because the branches of an unbiased
        // `select!` are polled in random order: whatever the last iteration was doing, a cancelled
        // task stops on THIS iteration rather than eventually.
        if cancel.is_cancelled() {
            tracing::info!(instance = %cfg.id, "shutdown: flushing the open windows");
            return stop_cleanly(
                cfg,
                &mut shaper,
                &mut session,
                wire,
                dm,
                health,
                &mut watchdog,
            )
            .await;
        }

        // ONE deadline per instance task: the earliest open batch window.
        let batch_deadline = shaper.next_deadline();
        tokio::select! {
            // Shutdown (P1-7). Buffered readings are data: they are flushed and published while the
            // messaging facade is still alive, and the session is closed — detaching it from the
            // shared agent runtime — before this task returns to be joined.
            () = cancel.cancelled() => {
                tracing::info!(instance = %cfg.id, "shutdown: flushing the open windows");
                return stop_cleanly(cfg, &mut shaper, &mut session, wire, dm, health, &mut watchdog).await;
            }

            // Poll and control share this one task, so a write can never race a read on the same
            // connection — most device protocols are a single request/response channel.
            ctrl = control.recv() => {
                let Some(ctrl) = ctrl else {
                    // The command surface went away: flush the open batch windows — no exit may
                    // lose the readings a window was still coalescing — and detach.
                    return stop_cleanly(cfg, &mut shaper, &mut session, wire, dm, health, &mut watchdog).await;
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
                            let _ = wire
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
                            let _ = wire
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
                        if changed {
                            // The inventory is read HERE, at resume time, and never frozen at
                            // connect (C-1): a configuration reload may have added signals while
                            // this instance was paused, and a snapshot that omitted them would
                            // leave them unpublished until they happened to change. The backend
                            // reads the live signal slot, so the answer is this instant's truth.
                            let inventory: Vec<String> = backend
                                .inventory(&cfg.connection)
                                .into_iter()
                                .map(|s| s.id)
                                .collect();
                            if !inventory.is_empty() {
                                // The snapshot below IS the re-baseline: forget what was held
                                // before the pause (and any passive degradation applied to it) so
                                // the fresh readings rebuild it.
                                watchdog.on_rebaseline();
                                let outcome = session.read_named(&inventory).await;
                                emit_notices(cfg, &mut session, wire).await;
                                match outcome {
                                    Ok(mut readings) => {
                                        stamp_received(&mut readings, &now_iso());
                                        publish_readings(cfg, &readings, wire, dm, health, &mut watchdog).await;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            instance = %cfg.id, error = %e,
                                            "resume snapshot failed; on-change flow resumes without it"
                                        );
                                    }
                                }
                            }
                            shaper.reset_deadband();
                        }
                    }
                    DeviceControl::Reconnect { reply } => {
                        // Buffered readings are data: flush them before dropping the session.
                        flush_open_windows(cfg, &mut shaper, wire, dm, health, &mut watchdog).await;
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
                            emit_notices(cfg, &mut session, wire).await;
                            match result {
                                Ok(mut readings) => {
                                    stamp_received(&mut readings, &now_iso());
                                    let n =
                                        publish_readings(cfg, &readings, wire, dm, health, &mut watchdog).await;
                                    let _ = reply.send(Ok(n));
                                }
                                Err(e) => {
                                    tracing::warn!(instance = %cfg.id, error = %e, "repoll failed");
                                    health.read_errors.fetch_add(1, Ordering::Relaxed);
                                    let _ = reply.send(Err(e.to_string()));
                                    flush_open_windows(cfg, &mut shaper, wire, dm, health, &mut watchdog).await;
                                    publish_passive(
                                        cfg, lost_link(session.passive_input()), &mut watchdog,
                                        stale_after, wire, dm, health,
                                    ).await;
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
                emit_notices(cfg, &mut session, wire).await;
                sync_served_signals(session.as_ref(), health);
                match outcome {
                    Err(()) => {
                        // The link broke: flush the open windows so nothing buffered is lost...
                        flush_open_windows(cfg, &mut shaper, wire, dm, health, &mut watchdog).await;
                        // ...and say so about every value this session was holding, BEFORE it dies
                        // (§7.2.3): the fleet must see BAD, not a GOOD value frozen at the moment
                        // the link went. The next session re-baselines from its attach snapshot.
                        publish_passive(
                            cfg, lost_link(session.passive_input()), &mut watchdog,
                            stale_after, wire, dm, health,
                        ).await;
                        session.close().await;
                        return PollExit::LinkLost;
                    }
                    // Paused: the drain kept the caches current, and publication — synthetic
                    // quality transitions included — stays gated (HLD §7).
                    Ok(None) => {}
                    Ok(Some(readings)) => {
                        // A drained snapshot re-baselined the view (attach, resync ladder): the
                        // next reading of every signal must pass the deadband as fresh, and what
                        // the watchdog was holding is superseded by the snapshot itself.
                        if session.take_resync() {
                            shaper.reset_deadband();
                            watchdog.on_rebaseline();
                        }
                        // A reload or model drift recompiled the served set inside the read: swap
                        // the policy table with it, flushing changed windows on the OLD policy.
                        if let Some(next) = session.shaping_generation() {
                            if shaping_gen.as_deref() != Some(next.as_str()) {
                                shaping_gen = Some(next);
                                let flushed = shaper.set_policies(session.shaping_policies());
                                publish_shaped(cfg, flushed, wire, dm, health, &mut watchdog).await;
                            }
                        }
                        let now = Instant::now();
                        let mut updates = Vec::new();
                        for reading in readings {
                            updates.extend(shaper.offer(reading, now));
                        }
                        publish_shaped(cfg, updates, wire, dm, health, &mut watchdog).await;
                        drain_shaping(dm, &mut shaper);
                        // Passive quality (P1-5): what this tick published is held; what it did NOT
                        // publish is judged against the link. An agent that has stopped vouching
                        // for currency degrades every held value — MTConnect is on-change, so
                        // silence alone proves nothing, but a missed heartbeat does (D-R12).
                        publish_passive(
                            cfg, session.passive_input(), &mut watchdog,
                            stale_after, wire, dm, health,
                        ).await;
                    }
                }
            }

            // A batch window expired: flush it — ONE update whose samples[] carries the window's
            // readings in arrival order. One deadline serves every window (no per-signal timers).
            () = sleep_until_deadline(batch_deadline), if batch_deadline.is_some() => {
                let due = shaper.due(Instant::now());
                publish_shaped(cfg, due, wire, dm, health, &mut watchdog).await;
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
    session: &mut Box<dyn DeviceSession>,
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
    // The fallback receive stamp, for a backend that did not stamp arrival itself (the simulator,
    // whose read IS its arrival). An MTConnect reading already carries the moment its document was
    // ingested (C-6) and is left alone.
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
    wire: &dyn Wire,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
    watchdog: &mut QualityWatchdog,
) -> u64 {
    if updates.is_empty() {
        return 0;
    }
    let publish_started = Instant::now();
    let mut published = 0u64;
    for readings in &updates {
        let Some(first) = readings.first() else {
            continue;
        };
        // The `Wire` hands this to the data() facade, which builds the SouthboundSignalUpdate
        // body, mints the topic, and stamps identity. Every reading becomes one sample via the
        // unit-tested `build_sample`.
        let mut signal = SignalUpdate::builder().signal_id(&first.signal_id);
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

        // ONE componentPath for the whole flushed window: the path is per-signal-static and a
        // window is one signal's readings, so it belongs on the update, not on every sample.
        if let Err(e) = wire.publish(&update, first.component_path.as_deref()).await {
            tracing::warn!(instance = %cfg.id, signal = %first.signal_id, error = %e, "publish failed");
        } else {
            published += 1;
            let at = Instant::now();
            dm.on_signal_update(&first.signal_id, at);
            // Every reading of the window reached the wire; the last one is what the fleet is now
            // holding, and is what a passive transition would republish.
            for reading in readings {
                watchdog.on_published(reading, at);
            }
        }
    }
    let publish_latency = u64::try_from(publish_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    health
        .publish_latency_ms
        .store(publish_latency, Ordering::Relaxed);
    published
}

/// A deliberate stop's tail: flush and publish the open batch windows while the messaging facade is
/// still alive, then close the session — which, for an MTConnect instance, detaches it from the
/// shared agent runtime that is still running behind it (P1-7's ordering).
async fn stop_cleanly(
    cfg: &DeviceConfig,
    shaper: &mut Shaper,
    session: &mut Box<dyn DeviceSession>,
    wire: &dyn Wire,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
    watchdog: &mut QualityWatchdog,
) -> PollExit {
    flush_open_windows(cfg, shaper, wire, dm, health, watchdog).await;
    session.close().await;
    PollExit::Closed
}

/// Flush every open batch window, publish what came out, and hand the shaper's counters to the
/// metrics feed — the shared tail of **every** way out of the poll loop (shutdown, cancellation,
/// link loss, reconnect, a failed repoll). Buffered readings are data: no exit may drop them.
async fn flush_open_windows(
    cfg: &DeviceConfig,
    shaper: &mut Shaper,
    wire: &dyn Wire,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
    watchdog: &mut QualityWatchdog,
) {
    publish_shaped(cfg, shaper.flush_all(), wire, dm, health, watchdog).await;
    drain_shaping(dm, shaper);
}

/// Move the shaper's counters into the `MtconnectAdapterShaping` family's feed.
fn drain_shaping(dm: &Arc<DeviceMetrics>, shaper: &mut Shaper) {
    if let Some(counters) = shaper.take_counters() {
        dm.on_shaping(counters);
    }
}

/// Publish a batch of readings through the `data()` facade, recording publish latency and feeding
/// the staleness tracker. Shared by `repoll`, the resume-time snapshot, and the passive-quality
/// transitions (which bypass the shaper — a quality change never sits in a batch window).
async fn publish_readings(
    cfg: &DeviceConfig,
    readings: &[crate::device::Reading],
    wire: &dyn Wire,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
    watchdog: &mut QualityWatchdog,
) -> u64 {
    let publish_started = Instant::now();
    let mut published = 0u64;
    for r in readings {
        // The `Wire` hands this to the data() facade, which builds the SouthboundSignalUpdate
        // body, mints the topic, and stamps identity. Do not hand-build any of the three. The whole
        // value/quality/timestamp/extras mapping lives in the unit-tested `build_sample`
        // (docs/SOUTHBOUND.md §2).
        let sample = build_sample(r);

        let mut signal = SignalUpdate::builder().signal_id(&r.signal_id);
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

        if let Err(e) = wire.publish(&update, r.component_path.as_deref()).await {
            tracing::warn!(instance = %cfg.id, signal = %r.signal_id, error = %e, "publish failed");
        } else {
            published += 1;
            // A synthetic quality transition is the watchdog's own output: it says nothing about
            // value silence, so it feeds NEITHER tracker (D-R13) — it must not reset the
            // `staleSignals` metric's age, and it must not overwrite the hold it is reporting on.
            if !crate::staleness::is_synthetic(r) {
                let at = Instant::now();
                // Feed the staleness tracker — a signal that keeps updating is not stale.
                dm.on_signal_update(&r.signal_id, at);
                watchdog.on_published(r, at);
            }
        }
    }
    let publish_latency = u64::try_from(publish_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    health
        .publish_latency_ms
        .store(publish_latency, Ordering::Relaxed);
    published
}

/// Evaluate the link against everything this instance is holding and publish the quality
/// transitions it produced (HLD §6 rows 2-3, P1-5).
///
/// The readings are synthetic: the **held** value with a degraded verdict and the `passive` marker
/// (D-R14). They BYPASS the shaper — a quality transition is news, not a value in a batch window —
/// and they feed neither the `staleSignals` tracker nor the watchdog itself (D-R13, enforced in
/// [`publish_readings`]). A backend with no mediated liveness (`link` is `None` — the simulator,
/// whose read IS its liveness) has nothing to evaluate.
///
/// A steady state returns nothing, so this costs one comparison per tick until something changes.
async fn publish_passive(
    cfg: &DeviceConfig,
    link: Option<PassiveLink>,
    watchdog: &mut QualityWatchdog,
    stale_after: Duration,
    wire: &dyn Wire,
    dm: &Arc<DeviceMetrics>,
    health: &Arc<Health>,
) {
    let Some(link) = link else {
        return;
    };
    let mut synthetic = watchdog.evaluate(link, stale_after, Instant::now());
    if synthetic.is_empty() {
        return;
    }
    tracing::info!(
        instance = %cfg.id, phase = ?watchdog.phase(), signals = synthetic.len(),
        "passive quality transition: republishing held values with a new verdict"
    );
    // The emission moment is the adapter's receive moment for these samples; the value's own
    // capture stamp rides on untouched as `serverTs`.
    stamp_received(&mut synthetic, &now_iso());
    publish_readings(cfg, &synthetic, wire, dm, health, watchdog).await;
}

/// The link facts a **lost** link presents, whatever the authority's flag reads at this instant:
/// the read that just failed is itself the evidence, and this session is about to be dropped.
fn lost_link(link: Option<PassiveLink>) -> Option<PassiveLink> {
    link.map(|link| PassiveLink {
        unreachable: true,
        ..link
    })
}

/// Drain the session's protocol notices and publish each as a UNS event through the `events()`
/// facade — the HLD §9 event surface (`MtconnectAgentEvent`, `MtconnectDataLossEvent`,
/// `MtconnectModelDriftEvent`, `MtconnectConditionEvent`). The *mapping* lives in `device.rs`; this
/// only carries it to the wire.
async fn emit_notices(cfg: &DeviceConfig, session: &mut Box<dyn DeviceSession>, wire: &dyn Wire) {
    for notice in session.take_notices() {
        if let Err(e) = wire
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
///
/// A backoff window can be minutes long, so shutdown is an arm of the same `select!`: cancellation
/// ends the wait at once rather than after it (P1-7). There is nothing buffered while down.
async fn serve_while_down(
    control: &mut mpsc::Receiver<DeviceControl>,
    wire: &dyn Wire,
    health: &Arc<Health>,
    wait: Duration,
    cancel: &CancellationToken,
) -> DownOutcome {
    let deadline = Instant::now() + wait;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return DownOutcome::Elapsed;
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return DownOutcome::Closed,
            ctrl = control.recv() => {
                match ctrl {
                    None => return DownOutcome::Closed,
                    Some(DeviceControl::Reconnect { reply }) => return DownOutcome::Reconnect(reply),
                    Some(DeviceControl::Pause { reply }) => {
                        let changed = set_paused(health, true);
                        if changed {
                            let _ = wire.emit(Severity::Warning, "adapter-paused", None, None).await;
                        }
                        let _ = reply.send(changed);
                    }
                    Some(DeviceControl::Resume { reply }) => {
                        let changed = set_paused(health, false);
                        if changed {
                            let _ = wire.emit(Severity::Info, "adapter-resumed", None, None).await;
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
    let n = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    (n % 1_000_000) as f64 / 1_000_000.0
}

// =================================================================================================
// Tests — the whole orchestration, driven by a fake backend/session over a recording wire
// =================================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use edgecommons::config::model::Config;
    use edgecommons::metrics::{Metric, MetricService};
    use serde_json::json;

    use crate::device::{BrowsePage, ConnectionConfig, DeviceError, Notice, Reading, SignalInfo};
    use crate::shaping::{PublishPolicy, SignalRoute};
    use crate::staleness::{PASSIVE_EXTRA_KEY, PassiveLink};

    const COMPONENT: &str = "MtconnectAdapter";
    const THING: &str = "test-thing";

    // ---------------------------------------------------------------------------------------
    // The recording wire
    // ---------------------------------------------------------------------------------------

    /// One event the drivers put on the `evt` class.
    #[derive(Debug, Clone, PartialEq)]
    struct EmittedEvent {
        severity: Severity,
        event_type: String,
        alarm: Option<bool>,
        context: Option<Value>,
    }

    /// Records everything that reached the wire, and can be told to fail every publish.
    #[derive(Default)]
    struct RecordingWire {
        published: Mutex<Vec<(SignalUpdate, Option<String>)>>,
        events: Mutex<Vec<EmittedEvent>>,
        publish_fails: AtomicBool,
    }

    impl RecordingWire {
        fn published(&self) -> Vec<(SignalUpdate, Option<String>)> {
            self.published.lock().unwrap().clone()
        }

        /// The signal ids published, in order, one entry per update.
        fn signals(&self) -> Vec<String> {
            self.published()
                .iter()
                .map(|(u, _)| u.signal_id.clone().unwrap_or_default())
                .collect()
        }

        /// Every sample of every update for one signal, in publish order.
        fn samples_of(&self, signal_id: &str) -> Vec<Sample> {
            self.published()
                .iter()
                .filter(|(u, _)| u.signal_id.as_deref() == Some(signal_id))
                .flat_map(|(u, _)| u.samples.clone())
                .collect()
        }

        fn events(&self) -> Vec<EmittedEvent> {
            self.events.lock().unwrap().clone()
        }

        fn event_types(&self) -> Vec<String> {
            self.events().into_iter().map(|e| e.event_type).collect()
        }
    }

    #[async_trait]
    impl Wire for RecordingWire {
        async fn publish(
            &self,
            update: &SignalUpdate,
            component_path: Option<&str>,
        ) -> edgecommons::Result<()> {
            if self.publish_fails.load(Ordering::Relaxed) {
                return Err(EdgeCommonsError::Facade("no broker".to_string()));
            }
            self.published
                .lock()
                .unwrap()
                .push((update.clone(), component_path.map(str::to_string)));
            Ok(())
        }

        async fn emit(
            &self,
            severity: Severity,
            event_type: &str,
            _message: Option<String>,
            context: Option<Value>,
        ) -> edgecommons::Result<()> {
            self.events.lock().unwrap().push(EmittedEvent {
                severity,
                event_type: event_type.to_string(),
                alarm: None,
                context,
            });
            Ok(())
        }

        async fn raise_alarm(
            &self,
            severity: Severity,
            event_type: &str,
            _message: Option<String>,
            context: Option<Value>,
        ) -> edgecommons::Result<()> {
            self.events.lock().unwrap().push(EmittedEvent {
                severity,
                event_type: event_type.to_string(),
                alarm: Some(true),
                context,
            });
            Ok(())
        }

        async fn clear_alarm(
            &self,
            severity: Severity,
            event_type: &str,
            context: Option<Value>,
        ) -> edgecommons::Result<()> {
            self.events.lock().unwrap().push(EmittedEvent {
                severity,
                event_type: event_type.to_string(),
                alarm: Some(false),
                context,
            });
            Ok(())
        }
    }

    // ---------------------------------------------------------------------------------------
    // The scripted backend + session
    // ---------------------------------------------------------------------------------------

    /// Everything the fake device does and everything it was asked to do. Shared (`Arc`) so a test
    /// can restage it — a reload adding a signal, a link going down — while the loop is running.
    struct Script {
        /// What successive `read_signals` calls answer. Exhausted ⇒ "nothing changed", which is
        /// what an on-change protocol says most of the time.
        reads: Mutex<VecDeque<std::result::Result<Vec<Reading>, String>>>,
        /// What `read_named`/`snapshot_now` answer.
        named: Mutex<std::result::Result<Vec<Reading>, String>>,
        notices: Mutex<Vec<Notice>>,
        resync: Mutex<bool>,
        generation: Mutex<Option<String>>,
        policies: Mutex<HashMap<String, PublishPolicy>>,
        passive: Mutex<Option<PassiveLink>>,
        served: Mutex<Option<u64>>,
        inventory: Mutex<Vec<SignalInfo>>,
        /// What successive `connect` calls answer: `Ok` or `Err((transient, message))`.
        connects: Mutex<VecDeque<std::result::Result<(), (bool, String)>>>,

        closed: AtomicUsize,
        connect_calls: AtomicUsize,
        read_calls: AtomicUsize,
        named_calls: Mutex<Vec<Vec<String>>>,
        snapshot_calls: AtomicUsize,
        browse_calls: AtomicUsize,
        writes: Mutex<Vec<(String, Value)>>,
    }

    impl Default for Script {
        fn default() -> Self {
            Self {
                reads: Mutex::new(VecDeque::new()),
                named: Mutex::new(Ok(Vec::new())),
                notices: Mutex::new(Vec::new()),
                resync: Mutex::new(false),
                generation: Mutex::new(None),
                policies: Mutex::new(HashMap::new()),
                passive: Mutex::new(None),
                served: Mutex::new(None),
                inventory: Mutex::new(Vec::new()),
                connects: Mutex::new(VecDeque::new()),
                closed: AtomicUsize::new(0),
                connect_calls: AtomicUsize::new(0),
                read_calls: AtomicUsize::new(0),
                named_calls: Mutex::new(Vec::new()),
                snapshot_calls: AtomicUsize::new(0),
                browse_calls: AtomicUsize::new(0),
                writes: Mutex::new(Vec::new()),
            }
        }
    }

    impl Script {
        fn deliver(&self, readings: Vec<Reading>) {
            self.reads.lock().unwrap().push_back(Ok(readings));
        }
        fn break_link(&self) {
            self.reads
                .lock()
                .unwrap()
                .push_back(Err("the agent stopped answering".to_string()));
        }
        fn set_inventory(&self, ids: &[&str]) {
            *self.inventory.lock().unwrap() = ids
                .iter()
                .map(|id| SignalInfo {
                    id: (*id).to_string(),
                    name: None,
                })
                .collect();
        }
        fn set_named(&self, readings: Vec<Reading>) {
            *self.named.lock().unwrap() = Ok(readings);
        }
        fn set_policies(&self, policies: HashMap<String, PublishPolicy>) {
            *self.policies.lock().unwrap() = policies;
        }
        fn set_passive(&self, link: PassiveLink) {
            *self.passive.lock().unwrap() = Some(link);
        }
        fn closes(&self) -> usize {
            self.closed.load(Ordering::Relaxed)
        }
        fn reads_done(&self) -> usize {
            self.read_calls.load(Ordering::Relaxed)
        }
        fn named_calls(&self) -> Vec<Vec<String>> {
            self.named_calls.lock().unwrap().clone()
        }
    }

    struct FakeSession(Arc<Script>);

    #[async_trait]
    impl DeviceSession for FakeSession {
        async fn read_signals(&mut self) -> crate::device::Result<Vec<Reading>> {
            self.0.read_calls.fetch_add(1, Ordering::Relaxed);
            let next = self.0.reads.lock().unwrap().pop_front();
            match next {
                Some(Ok(readings)) => Ok(readings),
                Some(Err(e)) => Err(DeviceError::Transient(anyhow::anyhow!(e))),
                None => Ok(Vec::new()),
            }
        }

        async fn read_named(&mut self, ids: &[String]) -> crate::device::Result<Vec<Reading>> {
            self.0.named_calls.lock().unwrap().push(ids.to_vec());
            let answer = self.0.named.lock().unwrap().clone();
            match answer {
                Ok(readings) => Ok(readings
                    .into_iter()
                    .filter(|r| ids.iter().any(|id| id == &r.signal_id))
                    .collect()),
                Err(e) => Err(DeviceError::Transient(anyhow::anyhow!(e))),
            }
        }

        async fn write_signal(
            &mut self,
            signal_id: &str,
            value: &Value,
        ) -> crate::device::Result<()> {
            self.0
                .writes
                .lock()
                .unwrap()
                .push((signal_id.to_string(), value.clone()));
            if signal_id == "refused" {
                return Err(DeviceError::Permanent(anyhow::anyhow!("read-only")));
            }
            Ok(())
        }

        async fn browse(
            &mut self,
            _cursor: Option<String>,
            _max: usize,
        ) -> std::result::Result<BrowsePage, BrowseError> {
            self.0.browse_calls.fetch_add(1, Ordering::Relaxed);
            Ok(BrowsePage {
                entries: Vec::new(),
                next_cursor: None,
            })
        }

        async fn snapshot_now(&mut self) -> crate::device::Result<Vec<Reading>> {
            self.0.snapshot_calls.fetch_add(1, Ordering::Relaxed);
            let answer = self.0.named.lock().unwrap().clone();
            answer.map_err(|e| DeviceError::Transient(anyhow::anyhow!(e)))
        }

        fn take_notices(&mut self) -> Vec<Notice> {
            std::mem::take(&mut *self.0.notices.lock().unwrap())
        }

        fn served_signals(&self) -> Option<u64> {
            *self.0.served.lock().unwrap()
        }

        fn shaping_generation(&self) -> Option<String> {
            self.0.generation.lock().unwrap().clone()
        }

        fn shaping_policies(&self) -> HashMap<String, PublishPolicy> {
            self.0.policies.lock().unwrap().clone()
        }

        fn take_resync(&mut self) -> bool {
            std::mem::replace(&mut *self.0.resync.lock().unwrap(), false)
        }

        fn passive_input(&self) -> Option<PassiveLink> {
            *self.0.passive.lock().unwrap()
        }

        async fn close(&mut self) {
            self.0.closed.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct FakeBackend(Arc<Script>);

    #[async_trait]
    impl DeviceBackend for FakeBackend {
        fn kind(&self) -> &'static str {
            "fake"
        }

        fn inventory(&self, _cfg: &ConnectionConfig) -> Vec<SignalInfo> {
            self.0.inventory.lock().unwrap().clone()
        }

        async fn connect(
            &self,
            _cfg: &ConnectionConfig,
        ) -> crate::device::Result<Box<dyn DeviceSession>> {
            self.0.connect_calls.fetch_add(1, Ordering::Relaxed);
            let next = self.0.connects.lock().unwrap().pop_front();
            match next {
                None | Some(Ok(())) => Ok(Box::new(FakeSession(Arc::clone(&self.0)))),
                Some(Err((true, msg))) => Err(DeviceError::Transient(anyhow::anyhow!(msg))),
                Some(Err((false, msg))) => Err(DeviceError::Permanent(anyhow::anyhow!(msg))),
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Wiring
    // ---------------------------------------------------------------------------------------

    #[derive(Default)]
    struct NoopMetrics;

    #[async_trait]
    impl MetricService for NoopMetrics {
        fn define_metric(&self, _metric: Metric) {}
        fn is_metric_defined(&self, _name: &str) -> bool {
            true
        }
        async fn emit_metric(
            &self,
            _name: &str,
            _values: HashMap<String, f64>,
        ) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn emit_metric_now(
            &self,
            _name: &str,
            _values: HashMap<String, f64>,
        ) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn flush_metrics(&self) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn shutdown(&self) {}
    }

    fn config() -> Arc<Config> {
        Arc::new(Config::from_value(COMPONENT, THING, json!({})).unwrap())
    }

    fn device_cfg() -> DeviceConfig {
        serde_json::from_value(json!({
            "id": "cnc-1",
            "adapter": "fake",
            "pollIntervalMs": 5,
            "connection": { "endpoint": "fake://cnc-1" },
            "signals": [],
        }))
        .unwrap()
    }

    fn metrics(health: &Arc<Health>) -> Arc<DeviceMetrics> {
        Arc::new(DeviceMetrics::new(
            Arc::new(NoopMetrics),
            config(),
            "cnc-1".to_string(),
            Arc::clone(health),
            30,
            None,
        ))
    }

    fn reading(id: &str, value: f64) -> crate::device::Reading {
        crate::device::Reading::good(id, json!(value))
    }

    /// A batching policy for one signal, so a test can leave a window open on purpose.
    fn batching(batch_ms: u32, route: SignalRoute) -> HashMap<String, PublishPolicy> {
        HashMap::from([(
            "x-position".to_string(),
            PublishPolicy {
                batch_ms,
                latest_only: false,
                deadband: None,
                route,
            },
        )])
    }

    /// Link facts: reachable, with `age_ms` since the agent last vouched, against a 10 s window.
    fn link(unreachable: bool, age_ms: u64) -> PassiveLink {
        PassiveLink {
            unreachable,
            liveness_age: Some(Duration::from_millis(age_ms)),
            liveness_window: Duration::from_secs(10),
        }
    }

    /// Poll until `cond` holds, or fail loudly. Real time, a millisecond at a time — the loop under
    /// test runs on a 5 ms tick.
    async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..3_000 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// A running `run_polling` plus the handles a test steers it with.
    struct Driven {
        task: Mutex<Option<tokio::task::JoinHandle<PollExit>>>,
        control: Mutex<Option<mpsc::Sender<DeviceControl>>>,
        wire: Arc<RecordingWire>,
        health: Arc<Health>,
        cancel: CancellationToken,
    }

    impl Driven {
        fn start(script: &Arc<Script>) -> Self {
            let wire = Arc::new(RecordingWire::default());
            let health = Arc::new(Health::default());
            let dm = metrics(&health);
            let cancel = CancellationToken::new();
            let (control, mut control_rx) = mpsc::channel::<DeviceControl>(16);
            let backend: Arc<dyn DeviceBackend> = Arc::new(FakeBackend(Arc::clone(script)));
            let session: Box<dyn DeviceSession> = Box::new(FakeSession(Arc::clone(script)));
            let cfg = device_cfg();

            let task = {
                let (wire, health, cancel) =
                    (Arc::clone(&wire), Arc::clone(&health), cancel.clone());
                tokio::spawn(async move {
                    run_polling(
                        &cfg,
                        session,
                        &backend,
                        wire.as_ref(),
                        &dm,
                        &health,
                        &mut control_rx,
                        30,
                        cancel,
                    )
                    .await
                })
            };
            Self {
                task: Mutex::new(Some(task)),
                control: Mutex::new(Some(control)),
                wire,
                health,
                cancel,
            }
        }

        /// Hand one control message to the loop.
        async fn send(&self, message: DeviceControl) {
            let control = self.control.lock().unwrap().clone().expect("still running");
            control.send(message).await.ok();
        }

        /// Join the loop and take its exit.
        async fn join(&self) -> PollExit {
            let task = self.task.lock().unwrap().take().expect("joined once");
            task.await.expect("the poll loop returns")
        }

        /// Send one control message whose reply proves the loop has processed it — the natural
        /// synchronization point, so no test has to guess at interleaving.
        async fn pause(&self) -> bool {
            let (tx, rx) = oneshot::channel();
            self.send(DeviceControl::Pause { reply: tx }).await;
            rx.await.expect("the loop answers a pause")
        }

        async fn resume(&self) -> bool {
            let (tx, rx) = oneshot::channel();
            self.send(DeviceControl::Resume { reply: tx }).await;
            rx.await.expect("the loop answers a resume")
        }

        async fn repoll(&self) -> std::result::Result<u64, String> {
            let (tx, rx) = oneshot::channel();
            self.send(DeviceControl::Repoll { reply: tx }).await;
            rx.await.expect("the loop answers a repoll")
        }

        /// Close the control channel and take the loop's exit.
        async fn finish(&self) -> PollExit {
            self.control.lock().unwrap().take();
            self.join().await
        }
    }

    // ---------------------------------------------------------------------------------------
    // Ordinary flow
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_tick_publishes_what_it_read_with_the_canonical_component_path() {
        let script = Arc::new(Script::default());
        script.deliver(vec![crate::device::Reading {
            component_path: Some("Axes/Linear[X]".into()),
            channel: Some("axes/x/position".into()),
            name: Some("X position".into()),
            ..reading("x-position", 1.0)
        }]);
        let d = Driven::start(&script);

        let wire = Arc::clone(&d.wire);
        wait_for("the first update", || !wire.published().is_empty()).await;
        let exit = d.finish().await;
        assert!(matches!(exit, PollExit::Closed));

        let (update, component_path) = wire.published().remove(0);
        assert_eq!(update.signal_id.as_deref(), Some("x-position"));
        assert_eq!(update.signal_name.as_deref(), Some("X position"));
        assert_eq!(update.signal_path.as_deref(), Some("axes/x/position"));
        assert_eq!(
            component_path.as_deref(),
            Some("Axes/Linear[X]"),
            "the canonical path rides the UPDATE, once, never per sample"
        );
        assert_eq!(update.samples.len(), 1);
        // The worker's fallback stamp filled the receive slot the fake backend left empty, and a
        // direct client's receive moment IS its capture moment.
        assert!(update.samples[0].server_ts.is_some());
        assert_eq!(script.closes(), 1, "the exit detaches the session");
    }

    #[tokio::test]
    async fn a_flushed_window_becomes_one_update_carrying_the_windows_readings() {
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(40, SignalRoute::default()));
        script.deliver(vec![reading("x-position", 1.0)]);
        script.deliver(vec![reading("x-position", 2.0)]);
        let d = Driven::start(&script);

        let wire = Arc::clone(&d.wire);
        wait_for("the window to flush", || !wire.published().is_empty()).await;
        d.finish().await;

        let samples = wire.samples_of("x-position");
        assert_eq!(
            wire.published().len(),
            1,
            "ONE update for the window, not one per reading"
        );
        assert_eq!(samples.len(), 2, "both readings, in arrival order");
        assert_eq!(samples[0].value, Some(json!(1.0)));
        assert_eq!(samples[1].value, Some(json!(2.0)));
    }

    #[tokio::test]
    async fn a_publish_failure_is_survived_rather_than_silently_counted() {
        let script = Arc::new(Script::default());
        script.deliver(vec![reading("x-position", 1.0)]);
        let d = Driven::start(&script);
        d.wire.publish_fails.store(true, Ordering::Relaxed);

        let s = Arc::clone(&script);
        wait_for("the read", || s.reads_done() > 1).await;
        d.finish().await;
        assert!(
            d.wire.published().is_empty(),
            "nothing reached the wire, and the loop kept running"
        );
    }

    // ---------------------------------------------------------------------------------------
    // Pause / resume — C-1: the resume inventory is read at RESUME time
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn resume_snapshots_the_live_inventory_including_signals_added_while_paused() {
        // C-1. The inventory used to be computed once per session, so a signal added by a live
        // reload during a pause was missing from the resume snapshot and stayed unpublished until
        // it happened to change on its own.
        let script = Arc::new(Script::default());
        script.set_inventory(&["x-position"]);
        script.set_named(vec![reading("x-position", 1.0)]);
        let d = Driven::start(&script);

        assert!(d.pause().await, "the instance was running");
        assert!(d.health.is_paused());

        // The reload lands while the instance is paused: a second signal now exists.
        script.set_inventory(&["x-position", "spindle-speed"]);
        script.set_named(vec![
            reading("x-position", 1.0),
            reading("spindle-speed", 900.0),
        ]);

        assert!(d.resume().await);
        let wire = Arc::clone(&d.wire);
        wait_for("the resume snapshot", || wire.signals().len() >= 2).await;
        d.finish().await;

        assert_eq!(
            script.named_calls().last().expect("a resume read"),
            &vec!["x-position".to_string(), "spindle-speed".to_string()],
            "the snapshot asked for the LIVE inventory, not the one frozen at connect"
        );
        assert!(
            wire.signals().contains(&"spindle-speed".to_string()),
            "the signal the reload added is in the resume snapshot: {:?}",
            wire.signals()
        );
        assert_eq!(
            wire.event_types(),
            vec!["adapter-paused".to_string(), "adapter-resumed".to_string()]
        );
    }

    #[tokio::test]
    async fn an_empty_inventory_resumes_without_a_snapshot() {
        let script = Arc::new(Script::default());
        let d = Driven::start(&script);
        assert!(d.pause().await);
        assert!(d.resume().await);
        assert!(!d.resume().await, "already running: nothing changed");
        d.finish().await;
        assert!(
            script.named_calls().is_empty(),
            "there was nothing to snapshot"
        );
    }

    #[tokio::test]
    async fn a_failed_resume_snapshot_lets_on_change_flow_resume_anyway() {
        let script = Arc::new(Script::default());
        script.set_inventory(&["x-position"]);
        *script.named.lock().unwrap() = Err("the agent stopped answering".to_string());
        let d = Driven::start(&script);

        assert!(d.pause().await);
        assert!(d.resume().await);
        let s = Arc::clone(&script);
        wait_for("the failed snapshot", || !s.named_calls().is_empty()).await;
        let exit = d.finish().await;

        assert!(
            matches!(exit, PollExit::Closed),
            "a failed resume snapshot is not a link failure"
        );
        assert!(d.wire.published().is_empty());
    }

    #[tokio::test]
    async fn pause_gates_the_wire_and_discards_the_open_windows() {
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(60_000, SignalRoute::default()));
        script.deliver(vec![reading("x-position", 1.0)]);
        let d = Driven::start(&script);

        // Let the reading enter the (long) window, then pause.
        let s = Arc::clone(&script);
        wait_for("the buffering read", || s.reads_done() > 1).await;
        assert!(d.pause().await);

        // Readings keep being drained while paused — the backend's caches stay current — but
        // nothing may reach the wire.
        script.deliver(vec![reading("x-position", 2.0)]);
        wait_for("a paused read", || s.reads_done() > 3).await;
        let exit = d.finish().await;

        assert!(matches!(exit, PollExit::Closed));
        assert!(
            d.wire.published().is_empty(),
            "the pre-pause window was CLEARED, not flushed after the pause: {:?}",
            d.wire.signals()
        );
    }

    #[tokio::test]
    async fn a_repoll_publishes_a_fresh_snapshot_and_is_refused_while_paused() {
        let script = Arc::new(Script::default());
        script.set_named(vec![
            reading("x-position", 7.0),
            reading("spindle-speed", 1.0),
        ]);
        let d = Driven::start(&script);

        assert_eq!(d.repoll().await, Ok(2), "both readings published, now");
        assert_eq!(script.snapshot_calls.load(Ordering::Relaxed), 1);

        assert!(d.pause().await);
        assert_eq!(
            d.repoll().await,
            Err("instance is paused - resume first".to_string())
        );
        assert_eq!(
            script.snapshot_calls.load(Ordering::Relaxed),
            1,
            "a refused repoll never touches the device"
        );
        d.finish().await;
    }

    #[tokio::test]
    async fn a_failed_repoll_flushes_says_bad_and_ends_the_session() {
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(60_000, SignalRoute::default()));
        script.set_passive(link(false, 10));
        script.deliver(vec![reading("x-position", 1.0)]);
        *script.named.lock().unwrap() = Err("the agent stopped answering".to_string());
        let d = Driven::start(&script);

        let s = Arc::clone(&script);
        wait_for("the buffering read", || s.reads_done() > 1).await;
        assert!(d.repoll().await.is_err());

        let exit = d.join().await;
        assert!(matches!(exit, PollExit::LinkLost));
        assert_eq!(
            d.wire.signals(),
            vec!["x-position".to_string(), "x-position".to_string()],
            "the open window was flushed, then the held value republished as BAD"
        );
        let last = d.wire.samples_of("x-position").pop().expect("a sample");
        assert_eq!(last.quality, Some(edgecommons::facades::Quality::Bad));
        assert_eq!(
            last.extra.as_ref().and_then(|e| e.get(PASSIVE_EXTRA_KEY)),
            Some(&json!("unreachable"))
        );
        assert_eq!(script.closes(), 1);
        assert_eq!(d.health.read_errors.load(Ordering::Relaxed), 1);
    }

    // ---------------------------------------------------------------------------------------
    // Link loss, reconnect, cancellation
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_broken_read_flushes_the_window_degrades_the_held_values_and_detaches() {
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(60_000, SignalRoute::default()));
        script.set_passive(link(false, 10));
        script.deliver(vec![reading("x-position", 42.0)]);
        script.break_link();
        let d = Driven::start(&script);

        let exit = d.join().await;
        assert!(matches!(exit, PollExit::LinkLost));
        // The buffered reading is data and was flushed; then the fleet was told the held value is
        // no longer trustworthy, BEFORE the session died.
        let samples = d.wire.samples_of("x-position");
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].value, Some(json!(42.0)));
        assert_eq!(
            samples[0].quality,
            Some(edgecommons::facades::Quality::Good)
        );
        assert_eq!(samples[1].value, Some(json!(42.0)), "the HELD value");
        assert_eq!(samples[1].quality, Some(edgecommons::facades::Quality::Bad));
        assert_eq!(script.closes(), 1);
    }

    #[tokio::test]
    async fn a_reconnect_command_flushes_the_open_window_before_dropping_the_session() {
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(60_000, SignalRoute::default()));
        script.deliver(vec![reading("x-position", 5.0)]);
        let d = Driven::start(&script);

        let s = Arc::clone(&script);
        wait_for("the buffering read", || s.reads_done() > 1).await;
        let (tx, _rx) = oneshot::channel();
        d.send(DeviceControl::Reconnect { reply: tx }).await;

        let exit = d.join().await;
        assert!(matches!(exit, PollExit::Reconnect(_)));
        assert_eq!(
            d.wire.samples_of("x-position").len(),
            1,
            "the window's reading was published, not dropped with the session"
        );
        assert_eq!(script.closes(), 1);
    }

    #[tokio::test]
    async fn cancellation_flushes_the_open_window_before_the_session_detaches() {
        // Phase 4's deferred device-task test. A SIGTERM lands between two awaits; buffered
        // readings are data, so they are published while the facade is still alive and only then
        // is the session detached from the shared agent runtime.
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(60_000, SignalRoute::default()));
        script.deliver(vec![reading("x-position", 11.0)]);
        script.deliver(vec![reading("x-position", 12.0)]);
        let d = Driven::start(&script);

        let s = Arc::clone(&script);
        wait_for("both readings buffered", || s.reads_done() > 2).await;
        assert!(
            d.wire.published().is_empty(),
            "still inside the batch window"
        );

        d.cancel.cancel();
        let exit = d.join().await;
        assert!(matches!(exit, PollExit::Closed));

        let samples = d.wire.samples_of("x-position");
        assert_eq!(
            samples.len(),
            2,
            "the whole open window reached the wire on the way out"
        );
        assert_eq!(samples[0].value, Some(json!(11.0)));
        assert_eq!(samples[1].value, Some(json!(12.0)));
        assert_eq!(script.closes(), 1, "and only then did it detach");
    }

    #[tokio::test]
    async fn the_control_channel_closing_stops_the_loop_and_flushes() {
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(60_000, SignalRoute::default()));
        script.deliver(vec![reading("x-position", 3.0)]);
        let d = Driven::start(&script);

        let s = Arc::clone(&script);
        wait_for("the buffering read", || s.reads_done() > 1).await;
        let exit = d.finish().await;
        assert!(matches!(exit, PollExit::Closed));
        assert_eq!(
            d.wire.samples_of("x-position").len(),
            1,
            "no exit may lose what a window was still coalescing"
        );
        assert_eq!(script.closes(), 1);
    }

    // ---------------------------------------------------------------------------------------
    // Generation safety (Phase 3's deferred driver test) and passive quality (Phase 6's)
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_shaping_generation_swap_flushes_the_open_window_on_the_old_route() {
        // Phase 3's deferred mixed-generation regression (P1-8/D-R16), at the level that actually
        // composes it. A reload changes only the signal's ROUTE; because routing is part of the
        // policy identity, the open window flushes with its OLD readings on their OLD channel, and
        // post-swap readings open a fresh window on the new one. One update never mixes routes.
        let route_a = SignalRoute {
            channel: Some("axes/x/position".into()),
            component_path: Some("Axes/Linear[X]".into()),
            name: Some("X position".into()),
        };
        let route_b = SignalRoute {
            channel: Some("axes/x-axis/position".into()),
            component_path: Some("Axes/Linear[XAxis]".into()),
            name: Some("X position".into()),
        };
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(60_000, route_a.clone()));
        script.deliver(vec![crate::device::Reading {
            channel: route_a.channel.clone(),
            component_path: route_a.component_path.clone(),
            ..reading("x-position", 1.0)
        }]);
        let d = Driven::start(&script);

        let s = Arc::clone(&script);
        wait_for("the old-route reading to buffer", || s.reads_done() > 1).await;
        assert!(d.wire.published().is_empty(), "still buffering on route A");

        // The reload: same batching, new route — and one reading that arrives under it.
        script.set_policies(batching(60_000, route_b.clone()));
        *script.generation.lock().unwrap() = Some("gen-2".to_string());
        script.deliver(vec![crate::device::Reading {
            channel: route_b.channel.clone(),
            component_path: route_b.component_path.clone(),
            ..reading("x-position", 2.0)
        }]);

        let wire = Arc::clone(&d.wire);
        wait_for("the old window to flush on the swap", || {
            !wire.published().is_empty()
        })
        .await;
        d.cancel.cancel();
        d.join().await;

        let published = wire.published();
        assert_eq!(
            published.len(),
            2,
            "two updates: the old window on route A, then the new window on route B"
        );
        assert_eq!(
            published[0].0.signal_path.as_deref(),
            route_a.channel.as_deref()
        );
        assert_eq!(published[0].1.as_deref(), route_a.component_path.as_deref());
        assert_eq!(published[0].0.samples.len(), 1);
        assert_eq!(published[0].0.samples[0].value, Some(json!(1.0)));

        assert_eq!(
            published[1].0.signal_path.as_deref(),
            route_b.channel.as_deref()
        );
        assert_eq!(published[1].1.as_deref(), route_b.component_path.as_deref());
        assert_eq!(published[1].0.samples.len(), 1);
        assert_eq!(published[1].0.samples[0].value, Some(json!(2.0)));
    }

    #[tokio::test]
    async fn a_resync_re_baselines_the_deadband() {
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(HashMap::from([(
            "x-position".to_string(),
            PublishPolicy {
                batch_ms: 0,
                latest_only: false,
                deadband: Some(5.0),
                route: SignalRoute::default(),
            },
        )]));
        script.deliver(vec![reading("x-position", 10.0)]);
        // Inside the deadband, and normally suppressed...
        script.deliver(vec![reading("x-position", 11.0)]);
        let d = Driven::start(&script);

        let wire = Arc::clone(&d.wire);
        wait_for("the first publish", || !wire.published().is_empty()).await;
        let s = Arc::clone(&script);
        wait_for("the suppressed reading", || s.reads_done() > 2).await;
        assert_eq!(wire.published().len(), 1, "11.0 was inside the deadband");

        // ...until a re-baseline says the fleet's view was superseded, and the next reading of
        // every signal must pass as fresh.
        *script.resync.lock().unwrap() = true;
        script.deliver(vec![reading("x-position", 11.0)]);
        wait_for("the post-resync publish", || wire.published().len() == 2).await;
        d.finish().await;
        assert_eq!(
            wire.samples_of("x-position")[1].value,
            Some(json!(11.0)),
            "the first reading after a resync always passes the deadband"
        );
    }

    #[tokio::test]
    async fn the_passive_quality_transitions_reach_the_wire_from_the_tick() {
        // Phase 6's deferred driver test: deleting the `publish_passive` call from the tick arm
        // must break something. The link stops vouching for currency while the machine is simply
        // not changing — the held value is republished UNCERTAIN, then BAD, without any new read.
        let script = Arc::new(Script::default());
        script.set_passive(link(false, 0));
        script.deliver(vec![crate::device::Reading {
            component_path: Some("Axes/Linear[X]".into()),
            ..reading("x-position", 123.5).with_extra("sequence", json!(37))
        }]);
        let d = Driven::start(&script);

        let wire = Arc::clone(&d.wire);
        wait_for("the delivered reading", || !wire.published().is_empty()).await;

        // One missed heartbeat: the value is held, and the adapter stops vouching for it.
        script.set_passive(link(false, 12_000));
        wait_for("the stale transition", || wire.published().len() == 2).await;

        // Past `staleSignalSecs`, the same held value is BAD.
        script.set_passive(link(false, 45_000));
        wait_for("the expiry transition", || wire.published().len() == 3).await;
        d.finish().await;

        let samples = wire.samples_of("x-position");
        assert_eq!(
            samples[0].quality,
            Some(edgecommons::facades::Quality::Good)
        );

        let stale = &samples[1];
        assert_eq!(stale.value, Some(json!(123.5)), "the HELD value");
        assert_eq!(
            stale.quality,
            Some(edgecommons::facades::Quality::Uncertain)
        );
        assert_eq!(stale.quality_raw.as_deref(), Some("MTC_STALE:12000"));
        let extra = stale.extra.as_ref().expect("the marker extras");
        assert_eq!(extra[PASSIVE_EXTRA_KEY], json!("stale"));
        assert_eq!(extra["sequence"], json!(37), "the held sequence survives");

        let expired = &samples[2];
        assert_eq!(expired.value, Some(json!(123.5)));
        assert_eq!(expired.quality, Some(edgecommons::facades::Quality::Bad));
        assert_eq!(expired.quality_raw.as_deref(), Some("MTC_STALE:45000"));
        assert_eq!(
            expired.extra.as_ref().expect("extras")[PASSIVE_EXTRA_KEY],
            json!("expired")
        );
        assert_eq!(
            wire.published()[2].1.as_deref(),
            Some("Axes/Linear[X]"),
            "a synthetic update still carries the signal's canonical path"
        );
    }

    #[tokio::test]
    async fn a_synthetic_transition_bypasses_an_open_batch_window() {
        // A quality change is news, not a value in a window: it must not sit behind a 60 s batch.
        let script = Arc::new(Script::default());
        *script.generation.lock().unwrap() = Some("gen-1".to_string());
        script.set_policies(batching(60_000, SignalRoute::default()));
        script.set_passive(link(true, 0));
        // Published outside the shaper (a repoll), so the watchdog holds it while the window that
        // follows stays open.
        script.set_named(vec![reading("x-position", 8.0)]);
        let d = Driven::start(&script);
        assert_eq!(d.repoll().await, Ok(1));

        script.deliver(vec![reading("x-position", 9.0)]);
        let wire = Arc::clone(&d.wire);
        wait_for("the unreachable transition", || wire.published().len() >= 2).await;
        d.finish().await;

        let samples = wire.samples_of("x-position");
        assert_eq!(
            samples[1].value,
            Some(json!(8.0)),
            "the HELD value, not 9.0"
        );
        assert_eq!(samples[1].quality, Some(edgecommons::facades::Quality::Bad));
        assert_eq!(
            samples[1].quality_raw.as_deref(),
            Some(crate::staleness::QUALITY_AGENT_UNREACHABLE)
        );
    }

    // ---------------------------------------------------------------------------------------
    // Notices, served signals, the control verbs
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn protocol_notices_become_events_at_their_own_severity() {
        let script = Arc::new(Script::default());
        *script.notices.lock().unwrap() = vec![
            Notice {
                event_type: crate::device::EVENT_AGENT,
                level: NoticeLevel::Critical,
                message: "agent unreachable".to_string(),
                context: json!({ "state": "down" }),
            },
            Notice {
                event_type: crate::device::EVENT_MODEL_DRIFT,
                level: NoticeLevel::Warning,
                message: "the model changed".to_string(),
                context: json!({}),
            },
            Notice {
                event_type: crate::device::EVENT_SIGNAL_SET,
                level: NoticeLevel::Info,
                message: "the served set changed".to_string(),
                context: json!({}),
            },
        ];
        *script.served.lock().unwrap() = Some(17);
        let d = Driven::start(&script);

        let wire = Arc::clone(&d.wire);
        wait_for("the notices", || wire.events().len() == 3).await;
        d.finish().await;

        let events = wire.events();
        assert_eq!(events[0].severity, Severity::Critical);
        assert_eq!(events[0].event_type, crate::device::EVENT_AGENT);
        assert_eq!(events[0].context, Some(json!({ "state": "down" })));
        assert_eq!(events[1].severity, Severity::Warning);
        assert_eq!(events[2].severity, Severity::Info);
        assert_eq!(
            d.health.signals_subscribed(),
            0,
            "the gauge reports only while ONLINE"
        );
    }

    #[tokio::test]
    async fn write_read_and_browse_are_served_on_the_poll_task() {
        let script = Arc::new(Script::default());
        script.set_named(vec![reading("x-position", 4.0)]);
        let d = Driven::start(&script);

        let (tx, rx) = oneshot::channel();
        d.send(DeviceControl::Write(crate::app::WriteRequest {
            signal_id: "setpoint-1".to_string(),
            value: json!(5),
            ack: tx,
        }))
        .await;
        assert_eq!(rx.await.unwrap(), Ok(()));

        let (tx, rx) = oneshot::channel();
        d.send(DeviceControl::Write(crate::app::WriteRequest {
            signal_id: "refused".to_string(),
            value: json!(5),
            ack: tx,
        }))
        .await;
        assert!(rx.await.unwrap().is_err(), "a refused write answers so");

        let (tx, rx) = oneshot::channel();
        d.send(DeviceControl::ReadNow {
            ids: vec!["x-position".to_string()],
            reply: tx,
        })
        .await;
        assert_eq!(rx.await.unwrap().unwrap().len(), 1);

        let (tx, rx) = oneshot::channel();
        d.send(DeviceControl::Browse {
            cursor: None,
            max: 10,
            reply: tx,
        })
        .await;
        assert!(rx.await.unwrap().is_ok());
        d.finish().await;

        assert_eq!(
            script.writes.lock().unwrap().as_slice(),
            &[
                ("setpoint-1".to_string(), json!(5)),
                ("refused".to_string(), json!(5))
            ]
        );
        assert_eq!(script.browse_calls.load(Ordering::Relaxed), 1);
    }

    // ---------------------------------------------------------------------------------------
    // serve_while_down — the control surface while the link is down
    // ---------------------------------------------------------------------------------------

    /// Drive `serve_while_down` over a pre-loaded control channel.
    async fn while_down(
        messages: Vec<DeviceControl>,
        drop_sender: bool,
        wait: Duration,
        cancel: &CancellationToken,
    ) -> (DownOutcome, Arc<RecordingWire>, Arc<Health>) {
        let (tx, mut rx) = mpsc::channel::<DeviceControl>(16);
        for message in messages {
            tx.send(message).await.ok();
        }
        if drop_sender {
            drop(tx);
        }
        let wire = Arc::new(RecordingWire::default());
        let health = Arc::new(Health::default());
        let outcome = serve_while_down(&mut rx, wire.as_ref(), &health, wait, cancel).await;
        (outcome, wire, health)
    }

    #[tokio::test]
    async fn the_backoff_window_elapses_when_nobody_asks_for_anything() {
        let (outcome, _, _) = while_down(
            Vec::new(),
            false,
            Duration::from_millis(20),
            &CancellationToken::new(),
        )
        .await;
        assert!(matches!(outcome, DownOutcome::Elapsed));

        // A zero window is already over.
        let (outcome, _, _) =
            while_down(Vec::new(), false, Duration::ZERO, &CancellationToken::new()).await;
        assert!(matches!(outcome, DownOutcome::Elapsed));
    }

    #[tokio::test]
    async fn pause_and_resume_take_effect_while_the_link_is_down() {
        let (pause_tx, pause_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        let (outcome, wire, health) = while_down(
            vec![
                DeviceControl::Pause { reply: pause_tx },
                DeviceControl::Resume { reply: resume_tx },
            ],
            true,
            Duration::from_secs(30),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(outcome, DownOutcome::Closed));
        assert!(pause_rx.await.unwrap(), "pausing changed the state");
        assert!(resume_rx.await.unwrap());
        assert!(!health.is_paused());
        assert_eq!(
            wire.event_types(),
            vec!["adapter-paused".to_string(), "adapter-resumed".to_string()]
        );
    }

    #[tokio::test]
    async fn the_io_verbs_answer_disconnected_while_the_link_is_down() {
        let (write_tx, write_rx) = oneshot::channel();
        let (read_tx, read_rx) = oneshot::channel();
        let (repoll_tx, repoll_rx) = oneshot::channel();
        let (browse_tx, browse_rx) = oneshot::channel();
        let (outcome, _, _) = while_down(
            vec![
                DeviceControl::Write(crate::app::WriteRequest {
                    signal_id: "setpoint-1".to_string(),
                    value: json!(1),
                    ack: write_tx,
                }),
                DeviceControl::ReadNow {
                    ids: vec!["x".to_string()],
                    reply: read_tx,
                },
                DeviceControl::Repoll { reply: repoll_tx },
                DeviceControl::Browse {
                    cursor: None,
                    max: 1,
                    reply: browse_tx,
                },
            ],
            true,
            Duration::from_secs(30),
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(outcome, DownOutcome::Closed));
        assert_eq!(
            write_rx.await.unwrap(),
            Err("device is disconnected".into())
        );
        assert_eq!(read_rx.await.unwrap(), Err("device is disconnected".into()));
        assert_eq!(
            repoll_rx.await.unwrap(),
            Err("device is disconnected".into())
        );
        assert!(matches!(
            browse_rx.await.unwrap(),
            Err(BrowseError::Failed(_))
        ));
    }

    #[tokio::test]
    async fn a_reconnect_cuts_the_backoff_short_and_a_cancellation_ends_it_at_once() {
        let (tx, _rx) = oneshot::channel();
        let (outcome, _, _) = while_down(
            vec![DeviceControl::Reconnect { reply: tx }],
            false,
            Duration::from_secs(30),
            &CancellationToken::new(),
        )
        .await;
        assert!(matches!(outcome, DownOutcome::Reconnect(_)));

        // A backoff window can be minutes long; shutdown does not wait for it.
        let cancel = CancellationToken::new();
        cancel.cancel();
        let started = Instant::now();
        let (outcome, _, _) =
            while_down(Vec::new(), false, Duration::from_secs(600), &cancel).await;
        assert!(matches!(outcome, DownOutcome::Closed));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    // ---------------------------------------------------------------------------------------
    // run_device — the connect loop
    // ---------------------------------------------------------------------------------------

    /// Start `run_device` over the fake backend and hand back what steers and observes it.
    fn start_device(
        script: &Arc<Script>,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Sender<DeviceControl>,
        Arc<RecordingWire>,
        Arc<Health>,
        CancellationToken,
    ) {
        let wire = Arc::new(RecordingWire::default());
        let health = Arc::new(Health::default());
        let dm = metrics(&health);
        let cancel = CancellationToken::new();
        let (control, control_rx) = mpsc::channel::<DeviceControl>(16);
        let backend: Arc<dyn DeviceBackend> = Arc::new(FakeBackend(Arc::clone(script)));
        let task = tokio::spawn(run_device(
            device_cfg(),
            backend,
            Arc::clone(&wire) as Arc<dyn Wire>,
            dm,
            Arc::clone(&health),
            control_rx,
            30,
            cancel.clone(),
        ));
        (task, control, wire, health, cancel)
    }

    #[tokio::test]
    async fn a_connect_publish_link_loss_cycle_announces_itself_on_the_event_surface() {
        let script = Arc::new(Script::default());
        script.deliver(vec![reading("x-position", 1.0)]);
        script.break_link();
        // The first connect succeeds; every reconnect attempt after the link loss is refused, so
        // the task is still alive to be cancelled.
        script.connects.lock().unwrap().push_back(Ok(()));
        for _ in 0..64 {
            script
                .connects
                .lock()
                .unwrap()
                .push_back(Err((true, "refused".to_string())));
        }

        let (task, control, wire, health, cancel) = start_device(&script);
        let w = Arc::clone(&wire);
        wait_for("the unreachable alarm", || {
            w.events()
                .iter()
                .any(|e| e.event_type == "device-unreachable" && e.alarm == Some(true))
        })
        .await;
        cancel.cancel();
        task.await.expect("the device task returns");
        drop(control);

        assert_eq!(
            wire.event_types(),
            vec![
                "device-connected".to_string(),
                "device-unreachable".to_string(), // the clear that accompanies a connect
                "device-unreachable".to_string(), // the raise, when the link went
            ]
        );
        assert_eq!(
            wire.events()[1].alarm,
            Some(false),
            "connecting CLEARS the alarm"
        );
        assert!(
            !wire.published().is_empty(),
            "the good read reached the wire"
        );
        assert_ne!(
            health.link(),
            LinkState::Online,
            "a lost link is never reported ONLINE, whichever reconnect state it is in"
        );
        assert_eq!(health.connection_state.load(Ordering::Relaxed), 0);
        assert_eq!(script.closes(), 1, "the dead session was detached");
        assert!(
            script.connect_calls.load(Ordering::Relaxed) >= 2,
            "and the connect loop went round again"
        );
    }

    #[tokio::test]
    async fn a_cancelled_connect_returns_at_once_and_attempts_nothing() {
        let script = Arc::new(Script::default());
        let wire = Arc::new(RecordingWire::default());
        let health = Arc::new(Health::default());
        let dm = metrics(&health);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (_control, control_rx) = mpsc::channel::<DeviceControl>(16);
        let backend: Arc<dyn DeviceBackend> = Arc::new(FakeBackend(Arc::clone(&script)));

        let started = Instant::now();
        run_device(
            device_cfg(),
            backend,
            Arc::clone(&wire) as Arc<dyn Wire>,
            dm,
            Arc::clone(&health),
            control_rx,
            30,
            cancel,
        )
        .await;

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(wire.events().is_empty());
        assert!(wire.published().is_empty());
        assert_eq!(
            script.connect_calls.load(Ordering::Relaxed),
            0,
            "nothing was buffered, and nothing was attempted"
        );
    }

    #[tokio::test]
    async fn a_permanent_connect_failure_backs_off_hard_and_a_reconnect_cuts_it_short() {
        let script = Arc::new(Script::default());
        script
            .connects
            .lock()
            .unwrap()
            .push_back(Err((false, "bad credentials".to_string())));

        let (task, control, _wire, health, cancel) = start_device(&script);
        let s = Arc::clone(&script);
        wait_for("the first, failing connect", || {
            s.connect_calls.load(Ordering::Relaxed) >= 1
        })
        .await;
        assert_eq!(health.link(), LinkState::Connecting);

        // The ceiling wait is a minute; a `reconnect` cuts it short and is settled by the connect
        // that follows.
        let (tx, rx) = oneshot::channel();
        control
            .send(DeviceControl::Reconnect { reply: tx })
            .await
            .ok();
        assert_eq!(rx.await.expect("the reconnect is settled"), Ok(()));
        assert_eq!(health.link(), LinkState::Online);

        cancel.cancel();
        task.await.expect("the device task returns");
    }

    #[tokio::test]
    async fn a_shutdown_during_the_backoff_wait_ends_the_device_task() {
        let script = Arc::new(Script::default());
        for _ in 0..8 {
            script
                .connects
                .lock()
                .unwrap()
                .push_back(Err((true, "refused".to_string())));
        }
        let (task, _control, wire, _health, cancel) = start_device(&script);
        let s = Arc::clone(&script);
        wait_for("the failing connect", || {
            s.connect_calls.load(Ordering::Relaxed) >= 1
        })
        .await;
        cancel.cancel();
        task.await.expect("the device task returns");
        assert!(
            wire.published().is_empty(),
            "nothing is buffered while the link is down"
        );
    }

    // ---------------------------------------------------------------------------------------
    // The small pure pieces
    // ---------------------------------------------------------------------------------------

    #[test]
    fn a_lost_link_is_unreachable_whatever_the_authority_currently_reads() {
        assert_eq!(lost_link(None), None);
        let live = link(false, 1_000);
        let lost = lost_link(Some(live)).expect("the same link, degraded");
        assert!(
            lost.unreachable,
            "the read that just failed IS the evidence"
        );
        assert_eq!(lost.liveness_age, live.liveness_age);
        assert_eq!(lost.liveness_window, live.liveness_window);
    }

    #[test]
    fn a_notice_level_maps_onto_one_library_severity() {
        assert_eq!(severity_of(NoticeLevel::Info), Severity::Info);
        assert_eq!(severity_of(NoticeLevel::Warning), Severity::Warning);
        assert_eq!(severity_of(NoticeLevel::Critical), Severity::Critical);
    }

    #[test]
    fn the_served_gauge_follows_a_session_that_knows_better_than_the_configuration() {
        let health = Arc::new(Health::default());
        health.set_link(LinkState::Online);
        health.set_signal_inventory(4);

        let script = Arc::new(Script::default());
        let session = FakeSession(Arc::clone(&script));
        sync_served_signals(&session, &health);
        assert_eq!(
            health.signals_subscribed(),
            4,
            "a backend with no compile step keeps the configured size"
        );

        *script.served.lock().unwrap() = Some(2);
        sync_served_signals(&session, &health);
        assert_eq!(health.signals_subscribed(), 2);
    }

    #[tokio::test]
    async fn a_paused_poll_still_reads_but_returns_nothing_to_publish() {
        let script = Arc::new(Script::default());
        script.deliver(vec![reading("x-position", 1.0)]);
        let health = Arc::new(Health::default());
        let mut session: Box<dyn DeviceSession> = Box::new(FakeSession(Arc::clone(&script)));
        let cfg = device_cfg();

        assert_eq!(
            poll_once(&cfg, &mut session, &health, false).await,
            Ok(None),
            "the drain ran; publication is gated"
        );
        assert_eq!(script.reads_done(), 1);

        // A broken read is the connection's problem, and is counted as one.
        script.break_link();
        assert_eq!(poll_once(&cfg, &mut session, &health, true).await, Err(()));
        assert_eq!(health.read_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_backoff_jitter_stays_inside_the_unit_interval() {
        for _ in 0..64 {
            let r = rand01();
            assert!((0.0..1.0).contains(&r), "{r} is not a jitter fraction");
        }
    }

    #[tokio::test]
    async fn a_deadline_of_none_never_fires() {
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sleep_until_deadline(None))
                .await
                .is_err(),
            "an absent deadline is a future that never completes"
        );
        sleep_until_deadline(Some(Instant::now())).await;
    }

    #[test]
    fn the_receive_stamp_is_an_iso_8601_instant() {
        let now = now_iso();
        assert!(now.ends_with('Z'), "{now} is not ISO-8601 UTC");
        assert!(now.len() >= 20, "{now} is too short to be a timestamp");
    }
}
