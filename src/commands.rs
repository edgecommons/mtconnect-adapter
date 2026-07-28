//! # The southbound command surface — the `sb/*` verbs + the three edge-console panels
//!
//! This module owns the whole `gg.commands()` registration for `MtconnectAdapter`: `sb/status`,
//! `sb/read`, `sb/write`, `sb/signals`, `sb/browse`, `sb/pause`, `sb/resume`, `reconnect`, `repoll`.
//! It is the generic southbound command family (SOUTHBOUND.md §2.2) every adapter serves — a real
//! adapter changes the *seam* behind it, not this surface.
//!
//! ## Conventions every verb follows
//!
//! * **Instance addressing — the library resolves it, this module only looks the device up.** Every
//!   verb declares [`CommandScope::Instance`]. Before a handler runs, the inbox resolves the
//!   delivery's addressing: the topic's instance token
//!   (`ecv1/{device}/{component}/{instance}/cmd/{verb}`) is authoritative, a body `instance` that
//!   disagrees with it is refused with `BAD_ARGS`, and the handler is handed the resolved
//!   `addressed_instance`. What is left here is the part that needs *this component's
//!   configuration*: an unknown id is `NO_SUCH_INSTANCE`, and `None` (a component-addressed
//!   delivery that named no instance) resolves to the sole configured device — with two or more it
//!   is `BAD_ARGS`.
//! * **Standardized error codes:** `BAD_ARGS`, `NO_SUCH_INSTANCE`, `WRITE_NOT_ALLOWED`,
//!   `WRITE_FAILED`, `DEVICE_UNAVAILABLE`, `READ_FAILED`, `RECONNECT_FAILED`, `BROWSE_UNSUPPORTED`,
//!   `BROWSE_FAILED`, `PAUSED`.
//! * **The session is never touched here.** Every verb that reads/writes/reconnects/pauses is sent
//!   to the device's own task as a [`DeviceControl`] and *confirmed* through the reply that rides it,
//!   because the session lives in that task and is not `Sync`.
//! * **`sb/write` allow-lists BEFORE any device I/O.** A refused entry never becomes a
//!   [`DeviceControl::Write`] — an adapter that writes whatever it is asked to is a control-system
//!   vulnerability, not a feature.
//! * Every verb records into the `MtconnectAdapterCommand` metric family (`instance`×`verb`×`result`).
//!
//! Three panels (`overview`, `signals`, `diagnostics`) are registered via `commands.register_panel`
//! for the edge-console descriptor surface — each `scope: "instance"`, `order` 10/20/30.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use edgecommons::messaging::Message;
use edgecommons::prelude::{command_handler, CommandError, CommandHandler, CommandInbox, CommandScope};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::app::{DeviceConfig, DeviceControl, Health, LinkState, WriteRequest};
use crate::device::{BrowseError, Quality, Reading, SignalInfo};
use crate::metrics::DeviceMetrics;

/// The per-device handles the command surface routes on: the config (routing, allow-list, inventory),
/// the control channel (session-touching verbs), the shared health (status/paused), and the metrics
/// emitter (per-verb command counters).
pub struct DeviceHandle {
    pub cfg: DeviceConfig,
    pub control: mpsc::Sender<DeviceControl>,
    pub health: Arc<Health>,
    pub dm: Arc<DeviceMetrics>,
    /// The signal inventory `sb/signals` returns — a config/backend view, no device round-trip.
    pub signals: Vec<SignalInfo>,
}

/// The verbs this module puts on the inbox — every one of them [`CommandScope::Instance`].
pub const VERBS: [&str; 9] = [
    "sb/status",
    "sb/read",
    "sb/write",
    "sb/signals",
    "sb/browse",
    "sb/pause",
    "sb/resume",
    "reconnect",
    "repoll",
];

/// Register every `sb/*` verb + the three edge-console panels on the inbox.
///
/// Every verb declares [`CommandScope::Instance`]: it acts on one device, so the inbox resolves the
/// delivery's addressing (topic token, then body `instance`, a conflict between them refused with
/// `BAD_ARGS`) and hands each handler the resolved `addressed_instance`. This module never reads
/// `body.instance`.
///
/// # Errors
/// Propagates [`CommandInbox::register`] / [`CommandInbox::register_panel`] failures (a verb/panel
/// name clash or an invalid token).
pub fn register_all(commands: &CommandInbox, handles: Vec<DeviceHandle>) -> anyhow::Result<()> {
    let commander = Arc::new(Commander::new(handles));
    for (verb, scope, handler) in registrations(&commander) {
        commands.register(verb, scope, handler)?;
    }
    for panel in panels() {
        commands.register_panel(panel)?;
    }
    Ok(())
}

/// The `(verb, scope, handler)` triples [`register_all`] installs, in [`VERBS`] order.
///
/// Every handler takes the `addressed_instance` the inbox resolved and passes it straight to the
/// commander — none of them reads `body.instance`.
fn registrations(
    commander: &Arc<Commander>,
) -> Vec<(&'static str, CommandScope, Arc<dyn CommandHandler>)> {
    macro_rules! verb {
        ($name:expr, $method:ident) => {{
            let c = Arc::clone(commander);
            (
                $name,
                CommandScope::Instance,
                command_handler(move |req: Message, addressed| {
                    let c = Arc::clone(&c);
                    async move { c.$method(addressed.as_deref(), &req.body).await }
                }),
            )
        }};
        ($name:expr, $method:ident, no_body) => {{
            let c = Arc::clone(commander);
            (
                $name,
                CommandScope::Instance,
                command_handler(move |_req: Message, addressed| {
                    let c = Arc::clone(&c);
                    async move { c.$method(addressed.as_deref()).await }
                }),
            )
        }};
    }

    // `sb/pause` additionally carries the requester identity path (the `by` field of the event).
    let pause = {
        let c = Arc::clone(commander);
        (
            "sb/pause",
            CommandScope::Instance,
            command_handler(move |req: Message, addressed| {
                let c = Arc::clone(&c);
                async move {
                    let by = req.identity.as_ref().map(|i| i.path().to_string());
                    c.pause(addressed.as_deref(), by).await
                }
            }),
        )
    };

    vec![
        verb!("sb/status", status, no_body),
        verb!("sb/read", read),
        verb!("sb/write", write),
        verb!("sb/signals", signals, no_body),
        verb!("sb/browse", browse),
        pause,
        verb!("sb/resume", resume, no_body),
        verb!("reconnect", reconnect, no_body),
        verb!("repoll", repoll, no_body),
    ]
}

/// The three edge-console panel descriptors. Core validates `id`/`title`/uniqueness; the widget kinds
/// and bound verbs are console-interpreted, so they ride verbatim. `order` 10/20/30,
/// `scope: "instance"` — repeated on each command-backed widget, which the console renderer requires.
/// No widget names a `writeVerb`: writes stay on the command surface behind the allow-list.
#[must_use]
pub fn panels() -> Vec<Value> {
    vec![
        json!({
            "id": "overview", "title": "Overview", "order": 10, "scope": "instance",
            "widgets": [
                {
                    "kind": "summary", "id": "overview-summary", "title": "Adapter overview",
                    "rows": [
                        { "label": "Signals", "value": "Configured signal inventory via cmd/sb/signals" },
                        { "label": "Lifecycle", "value": "Pause, resume, reconnect, and repoll the instance" },
                        { "label": "Writes", "value": "Allow-listed via writes.allow[]; checked before device I/O" }
                    ]
                },
                {
                    "kind": "commandSummary", "id": "overview-lifecycle", "title": "Lifecycle bindings",
                    "verbs": ["sb/status", "reconnect", "sb/pause", "sb/resume", "repoll"]
                }
            ],
            "verbs": ["sb/status", "reconnect", "sb/pause", "sb/resume"]
        }),
        // Descriptor-compat hint: the shipped edge-console signalGrid reads `subscriptionsVerb`
        // (falling back to the removed `sb/subscriptions`). Point that key at the `sb/signals` verb
        // too, so the current console binds correctly until it reads `signalsVerb`. This is a
        // descriptor field alias, NOT a wire-verb alias — no `sb/subscriptions` verb exists.
        json!({
            "id": "signals", "title": "Signals", "order": 20, "scope": "instance",
            "widgets": [
                {
                    "kind": "signalGrid", "id": "configured-signals", "title": "Configured signals",
                    "scope": "instance",
                    "signalsVerb": "sb/signals",
                    "subscriptionsVerb": "sb/signals",
                    "readVerb": "sb/read"
                }
            ],
            "verbs": ["sb/signals", "sb/read", "sb/write", "repoll"]
        }),
        json!({
            "id": "diagnostics", "title": "Diagnostics", "order": 30, "scope": "instance",
            "widgets": [
                {
                    "kind": "treeBrowser", "id": "inventory-tree", "title": "Inventory",
                    "scope": "instance", "mode": "hierarchical", "rootRef": "root",
                    "depth": 1, "maxRefs": 200,
                    "browseVerb": "sb/browse", "readVerb": "sb/read"
                },
                {
                    "kind": "commandSummary", "id": "diagnostic-commands", "title": "Diagnostic commands",
                    "verbs": ["sb/status", "sb/browse"]
                }
            ],
            "verbs": ["sb/browse", "sb/status"]
        }),
    ]
}

/// The command dispatcher: owns the per-device handles + the config order (for the single-instance
/// default).
struct Commander {
    devices: HashMap<String, DeviceHandle>,
    ids: Vec<String>,
}

type Reply = std::result::Result<Option<Value>, CommandError>;

impl Commander {
    fn new(handles: Vec<DeviceHandle>) -> Self {
        let ids: Vec<String> = handles.iter().map(|h| h.cfg.id.clone()).collect();
        let devices = handles.into_iter().map(|h| (h.cfg.id.clone(), h)).collect();
        Self { devices, ids }
    }

    /// Map the library-resolved `addressed_instance` onto a configured device.
    ///
    /// The addressing itself (topic token vs body `instance`, and the conflict between them) was
    /// settled by the inbox before this ran. Only the configuration-dependent half lives here: an
    /// instance this component does not have is `NO_SUCH_INSTANCE`, and `None` — a
    /// component-addressed delivery that named no instance — resolves to the sole configured
    /// device, or is `BAD_ARGS` when there are two or more.
    fn resolve(&self, instance: Option<&str>) -> std::result::Result<&DeviceHandle, CommandError> {
        match instance {
            Some(id) => self
                .devices
                .get(id)
                .ok_or_else(|| CommandError::new("NO_SUCH_INSTANCE", format!("no configured device `{id}`"))),
            None => {
                if self.ids.len() == 1 {
                    Ok(self.devices.get(&self.ids[0]).expect("one device"))
                } else {
                    Err(CommandError::new(
                        "BAD_ARGS",
                        "field `instance` is required when multiple devices are configured",
                    ))
                }
            }
        }
    }

    // --- sb/status ---------------------------------------------------------------------------------

    async fn status(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let link = h.health.link();
        let connected = link == LinkState::Online;
        let paused = h.health.is_paused();
        let state = if paused && connected { "PAUSED" } else { link.as_str() };
        let out = json!({
            "id": h.cfg.id,
            "adapter": h.cfg.adapter,
            "connected": connected,
            "state": state,
            "paused": paused,
            "endpoint": h.cfg.connection.endpoint,
            "metrics": h.dm.counters_view(),
        });
        h.dm.record_command("sb/status", true, ms(started));
        Ok(Some(out))
    }

    // --- sb/signals (the configured inventory, no device I/O) --------------------------------------

    async fn signals(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let signals: Vec<Value> = h
            .signals
            .iter()
            .map(|s| {
                json!({
                    "id": s.id,
                    "name": s.name,
                    "writable": h.cfg.writes.permits(&s.id),
                })
            })
            .collect();
        h.dm.record_command("sb/signals", true, ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "signals": signals })))
    }

    // --- sb/read (on-demand read of named signals) ------------------------------------------------

    async fn read(&self, instance: Option<&str>, body: &Value) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let refs = body
            .get("signals")
            .and_then(Value::as_array)
            .ok_or_else(|| CommandError::new("BAD_ARGS", "expected a `signals` array"))?;

        // Resolve each ref to a stable id (keeping the request order for the reply).
        let plan: Vec<std::result::Result<String, String>> =
            refs.iter().map(|r| self.resolve_ref(h, r)).collect();
        let ids: Vec<String> = plan.iter().filter_map(|r| r.clone().ok()).collect();

        let readings: HashMap<String, Reading> = if ids.is_empty() {
            HashMap::new()
        } else {
            let (tx, rx) = oneshot::channel();
            h.control
                .send(DeviceControl::ReadNow { ids, reply: tx })
                .await
                .map_err(|_| device_unavailable())?;
            match rx.await {
                Ok(Ok(rs)) => rs.into_iter().map(|r| (r.signal_id.clone(), r)).collect(),
                Ok(Err(e)) => {
                    h.dm.record_command("sb/read", false, ms(started));
                    return Err(CommandError::new("READ_FAILED", e));
                }
                Err(_) => {
                    h.dm.record_command("sb/read", false, ms(started));
                    return Err(device_unavailable());
                }
            }
        };

        let reads: Vec<Value> = plan
            .into_iter()
            .map(|entry| match entry {
                Ok(id) => match readings.get(&id) {
                    Some(r) => json!({
                        "signal": { "id": id },
                        "value": r.value,
                        "quality": quality_str(r.quality),
                        "qualityRaw": r.quality_raw,
                    }),
                    None => bad_read(&id, "NO_DATA"),
                },
                Err(label) => bad_read(&label, "UNRESOLVED_REF"),
            })
            .collect();

        h.dm.record_command("sb/read", true, ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "reads": reads })))
    }

    // --- sb/write (§2.2 batch shape; allow-list BEFORE any device I/O; confirmed) ------------------

    async fn write(&self, instance: Option<&str>, body: &Value) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let entries = write_entries(body)?;

        let mut results = Vec::with_capacity(entries.len());
        let mut refused = 0usize;
        let mut attempted = 0usize;
        let mut succeeded = 0usize;

        for entry in &entries {
            let id = match self.resolve_ref(h, entry) {
                Ok(id) => id,
                Err(label) => {
                    results.push(json!({ "signal": label, "ok": false, "error": "unresolved ref" }));
                    continue;
                }
            };
            // THE ALLOW-LIST — checked here, BEFORE the write ever reaches the device.
            if !h.cfg.writes.permits(&id) {
                refused += 1;
                results.push(json!({ "signal": id, "ok": false, "error": "not in writes.allow" }));
                continue;
            }
            let Some(value) = entry.get("value").cloned() else {
                results.push(json!({ "signal": id, "ok": false, "error": "missing value" }));
                continue;
            };

            // An entry that fails on the DEVICE PATH — rejected by the device, or aborted because
            // the device task became unavailable — feeds `southbound_health.writeErrors` (drained
            // on emit, exactly like `readErrors`). Entries that never reach the device (unresolved
            // refs, allow-list refusals, missing values) are per-entry results, not write errors.
            attempted += 1;
            let (tx, rx) = oneshot::channel();
            h.control
                .send(DeviceControl::Write(WriteRequest { signal_id: id.clone(), value: value.clone(), ack: tx }))
                .await
                .map_err(|_| {
                    h.health.write_errors.fetch_add(1, Ordering::Relaxed);
                    device_unavailable()
                })?;
            match rx.await {
                Ok(Ok(())) => {
                    succeeded += 1;
                    results.push(json!({ "signal": id, "value": value, "ok": true }));
                }
                Ok(Err(e)) => {
                    h.health.write_errors.fetch_add(1, Ordering::Relaxed);
                    results.push(json!({ "signal": id, "value": value, "ok": false, "error": e }));
                }
                Err(_) => {
                    h.health.write_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(device_unavailable());
                }
            }
        }

        // WRITE_NOT_ALLOWED only when EVERY entry was an allow-list refusal (nothing else attempted).
        if !entries.is_empty() && refused == entries.len() {
            h.dm.record_command("sb/write", false, ms(started));
            return Err(CommandError::new("WRITE_NOT_ALLOWED", "no entry is in this instance's writes.allow list"));
        }
        // WRITE_FAILED when every allowed write reached the device and every one failed.
        if attempted > 0 && succeeded == 0 {
            h.dm.record_command("sb/write", false, ms(started));
            return Err(CommandError::new("WRITE_FAILED", "every attempted write was rejected by the device"));
        }

        h.dm.record_command("sb/write", true, ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "written": succeeded, "results": results })))
    }

    // --- sb/browse (paged address-space discovery + the hierarchical panel mode) ------------------

    async fn browse(&self, instance: Option<&str>, body: &Value) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        // The two request forms are mutually exclusive: `ref`/`depth`/`maxRefs` select the
        // hierarchical panel mode, `cursor`/`max` the paged one — and the hierarchical-only
        // arguments are meaningless without a `ref`.
        let hierarchical_keys = ["ref", "depth", "maxRefs"].iter().any(|k| body.get(*k).is_some());
        let paged_keys = ["cursor", "max"].iter().any(|k| body.get(*k).is_some());
        if hierarchical_keys && paged_keys {
            h.dm.record_command("sb/browse", false, ms(started));
            return Err(CommandError::new(
                "BAD_ARGS",
                "`ref`/`depth`/`maxRefs` (hierarchical) and `cursor`/`max` (paged) are mutually exclusive",
            ));
        }
        if hierarchical_keys && body.get("ref").is_none() {
            h.dm.record_command("sb/browse", false, ms(started));
            return Err(CommandError::new(
                "BAD_ARGS",
                "`depth`/`maxRefs` are hierarchical-mode arguments and require `ref`",
            ));
        }
        if body.get("ref").is_some() {
            let result = self.browse_hierarchical(h, body).await;
            h.dm.record_command("sb/browse", result.is_ok(), ms(started));
            return result;
        }
        let cursor = body.get("cursor").and_then(Value::as_str).map(str::to_string);
        let max = body.get("max").and_then(Value::as_u64).unwrap_or(200).clamp(1, 1000) as usize;

        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Browse { cursor, max, reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        let result = match rx.await {
            Ok(Ok(page)) => {
                let entries: Vec<Value> = page
                    .entries
                    .iter()
                    .map(|e| json!({ "id": e.id, "name": e.name, "type": e.type_name }))
                    .collect();
                let mut out = json!({ "id": h.cfg.id, "entries": entries });
                if let Some(cursor) = page.next_cursor {
                    out["cursor"] = json!(cursor);
                }
                Ok(Some(out))
            }
            Ok(Err(BrowseError::Unsupported)) => {
                Err(CommandError::new("BROWSE_UNSUPPORTED", "this adapter has no discovery service"))
            }
            Ok(Err(BrowseError::Failed(e))) => Err(CommandError::new("BROWSE_FAILED", e)),
            Err(_) => Err(device_unavailable()),
        };
        h.dm.record_command("sb/browse", result.is_ok(), ms(started));
        result
    }

    /// The `treeBrowser` panel mode of `sb/browse`: `ref` names a node in the **same**
    /// [`BrowsedSignal`](crate::device::BrowsedSignal) inventory the paged mode serves. `"root"` is
    /// the device node, whose `contains` refs are the inventory (bounded by `maxRefs`); a signal id
    /// is a known leaf (`"refs": []`); an unknown ref is `BAD_ARGS`. `depth` and `maxRefs` are
    /// clamped to 1..4 / 1..1000 (the same convention as the paged `max`); the template inventory is
    /// flat, so a deeper `depth` finds no grandchildren — it is still validated and echoed.
    async fn browse_hierarchical(&self, h: &DeviceHandle, body: &Value) -> Reply {
        let Some(ref_id) = body.get("ref").and_then(Value::as_str).filter(|r| !r.is_empty()) else {
            return Err(CommandError::new("BAD_ARGS", "`ref` must be a non-empty string"));
        };
        let depth = body.get("depth").and_then(Value::as_u64).unwrap_or(1).clamp(1, 4);
        let max_refs = body.get("maxRefs").and_then(Value::as_u64).unwrap_or(200).clamp(1, 1000) as usize;

        // Collect the whole inventory through the same control channel the paged mode uses,
        // following its cursors — one source, both browse modes.
        let mut entries = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let (tx, rx) = oneshot::channel();
            h.control
                .send(DeviceControl::Browse { cursor: cursor.clone(), max: 1000, reply: tx })
                .await
                .map_err(|_| device_unavailable())?;
            let page = match rx.await {
                Ok(Ok(page)) => page,
                Ok(Err(BrowseError::Unsupported)) => {
                    return Err(CommandError::new("BROWSE_UNSUPPORTED", "this adapter has no discovery service"));
                }
                Ok(Err(BrowseError::Failed(e))) => return Err(CommandError::new("BROWSE_FAILED", e)),
                Err(_) => return Err(device_unavailable()),
            };
            entries.extend(page.entries);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        if ref_id == "root" {
            let refs: Vec<Value> = entries
                .iter()
                .take(max_refs)
                .map(|e| {
                    json!({
                        "referenceType": "contains",
                        "target": { "nodeId": e.id, "name": e.name, "nodeClass": "signal",
                                    "dataType": e.type_name }
                    })
                })
                .collect();
            let ref_count = refs.len();
            return Ok(Some(json!({
                "id": h.cfg.id,
                "mode": "hierarchical",
                "root": { "nodeId": "root", "name": h.cfg.id, "nodeClass": "device",
                          "dataType": Value::Null, "refs": refs },
                "refCount": ref_count,
                "depth": depth,
                "truncated": entries.len() > max_refs
            })));
        }

        let Some(node) = entries.iter().find(|e| e.id == ref_id) else {
            return Err(CommandError::new("BAD_ARGS", format!("unknown browse ref `{ref_id}`")));
        };
        Ok(Some(json!({
            "id": h.cfg.id,
            "mode": "hierarchical",
            "root": { "nodeId": node.id, "name": node.name, "nodeClass": "signal",
                      "dataType": node.type_name, "refs": [] },
            "refCount": 0,
            "depth": depth,
            "truncated": false
        })))
    }

    // --- sb/pause + sb/resume (idempotent {paused, changed}) --------------------------------------

    async fn pause(&self, instance: Option<&str>, _by: Option<String>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Pause { reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        let changed = rx.await.map_err(|_| device_unavailable())?;
        h.dm.record_command("sb/pause", true, ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "paused": true, "changed": changed })))
    }

    async fn resume(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Resume { reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        let changed = rx.await.map_err(|_| device_unavailable())?;
        h.dm.record_command("sb/resume", true, ms(started));
        Ok(Some(json!({ "id": h.cfg.id, "paused": false, "changed": changed })))
    }

    // --- reconnect ---------------------------------------------------------------------------------

    async fn reconnect(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Reconnect { reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        match rx.await.map_err(|_| device_unavailable())? {
            Ok(()) => {
                h.dm.record_command("reconnect", true, ms(started));
                Ok(Some(json!({ "id": h.cfg.id, "connected": true })))
            }
            Err(e) => {
                h.dm.record_command("reconnect", false, ms(started));
                Err(CommandError::new("RECONNECT_FAILED", e))
            }
        }
    }

    // --- repoll (refused with PAUSED while paused) ------------------------------------------------

    async fn repoll(&self, instance: Option<&str>) -> Reply {
        let h = self.resolve(instance)?;
        let started = Instant::now();
        if h.health.is_paused() {
            h.dm.record_command("repoll", false, ms(started));
            return Err(CommandError::new("PAUSED", "instance is paused - resume first"));
        }
        let (tx, rx) = oneshot::channel();
        h.control
            .send(DeviceControl::Repoll { reply: tx })
            .await
            .map_err(|_| device_unavailable())?;
        match rx.await.map_err(|_| device_unavailable())? {
            Ok(polled) => {
                h.dm.record_command("repoll", true, ms(started));
                Ok(Some(json!({ "id": h.cfg.id, "polled": polled })))
            }
            Err(e) if e.contains("paused") => {
                // The device task raced a pause in ahead of us — same refusal, same code.
                h.dm.record_command("repoll", false, ms(started));
                Err(CommandError::new("PAUSED", e))
            }
            Err(e) => {
                h.dm.record_command("repoll", false, ms(started));
                Err(CommandError::new("DEVICE_UNAVAILABLE", e))
            }
        }
    }

    /// Resolve a `sb/read`/`sb/write` signal-ref to its stable id: `{"signalId"}` / `{"id"}` directly,
    /// or `{"name"}` looked up against the configured inventory. `Err` carries a label for the BAD /
    /// unresolved entry.
    fn resolve_ref(&self, h: &DeviceHandle, r: &Value) -> std::result::Result<String, String> {
        if let Some(id) = r.get("signalId").and_then(Value::as_str) {
            return Ok(id.to_string());
        }
        if let Some(id) = r.get("id").and_then(Value::as_str) {
            return Ok(id.to_string());
        }
        if let Some(name) = r.get("name").and_then(Value::as_str) {
            return h
                .signals
                .iter()
                .find(|s| s.name.as_deref() == Some(name))
                .map(|s| s.id.clone())
                .ok_or_else(|| name.to_string());
        }
        Err("<invalid ref>".to_string())
    }
}

// =================================================================================================
// Helpers
// =================================================================================================

fn ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn device_unavailable() -> CommandError {
    CommandError::new("DEVICE_UNAVAILABLE", "device task is unavailable")
}

fn quality_str(q: Quality) -> &'static str {
    match q {
        Quality::Good => "GOOD",
        Quality::Bad => "BAD",
        Quality::Uncertain => "UNCERTAIN",
    }
}

fn bad_read(id: &str, raw: &str) -> Value {
    json!({ "signal": { "id": id }, "value": Value::Null, "quality": "BAD", "qualityRaw": raw })
}

/// Normalize an `sb/write` body to a list of `{ref…, value}` entries: a `writes` array, or a single
/// object carrying `value` (§2.2). `Err(BAD_ARGS)` when neither form is present.
fn write_entries(body: &Value) -> std::result::Result<Vec<Value>, CommandError> {
    if let Some(arr) = body.get("writes").and_then(Value::as_array) {
        return Ok(arr.clone());
    }
    if body.get("value").is_some() {
        return Ok(vec![body.clone()]);
    }
    Err(CommandError::new("BAD_ARGS", "expected a `writes` array or a single write object with `value`"))
}

#[cfg(test)]
mod tests {
    //! Every verb's happy path + each error code + the single-instance default; the allow-list
    //! refusal proven to happen BEFORE any device I/O; pause gating a poll; and the panel registration.
    //! A mock device task services the control channel and RECORDS every write that reaches it — no
    //! device, no socket.
    use super::*;
    use std::sync::Mutex;

    use edgecommons::prelude::{Config, Metric, MetricService};

    use crate::app::{set_paused, Health};
    use crate::device::{BrowsePage, BrowsedSignal};

    // --- a no-op MetricService + Config so DeviceMetrics can be built without a live runtime --------

    #[derive(Default)]
    struct NoopMetrics;

    #[async_trait::async_trait]
    impl MetricService for NoopMetrics {
        fn define_metric(&self, _metric: Metric) {}
        fn is_metric_defined(&self, _name: &str) -> bool {
            true
        }
        async fn emit_metric(&self, _name: &str, _values: HashMap<String, f64>) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn emit_metric_now(&self, _name: &str, _values: HashMap<String, f64>) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn flush_metrics(&self) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn shutdown(&self) {}
    }

    fn config() -> Arc<Config> {
        Arc::new(
            Config::from_value(
                "com.example.MyAdapter",
                "thing-1",
                json!({ "metricEmission": { "target": "log", "namespace": "test" } }),
            )
            .unwrap(),
        )
    }

    fn dev(v: Value) -> DeviceConfig {
        serde_json::from_value(v).unwrap()
    }

    fn a_device() -> DeviceConfig {
        dev(json!({
            "id": "plc-1",
            "adapter": "sim",
            "connection": { "endpoint": "sim://plc-1" },
            "writes": { "allow": ["setpoint-1"] }
        }))
    }

    fn sim_signals() -> Vec<SignalInfo> {
        vec![
            SignalInfo { id: "temperature-1".into(), name: Some("Ambient temperature".into()) },
            SignalInfo { id: "setpoint-1".into(), name: Some("Setpoint".into()) },
        ]
    }

    #[derive(Clone)]
    enum BrowseKind {
        One,
        /// Two entries over two pages, so cursor-following and truncation are observable.
        Paged,
        Unsupported,
        Failed,
    }

    #[derive(Clone)]
    struct MockOpts {
        write_ok: bool,
        read_ok: bool,
        reconnect_ok: bool,
        repoll_ok: bool,
        /// The device task answers a repoll with the paused refusal (a pause raced in ahead).
        repoll_paused: bool,
        browse: BrowseKind,
    }

    impl Default for MockOpts {
        fn default() -> Self {
            Self {
                write_ok: true,
                read_ok: true,
                reconnect_ok: true,
                repoll_ok: true,
                repoll_paused: false,
                browse: BrowseKind::One,
            }
        }
    }

    struct Harness {
        commander: Arc<Commander>,
        /// Every write that REACHED the device — empty proves the allow-list refused before any I/O.
        writes: Arc<Mutex<Vec<(String, Value)>>>,
        health: Arc<Health>,
        _task: tokio::task::JoinHandle<()>,
    }

    fn make_dm(cfg: &DeviceConfig, health: Arc<Health>) -> Arc<DeviceMetrics> {
        Arc::new(DeviceMetrics::new(Arc::new(NoopMetrics), config(), cfg.id.clone(), health, 30))
    }

    /// Build a single-device commander whose control channel is served by a mock device task.
    fn harness(cfg: DeviceConfig, opts: MockOpts) -> Harness {
        let (tx, mut rx) = mpsc::channel::<DeviceControl>(16);
        let health = Arc::new(Health::default());
        health.set_link(LinkState::Online);
        let dm = make_dm(&cfg, Arc::clone(&health));
        let writes = Arc::new(Mutex::new(Vec::new()));

        let t_health = Arc::clone(&health);
        let t_writes = Arc::clone(&writes);
        let task = tokio::spawn(async move {
            while let Some(ctrl) = rx.recv().await {
                match ctrl {
                    DeviceControl::Write(req) => {
                        t_writes.lock().unwrap().push((req.signal_id.clone(), req.value.clone()));
                        let _ = req.ack.send(if opts.write_ok { Ok(()) } else { Err("device rejected".into()) });
                    }
                    DeviceControl::ReadNow { ids, reply } => {
                        if opts.read_ok {
                            let rs = ids
                                .iter()
                                .map(|id| Reading {
                                    signal_id: id.clone(),
                                    name: None,
                                    value: json!(42.0),
                                    quality: Quality::Good,
                                    quality_raw: Some("OK".into()),
                                    source_ts: None,
                                    capture_ts: None,
                                    received_ts: None,
                                })
                                .collect();
                            let _ = reply.send(Ok(rs));
                        } else {
                            let _ = reply.send(Err("link error".into()));
                        }
                    }
                    DeviceControl::Browse { cursor, reply, .. } => {
                        let temperature = BrowsedSignal {
                            id: "temperature-1".into(),
                            name: Some("Ambient temperature".into()),
                            type_name: "REAL".into(),
                        };
                        let r = match opts.browse {
                            BrowseKind::One => {
                                Ok(BrowsePage { entries: vec![temperature], next_cursor: None })
                            }
                            BrowseKind::Paged => {
                                if cursor.is_none() {
                                    Ok(BrowsePage {
                                        entries: vec![temperature],
                                        next_cursor: Some("page-2".into()),
                                    })
                                } else {
                                    Ok(BrowsePage {
                                        entries: vec![BrowsedSignal {
                                            id: "pressure-1".into(),
                                            name: Some("Line pressure".into()),
                                            type_name: "REAL".into(),
                                        }],
                                        next_cursor: None,
                                    })
                                }
                            }
                            BrowseKind::Unsupported => Err(BrowseError::Unsupported),
                            BrowseKind::Failed => Err(BrowseError::Failed("mid-browse error".into())),
                        };
                        let _ = reply.send(r);
                    }
                    DeviceControl::Pause { reply } => {
                        let _ = reply.send(set_paused(&t_health, true));
                    }
                    DeviceControl::Resume { reply } => {
                        let _ = reply.send(set_paused(&t_health, false));
                    }
                    DeviceControl::Reconnect { reply } => {
                        let _ = reply.send(if opts.reconnect_ok { Ok(()) } else { Err("no route to host".into()) });
                    }
                    DeviceControl::Repoll { reply } => {
                        let r = if opts.repoll_paused {
                            Err("instance is paused - resume first".to_string())
                        } else if opts.repoll_ok {
                            Ok(2)
                        } else {
                            Err("link error".into())
                        };
                        let _ = reply.send(r);
                    }
                }
            }
        });

        let handle = DeviceHandle { cfg, control: tx, health: Arc::clone(&health), dm, signals: sim_signals() };
        let commander = Arc::new(Commander::new(vec![handle]));
        Harness { commander, writes, health, _task: task }
    }

    fn ok(reply: Reply) -> Value {
        reply.expect("command succeeded").expect("a result object")
    }
    fn err_code(reply: Reply) -> String {
        reply.expect_err("command failed").code
    }

    // --- routing: the library hands over the addressed instance, this maps it to a device ---------

    #[tokio::test]
    async fn instance_defaults_to_the_sole_device_and_unknown_or_missing_ids_error() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.status(None).await);
        assert_eq!(out["id"], json!("plc-1"));
        assert_eq!(err_code(h.commander.status(Some("nope")).await), "NO_SUCH_INSTANCE");

        // Two devices: a missing `instance` is BAD_ARGS.
        let mk = |cfg: DeviceConfig| {
            let (tx, _rx) = mpsc::channel(1);
            let health = Arc::new(Health::default());
            let dm = make_dm(&cfg, Arc::clone(&health));
            DeviceHandle { cfg, control: tx, health, dm, signals: sim_signals() }
        };
        let mut b = a_device();
        b.id = "plc-2".into();
        let multi = Commander::new(vec![mk(a_device()), mk(b)]);
        assert_eq!(err_code(multi.status(None).await), "BAD_ARGS");
        assert_eq!(ok(multi.status(Some("plc-2")).await)["id"], json!("plc-2"));
    }

    // --- sb/status ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn status_reports_connected_state_paused_and_a_counter_snapshot() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.status(None).await);
        assert_eq!(out["connected"], json!(true));
        assert_eq!(out["state"], json!("ONLINE"));
        assert_eq!(out["paused"], json!(false));
        assert_eq!(out["adapter"], json!("sim"));
        assert!(out["metrics"].get("connectAttempts").is_some());
    }

    // --- sb/signals --------------------------------------------------------------------------------

    #[tokio::test]
    async fn signals_lists_the_inventory_with_the_writable_flag() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.signals(None).await);
        let sigs = out["signals"].as_array().unwrap();
        assert_eq!(sigs.len(), 2);
        let setpoint = sigs.iter().find(|s| s["id"] == json!("setpoint-1")).unwrap();
        assert_eq!(setpoint["writable"], json!(true), "setpoint-1 is on the allow-list");
        let temp = sigs.iter().find(|s| s["id"] == json!("temperature-1")).unwrap();
        assert_eq!(temp["writable"], json!(false), "temperature-1 is not");
    }

    // --- sb/read -----------------------------------------------------------------------------------

    #[tokio::test]
    async fn read_returns_values_by_id_and_by_name_and_marks_unresolved_refs() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h
            .commander
            .read(None, &json!({ "signals": [ { "signalId": "temperature-1" }, { "name": "Setpoint" }, { "name": "ghost" } ] }))
            .await);
        let reads = out["reads"].as_array().unwrap();
        assert_eq!(reads[0]["signal"]["id"], json!("temperature-1"));
        assert_eq!(reads[0]["quality"], json!("GOOD"));
        assert_eq!(reads[1]["signal"]["id"], json!("setpoint-1"), "resolved by name");
        assert_eq!(reads[2]["quality"], json!("BAD"), "an unknown name is a BAD/unresolved entry");
        assert_eq!(reads[2]["qualityRaw"], json!("UNRESOLVED_REF"));
    }

    #[tokio::test]
    async fn read_without_a_signals_array_is_bad_args_and_a_link_error_is_read_failed() {
        let h = harness(a_device(), MockOpts::default());
        assert_eq!(err_code(h.commander.read(None, &json!({})).await), "BAD_ARGS");

        let h = harness(a_device(), MockOpts { read_ok: false, ..MockOpts::default() });
        assert_eq!(
            err_code(h.commander.read(None, &json!({ "signals": [ { "signalId": "temperature-1" } ] })).await),
            "READ_FAILED"
        );
    }

    // --- sb/write: allow-list BEFORE any device I/O (the security guarantee) -----------------------

    #[tokio::test]
    async fn a_refused_write_never_reaches_the_device() {
        let h = harness(a_device(), MockOpts::default());
        // temperature-1 is NOT on the allow-list.
        let code = err_code(
            h.commander
                .write(None, &json!({ "writes": [ { "signalId": "temperature-1", "value": 1 } ] }))
                .await,
        );
        assert_eq!(code, "WRITE_NOT_ALLOWED");
        assert!(h.writes.lock().unwrap().is_empty(), "the refused write must never reach the device");
    }

    #[tokio::test]
    async fn an_allow_listed_write_is_confirmed_and_batches_mix_results() {
        let h = harness(a_device(), MockOpts::default());
        // A single allowed write (single-object shorthand).
        let out = ok(h.commander.write(None, &json!({ "signalId": "setpoint-1", "value": 42 })).await);
        assert_eq!(out["written"], json!(1));
        assert_eq!(h.writes.lock().unwrap().len(), 1, "the allowed write reached the device");

        // A batch: one allowed (written), one refused (never sent).
        let out = ok(h
            .commander
            .write(None, &json!({ "writes": [
                { "signalId": "setpoint-1", "value": 7 },
                { "signalId": "temperature-1", "value": 8 }
            ] }))
            .await);
        assert_eq!(out["written"], json!(1), "only the allow-listed entry is written");
        let results = out["results"].as_array().unwrap();
        assert_eq!(results.iter().filter(|r| r["ok"] == json!(true)).count(), 1);
        assert_eq!(results.iter().filter(|r| r["error"] == json!("not in writes.allow")).count(), 1);
        // Two device writes total (one from each successful call); the refused entry added none.
        assert_eq!(h.writes.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_write_the_device_rejects_is_write_failed_and_counts_a_write_error() {
        let h = harness(a_device(), MockOpts { write_ok: false, ..MockOpts::default() });
        let code = err_code(h.commander.write(None, &json!({ "signalId": "setpoint-1", "value": 42 })).await);
        assert_eq!(code, "WRITE_FAILED");
        assert_eq!(
            h.health.write_errors.load(Ordering::Relaxed),
            1,
            "one rejected entry feeds southbound_health.writeErrors"
        );

        // An allow-list refusal is policy, not a device write error — nothing accrues.
        let h = harness(a_device(), MockOpts::default());
        let _ = h.commander.write(None, &json!({ "signalId": "temperature-1", "value": 1 })).await;
        assert_eq!(h.health.write_errors.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_write_with_no_writes_or_value_is_bad_args() {
        let h = harness(a_device(), MockOpts::default());
        assert_eq!(err_code(h.commander.write(None, &json!({})).await), "BAD_ARGS");
    }

    // --- sb/browse ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn browse_returns_a_page_or_the_right_error_code() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.browse(None, &json!({})).await);
        assert_eq!(out["entries"].as_array().unwrap().len(), 1);
        assert_eq!(out["entries"][0]["id"], json!("temperature-1"));

        let h = harness(a_device(), MockOpts { browse: BrowseKind::Unsupported, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.browse(None, &json!({})).await), "BROWSE_UNSUPPORTED");

        let h = harness(a_device(), MockOpts { browse: BrowseKind::Failed, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.browse(None, &json!({})).await), "BROWSE_FAILED");
    }

    // --- sb/browse: the hierarchical panel mode ----------------------------------------------------

    #[tokio::test]
    async fn hierarchical_browse_of_root_lists_the_inventory_as_contains_refs() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.browse(None, &json!({ "ref": "root" })).await);
        assert_eq!(out["mode"], json!("hierarchical"));
        let root = &out["root"];
        assert_eq!(root["nodeId"], json!("root"));
        assert_eq!(root["name"], json!("plc-1"), "the root node is the instance");
        assert_eq!(root["nodeClass"], json!("device"));
        assert_eq!(root["dataType"], Value::Null);
        assert_eq!(
            root["refs"],
            json!([{
                "referenceType": "contains",
                "target": { "nodeId": "temperature-1", "name": "Ambient temperature",
                            "nodeClass": "signal", "dataType": "REAL" }
            }])
        );
        assert_eq!(out["refCount"], json!(1));
        assert_eq!(out["depth"], json!(1));
        assert_eq!(out["truncated"], json!(false));
    }

    #[tokio::test]
    async fn hierarchical_browse_of_a_signal_is_a_known_leaf() {
        let h = harness(a_device(), MockOpts::default());
        let out = ok(h.commander.browse(None, &json!({ "ref": "temperature-1" })).await);
        let root = &out["root"];
        assert_eq!(root["nodeId"], json!("temperature-1"));
        assert_eq!(root["nodeClass"], json!("signal"));
        assert_eq!(root["dataType"], json!("REAL"));
        assert_eq!(root["refs"], json!([]), "a known leaf carries an explicit empty refs");
        assert_eq!(out["refCount"], json!(0));
        assert_eq!(out["truncated"], json!(false));
    }

    #[tokio::test]
    async fn hierarchical_browse_rejects_unknown_refs_and_mode_mixing() {
        let h = harness(a_device(), MockOpts::default());
        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": "ghost" })).await), "BAD_ARGS");
        // `ref`/`depth`/`maxRefs` (hierarchical) and `cursor`/`max` (paged) are mutually exclusive.
        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": "root", "cursor": "page-2" })).await), "BAD_ARGS");
        assert_eq!(err_code(h.commander.browse(None, &json!({ "depth": 2, "max": 10 })).await), "BAD_ARGS");
        // The hierarchical-only arguments are rejected without a `ref` — no silent paged fallback.
        assert_eq!(err_code(h.commander.browse(None, &json!({ "depth": 2 })).await), "BAD_ARGS");
        assert_eq!(err_code(h.commander.browse(None, &json!({ "maxRefs": 10 })).await), "BAD_ARGS");
        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": 7 })).await), "BAD_ARGS", "a non-string ref is malformed");
    }

    #[tokio::test]
    async fn hierarchical_browse_bounds_depth_and_max_refs() {
        let h = harness(a_device(), MockOpts::default());
        // Out-of-range values are clamped into 1..4 / 1..1000, the same convention as the paged `max`.
        let out = ok(h.commander.browse(None, &json!({ "ref": "root", "depth": 99, "maxRefs": 5000 })).await);
        assert_eq!(out["depth"], json!(4));
        let out = ok(h.commander.browse(None, &json!({ "ref": "root", "depth": 0 })).await);
        assert_eq!(out["depth"], json!(1));
    }

    #[tokio::test]
    async fn hierarchical_browse_truncates_at_max_refs_across_pages() {
        let h = harness(a_device(), MockOpts { browse: BrowseKind::Paged, ..MockOpts::default() });
        // The paged seam serves two entries over two pages; maxRefs 1 truncates the root's refs.
        let out = ok(h.commander.browse(None, &json!({ "ref": "root", "maxRefs": 1 })).await);
        assert_eq!(out["refCount"], json!(1));
        assert_eq!(out["truncated"], json!(true));
        // The second page's entry is still resolvable as a leaf ref — the whole inventory is one tree.
        let out = ok(h.commander.browse(None, &json!({ "ref": "pressure-1" })).await);
        assert_eq!(out["root"]["nodeClass"], json!("signal"));
    }

    #[tokio::test]
    async fn hierarchical_browse_maps_the_seam_errors_to_the_same_codes() {
        let h = harness(a_device(), MockOpts { browse: BrowseKind::Unsupported, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": "root" })).await), "BROWSE_UNSUPPORTED");

        let h = harness(a_device(), MockOpts { browse: BrowseKind::Failed, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.browse(None, &json!({ "ref": "root" })).await), "BROWSE_FAILED");
    }

    // --- pause / resume / repoll -------------------------------------------------------------------

    #[tokio::test]
    async fn pause_is_idempotent_and_repoll_is_refused_while_paused() {
        let h = harness(a_device(), MockOpts::default());

        // repoll works while running.
        assert_eq!(ok(h.commander.repoll(None).await)["polled"], json!(2));

        let out = ok(h.commander.pause(None, None).await);
        assert_eq!(out["paused"], json!(true));
        assert_eq!(out["changed"], json!(true));
        assert!(h.health.is_paused());

        // repoll is refused while paused, with the dedicated PAUSED code.
        assert_eq!(err_code(h.commander.repoll(None).await), "PAUSED");

        // pausing again is idempotent.
        assert_eq!(ok(h.commander.pause(None, None).await)["changed"], json!(false));

        // resume clears it and repoll works again.
        let out = ok(h.commander.resume(None).await);
        assert_eq!(out["paused"], json!(false));
        assert_eq!(out["changed"], json!(true));
        assert!(!h.health.is_paused());
        assert_eq!(ok(h.commander.repoll(None).await)["polled"], json!(2));
    }

    #[tokio::test]
    async fn a_paused_refusal_from_the_device_task_is_also_paused() {
        // The command layer saw an unpaused instance, but a pause raced in ahead of the repoll —
        // the device task's refusal maps to the same PAUSED code.
        let h = harness(a_device(), MockOpts { repoll_paused: true, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.repoll(None).await), "PAUSED");
    }

    // --- reconnect ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn reconnect_confirms_or_reports_reconnect_failed() {
        let h = harness(a_device(), MockOpts::default());
        assert_eq!(ok(h.commander.reconnect(None).await)["connected"], json!(true));

        let h = harness(a_device(), MockOpts { reconnect_ok: false, ..MockOpts::default() });
        assert_eq!(err_code(h.commander.reconnect(None).await), "RECONNECT_FAILED");
    }

    #[tokio::test]
    async fn device_unavailable_when_the_task_is_gone() {
        // Drop the receiver so the control channel is closed.
        let (tx, rx) = mpsc::channel::<DeviceControl>(1);
        drop(rx);
        let cfg = a_device();
        let health = Arc::new(Health::default());
        let dm = make_dm(&cfg, Arc::clone(&health));
        let handle = DeviceHandle { cfg, control: tx, health: Arc::clone(&health), dm, signals: sim_signals() };
        let commander = Commander::new(vec![handle]);
        assert_eq!(err_code(commander.reconnect(None).await), "DEVICE_UNAVAILABLE");

        // An attempted (allow-listed) write aborted on the device path counts a write error.
        let code = err_code(commander.write(None, &json!({ "signalId": "setpoint-1", "value": 1 })).await);
        assert_eq!(code, "DEVICE_UNAVAILABLE");
        assert_eq!(health.write_errors.load(Ordering::Relaxed), 1);
    }

    // --- the registered wiring: verbs, declared scope, and scoped delivery ------------------------

    /// A well-formed request carrying `body`.
    fn request(body: Value) -> Message {
        edgecommons::messaging::MessageBuilder::new("sb/status", "1.0")
            .payload(body)
            .build()
    }

    #[tokio::test]
    async fn every_verb_is_registered_once_and_declares_instance_scope() {
        // Every sb/* verb acts on ONE device, so each declares CommandScope::Instance — the inbox
        // then resolves the topic/body addressing before the handler runs and hands the instance
        // over. Registering the same set twice on one inbox would clash, so the list is also the
        // uniqueness check.
        let h = harness(a_device(), MockOpts::default());
        let regs = registrations(&h.commander);
        let verbs: Vec<&str> = regs.iter().map(|(v, _, _)| *v).collect();
        assert_eq!(verbs, VERBS.to_vec(), "the nine verbs, in the documented order");
        for (verb, scope, _) in &regs {
            assert_eq!(*scope, CommandScope::Instance, "{verb} is instance-scoped");
        }
    }

    #[tokio::test]
    async fn a_registered_handler_acts_on_the_addressed_instance_the_library_resolved() {
        // The handler objects the inbox invokes, driven exactly as the inbox drives them: the
        // component-addressed delivery (`None`) resolves to the sole device, the instance-addressed
        // one (`Some(token)`) names it.
        let h = harness(a_device(), MockOpts::default());
        let regs = registrations(&h.commander);
        let status = &regs.iter().find(|(v, _, _)| *v == "sb/status").unwrap().2;

        let out = status.handle(request(json!({})), None).await.unwrap().unwrap();
        assert_eq!(out["id"], json!("plc-1"), "component-addressed: the sole configured device");
        let out = status
            .handle(request(json!({})), Some("plc-1".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out["id"], json!("plc-1"), "instance-addressed: the topic's token");
    }

    #[tokio::test]
    async fn a_registered_handler_never_reads_body_instance() {
        // With two devices and no addressed instance the answer is BAD_ARGS even though the body
        // names one: the library owns that fold-in, so the handler must not second-guess it. An
        // instance this component does not have is NO_SUCH_INSTANCE.
        let mk = |cfg: DeviceConfig| {
            let (tx, _rx) = mpsc::channel(1);
            let health = Arc::new(Health::default());
            let dm = make_dm(&cfg, Arc::clone(&health));
            DeviceHandle { cfg, control: tx, health, dm, signals: sim_signals() }
        };
        let mut b = a_device();
        b.id = "plc-2".into();
        let commander = Arc::new(Commander::new(vec![mk(a_device()), mk(b)]));
        let regs = registrations(&commander);
        let status = &regs.iter().find(|(v, _, _)| *v == "sb/status").unwrap().2;

        let err = status
            .handle(request(json!({ "instance": "plc-2" })), None)
            .await
            .expect_err("a body instance is not addressing");
        assert_eq!(err.code, "BAD_ARGS");
        let out = status
            .handle(request(json!({})), Some("plc-2".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out["id"], json!("plc-2"));
        let err = status
            .handle(request(json!({})), Some("ghost".to_string()))
            .await
            .expect_err("an unknown instance");
        assert_eq!(err.code, "NO_SUCH_INSTANCE");
    }

    // --- panels ------------------------------------------------------------------------------------

    #[test]
    fn the_three_panels_are_registered_with_the_right_ids_orders_and_scope() {
        let ps = panels();
        let ids: Vec<&str> = ps.iter().map(|p| p["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["overview", "signals", "diagnostics"]);
        let orders: Vec<u64> = ps.iter().map(|p| p["order"].as_u64().unwrap()).collect();
        assert_eq!(orders, vec![10, 20, 30]);
        for p in &ps {
            assert_eq!(p["scope"], json!("instance"), "every panel is instance-scoped");
        }
        // The signals panel binds the signal verbs; diagnostics binds browse.
        assert_eq!(ps[1]["verbs"], json!(["sb/signals", "sb/read", "sb/write", "repoll"]));
        assert_eq!(ps[2]["verbs"], json!(["sb/browse", "sb/status"]));
    }

    #[test]
    fn the_overview_panel_carries_the_summary_rows_and_lifecycle_bindings() {
        let ps = panels();
        let widgets = ps[0]["widgets"].as_array().unwrap();
        let (summary, lifecycle) = (&widgets[0], &widgets[1]);
        assert_eq!(summary["kind"], json!("summary"));
        assert_eq!(summary["id"], json!("overview-summary"));
        assert_eq!(summary["title"], json!("Adapter overview"));
        assert_eq!(
            summary["rows"],
            json!([
                { "label": "Signals", "value": "Configured signal inventory via cmd/sb/signals" },
                { "label": "Lifecycle", "value": "Pause, resume, reconnect, and repoll the instance" },
                { "label": "Writes", "value": "Allow-listed via writes.allow[]; checked before device I/O" }
            ])
        );
        assert_eq!(
            *lifecycle,
            json!({ "kind": "commandSummary", "id": "overview-lifecycle",
                    "title": "Lifecycle bindings",
                    "verbs": ["sb/status", "reconnect", "sb/pause", "sb/resume", "repoll"] })
        );
    }

    #[test]
    fn the_signal_grid_binds_sb_signals_through_both_verb_keys_and_never_a_write_verb() {
        let ps = panels();
        let widgets = ps[1]["widgets"].as_array().unwrap();
        assert_eq!(widgets.len(), 1);
        let grid = &widgets[0];
        assert_eq!(grid["kind"], json!("signalGrid"));
        assert_eq!(grid["id"], json!("configured-signals"));
        assert_eq!(grid["title"], json!("Configured signals"));
        assert_eq!(grid["scope"], json!("instance"), "command-backed widgets repeat the view scope");
        assert_eq!(grid["signalsVerb"], json!("sb/signals"));
        assert_eq!(grid["subscriptionsVerb"], json!("sb/signals"), "the renderer-compat alias binds the same verb");
        assert_eq!(grid["readVerb"], json!("sb/read"));
        assert!(grid.get("writeVerb").is_none(), "panels never advertise a write surface");
    }

    #[test]
    fn the_diagnostics_tree_browser_is_hierarchical_and_bounded() {
        let ps = panels();
        let widgets = ps[2]["widgets"].as_array().unwrap();
        let (tree, commands_widget) = (&widgets[0], &widgets[1]);
        assert_eq!(tree["kind"], json!("treeBrowser"));
        assert_eq!(tree["id"], json!("inventory-tree"));
        assert_eq!(tree["title"], json!("Inventory"));
        assert_eq!(tree["scope"], json!("instance"));
        assert_eq!(tree["mode"], json!("hierarchical"));
        assert_eq!(tree["rootRef"], json!("root"));
        assert_eq!(tree["depth"], json!(1));
        assert_eq!(tree["maxRefs"], json!(200));
        assert_eq!(tree["browseVerb"], json!("sb/browse"));
        assert_eq!(tree["readVerb"], json!("sb/read"));
        assert!(tree.get("writeVerb").is_none(), "panels never advertise a write surface");
        assert_eq!(
            *commands_widget,
            json!({ "kind": "commandSummary", "id": "diagnostic-commands",
                    "title": "Diagnostic commands", "verbs": ["sb/status", "sb/browse"] })
        );
    }
}
