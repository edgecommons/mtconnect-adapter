//! # Configuration reload (LLD §8)
//!
//! Two very different things can change under a running adapter, and they are not reloadable in the
//! same way:
//!
//! * **`component.global.agents[]`** owns live sockets, a multipart stream, and a sequence position
//!   in an agent's buffer. Re-pointing it under a running acquisition would silently orphan
//!   everything attached to it, so a candidate that changes it is **rejected** with
//!   [`RESTART_REQUIRED`] and the component keeps running on its last-good configuration. The same
//!   goes for adding or removing an instance: an instance owns a supervisor task and a session,
//!   and neither exists to be handed to a new one.
//! * **An existing instance's `signals[]`** owns nothing but a mapping. It is recompiled against the
//!   **cached probe model** — no agent round-trip, so a reload works while the agent is unreachable
//!   — and swapped atomically into a [`SignalSlot`] that the command surface and the device session
//!   both read. A reader that loads the slot gets one whole generation, never half of two.
//!
//! Both halves are pure and live here so the runtime only has to call them:
//!
//! * [`classify`] is the side-effect-free pre-commit verdict, registered as the library's
//!   configuration validator (`src/main.rs`). A rejected candidate never reaches a listener, so the
//!   live runtime cannot be left half-applied.
//! * [`SignalRegistry::apply`] prepares **every** instance's new generation before it swaps any of
//!   them, so one malformed instance cannot leave the others on a new configuration.
//!
//! ## Browse cursors invalidate on either generation
//!
//! `sb/browse` publishes `viewGeneration` and refuses a cursor minted against a different one. The
//! probe digest alone is not enough: an entry's `Configured` flag comes from the *signal set*, so a
//! reload that binds a new data item changes what the browse view says without changing the probe.
//! [`view_generation`] therefore composes both, and a cursor from before a reload is refused rather
//! than paging through a view that no longer exists.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::app::DeviceConfig;
use crate::mtconnect::config::{DeviceConfig as MtcDeviceConfig, SignalConfig};
use crate::mtconnect::SelectionConfig;

/// The rejection code for a candidate that changes something only a restart can apply.
pub const RESTART_REQUIRED: &str = "RESTART_REQUIRED";
/// The rejection code for a candidate whose MTConnect configuration does not compile.
pub const INVALID_CONFIG: &str = "INVALID_MTCONNECT_CONFIG";

/// A pre-commit verdict on one candidate configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The candidate may commit.
    Accept,
    /// The candidate is refused; the component keeps its last-good configuration.
    Reject {
        /// A stable, machine-readable code.
        code: &'static str,
        /// An operator-safe diagnostic. Never carries a secret — this module only ever reads
        /// structure, and credentials are references (`secretRef`), not values.
        message: String,
    },
}

impl Verdict {
    fn reject(code: &'static str, message: impl Into<String>) -> Self {
        Self::Reject { code, message: message.into() }
    }
}

/// `component.global.agents` of a raw configuration document, or `null` when it declares none.
fn agents_of(config: &Value) -> &Value {
    config.pointer("/component/global/agents").unwrap_or(&Value::Null)
}

/// `component.instances` of a raw configuration document.
fn instances_of(config: &Value) -> &[Value] {
    config
        .pointer("/component/instances")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// The instance ids a raw configuration declares, in order.
fn instance_ids_of(config: &Value) -> Vec<&str> {
    instances_of(config).iter().filter_map(|i| i.get("id").and_then(Value::as_str)).collect()
}

/// The pre-commit verdict on one candidate. `current` is the configuration in force (`None` on the
/// initial load, where there is nothing to compare against but everything still to validate).
///
/// This is deliberately total and side-effect free: it reads two JSON documents and returns a
/// verdict, which is what makes it safe as a validator callback.
#[must_use]
pub fn classify(candidate: &Value, current: Option<&Value>) -> Verdict {
    if let Some(current) = current {
        if agents_of(candidate) != agents_of(current) {
            return Verdict::reject(
                RESTART_REQUIRED,
                "component.global.agents[] changed: an agent owns live sockets, an open stream and \
                 a position in the agent's buffer, so it is applied by restarting the component",
            );
        }
        let (before, after) = (instance_ids_of(current), instance_ids_of(candidate));
        if before != after {
            return Verdict::reject(
                RESTART_REQUIRED,
                "component.instances[] added, removed or reordered an instance: an instance owns a \
                 supervisor task and a session, so it is applied by restarting the component",
            );
        }
    }
    match compile(candidate) {
        Ok(_) => Verdict::Accept,
        Err(e) => Verdict::reject(INVALID_CONFIG, e),
    }
}

/// Compile a raw configuration's MTConnect instances against its agents — the same semantic
/// validation startup performs, without touching anything.
///
/// # Errors
/// A message naming the offending instance or agent.
pub fn compile(config: &Value) -> Result<Vec<MtcDeviceConfig>, String> {
    let mut devices: Vec<DeviceConfig> = Vec::new();
    for raw in instances_of(config) {
        let id = raw.get("id").and_then(Value::as_str).unwrap_or("<unnamed>").to_string();
        let device: DeviceConfig = serde_json::from_value(raw.clone())
            .map_err(|e| format!("instance `{id}`: {e}"))?;
        devices.push(device);
    }
    let needs_agents = devices.iter().any(|d| d.adapter == crate::device::KIND);
    if !needs_agents {
        // Still refuse a `sim` instance carrying a `selection` — there is no probe behind it.
        crate::app::compile_mtconnect(&mut devices, &[], crate::app::PublishDefaults::default())
            .map_err(|e| e.to_string())?;
        return Ok(Vec::new());
    }
    let global = agent_host(config);
    let agents =
        crate::mtconnect::config::parse_agents(&global).map_err(|e| e.to_string())?;
    let defaults = crate::app::publish_defaults_of(&global);
    crate::app::compile_mtconnect(&mut devices, &agents, defaults).map_err(|e| e.to_string())
}

/// `parse_agents` reads `component.global`; hand it exactly that subtree.
fn agent_host(config: &Value) -> Value {
    config.pointer("/component/global").cloned().unwrap_or(Value::Null)
}

// =================================================================================================
// The live signal set
// =================================================================================================

/// One instance's compiled signal configuration — the explicit set **and** its `selection` block —
/// plus the generation token that identifies it. A reader takes the whole thing at once, so a
/// browse page and the signal list behind it always agree. The derived half of the served set is
/// not stored here: it is a function of this configuration and the probe model, and the model's
/// own digest is the other half of the browse `viewGeneration`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceSignals {
    /// A stable content hash of the signal configuration — half of the browse `viewGeneration`.
    pub generation: String,
    pub signals: Vec<SignalConfig>,
    /// The probe-derived selection in force (R1.1), swapped with the signals as one unit.
    pub selection: Option<SelectionConfig>,
}

impl InstanceSignals {
    /// Build a generation from a signal configuration. Content-addressed, so an edit that changes
    /// nothing observable does not invalidate a consumer's cursors.
    #[must_use]
    pub fn new(signals: Vec<SignalConfig>, selection: Option<SelectionConfig>) -> Self {
        let generation = generation_of(&signals, selection.as_ref());
        Self { generation, signals, selection }
    }
}

/// The content hash of one signal configuration: the fields that change what is published or
/// browsable, in configuration order — the explicit entries (presence of an unset
/// `conditionBinding`/`publish` is hashed too, because under a selection absence inherits the
/// derived value) and the whole `selection` block.
#[must_use]
pub fn generation_of(signals: &[SignalConfig], selection: Option<&SelectionConfig>) -> String {
    let mut hasher = Sha256::new();
    let opt = |hasher: &mut Sha256, v: Option<&str>| {
        match v {
            None => hasher.update([0x00]),
            Some(v) => {
                hasher.update([0x01]);
                hasher.update(v.as_bytes());
            }
        }
        hasher.update([0x1f]);
    };
    for s in signals {
        hasher.update(s.id.as_bytes());
        hasher.update([0x1f]);
        hasher.update(s.data_item_id.as_bytes());
        hasher.update([0x1f]);
        opt(&mut hasher, s.name.as_deref());
        opt(&mut hasher, s.channel.as_deref());
        match &s.condition_binding {
            None => hasher.update([0x00]),
            Some(bindings) => {
                hasher.update([0x01]);
                for c in bindings {
                    hasher.update(c.as_bytes());
                    hasher.update([0x1e]);
                }
            }
        }
        hasher.update([0x1f]);
        match &s.publish {
            None => hasher.update([0x00]),
            Some(p) => {
                hasher.update([0x01]);
                hasher.update(format!("{:?}|{}|{:?}", p.mode, p.batch_ms, p.deadband).as_bytes());
            }
        }
        hasher.update([0x1d]);
    }
    if let Some(sel) = selection {
        hasher.update([0x02]);
        hasher.update(sel.mode.as_str().as_bytes());
        hasher.update([0x1f]);
        let matcher = |hasher: &mut Sha256, m: &crate::mtconnect::Matcher| {
            opt(hasher, m.category.as_deref());
            opt(hasher, m.type_.as_deref());
            opt(hasher, m.sub_type.as_deref());
            opt(hasher, m.id_match.as_deref());
            opt(hasher, m.path.as_deref());
            hasher.update([0x1e]);
        };
        for m in &sel.include {
            matcher(&mut hasher, m);
        }
        hasher.update([0x1d]);
        for m in &sel.exclude {
            matcher(&mut hasher, m);
        }
        hasher.update([0x1d]);
        hasher.update(sel.max_signals.to_le_bytes());
        hasher.update([u8::from(sel.auto_condition_binding)]);
        hasher.update(sel.default_batch_ms.to_le_bytes());
        hasher.update(format!("{:?}", sel.default_publish_mode).as_bytes());
    }
    let digest = hasher.finalize();
    digest[..8].iter().fold(String::with_capacity(16), |mut acc, b| {
        acc.push_str(&format!("{b:02x}"));
        acc
    })
}

/// The `sb/browse` view generation: the probe model's digest **and** the signal set's, because both
/// decide what a browse page says.
#[must_use]
pub fn view_generation(probe_digest: &str, signal_generation: &str) -> String {
    format!("{probe_digest}.{signal_generation}")
}

/// One instance's live signal set. Readers load; a reload stores. Lock-free either way, so a
/// command handler can never block acquisition and vice versa.
#[derive(Debug)]
pub struct SignalSlot(ArcSwap<InstanceSignals>);

impl SignalSlot {
    /// The slot for one instance's starting configuration.
    #[must_use]
    pub fn new(signals: Vec<SignalConfig>, selection: Option<SelectionConfig>) -> Self {
        Self(ArcSwap::from_pointee(InstanceSignals::new(signals, selection)))
    }

    /// The generation in force — a whole, self-consistent snapshot.
    #[must_use]
    pub fn load(&self) -> Arc<InstanceSignals> {
        self.0.load_full()
    }

    /// Install a new generation.
    pub fn store(&self, next: Arc<InstanceSignals>) {
        self.0.store(next);
    }
}

/// Every instance's slot, keyed by instance id.
#[derive(Debug, Default)]
pub struct SignalRegistry {
    slots: HashMap<String, Arc<SignalSlot>>,
}

impl SignalRegistry {
    /// Build the registry for the configured devices.
    #[must_use]
    pub fn new(devices: &[MtcDeviceConfig]) -> Self {
        Self {
            slots: devices
                .iter()
                .map(|d| {
                    (
                        d.id.clone(),
                        Arc::new(SignalSlot::new(d.signals.clone(), d.selection.clone())),
                    )
                })
                .collect(),
        }
    }

    /// One instance's slot.
    #[must_use]
    pub fn slot(&self, instance_id: &str) -> Option<Arc<SignalSlot>> {
        self.slots.get(instance_id).cloned()
    }

    /// Apply a whole candidate: every instance's new generation is prepared **first**, and only
    /// then are the slots swapped. A candidate that does not compile changes nothing.
    ///
    /// Returns the ids whose signal set actually changed (an unchanged instance keeps its
    /// generation, so its consumers' browse cursors stay valid).
    ///
    /// # Errors
    /// A message naming the offending instance, when the candidate does not compile.
    pub fn apply(&self, config: &Value) -> Result<Vec<String>, String> {
        let compiled = compile(config)?;
        let mut staged: Vec<(Arc<SignalSlot>, Arc<InstanceSignals>, String)> = Vec::new();
        for device in compiled {
            let Some(slot) = self.slots.get(&device.id) else {
                // A new instance has no supervisor task; `classify` already refuses that candidate.
                continue;
            };
            let next = Arc::new(InstanceSignals::new(device.signals, device.selection));
            if slot.load().generation != next.generation {
                staged.push((Arc::clone(slot), next, device.id));
            }
        }
        let changed = staged.iter().map(|(_, _, id)| id.clone()).collect();
        for (slot, next, _) in staged {
            slot.store(next);
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config(agents: Value, instances: Value) -> Value {
        json!({ "component": { "global": { "agents": agents }, "instances": instances } })
    }

    fn instance(signals: Value) -> Value {
        json!({
            "id": "cnc-1",
            "adapter": "mtconnect",
            "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.123456" },
            "signals": signals
        })
    }

    fn agents() -> Value {
        json!([{ "id": "line-a-agent", "url": "http://agent:5000" }])
    }

    fn one_signal() -> Value {
        json!([{ "id": "x-position", "dataItemId": "Xabs" }])
    }

    #[test]
    fn changing_an_agent_is_restart_required_because_it_owns_the_live_stream() {
        let current = config(agents(), json!([instance(one_signal())]));
        let candidate = config(
            json!([{ "id": "line-a-agent", "url": "http://other-agent:5000" }]),
            json!([instance(one_signal())]),
        );
        let Verdict::Reject { code, message } = classify(&candidate, Some(&current)) else {
            panic!("re-pointing an agent under a running stream cannot be hot-applied")
        };
        assert_eq!(code, RESTART_REQUIRED);
        assert!(message.contains("agents[]"), "{message}");

        // Adding an agent is the same answer, for the same reason.
        let added = config(
            json!([
                { "id": "line-a-agent", "url": "http://agent:5000" },
                { "id": "line-b-agent", "url": "http://agent-b:5000" }
            ]),
            json!([instance(one_signal())]),
        );
        assert!(matches!(
            classify(&added, Some(&current)),
            Verdict::Reject { code: RESTART_REQUIRED, .. }
        ));
    }

    #[test]
    fn adding_or_removing_an_instance_is_restart_required() {
        let current = config(agents(), json!([instance(one_signal())]));
        let mut second = instance(one_signal());
        second["id"] = json!("cnc-2");
        second["connection"]["deviceUuid"] = json!("MAZAK.999");
        let candidate = config(agents(), json!([instance(one_signal()), second]));

        let Verdict::Reject { code, message } = classify(&candidate, Some(&current)) else {
            panic!("a new instance has no supervisor task to hand it to")
        };
        assert_eq!(code, RESTART_REQUIRED);
        assert!(message.contains("instances[]"), "{message}");

        // Removing one, likewise.
        let emptied = config(agents(), json!([]));
        assert!(matches!(
            classify(&emptied, Some(&current)),
            Verdict::Reject { code: RESTART_REQUIRED, .. }
        ));
    }

    #[test]
    fn changing_an_instances_signals_is_accepted() {
        let current = config(agents(), json!([instance(one_signal())]));
        let candidate = config(
            agents(),
            json!([instance(json!([
                { "id": "x-position", "dataItemId": "Xabs" },
                { "id": "spindle-speed", "dataItemId": "Sspeed" }
            ]))]),
        );
        assert_eq!(classify(&candidate, Some(&current)), Verdict::Accept);
    }

    #[test]
    fn a_candidate_that_does_not_compile_is_refused_before_it_commits() {
        let current = config(agents(), json!([instance(one_signal())]));

        // An instance pointing at an agent nobody declared.
        let mut orphan = instance(one_signal());
        orphan["connection"]["agentId"] = json!("no-such-agent");
        let candidate = config(agents(), json!([orphan]));
        let Verdict::Reject { code, message } = classify(&candidate, Some(&current)) else {
            panic!("an unresolvable binding must not commit")
        };
        assert_eq!(code, INVALID_CONFIG);
        assert!(message.contains("no-such-agent"), "{message}");

        // A signal whose condition binding names its own data item.
        let candidate = config(
            agents(),
            json!([instance(json!([
                { "id": "x", "dataItemId": "Xabs", "conditionBinding": ["Xabs"] }
            ]))]),
        );
        assert!(matches!(classify(&candidate, Some(&current)), Verdict::Reject { .. }));

        // A typo'd key is a mistake, not a no-op.
        let candidate = config(
            agents(),
            json!([instance(json!([{ "id": "x", "dataItemID": "Xabs" }]))]),
        );
        assert!(matches!(classify(&candidate, Some(&current)), Verdict::Reject { .. }));
    }

    #[test]
    fn the_initial_load_has_nothing_to_compare_against_but_is_still_validated() {
        assert_eq!(classify(&config(agents(), json!([instance(one_signal())])), None), Verdict::Accept);

        let broken = config(json!([]), json!([instance(one_signal())]));
        assert!(matches!(classify(&broken, None), Verdict::Reject { code: INVALID_CONFIG, .. }));

        // A simulator-only deployment declares no agents at all, and that is fine.
        let sim = json!({ "component": { "global": {}, "instances": [
            { "id": "plc-1", "adapter": "sim", "connection": { "endpoint": "sim://plc-1" } }
        ] } });
        assert_eq!(classify(&sim, None), Verdict::Accept);
    }

    #[test]
    fn the_generation_is_content_addressed_so_only_a_real_change_invalidates_cursors() {
        let compile_one = |raw: Value| -> Vec<SignalConfig> {
            compile(&config(agents(), json!([instance(raw)]))).unwrap().remove(0).signals
        };
        let a = compile_one(one_signal());
        let same = compile_one(json!([{ "id": "x-position", "dataItemId": "Xabs" }]));
        assert_eq!(
            generation_of(&a, None),
            generation_of(&same, None),
            "the same set is the same generation"
        );

        let renamed = compile_one(json!([{ "id": "x-position", "dataItemId": "Xabs", "name": "X" }]));
        assert_ne!(
            generation_of(&a, None),
            generation_of(&renamed, None),
            "a label change IS visible in sb/signals"
        );

        let rebound = compile_one(json!([{ "id": "x-position", "dataItemId": "Sspeed" }]));
        assert_ne!(generation_of(&a, None), generation_of(&rebound, None));

        let bound = compile_one(
            json!([{ "id": "x-position", "dataItemId": "Xabs", "conditionBinding": ["Xtravel"] }]),
        );
        assert_ne!(generation_of(&a, None), generation_of(&bound, None));

        // An EMPTY conditionBinding is a statement (it clears a derived auto binding), so it is a
        // different generation than an absent one.
        let cleared = compile_one(
            json!([{ "id": "x-position", "dataItemId": "Xabs", "conditionBinding": [] }]),
        );
        assert_ne!(generation_of(&a, None), generation_of(&cleared, None));

        // A publish-policy edit swaps too: the served policy is observable.
        let policed = compile_one(
            json!([{ "id": "x-position", "dataItemId": "Xabs",
                     "publish": { "mode": "interval", "batchMs": 100 } }]),
        );
        assert_ne!(generation_of(&a, None), generation_of(&policed, None));

        assert_eq!(generation_of(&[], None).len(), 16, "a short, stable token");
    }

    #[test]
    fn the_selection_block_moves_the_generation_because_it_changes_the_served_set() {
        let sel = |v: Value| -> Option<crate::mtconnect::SelectionConfig> {
            Some(serde_json::from_value(v).unwrap())
        };
        let none = generation_of(&[], None);
        let all = generation_of(&[], sel(json!({ "mode": "all" })).as_ref());
        assert_ne!(none, all, "adding a selection is a new served set");
        assert_eq!(
            all,
            generation_of(&[], sel(json!({ "mode": "all" })).as_ref()),
            "content-addressed: the same block hashes the same"
        );
        let filtered = generation_of(
            &[],
            sel(json!({ "mode": "include", "include": [{ "type": "POSITION" }] })).as_ref(),
        );
        assert_ne!(all, filtered);
        let capped = generation_of(&[], sel(json!({ "mode": "all", "maxSignals": 3 })).as_ref());
        assert_ne!(all, capped);
        let unbound =
            generation_of(&[], sel(json!({ "mode": "all", "autoConditionBinding": false })).as_ref());
        assert_ne!(all, unbound);
    }

    #[test]
    fn a_selection_change_is_accepted_live_and_rides_the_atomic_swap() {
        // Same agents, same instances — only the selection block changes: NOT restart-required.
        let with_selection = |sel: Value| {
            let mut inst = instance(json!([]));
            inst["selection"] = sel;
            config(agents(), json!([inst]))
        };
        let current = with_selection(json!({ "mode": "all" }));
        let candidate = with_selection(json!({ "mode": "all", "maxSignals": 3 }));
        assert_eq!(classify(&candidate, Some(&current)), Verdict::Accept);

        // ... and the swap installs the new selection as one unit with the signals.
        let devices = compile(&current).unwrap();
        let registry = SignalRegistry::new(&devices);
        let before = registry.slot("cnc-1").unwrap().load();
        assert_eq!(before.selection.as_ref().unwrap().max_signals, 500);

        let changed = registry.apply(&candidate).unwrap();
        assert_eq!(changed, vec!["cnc-1".to_string()]);
        let after = registry.slot("cnc-1").unwrap().load();
        assert_eq!(after.selection.as_ref().unwrap().max_signals, 3);
        assert_ne!(after.generation, before.generation, "browse cursors are void");

        // A selection whose regex does not compile is refused BEFORE it commits.
        let broken = with_selection(json!({ "mode": "include", "include": [{ "type": "(" }] }));
        let Verdict::Reject { code, message } = classify(&broken, Some(&current)) else {
            panic!("a bad regex must not commit")
        };
        assert_eq!(code, INVALID_CONFIG);
        assert!(message.contains("regex"), "{message}");
        assert!(registry.apply(&broken).is_err());
        assert_eq!(
            registry.slot("cnc-1").unwrap().load().generation,
            after.generation,
            "the live generation is untouched"
        );

        // A selection on a sim instance is refused: there is no probe behind it.
        let sim = json!({ "component": { "global": {}, "instances": [{
            "id": "plc-1", "adapter": "sim",
            "connection": { "endpoint": "sim://plc-1" },
            "selection": { "mode": "all" }
        }] } });
        assert!(matches!(classify(&sim, None), Verdict::Reject { code: INVALID_CONFIG, .. }));
    }

    #[test]
    fn the_view_generation_moves_when_either_half_does() {
        let probe = "sha256:abcd";
        let g = view_generation(probe, "0011223344556677");
        assert!(g.starts_with(probe), "the probe digest still leads the token");
        assert_ne!(g, view_generation("sha256:beef", "0011223344556677"), "a new probe model");
        assert_ne!(g, view_generation(probe, "ffffffffffffffff"), "a new signal set");
    }

    #[test]
    fn applying_a_candidate_swaps_only_what_changed_and_nothing_when_it_does_not_compile() {
        let devices = compile(&config(agents(), json!([instance(one_signal())]))).unwrap();
        let registry = SignalRegistry::new(&devices);
        let before = registry.slot("cnc-1").unwrap().load();
        assert_eq!(before.signals.len(), 1);

        // The same configuration again changes nothing — cursors stay valid.
        let unchanged = registry.apply(&config(agents(), json!([instance(one_signal())]))).unwrap();
        assert!(unchanged.is_empty());
        assert_eq!(registry.slot("cnc-1").unwrap().load().generation, before.generation);

        // A real edit swaps the whole set at once.
        let changed = registry
            .apply(&config(
                agents(),
                json!([instance(json!([
                    { "id": "x-position", "dataItemId": "Xabs" },
                    { "id": "spindle-speed", "dataItemId": "Sspeed" }
                ]))]),
            ))
            .unwrap();
        assert_eq!(changed, vec!["cnc-1".to_string()]);
        let after = registry.slot("cnc-1").unwrap().load();
        assert_eq!(after.signals.len(), 2);
        assert_ne!(after.generation, before.generation, "browse cursors are void");

        // A candidate that does not compile leaves the live generation exactly where it was.
        let broken = config(agents(), json!([instance(json!([{ "id": "x" }]))]));
        assert!(registry.apply(&broken).is_err());
        assert_eq!(registry.slot("cnc-1").unwrap().load().generation, after.generation);

        assert!(registry.slot("nope").is_none());
    }
}
