//! # Probe-derived signal selection (R1.1)
//!
//! An instance may carry a `selection` block beside its explicit `signals[]`: instead of naming
//! every data item by hand, the operator describes **which** of the device's data items to publish
//! (`mode: "include"` with matchers, or `mode: "all"`), and this module derives one signal per
//! selected item from the cached probe model. Everything here is pure and EdgeCommons-free, like
//! the rest of `src/mtconnect/**`:
//!
//! * [`SelectionConfig`] / [`Matcher`] — the configuration shapes, validated side-effect-free at
//!   config load ([`validate_selection`] rejects bad regexes/globs before anything commits).
//! * [`served_set`] — the one merge everyone calls: explicit `signals[]` + the derived set, with
//!   explicit entries overriding derived ones **field-by-field** (matched by `dataItemId`), the
//!   `maxSignals` cap applied to the derived half only, and every entry tagged with its
//!   [`Provenance`] (`configured` vs `discovered`). `MtcSession`, `sb/signals`, `sb/browse`, and
//!   the inventory all consume this same function, so they can never disagree about what is served.
//! * [`sanitize_token`] / [`glob_match`] — the derivation helpers: lower-kebab id sanitization and
//!   the `**`-aware component-path glob.
//!
//! Matcher semantics: **AND within a matcher, OR across matchers**; `exclude` wins over `include`;
//! `mode: "all"` includes everything (excludes still apply). Regexes are anchored (a pattern must
//! match the whole field, so `POSITION` cannot accidentally select `PATH_POSITION`).

use std::collections::{BTreeSet, HashMap};

use regex::Regex;
use serde::Deserialize;

use super::config::{PublishCfg, PublishMode, SignalConfig};
use super::error::MtcError;
use super::model::{Category, DataItemMeta, ProbeModel};

/// The default derived-set cap.
pub const DEFAULT_MAX_SIGNALS: usize = 500;

// =================================================================================================
// Configuration shapes
// =================================================================================================

/// How an instance's published set is chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelectionMode {
    /// Only the explicit `signals[]` publish — the behavior of an instance with no `selection`.
    #[default]
    Explicit,
    /// Data items matching any `include` matcher publish (minus `exclude`).
    Include,
    /// Every data item publishes (minus `exclude`).
    All,
}

impl SelectionMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Include => "include",
            Self::All => "all",
        }
    }
}

/// One matcher against a probe data item. Every field is optional; the fields that are present are
/// **AND**ed. An empty matcher matches every data item.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Matcher {
    /// `SAMPLE` | `EVENT` | `CONDITION` — the item's category, exactly.
    #[serde(default)]
    pub category: Option<String>,
    /// Anchored regex on the item's `type` (`POSITION`, `EXECUTION`, …).
    #[serde(default, rename = "type")]
    pub type_: Option<String>,
    /// Anchored regex on the item's `subType`. An item with no subType never matches this field.
    #[serde(default)]
    pub sub_type: Option<String>,
    /// Anchored regex on the `dataItemId`.
    #[serde(default)]
    pub id_match: Option<String>,
    /// Glob on the component path (`Axes/Linear[X]`), matched segment-wise; `*`/`?` within a
    /// segment, `**` spans segments. The empty path is the device level.
    #[serde(default)]
    pub path: Option<String>,
}

/// The per-instance `selection` block.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SelectionConfig {
    #[serde(default)]
    pub mode: SelectionMode,
    #[serde(default)]
    pub include: Vec<Matcher>,
    #[serde(default)]
    pub exclude: Vec<Matcher>,
    /// Caps the **derived** set only — explicit `signals[]` never count against it. Exceeding it
    /// truncates deterministically in browse-tree order, with a warning event and log line.
    #[serde(default = "default_max_signals")]
    pub max_signals: usize,
    /// Whether each derived non-condition signal binds the CONDITION data items of its own
    /// component.
    #[serde(default = "default_true")]
    pub auto_condition_binding: bool,
    /// The `component.global.defaults.batchMs` in force — resolved at compile time, not part of the
    /// `selection` document itself. Derived SAMPLE signals publish with this coalescing window.
    #[serde(skip)]
    pub default_batch_ms: u32,
}

fn default_max_signals() -> usize {
    DEFAULT_MAX_SIGNALS
}
fn default_true() -> bool {
    true
}

impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            mode: SelectionMode::Explicit,
            include: Vec::new(),
            exclude: Vec::new(),
            max_signals: DEFAULT_MAX_SIGNALS,
            auto_condition_binding: true,
            default_batch_ms: 0,
        }
    }
}

// =================================================================================================
// Validation (side-effect-free; run at config load, before anything commits)
// =================================================================================================

/// Validate one instance's `selection` block: patterns compile, categories are real, and inert
/// combinations are refused rather than silently ignored (a matcher list under `mode: "explicit"`
/// would select nothing, which is a mistake, not a no-op).
///
/// # Errors
/// [`MtcError::Config`] naming the instance and the offending field.
pub fn validate_selection(instance_id: &str, sel: &SelectionConfig) -> Result<(), MtcError> {
    let err = |msg: String| MtcError::Config(format!("instance `{instance_id}` selection: {msg}"));
    if sel.max_signals == 0 {
        return Err(err("maxSignals must be >= 1".into()));
    }
    match sel.mode {
        SelectionMode::Explicit => {
            if !sel.include.is_empty() || !sel.exclude.is_empty() {
                return Err(err(
                    "include/exclude matchers require mode \"include\" or \"all\" - under \
                     \"explicit\" they would select nothing"
                        .into(),
                ));
            }
        }
        SelectionMode::Include => {
            if sel.include.is_empty() {
                return Err(err(
                    "mode \"include\" requires at least one include matcher".into(),
                ));
            }
        }
        SelectionMode::All => {
            if !sel.include.is_empty() {
                return Err(err(
                    "include matchers are meaningless under mode \"all\" (everything is already \
                     included) - use mode \"include\", or drop them"
                        .into(),
                ));
            }
        }
    }
    for (kind, list) in [("include", &sel.include), ("exclude", &sel.exclude)] {
        for (i, m) in list.iter().enumerate() {
            validate_matcher(m).map_err(|e| err(format!("{kind}[{i}]: {e}")))?;
        }
    }
    Ok(())
}

fn validate_matcher(m: &Matcher) -> Result<(), String> {
    if let Some(cat) = &m.category {
        if !matches!(cat.as_str(), "SAMPLE" | "EVENT" | "CONDITION") {
            return Err(format!(
                "category `{cat}` is not one of SAMPLE, EVENT, CONDITION"
            ));
        }
    }
    for (field, pattern) in [
        ("type", &m.type_),
        ("subType", &m.sub_type),
        ("idMatch", &m.id_match),
    ] {
        if let Some(p) = pattern {
            anchored(p).map_err(|e| format!("{field} regex `{p}` is invalid: {e}"))?;
        }
    }
    // Any glob string is syntactically valid (`**`, `*`, `?`, literals); nothing to reject.
    Ok(())
}

/// Compile one pattern anchored: selection regexes match the **whole** field.
fn anchored(pattern: &str) -> Result<Regex, regex::Error> {
    Regex::new(&format!("^(?:{pattern})$"))
}

// =================================================================================================
// Matching
// =================================================================================================

/// A matcher with its regexes compiled once.
struct CompiledMatcher {
    category: Option<Category>,
    type_: Option<Regex>,
    sub_type: Option<Regex>,
    id_match: Option<Regex>,
    path: Option<String>,
}

impl CompiledMatcher {
    /// Compile, or `None` for a matcher that cannot compile (validation already refused it; a
    /// non-compiling matcher reaching this point matches nothing rather than panicking a session).
    fn compile(m: &Matcher) -> Option<Self> {
        let rx = |p: &Option<String>| -> Result<Option<Regex>, ()> {
            match p {
                None => Ok(None),
                Some(p) => anchored(p).map(Some).map_err(|_| ()),
            }
        };
        let compiled = Self {
            category: m.category.as_deref().and_then(Category::parse),
            type_: rx(&m.type_).ok()?,
            sub_type: rx(&m.sub_type).ok()?,
            id_match: rx(&m.id_match).ok()?,
            path: m.path.clone(),
        };
        if m.category.is_some() && compiled.category.is_none() {
            return None;
        }
        Some(compiled)
    }

    /// AND across the present fields.
    fn matches(&self, item: &DataItemMeta) -> bool {
        if let Some(cat) = self.category {
            if item.category != cat {
                return false;
            }
        }
        if let Some(rx) = &self.type_ {
            if !rx.is_match(&item.type_) {
                return false;
            }
        }
        if let Some(rx) = &self.sub_type {
            match &item.sub_type {
                Some(sub) if rx.is_match(sub) => {}
                _ => return false,
            }
        }
        if let Some(rx) = &self.id_match {
            if !rx.is_match(&item.id) {
                return false;
            }
        }
        if let Some(glob) = &self.path {
            if !glob_match(glob, &item.component_path) {
                return false;
            }
        }
        true
    }
}

/// Segment-wise glob over a component path: `*` and `?` within a segment, `**` spans zero or more
/// whole segments. The empty path (a device-level item) has zero segments, so only `**` (alone)
/// or the empty pattern match it.
#[must_use]
pub fn glob_match(pattern: &str, path: &str) -> bool {
    let pat: Vec<&str> = if pattern.is_empty() { Vec::new() } else { pattern.split('/').collect() };
    let segs: Vec<&str> = if path.is_empty() { Vec::new() } else { path.split('/').collect() };
    match_segments(&pat, &segs)
}

fn match_segments(pat: &[&str], segs: &[&str]) -> bool {
    match pat.first() {
        None => segs.is_empty(),
        Some(&"**") => {
            match_segments(&pat[1..], segs)
                || (!segs.is_empty() && match_segments(pat, &segs[1..]))
        }
        Some(p) => {
            !segs.is_empty()
                && match_one(&p.chars().collect::<Vec<_>>(), &segs[0].chars().collect::<Vec<_>>())
                && match_segments(&pat[1..], &segs[1..])
        }
    }
}

fn match_one(pat: &[char], s: &[char]) -> bool {
    match pat.first() {
        None => s.is_empty(),
        Some('*') => match_one(&pat[1..], s) || (!s.is_empty() && match_one(pat, &s[1..])),
        Some('?') => !s.is_empty() && match_one(&pat[1..], &s[1..]),
        Some(&c) => s.first() == Some(&c) && match_one(&pat[1..], &s[1..]),
    }
}

// =================================================================================================
// Derivation
// =================================================================================================

/// Lower-kebab sanitization: camelCase boundaries split (`SpindleSpeed` → `spindle-speed`), runs of
/// anything outside `[a-z0-9]` collapse to one `-`, and a name that sanitizes to nothing becomes
/// `signal` (an id must exist).
#[must_use]
pub fn sanitize_token(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut prev: Option<char> = None;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() {
            if c.is_ascii_uppercase()
                && prev.is_some_and(|p| p.is_ascii_lowercase() || p.is_ascii_digit())
            {
                out.push('-');
            }
            out.push(c.to_ascii_lowercase());
        } else if !out.is_empty() && !out.ends_with('-') {
            out.push('-');
        }
        prev = Some(c);
    }
    let out = out.trim_matches('-').to_string();
    if out.is_empty() { "signal".to_string() } else { out }
}

/// The derived channel: the UNS-sanitized component path, then the signal id
/// (`Axes/Linear[X]` + `xabs` → `axes/linear-x/xabs`). A device-level item publishes on its id
/// alone.
#[must_use]
pub fn derive_channel(component_path: &str, id: &str) -> String {
    if component_path.is_empty() {
        return id.to_string();
    }
    let mut segments: Vec<String> =
        component_path.split('/').map(sanitize_token).collect();
    segments.push(id.to_string());
    segments.join("/")
}

/// Where a served signal came from — surfaced on `sb/signals` rows and browse entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provenance {
    /// An explicit `signals[]` entry (possibly enriched by a matching derived entry).
    Configured,
    /// Derived from the probe by the `selection` block.
    Discovered,
}

impl Provenance {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Configured => "configured",
            Self::Discovered => "discovered",
        }
    }
}

/// One entry of the served union.
#[derive(Debug, Clone, PartialEq)]
pub struct ServedSignal {
    pub signal: SignalConfig,
    pub provenance: Provenance,
}

/// The served union: explicit entries first (configuration order), then the derived entries in
/// browse-tree order.
#[derive(Debug, Clone, PartialEq)]
pub struct ServedSet {
    pub signals: Vec<ServedSignal>,
    /// Derived candidates that matched, before the `maxSignals` cap.
    pub derived_matched: usize,
    /// How many candidates the cap dropped. Never silent: the caller raises a warning event.
    pub derived_truncated: usize,
}

impl ServedSet {
    /// The ids of the discovered (derived) half, for drift comparison.
    #[must_use]
    pub fn derived_ids(&self) -> BTreeSet<String> {
        self.signals
            .iter()
            .filter(|s| s.provenance == Provenance::Discovered)
            .map(|s| s.signal.id.clone())
            .collect()
    }
}

/// The one merge everything consumes: explicit `signals[]` plus the selection-derived set against
/// one probe model.
///
/// * With no selection (or `mode: "explicit"`, or no model yet) the served set **is** the explicit
///   set — today's behavior, byte for byte.
/// * A derived entry whose `dataItemId` an explicit entry also binds does not appear twice: the
///   explicit entry wins, field-by-field — its unset fields (`name`, `channel`,
///   `conditionBinding`, `publish`) take the derived values, its set fields override them.
/// * `maxSignals` caps the derived half only, truncating in browse-tree order.
/// * Derived ids are the lower-kebab sanitization of the `dataItemId`; a collision gets a
///   deterministic `-2`, `-3`, … suffix (browse-tree order) and a warning log.
#[must_use]
pub fn served_set(
    explicit: &[SignalConfig],
    selection: Option<&SelectionConfig>,
    model: Option<&ProbeModel>,
) -> ServedSet {
    let mut out: Vec<ServedSignal> = explicit
        .iter()
        .map(|s| ServedSignal { signal: s.clone(), provenance: Provenance::Configured })
        .collect();

    let (Some(sel), Some(model)) = (selection, model) else {
        return ServedSet { signals: out, derived_matched: 0, derived_truncated: 0 };
    };
    if sel.mode == SelectionMode::Explicit {
        return ServedSet { signals: out, derived_matched: 0, derived_truncated: 0 };
    }

    let include: Vec<CompiledMatcher> =
        sel.include.iter().filter_map(CompiledMatcher::compile).collect();
    let exclude: Vec<CompiledMatcher> =
        sel.exclude.iter().filter_map(CompiledMatcher::compile).collect();

    // CONDITION items per component, in browse-tree order — what auto-binding binds.
    let mut conditions_by_component: HashMap<&str, Vec<&str>> = HashMap::new();
    for item in items_in_tree_order(model) {
        if item.category == Category::Condition {
            conditions_by_component
                .entry(item.component_path.as_str())
                .or_default()
                .push(item.id.as_str());
        }
    }

    // Which dataItemIds the explicit entries bind (they take the derived entry over).
    let explicit_items: HashMap<&str, Vec<usize>> = {
        let mut m: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, s) in explicit.iter().enumerate() {
            m.entry(s.data_item_id.as_str()).or_default().push(i);
        }
        m
    };

    let mut used_ids: BTreeSet<String> = explicit.iter().map(|s| s.id.clone()).collect();
    let mut derived_matched = 0usize;
    let mut derived_kept = 0usize;
    let mut derived_truncated = 0usize;

    for item in items_in_tree_order(model) {
        let selected = (sel.mode == SelectionMode::All
            || include.iter().any(|m| m.matches(item)))
            && !exclude.iter().any(|m| m.matches(item));
        if !selected {
            continue;
        }

        let auto_binding = || -> Option<Vec<String>> {
            if !sel.auto_condition_binding || item.category == Category::Condition {
                return Some(Vec::new());
            }
            Some(
                conditions_by_component
                    .get(item.component_path.as_str())
                    .map(|ids| ids.iter().map(|s| (*s).to_string()).collect())
                    .unwrap_or_default(),
            )
        };
        let default_publish = || -> PublishCfg {
            match item.category {
                Category::Sample => PublishCfg {
                    mode: PublishMode::OnChange,
                    batch_ms: sel.default_batch_ms,
                    deadband: None,
                },
                // EVENT/CONDITION: on-change, immediate.
                _ => PublishCfg { mode: PublishMode::OnChange, batch_ms: 0, deadband: None },
            }
        };
        let derived_id = || -> String {
            let base = sanitize_token(&item.id);
            if !used_ids.contains(&base) {
                return base;
            }
            let mut n = 2usize;
            loop {
                let candidate = format!("{base}-{n}");
                if !used_ids.contains(&candidate) {
                    tracing::warn!(
                        data_item = %item.id, id = %candidate,
                        "derived signal id collided; deterministic suffix applied"
                    );
                    return candidate;
                }
                n += 1;
            }
        };

        if let Some(indices) = explicit_items.get(item.id.as_str()) {
            // Explicit overrides derived, field-by-field: fill only what the entry left unset.
            for &i in indices {
                let served = &mut out[i].signal;
                if served.name.is_none() {
                    served.name =
                        Some(item.name.clone().unwrap_or_else(|| derived_name(item)));
                }
                if served.channel.is_none() {
                    let id = served.id.clone();
                    served.channel = Some(derive_channel(&item.component_path, &id));
                }
                if served.condition_binding.is_none() {
                    served.condition_binding = auto_binding();
                }
                if served.publish.is_none() {
                    served.publish = Some(default_publish());
                }
            }
            continue;
        }

        derived_matched += 1;
        if derived_kept >= sel.max_signals {
            derived_truncated += 1;
            continue;
        }
        derived_kept += 1;

        let id = derived_id();
        used_ids.insert(id.clone());
        let channel = derive_channel(&item.component_path, &id);
        out.push(ServedSignal {
            signal: SignalConfig {
                id,
                name: Some(item.name.clone().unwrap_or_else(|| derived_name(item))),
                channel: Some(channel),
                data_item_id: item.id.clone(),
                condition_binding: auto_binding(),
                publish: Some(default_publish()),
            },
            provenance: Provenance::Discovered,
        });
    }

    ServedSet { signals: out, derived_matched, derived_truncated }
}

/// The fallback name when the probe declares none: the type, plus the subType when there is one
/// (`POSITION ACTUAL`).
fn derived_name(item: &DataItemMeta) -> String {
    match &item.sub_type {
        Some(sub) => format!("{} {sub}", item.type_),
        None => item.type_.clone(),
    }
}

/// The model's data items in browse-tree order — the deterministic order derivation, collision
/// suffixes, and `maxSignals` truncation all use.
fn items_in_tree_order(model: &ProbeModel) -> impl Iterator<Item = &DataItemMeta> {
    model
        .tree
        .iter()
        .filter_map(|node| node.id.strip_prefix("mtc:/item/"))
        .filter_map(|id| model.items.get(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mtconnect::xml::parse_devices;
    use serde_json::json;

    const DEVICES_2_7: &str = include_str!("../../tests/fixtures/devices_2.7.xml");

    fn model() -> ProbeModel {
        ProbeModel::from_devices(&parse_devices(DEVICES_2_7).unwrap(), "OKUMA.123456").unwrap()
    }

    fn sel(v: serde_json::Value) -> SelectionConfig {
        serde_json::from_value(v).unwrap()
    }

    fn explicit(id: &str, item: &str) -> SignalConfig {
        serde_json::from_value(json!({ "id": id, "dataItemId": item })).unwrap()
    }

    fn ids(set: &ServedSet) -> Vec<&str> {
        set.signals.iter().map(|s| s.signal.id.as_str()).collect()
    }

    fn find<'a>(set: &'a ServedSet, id: &str) -> &'a ServedSignal {
        set.signals.iter().find(|s| s.signal.id == id).unwrap_or_else(|| panic!("no `{id}`"))
    }

    // --- configuration shape -------------------------------------------------------------------

    #[test]
    fn the_selection_block_deserializes_with_its_defaults() {
        let s = sel(json!({ "mode": "all" }));
        assert_eq!(s.mode, SelectionMode::All);
        assert!(s.include.is_empty() && s.exclude.is_empty());
        assert_eq!(s.max_signals, DEFAULT_MAX_SIGNALS, "the 500 default");
        assert!(s.auto_condition_binding, "auto conditionBinding is on by default");
        assert_eq!(s.default_batch_ms, 0);

        let s = sel(json!({
            "mode": "include",
            "include": [{ "category": "SAMPLE", "type": "POSITION", "subType": "ACTUAL",
                          "idMatch": "X.*", "path": "Axes/**" }],
            "exclude": [{ "idMatch": "Xload" }],
            "maxSignals": 10,
            "autoConditionBinding": false
        }));
        assert_eq!(s.mode, SelectionMode::Include);
        assert_eq!(s.include[0].category.as_deref(), Some("SAMPLE"));
        assert_eq!(s.include[0].type_.as_deref(), Some("POSITION"));
        assert_eq!(s.include[0].sub_type.as_deref(), Some("ACTUAL"));
        assert_eq!(s.include[0].id_match.as_deref(), Some("X.*"));
        assert_eq!(s.include[0].path.as_deref(), Some("Axes/**"));
        assert_eq!(s.max_signals, 10);
        assert!(!s.auto_condition_binding);

        // Matchers and the block itself are CLOSED objects.
        assert!(serde_json::from_value::<SelectionConfig>(
            json!({ "mode": "all", "nope": 1 })
        )
        .is_err());
        assert!(serde_json::from_value::<Matcher>(json!({ "types": "POSITION" })).is_err());
    }

    // --- validation ----------------------------------------------------------------------------

    #[test]
    fn validation_rejects_bad_patterns_and_inert_combinations() {
        // A regex that does not compile is refused at load, naming the field.
        let bad = sel(json!({ "mode": "include", "include": [{ "type": "(" }] }));
        let err = validate_selection("cnc-1", &bad).unwrap_err().to_string();
        assert!(err.contains("cnc-1") && err.contains("include[0]") && err.contains("type"), "{err}");

        let bad = sel(json!({ "mode": "all", "exclude": [{ "idMatch": "[" }] }));
        assert!(validate_selection("cnc-1", &bad).is_err());

        // An unknown category is a mistake, not a matcher that never fires.
        let bad = sel(json!({ "mode": "include", "include": [{ "category": "sample" }] }));
        assert!(validate_selection("cnc-1", &bad).is_err(), "categories are the exact tokens");

        // Inert combinations are refused rather than silently selecting nothing.
        let inert = sel(json!({ "mode": "explicit", "include": [{ "type": "POSITION" }] }));
        assert!(validate_selection("cnc-1", &inert).is_err());
        let inert = sel(json!({ "mode": "include" }));
        assert!(validate_selection("cnc-1", &inert).is_err(), "include mode needs matchers");
        let inert = sel(json!({ "mode": "all", "include": [{ "type": "POSITION" }] }));
        assert!(validate_selection("cnc-1", &inert).is_err(), "all already includes everything");

        // A zero cap can serve nothing and says so.
        let zero = sel(json!({ "mode": "all", "maxSignals": 0 }));
        assert!(validate_selection("cnc-1", &zero).is_err());

        // The valid shapes pass.
        validate_selection("cnc-1", &sel(json!({ "mode": "all" }))).unwrap();
        validate_selection("cnc-1", &sel(json!({ "mode": "all", "exclude": [{}] }))).unwrap();
        validate_selection(
            "cnc-1",
            &sel(json!({ "mode": "include", "include": [{ "path": "Axes/**" }] })),
        )
        .unwrap();
        validate_selection("cnc-1", &sel(json!({ "mode": "explicit" }))).unwrap();
    }

    // --- matcher semantics ---------------------------------------------------------------------

    #[test]
    fn fields_and_within_a_matcher_and_matchers_or_across_the_list() {
        let m = model();
        // AND: category + type both constrain. Type POSITION alone is the Xabs sample AND the
        // Xtravel condition (its type is POSITION too); adding the category narrows it to one.
        let and = sel(json!({ "mode": "include",
            "include": [{ "category": "SAMPLE", "type": "POSITION" }] }));
        assert_eq!(ids(&served_set(&[], Some(&and), Some(&m))), vec!["xabs"]);
        let loose = sel(json!({ "mode": "include", "include": [{ "type": "POSITION" }] }));
        assert_eq!(ids(&served_set(&[], Some(&loose), Some(&m))), vec!["xabs", "xtravel"]);
        let never = sel(json!({ "mode": "include",
            "include": [{ "category": "EVENT", "type": "POSITION" }] }));
        assert_eq!(served_set(&[], Some(&never), Some(&m)).signals.len(), 0);

        // OR: two matchers widen the selection (tree order preserved).
        let or = sel(json!({ "mode": "include",
            "include": [{ "type": "POSITION" }, { "type": "EXECUTION" }] }));
        let set = served_set(&[], Some(&or), Some(&m));
        assert_eq!(ids(&set), vec!["xabs", "xtravel", "execution"]);
    }

    #[test]
    fn exclude_wins_over_include_and_over_mode_all() {
        let m = model();
        let s = sel(json!({ "mode": "include",
            "include": [{ "type": "POSITION" }], "exclude": [{ "category": "CONDITION" }] }));
        assert_eq!(ids(&served_set(&[], Some(&s), Some(&m))), vec!["xabs"]);

        // mode "all" includes everything; excludes still apply.
        let s = sel(json!({ "mode": "all", "exclude": [{ "category": "CONDITION" }] }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert_eq!(set.signals.len(), 12, "14 items minus the 2 conditions");
        assert!(set.signals.iter().all(|s| s.signal.data_item_id != "Xtravel"));
        assert!(set.signals.iter().all(|s| s.signal.data_item_id != "logic"));
    }

    #[test]
    fn sub_type_and_id_match_are_anchored_regexes() {
        let m = model();
        // Anchored: `POSITION` must not select `PATH_POSITION` — the whole field matches.
        let s = sel(json!({ "mode": "include", "include": [{ "type": "POSITION" }] }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert!(set.signals.iter().all(|s| s.signal.data_item_id != "Ppos"),
            "no partial-match creep into PATH_POSITION");

        // subType: an item WITHOUT a subType never matches a subType regex.
        let s = sel(json!({ "mode": "include", "include": [{ "subType": "ACTUAL" }] }));
        assert_eq!(ids(&served_set(&[], Some(&s), Some(&m))), vec!["xabs", "sspeed"]);

        // idMatch is a real regex.
        let s = sel(json!({ "mode": "include", "include": [{ "idMatch": "X(abs|load)" }] }));
        assert_eq!(ids(&served_set(&[], Some(&s), Some(&m))), vec!["xabs", "xload"]);

        // An empty matcher matches everything (AND of no constraints).
        let s = sel(json!({ "mode": "include", "include": [{}] }));
        assert_eq!(served_set(&[], Some(&s), Some(&m)).signals.len(), 14);
    }

    #[test]
    fn path_globs_match_segment_wise_with_double_star() {
        assert!(glob_match("Axes/Linear[X]", "Axes/Linear[X]"));
        assert!(glob_match("Axes/*", "Axes/Linear[X]"));
        assert!(!glob_match("Axes/*", "Axes"), "* is one whole segment, not optional");
        assert!(glob_match("Axes/**", "Axes"));
        assert!(glob_match("Axes/**", "Axes/Linear[X]"));
        assert!(glob_match("**/Linear[X]", "Axes/Linear[X]"));
        assert!(glob_match("**", ""), "** spans zero segments: the device level matches");
        assert!(glob_match("", ""), "the empty pattern is the device level exactly");
        assert!(!glob_match("", "Axes"));
        assert!(glob_match("Axes/Linear[?]", "Axes/Linear[X]"));
        assert!(!glob_match("Axes/Linear[?]", "Axes/Linear[XY]"));
        assert!(glob_match("**/Path[P1]/**", "Controller/Path[P1]"), "trailing ** spans zero");
        assert!(!glob_match("Controller", "Controller/Path[P1]"));

        let m = model();
        let s = sel(json!({ "mode": "include", "include": [{ "path": "Axes/**" }] }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert_eq!(ids(&set), vec!["xabs", "xload", "xfreq", "xtravel", "sspeed"]);

        // The device level is the empty path.
        let s = sel(json!({ "mode": "include", "include": [{ "path": "" }] }));
        assert_eq!(ids(&served_set(&[], Some(&s), Some(&m))), vec!["avail", "asset-changed"]);
    }

    // --- derivation ----------------------------------------------------------------------------

    #[test]
    fn sanitization_is_lower_kebab_with_camel_case_split() {
        assert_eq!(sanitize_token("Xabs"), "xabs");
        assert_eq!(sanitize_token("SpindleSpeed"), "spindle-speed");
        assert_eq!(sanitize_token("tool-offsets"), "tool-offsets");
        assert_eq!(sanitize_token("d1-Xabs"), "d1-xabs");
        assert_eq!(sanitize_token("ID123"), "id123");
        assert_eq!(sanitize_token("a__b..c"), "a-b-c");
        assert_eq!(sanitize_token("Linear[X]"), "linear-x");
        assert_eq!(sanitize_token("__"), "signal", "an id must exist");
        assert_eq!(sanitize_token(""), "signal");
        // The result is always a valid UNS token.
        for raw in ["Xabs", "SpindleSpeed", "a__b", "-x-", "[X]"] {
            assert!(crate::mtconnect::config::is_lower_kebab(&sanitize_token(raw)), "{raw}");
        }
    }

    #[test]
    fn a_derived_signal_carries_id_name_channel_binding_and_publish_defaults() {
        let m = model();
        let s = sel(json!({ "mode": "include", "include": [{ "idMatch": "Xabs" }] }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert_eq!(set.signals.len(), 1);
        let x = &set.signals[0];
        assert_eq!(x.provenance, Provenance::Discovered);
        assert_eq!(x.signal.id, "xabs", "lower-kebab of the dataItemId");
        assert_eq!(x.signal.name.as_deref(), Some("Xabs"), "the probe's own name wins");
        assert_eq!(
            x.signal.channel.as_deref(),
            Some("axes/linear-x/xabs"),
            "componentPath/id, UNS-sanitized"
        );
        assert_eq!(x.signal.data_item_id, "Xabs");
        // Auto conditionBinding: the CONDITION items of its own component.
        assert_eq!(x.signal.condition_bindings(), ["Xtravel"]);
        // SAMPLE publish default: on-change, the defaults batch window, and NO derived deadband —
        // a units-aware default is not cleanly derivable (a millimeter on a micro-positioner and a
        // millimeter on a gantry are different facts), so none is invented.
        let p = x.signal.publish_policy();
        assert_eq!(p.mode, PublishMode::OnChange);
        assert_eq!(p.batch_ms, 0);
        assert_eq!(p.deadband, None);
    }

    #[test]
    fn derived_names_fall_back_to_type_plus_sub_type_and_events_publish_immediately() {
        let m = model();
        let s = sel(json!({ "mode": "all" }));
        let set = served_set(&[], Some(&s), Some(&m));

        // `asset-changed` has no name in the probe: the type stands in.
        let asset = find(&set, "asset-changed");
        assert_eq!(asset.signal.name.as_deref(), Some("ASSET_CHANGED"));
        // EVENT: on-change immediate, no batching even when a defaults window exists.
        assert_eq!(asset.signal.publish_policy(),
            PublishCfg { mode: PublishMode::OnChange, batch_ms: 0, deadband: None });

        // A nameless item WITH a subType names both.
        let doc = parse_devices(
            r#"<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:2.7">
                 <Header instanceId="1"/>
                 <Devices><Device uuid="U" name="N" id="d"><DataItems>
                   <DataItem id="p1" category="SAMPLE" type="POSITION" subType="COMMANDED"/>
                 </DataItems></Device></Devices>
               </MTConnectDevices>"#,
        )
        .unwrap();
        let tiny = ProbeModel::from_devices(&doc, "U").unwrap();
        let set2 = served_set(&[], Some(&sel(json!({ "mode": "all" }))), Some(&tiny));
        assert_eq!(set2.signals[0].signal.name.as_deref(), Some("POSITION COMMANDED"));

        // A device-level item publishes on its id alone.
        let avail = find(&set, "avail");
        assert_eq!(avail.signal.channel.as_deref(), Some("avail"));

        // A CONDITION signal publishes its state and binds no conditions of its own.
        let travel = find(&set, "xtravel");
        assert!(travel.signal.condition_bindings().is_empty());
    }

    #[test]
    fn the_sample_batch_window_comes_from_the_resolved_default() {
        let m = model();
        let mut s = sel(json!({ "mode": "include", "include": [{ "idMatch": "Xabs" }] }));
        s.default_batch_ms = 250;
        let set = served_set(&[], Some(&s), Some(&m));
        assert_eq!(set.signals[0].signal.publish_policy().batch_ms, 250);
        // ... and events stay immediate regardless.
        let mut s = sel(json!({ "mode": "include", "include": [{ "idMatch": "execution" }] }));
        s.default_batch_ms = 250;
        let set = served_set(&[], Some(&s), Some(&m));
        assert_eq!(set.signals[0].signal.publish_policy().batch_ms, 0);
    }

    #[test]
    fn id_collisions_get_a_deterministic_suffix() {
        let m = model();
        // An explicit signal already owns `xabs`; the derived one steps aside deterministically.
        let s = sel(json!({ "mode": "include", "include": [{ "idMatch": "Xabs" }] }));
        let owned = vec![explicit("xabs", "Sspeed")];
        let set = served_set(&owned, Some(&s), Some(&m));
        assert_eq!(ids(&set), vec!["xabs", "xabs-2"]);
        assert_eq!(find(&set, "xabs-2").signal.data_item_id, "Xabs");
        assert_eq!(find(&set, "xabs-2").provenance, Provenance::Discovered);
        // The suffix chain keeps stepping.
        let owned = vec![explicit("xabs", "Sspeed"), explicit("xabs-2", "Xload")];
        let set = served_set(&owned, Some(&s), Some(&m));
        assert_eq!(ids(&set), vec!["xabs", "xabs-2", "xabs-3"]);
    }

    #[test]
    fn auto_condition_binding_is_per_component_and_can_be_opted_out() {
        let m = model();
        let s = sel(json!({ "mode": "all" }));
        let set = served_set(&[], Some(&s), Some(&m));
        // Same component (Axes/Linear[X]): Xabs, Xload, Xfreq all bind Xtravel.
        for id in ["xabs", "xload", "xfreq"] {
            assert_eq!(find(&set, id).signal.condition_bindings(), ["Xtravel"], "{id}");
        }
        // A different component's items do not: Sspeed is on Rotary[C], which has no condition.
        assert!(find(&set, "sspeed").signal.condition_bindings().is_empty());
        // Path[P1] has its own condition (`logic`), bound by its non-condition items.
        assert_eq!(find(&set, "execution").signal.condition_bindings(), ["logic"]);
        // Device-level items bind device-level conditions (there are none in the fixture).
        assert!(find(&set, "avail").signal.condition_bindings().is_empty());

        // The opt-out flag turns the whole derivation off.
        let s = sel(json!({ "mode": "all", "autoConditionBinding": false }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert!(set.signals.iter().all(|s| s.signal.condition_bindings().is_empty()));
    }

    // --- precedence ----------------------------------------------------------------------------

    #[test]
    fn explicit_entries_override_derived_ones_field_by_field() {
        let m = model();
        let s = sel(json!({ "mode": "all" }));

        // A bare explicit entry: its id wins, and every unset field takes the derived value.
        let bare = vec![explicit("x-position", "Xabs")];
        let set = served_set(&bare, Some(&s), Some(&m));
        let x = find(&set, "x-position");
        assert_eq!(x.provenance, Provenance::Configured);
        assert_eq!(x.signal.name.as_deref(), Some("Xabs"), "unset name takes the derived one");
        assert_eq!(x.signal.channel.as_deref(), Some("axes/linear-x/x-position"),
            "the derived channel carries the EXPLICIT id");
        assert_eq!(x.signal.condition_bindings(), ["Xtravel"], "unset binding takes the auto one");
        assert_eq!(x.signal.publish_policy().mode, PublishMode::OnChange);
        // ... and no second entry serves Xabs.
        assert_eq!(set.signals.iter().filter(|s| s.signal.data_item_id == "Xabs").count(), 1);

        // Set fields override: name, channel, an EMPTY conditionBinding, and a publish policy all
        // beat their derived counterparts.
        let overriding: SignalConfig = serde_json::from_value(json!({
            "id": "x-position", "name": "X actual", "channel": "machining/x",
            "dataItemId": "Xabs", "conditionBinding": [],
            "publish": { "mode": "interval", "batchMs": 500 }
        }))
        .unwrap();
        let set = served_set(&[overriding], Some(&s), Some(&m));
        let x = find(&set, "x-position");
        assert_eq!(x.signal.name.as_deref(), Some("X actual"));
        assert_eq!(x.signal.channel.as_deref(), Some("machining/x"));
        assert!(x.signal.condition_bindings().is_empty(),
            "conditionBinding: [] explicitly clears the auto binding");
        assert_eq!(x.signal.publish_policy().mode, PublishMode::Interval);
        assert_eq!(x.signal.publish_policy().batch_ms, 500);
    }

    #[test]
    fn without_a_selection_explicit_entries_are_served_verbatim() {
        let m = model();
        let bare = vec![explicit("x-position", "Xabs")];
        // No selection at all, and mode "explicit": identical, untouched entries.
        for selection in [None, Some(sel(json!({ "mode": "explicit" })))] {
            let set = served_set(&bare, selection.as_ref(), Some(&m));
            assert_eq!(set.signals.len(), 1);
            assert_eq!(set.signals[0].signal, bare[0], "no enrichment without a selection");
            assert_eq!(set.signals[0].provenance, Provenance::Configured);
            assert_eq!(set.derived_matched, 0);
        }
        // A selection with no model yet: the explicit set, honestly.
        let set = served_set(&bare, Some(&sel(json!({ "mode": "all" }))), None);
        assert_eq!(set.signals.len(), 1);
        assert_eq!(set.signals[0].signal, bare[0]);
    }

    // --- maxSignals ----------------------------------------------------------------------------

    #[test]
    fn max_signals_caps_the_derived_set_in_tree_order_and_reports_the_cut() {
        let m = model();
        let s = sel(json!({ "mode": "all", "maxSignals": 3 }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert_eq!(ids(&set), vec!["avail", "asset-changed", "xabs"], "the FIRST three, tree order");
        assert_eq!(set.derived_matched, 14);
        assert_eq!(set.derived_truncated, 11, "the cut is counted, never silent");

        // Explicit signals do not count against the cap.
        let explicit = vec![explicit("spindle", "Sspeed"), explicit("prog", "program")];
        let set = served_set(&explicit, Some(&s), Some(&m));
        assert_eq!(set.signals.len(), 5, "2 explicit + 3 derived");
        assert_eq!(
            set.signals.iter().filter(|s| s.provenance == Provenance::Discovered).count(),
            3
        );
        // ... and the explicitly-bound items were never derived candidates at all.
        assert_eq!(set.derived_matched, 12);
    }
}
