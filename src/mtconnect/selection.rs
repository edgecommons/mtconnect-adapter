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
//! * [`ChannelBudget`] / [`derive_channel`] — the depth-aware channel rule: a derived channel is
//!   the **last k** component-path segments plus the signal id, k chosen per signal as the largest
//!   value that still fits the instance's real UNS topic budget.
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
// The channel budget
// =================================================================================================

/// What is left of a UNS data topic for one instance's **channel**, after its
/// `ecv1/…/{instance}/data/` prefix — the room a derived channel has to fit into.
///
/// A UNS topic is capped at 8 levels (7 `/` separators) and 256 UTF-8 bytes, so an
/// instance-scoped data topic leaves only a few tokens for the channel. MTConnect component paths
/// go deeper than that: the demo Mazak's `stock` sits on `Resources[resources]/
/// Materials[materials]/Stock[stock]`, which with its id is four channel tokens where three fit.
/// A budget is therefore resolved from the instance's **real** identity — device, component and
/// instance token lengths included — and stamped into the compiled [`SelectionConfig`], the same
/// way [`SelectionConfig::default_batch_ms`] is. It is plain arithmetic here: this module owns no
/// topic grammar, it is handed the room it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelBudget {
    /// How many `/`-separated tokens the channel may have. `0` means not even a one-token channel
    /// fits — the instance's own identity is already at the depth limit.
    pub max_tokens: usize,
    /// How many UTF-8 bytes the channel string itself may occupy (separators between its tokens
    /// included, the separator before it excluded).
    pub max_bytes: usize,
}

impl ChannelBudget {
    /// The token budget of the rootless, instance-scoped data grammar
    /// (`ecv1/{device}/{component}/{instance}/data/…`): the 8-level topic limit less its 5 fixed
    /// levels.
    pub const DEFAULT_MAX_TOKENS: usize = 3;
    /// The whole-topic byte limit. A resolved budget subtracts the instance's own prefix from it;
    /// this fallback subtracts nothing, so it constrains only the token count.
    pub const DEFAULT_MAX_BYTES: usize = 256;
}

impl Default for ChannelBudget {
    /// The fallback used when no identity has been resolved — configuration-shape validation and
    /// unit tests. The live path always stamps the instance's exact budget instead.
    fn default() -> Self {
        Self {
            max_tokens: Self::DEFAULT_MAX_TOKENS,
            max_bytes: Self::DEFAULT_MAX_BYTES,
        }
    }
}

/// One derived channel and how it had to be shaped to fit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedChannel {
    /// The channel to publish on.
    pub channel: String,
    /// How many **root-side** component-path segments were dropped to fit the budget. `0` is the
    /// full path.
    pub dropped: usize,
    /// `false` only in the pathological case where not even the id alone fits the budget. The
    /// channel is still the id, so the failure surfaces where it is loudest — the library refuses
    /// the topic with `DEPTH_EXCEEDED`/`LENGTH_EXCEEDED` on publish.
    pub fits: bool,
}

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
    /// The `component.global.defaults.publishMode` in force — resolved at compile time, like
    /// [`Self::default_batch_ms`]. Derived SAMPLE signals publish in this mode; derived
    /// EVENT/CONDITION signals are always `on-change`, immediate.
    #[serde(skip)]
    pub default_publish_mode: PublishMode,
    /// The room this instance's UNS data topic leaves for a channel — resolved from its real
    /// identity at compile time, like [`Self::default_batch_ms`], and never part of the
    /// `selection` document. Every channel [`derive_channel`] produces fits it.
    #[serde(skip)]
    pub channel_budget: ChannelBudget,
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
            default_publish_mode: PublishMode::OnChange,
            channel_budget: ChannelBudget::default(),
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
    let pat: Vec<&str> = if pattern.is_empty() {
        Vec::new()
    } else {
        pattern.split('/').collect()
    };
    let segs: Vec<&str> = if path.is_empty() {
        Vec::new()
    } else {
        path.split('/').collect()
    };
    match_segments(&pat, &segs)
}

fn match_segments(pat: &[&str], segs: &[&str]) -> bool {
    match pat.first() {
        None => segs.is_empty(),
        Some(&"**") => {
            match_segments(&pat[1..], segs) || (!segs.is_empty() && match_segments(pat, &segs[1..]))
        }
        Some(p) => {
            !segs.is_empty()
                && match_one(
                    &p.chars().collect::<Vec<_>>(),
                    &segs[0].chars().collect::<Vec<_>>(),
                )
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
    if out.is_empty() {
        "signal".to_string()
    } else {
        out
    }
}

/// The derived channel: the UNS-sanitized component path, then the signal id
/// (`Axes/Linear[X]` + `xabs` → `axes/linear-x/xabs`). A device-level item publishes on its id
/// alone.
///
/// **Depth-aware.** MTConnect component paths go deeper than a UNS topic can carry, so the channel
/// is the **last k** path segments plus the id, where `k` is the largest value that still fits
/// `budget`. The leaf-most segments are the informative ones — `Materials[materials]/Stock[stock]`
/// says what a signal is, `Resources[resources]` above it barely narrows anything — so the
/// root-side segments drop first. The id is the terminal segment and is never dropped: signal ids
/// are unique per instance (enforced at config load for the explicit half, and by the `-2`/`-3`
/// suffix chain for the derived half), which is what makes every derived channel unique however
/// much path was dropped.
///
/// The rule is deterministic and per-signal: two signals of one instance can drop different
/// numbers of segments, because a longer path or a longer id costs more. Nothing is lost — the
/// full, untruncated component path stays in the probe model and is served as
/// `signal.address.componentPath` on `sb/signals` and on `sb/browse`.
///
/// `k = 0` (the id alone) is the floor. Only if even that does not fit does
/// [`DerivedChannel::fits`] come back `false`; the channel is still the id, so the library's own
/// topic validation refuses it loudly on publish rather than this module inventing a name.
#[must_use]
pub fn derive_channel(component_path: &str, id: &str, budget: ChannelBudget) -> DerivedChannel {
    let segments: Vec<String> = if component_path.is_empty() {
        Vec::new()
    } else {
        component_path.split('/').map(sanitize_token).collect()
    };
    let total = segments.len();
    // Bytes of the channel built from the last `k` segments plus the id: each kept segment costs
    // its own length plus the `/` that follows it.
    let bytes_of = |k: usize| -> usize {
        segments[total - k..]
            .iter()
            .map(|s| s.len() + 1)
            .sum::<usize>()
            + id.len()
    };

    // One token of the budget belongs to the id; the rest is what the path may claim.
    let mut kept = total.min(budget.max_tokens.saturating_sub(1));
    while kept > 0 && bytes_of(kept) > budget.max_bytes {
        kept -= 1;
    }
    let fits = budget.max_tokens >= 1 && id.len() <= budget.max_bytes;

    let mut channel = String::new();
    for segment in &segments[total - kept..] {
        channel.push_str(segment);
        channel.push('/');
    }
    channel.push_str(id);
    DerivedChannel {
        channel,
        dropped: total - kept,
        fits,
    }
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
    /// How many served signals had root-side component-path segments dropped from their derived
    /// channel to fit the UNS topic budget. Ordinary derivation on a deep machine, not a fault —
    /// the caller logs it at debug.
    pub channel_truncated: usize,
    /// How many served signals could not fit the budget **even as their id alone** — the
    /// pathological floor, where the instance's own identity has consumed the topic. The caller
    /// raises a warning event: these signals will not publish.
    pub channel_unfit: usize,
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
/// * Every derived channel fits the instance's UNS topic budget ([`derive_channel`]): on a deep
///   component path the root-side segments drop, and the counts land in
///   [`ServedSet::channel_truncated`] / [`ServedSet::channel_unfit`].
#[must_use]
pub fn served_set(
    explicit: &[SignalConfig],
    selection: Option<&SelectionConfig>,
    model: Option<&ProbeModel>,
) -> ServedSet {
    let mut out: Vec<ServedSignal> = explicit
        .iter()
        .map(|s| ServedSignal {
            signal: s.clone(),
            provenance: Provenance::Configured,
        })
        .collect();

    let bare = |signals: Vec<ServedSignal>| ServedSet {
        signals,
        derived_matched: 0,
        derived_truncated: 0,
        channel_truncated: 0,
        channel_unfit: 0,
    };
    let (Some(sel), Some(model)) = (selection, model) else {
        return bare(out);
    };
    if sel.mode == SelectionMode::Explicit {
        return bare(out);
    }

    let include: Vec<CompiledMatcher> = sel
        .include
        .iter()
        .filter_map(CompiledMatcher::compile)
        .collect();
    let exclude: Vec<CompiledMatcher> = sel
        .exclude
        .iter()
        .filter_map(CompiledMatcher::compile)
        .collect();

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
    let mut channel_truncated = 0usize;
    let mut channel_unfit = 0usize;

    for item in items_in_tree_order(model) {
        let selected = (sel.mode == SelectionMode::All || include.iter().any(|m| m.matches(item)))
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
                    mode: sel.default_publish_mode,
                    batch_ms: sel.default_batch_ms,
                    deadband: None,
                },
                // EVENT/CONDITION: on-change, immediate.
                _ => PublishCfg {
                    mode: PublishMode::OnChange,
                    batch_ms: 0,
                    deadband: None,
                },
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
                    served.name = Some(item.name.clone().unwrap_or_else(|| derived_name(item)));
                }
                if served.channel.is_none() {
                    // An explicit entry that names no channel takes the derived one — so it is
                    // shaped to the same budget. A hand-set channel is never touched: it is the
                    // operator's statement, and the library refuses it loudly if it does not fit.
                    let id = served.id.clone();
                    let derived = derive_channel(&item.component_path, &id, sel.channel_budget);
                    if derived.dropped > 0 {
                        channel_truncated += 1;
                    }
                    if !derived.fits {
                        channel_unfit += 1;
                    }
                    served.channel = Some(derived.channel);
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
        let channel = derive_channel(&item.component_path, &id, sel.channel_budget);
        if channel.dropped > 0 {
            channel_truncated += 1;
        }
        if !channel.fits {
            channel_unfit += 1;
        }
        out.push(ServedSignal {
            signal: SignalConfig {
                id,
                name: Some(item.name.clone().unwrap_or_else(|| derived_name(item))),
                channel: Some(channel.channel),
                data_item_id: item.id.clone(),
                condition_binding: auto_binding(),
                publish: Some(default_publish()),
            },
            provenance: Provenance::Discovered,
        });
    }

    ServedSet {
        signals: out,
        derived_matched,
        derived_truncated,
        channel_truncated,
        channel_unfit,
    }
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
    /// The deep-path device: the demo Mazak's `stock` shape and worse.
    const DEVICES_DEEP: &str = include_str!("../../tests/fixtures/devices_deep_2.7.xml");

    fn model() -> ProbeModel {
        ProbeModel::from_devices(&parse_devices(DEVICES_2_7).unwrap(), "OKUMA.123456").unwrap()
    }

    fn deep_model() -> ProbeModel {
        ProbeModel::from_devices(&parse_devices(DEVICES_DEEP).unwrap(), "Mazak").unwrap()
    }

    /// The live demo Mazak's `stock` component path, verbatim from `demo.mtconnect.org/probe`.
    const MAZAK_STOCK_PATH: &str = "Resources[resources]/Materials[materials]/Stock[stock]";

    /// The budget of an ordinary rootless, instance-scoped adapter topic.
    fn budget(tokens: usize, bytes: usize) -> ChannelBudget {
        ChannelBudget {
            max_tokens: tokens,
            max_bytes: bytes,
        }
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
        set.signals
            .iter()
            .find(|s| s.signal.id == id)
            .unwrap_or_else(|| panic!("no `{id}`"))
    }

    // --- configuration shape -------------------------------------------------------------------

    #[test]
    fn the_selection_block_deserializes_with_its_defaults() {
        let s = sel(json!({ "mode": "all" }));
        assert_eq!(s.mode, SelectionMode::All);
        assert!(s.include.is_empty() && s.exclude.is_empty());
        assert_eq!(s.max_signals, DEFAULT_MAX_SIGNALS, "the 500 default");
        assert!(
            s.auto_condition_binding,
            "auto conditionBinding is on by default"
        );
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
        assert!(
            serde_json::from_value::<SelectionConfig>(json!({ "mode": "all", "nope": 1 })).is_err()
        );
        assert!(serde_json::from_value::<Matcher>(json!({ "types": "POSITION" })).is_err());
    }

    // --- validation ----------------------------------------------------------------------------

    #[test]
    fn validation_rejects_bad_patterns_and_inert_combinations() {
        // A regex that does not compile is refused at load, naming the field.
        let bad = sel(json!({ "mode": "include", "include": [{ "type": "(" }] }));
        let err = validate_selection("cnc-1", &bad).unwrap_err().to_string();
        assert!(
            err.contains("cnc-1") && err.contains("include[0]") && err.contains("type"),
            "{err}"
        );

        let bad = sel(json!({ "mode": "all", "exclude": [{ "idMatch": "[" }] }));
        assert!(validate_selection("cnc-1", &bad).is_err());

        // An unknown category is a mistake, not a matcher that never fires.
        let bad = sel(json!({ "mode": "include", "include": [{ "category": "sample" }] }));
        assert!(
            validate_selection("cnc-1", &bad).is_err(),
            "categories are the exact tokens"
        );

        // Inert combinations are refused rather than silently selecting nothing.
        let inert = sel(json!({ "mode": "explicit", "include": [{ "type": "POSITION" }] }));
        assert!(validate_selection("cnc-1", &inert).is_err());
        let inert = sel(json!({ "mode": "include" }));
        assert!(
            validate_selection("cnc-1", &inert).is_err(),
            "include mode needs matchers"
        );
        let inert = sel(json!({ "mode": "all", "include": [{ "type": "POSITION" }] }));
        assert!(
            validate_selection("cnc-1", &inert).is_err(),
            "all already includes everything"
        );

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
        assert_eq!(
            ids(&served_set(&[], Some(&loose), Some(&m))),
            vec!["xabs", "xtravel"]
        );
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
        assert!(set
            .signals
            .iter()
            .all(|s| s.signal.data_item_id != "Xtravel"));
        assert!(set.signals.iter().all(|s| s.signal.data_item_id != "logic"));
    }

    #[test]
    fn sub_type_and_id_match_are_anchored_regexes() {
        let m = model();
        // Anchored: `POSITION` must not select `PATH_POSITION` — the whole field matches.
        let s = sel(json!({ "mode": "include", "include": [{ "type": "POSITION" }] }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert!(
            set.signals.iter().all(|s| s.signal.data_item_id != "Ppos"),
            "no partial-match creep into PATH_POSITION"
        );

        // subType: an item WITHOUT a subType never matches a subType regex.
        let s = sel(json!({ "mode": "include", "include": [{ "subType": "ACTUAL" }] }));
        assert_eq!(
            ids(&served_set(&[], Some(&s), Some(&m))),
            vec!["xabs", "sspeed"]
        );

        // idMatch is a real regex.
        let s = sel(json!({ "mode": "include", "include": [{ "idMatch": "X(abs|load)" }] }));
        assert_eq!(
            ids(&served_set(&[], Some(&s), Some(&m))),
            vec!["xabs", "xload"]
        );

        // An empty matcher matches everything (AND of no constraints).
        let s = sel(json!({ "mode": "include", "include": [{}] }));
        assert_eq!(served_set(&[], Some(&s), Some(&m)).signals.len(), 14);
    }

    #[test]
    fn path_globs_match_segment_wise_with_double_star() {
        assert!(glob_match("Axes/Linear[X]", "Axes/Linear[X]"));
        assert!(glob_match("Axes/*", "Axes/Linear[X]"));
        assert!(
            !glob_match("Axes/*", "Axes"),
            "* is one whole segment, not optional"
        );
        assert!(glob_match("Axes/**", "Axes"));
        assert!(glob_match("Axes/**", "Axes/Linear[X]"));
        assert!(glob_match("**/Linear[X]", "Axes/Linear[X]"));
        assert!(
            glob_match("**", ""),
            "** spans zero segments: the device level matches"
        );
        assert!(
            glob_match("", ""),
            "the empty pattern is the device level exactly"
        );
        assert!(!glob_match("", "Axes"));
        assert!(glob_match("Axes/Linear[?]", "Axes/Linear[X]"));
        assert!(!glob_match("Axes/Linear[?]", "Axes/Linear[XY]"));
        assert!(
            glob_match("**/Path[P1]/**", "Controller/Path[P1]"),
            "trailing ** spans zero"
        );
        assert!(!glob_match("Controller", "Controller/Path[P1]"));

        let m = model();
        let s = sel(json!({ "mode": "include", "include": [{ "path": "Axes/**" }] }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert_eq!(
            ids(&set),
            vec!["xabs", "xload", "xfreq", "xtravel", "sspeed"]
        );

        // The device level is the empty path.
        let s = sel(json!({ "mode": "include", "include": [{ "path": "" }] }));
        assert_eq!(
            ids(&served_set(&[], Some(&s), Some(&m))),
            vec!["avail", "asset-changed"]
        );
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
            assert!(
                crate::mtconnect::config::is_lower_kebab(&sanitize_token(raw)),
                "{raw}"
            );
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
        assert_eq!(
            x.signal.name.as_deref(),
            Some("Xabs"),
            "the probe's own name wins"
        );
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
        assert_eq!(
            asset.signal.publish_policy(),
            PublishCfg {
                mode: PublishMode::OnChange,
                batch_ms: 0,
                deadband: None
            }
        );

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
        assert_eq!(
            set2.signals[0].signal.name.as_deref(),
            Some("POSITION COMMANDED")
        );

        // A device-level item publishes on its id alone.
        let avail = find(&set, "avail");
        assert_eq!(avail.signal.channel.as_deref(), Some("avail"));

        // A CONDITION signal publishes its state and binds no conditions of its own.
        let travel = find(&set, "xtravel");
        assert!(travel.signal.condition_bindings().is_empty());
    }

    #[test]
    fn the_sample_batch_window_and_mode_come_from_the_resolved_defaults() {
        let m = model();
        let mut s = sel(json!({ "mode": "include", "include": [{ "idMatch": "Xabs" }] }));
        s.default_batch_ms = 250;
        s.default_publish_mode = PublishMode::Interval;
        let set = served_set(&[], Some(&s), Some(&m));
        let p = set.signals[0].signal.publish_policy();
        assert_eq!(
            p.batch_ms, 250,
            "defaults.batchMs is the derived SAMPLE window"
        );
        assert_eq!(
            p.mode,
            PublishMode::Interval,
            "defaults.publishMode is the derived SAMPLE mode"
        );
        // ... and events stay on-change immediate regardless.
        let mut s = sel(json!({ "mode": "include", "include": [{ "idMatch": "execution" }] }));
        s.default_batch_ms = 250;
        s.default_publish_mode = PublishMode::Interval;
        let set = served_set(&[], Some(&s), Some(&m));
        let p = set.signals[0].signal.publish_policy();
        assert_eq!(p.batch_ms, 0);
        assert_eq!(
            p.mode,
            PublishMode::OnChange,
            "an EVENT's state is never latest-only coalesced"
        );
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
            assert_eq!(
                find(&set, id).signal.condition_bindings(),
                ["Xtravel"],
                "{id}"
            );
        }
        // A different component's items do not: Sspeed is on Rotary[C], which has no condition.
        assert!(find(&set, "sspeed").signal.condition_bindings().is_empty());
        // Path[P1] has its own condition (`logic`), bound by its non-condition items.
        assert_eq!(
            find(&set, "execution").signal.condition_bindings(),
            ["logic"]
        );
        // Device-level items bind device-level conditions (there are none in the fixture).
        assert!(find(&set, "avail").signal.condition_bindings().is_empty());

        // The opt-out flag turns the whole derivation off.
        let s = sel(json!({ "mode": "all", "autoConditionBinding": false }));
        let set = served_set(&[], Some(&s), Some(&m));
        assert!(set
            .signals
            .iter()
            .all(|s| s.signal.condition_bindings().is_empty()));
    }

    // --- depth-aware channel derivation ---------------------------------------------------------

    #[test]
    fn a_deep_path_keeps_its_leaf_most_segments_and_always_the_id() {
        // The acceptance case: the live demo Mazak's `stock`. Four channel tokens where three
        // fit — today's rule produced an unpublishable topic (DEPTH_EXCEEDED).
        let full = derive_channel(MAZAK_STOCK_PATH, "stock", budget(9, 256));
        assert_eq!(
            full.channel, "resources-resources/materials-materials/stock-stock/stock",
            "with room to spare the whole path is still the channel"
        );
        assert_eq!(full.dropped, 0);

        // Against the real budget of an instance-scoped topic the ROOT-side segment drops: what
        // is left names the thing (`materials/stock`), which is the informative half.
        let fitted = derive_channel(MAZAK_STOCK_PATH, "stock", budget(3, 256));
        assert_eq!(fitted.channel, "materials-materials/stock-stock/stock");
        assert_eq!(fitted.dropped, 1);
        assert!(fitted.fits);
        assert_eq!(
            fitted.channel.split('/').count(),
            3,
            "exactly the budget, never over"
        );

        // Deeper paths drop more, and the id is never one of the casualties.
        let six = "A/B/C/D/E/F";
        for tokens in 1..=7 {
            let d = derive_channel(six, "sig", budget(tokens, 256));
            assert_eq!(
                d.channel.split('/').count(),
                tokens.min(7),
                "tokens={tokens}"
            );
            assert!(
                d.channel.ends_with("sig"),
                "the id is terminal: {}",
                d.channel
            );
            assert_eq!(d.dropped, 6 - (tokens.min(7) - 1));
            // The kept segments are a SUFFIX of the path, in order.
            let kept: Vec<&str> = d.channel.split('/').collect();
            let expected: Vec<&str> = "a/b/c/d/e/f".split('/').skip(d.dropped).collect();
            assert_eq!(kept[..kept.len() - 1], expected[..]);
        }

        // A shallow path and a device-level item are untouched by the rule.
        assert_eq!(
            derive_channel("Axes/Linear[X]", "xabs", budget(3, 256)).channel,
            "axes/linear-x/xabs"
        );
        assert_eq!(derive_channel("", "avail", budget(3, 256)).channel, "avail");
        assert_eq!(derive_channel("", "avail", budget(3, 256)).dropped, 0);
    }

    #[test]
    fn k_is_the_largest_that_fits_and_the_choice_is_deterministic() {
        // Same inputs, same answer — twice, and independently of what came before it.
        let a = derive_channel(MAZAK_STOCK_PATH, "stock", budget(3, 256));
        let b = derive_channel(MAZAK_STOCK_PATH, "stock", budget(3, 256));
        assert_eq!(a, b);

        // k grows monotonically with the budget and never overshoots the path.
        let mut previous = 0usize;
        for tokens in 1..=8 {
            let kept = derive_channel(MAZAK_STOCK_PATH, "stock", budget(tokens, 256))
                .channel
                .split('/')
                .count()
                - 1;
            assert!(kept >= previous, "k must not shrink as the budget grows");
            assert!(kept <= 3, "there are only three path segments to keep");
            previous = kept;
        }
        assert_eq!(previous, 3, "a big enough budget keeps the whole path");
    }

    #[test]
    fn a_long_identity_squeezes_k_through_the_byte_budget() {
        // `Systems[systems]/Hydraulic[hydraulic]/Pump[pump]/Motor[motor]/Sensor[sensor]` — five
        // segments. With tokens to spare, BYTES decide how many survive.
        let path = "Systems[systems]/Hydraulic[hydraulic]/Pump[pump]/Motor[motor]/Sensor[sensor]";
        let whole = derive_channel(path, "ptemp", budget(9, 256));
        assert_eq!(whole.dropped, 0);
        let len = whole.channel.len();

        // One byte short of the whole thing: the root-side segment goes, nothing else.
        let squeezed = derive_channel(path, "ptemp", budget(9, len - 1));
        assert_eq!(squeezed.dropped, 1);
        assert!(squeezed.channel.len() < len);
        assert!(squeezed.channel.starts_with("hydraulic-hydraulic/"));

        // Squeezing harder keeps dropping, monotonically, down to the id alone.
        let mut previous = 0usize;
        for bytes in (5..=len).rev() {
            let d = derive_channel(path, "ptemp", budget(9, bytes));
            assert!(d.channel.len() <= bytes, "bytes={bytes}: {}", d.channel);
            assert!(
                d.dropped >= previous,
                "k must not grow as the budget shrinks"
            );
            assert!(d.fits, "the id itself always fits five bytes");
            previous = d.dropped;
        }
        assert_eq!(derive_channel(path, "ptemp", budget(9, 5)).channel, "ptemp");

        // Both limits bind at once: the tighter of the two wins.
        let by_tokens = derive_channel(path, "ptemp", budget(2, 256));
        assert_eq!(by_tokens.channel, "sensor-sensor/ptemp");
        let by_bytes = derive_channel(path, "ptemp", budget(9, "sensor-sensor/ptemp".len()));
        assert_eq!(by_bytes.channel, "sensor-sensor/ptemp");
    }

    #[test]
    fn the_floor_is_the_id_alone_and_says_so_when_even_that_does_not_fit() {
        // k = 0 is the floor: the id publishes on its own, and that is a fit.
        let floored = derive_channel(MAZAK_STOCK_PATH, "stock", budget(1, 256));
        assert_eq!(floored.channel, "stock");
        assert_eq!(floored.dropped, 3);
        assert!(floored.fits);

        // The pathological cases: no token budget at all, or an id longer than the bytes left.
        // The channel is still the id — this module invents no name — and `fits` is the flag the
        // caller turns into a warning, after which the library refuses the topic on publish.
        let no_tokens = derive_channel(MAZAK_STOCK_PATH, "stock", budget(0, 256));
        assert_eq!(no_tokens.channel, "stock");
        assert!(!no_tokens.fits);

        let no_bytes = derive_channel("", "a-very-long-signal-id", budget(3, 4));
        assert_eq!(no_bytes.channel, "a-very-long-signal-id");
        assert!(!no_bytes.fits);

        // Exactly at the byte limit still fits.
        assert!(derive_channel("", "stock", budget(1, 5)).fits);
        assert!(!derive_channel("", "stock", budget(1, 4)).fits);
    }

    #[test]
    fn the_mazak_stock_signal_publishes_under_mode_all() {
        // The bug, end to end: `mode: "all"` over the deep device, at the budget an ordinary
        // instance really has.
        let m = deep_model();
        assert_eq!(
            m.item("stock")
                .expect("the fixture reproduces the demo item")
                .component_path,
            MAZAK_STOCK_PATH,
            "the fixture is the live shape, not an approximation"
        );

        let mut s = sel(json!({ "mode": "all" }));
        s.channel_budget = budget(3, 210);
        let set = served_set(&[], Some(&s), Some(&m));

        // Every served channel fits — the whole point.
        for served in &set.signals {
            let channel = served.signal.channel.as_deref().expect("a derived channel");
            assert!(channel.split('/').count() <= 3, "{channel}");
            assert!(channel.len() <= 210, "{channel}");
        }

        // And `stock` in particular, which used to be dropped by the library as DEPTH_EXCEEDED.
        assert_eq!(
            find(&set, "stock").signal.channel.as_deref(),
            Some("materials-materials/stock-stock/stock")
        );
        // Its five-level sibling keeps its two leaf-most segments.
        assert_eq!(
            find(&set, "ptemp").signal.channel.as_deref(),
            Some("motor-motor/sensor-sensor/ptemp")
        );
        // Shallow signals on the same device are untouched.
        assert_eq!(find(&set, "avail").signal.channel.as_deref(), Some("avail"));
        assert_eq!(
            find(&set, "estop").signal.channel.as_deref(),
            Some("controller-controller/estop")
        );
        // A two-segment path with LONG tokens still fits whole - the byte budget is real room,
        // not a guess: 114 of the 210 bytes an ordinary instance has left.
        assert_eq!(
            find(&set, "etemp").signal.channel.as_deref(),
            Some(concat!(
                "auxiliaries-auxiliary-equipment-subsystem-assembly/",
                "work-envelope-work-envelope-conditioning-circuit-assembly/etemp"
            ))
        );

        // The counts: the three deep signals were shaped, none hit the floor.
        assert_eq!(set.channel_truncated, 3);
        assert_eq!(set.channel_unfit, 0);
    }

    #[test]
    fn the_terminal_id_keeps_every_derived_channel_unique() {
        // The uniqueness claim the rule rests on: ids are unique per instance (validated at config
        // load for the explicit half, suffixed `-2`/`-3` for the derived half), and the id is the
        // terminal segment — so dropping ANY amount of path cannot collide two channels.
        let m = deep_model();
        for tokens in 1..=4 {
            let mut s = sel(json!({ "mode": "all" }));
            s.channel_budget = budget(tokens, 256);
            let set = served_set(&[], Some(&s), Some(&m));
            let channels: BTreeSet<&str> = set
                .signals
                .iter()
                .filter_map(|x| x.signal.channel.as_deref())
                .collect();
            assert_eq!(
                channels.len(),
                set.signals.len(),
                "tokens={tokens}: channels collided"
            );
            let ids: BTreeSet<&str> = set.signals.iter().map(|x| x.signal.id.as_str()).collect();
            assert_eq!(ids.len(), set.signals.len(), "ids are unique per instance");
            for x in &set.signals {
                let channel = x.signal.channel.as_deref().unwrap();
                assert_eq!(channel.rsplit('/').next(), Some(x.signal.id.as_str()));
            }
        }

        // Even the pathological floor keeps them apart: at k = 0 every channel IS its id.
        let mut s = sel(json!({ "mode": "all" }));
        s.channel_budget = budget(1, 256);
        let set = served_set(&[], Some(&s), Some(&m));
        let channels: BTreeSet<&str> = set
            .signals
            .iter()
            .filter_map(|x| x.signal.channel.as_deref())
            .collect();
        assert_eq!(channels.len(), set.signals.len());
    }

    #[test]
    fn a_pathological_budget_is_counted_not_hidden() {
        let m = deep_model();
        let mut s = sel(json!({ "mode": "all" }));
        s.channel_budget = budget(0, 0);
        let set = served_set(&[], Some(&s), Some(&m));
        assert_eq!(
            set.channel_unfit,
            set.signals.len(),
            "every signal reports the floor"
        );
        // The channel is still the id, so the library's own validation is what refuses it.
        assert_eq!(find(&set, "stock").signal.channel.as_deref(), Some("stock"));
    }

    #[test]
    fn an_explicit_channel_is_never_reshaped_but_an_omitted_one_is() {
        let m = deep_model();
        let mut s = sel(json!({ "mode": "all" }));
        s.channel_budget = budget(3, 256);

        // A hand-set channel is the operator's statement: it is taken verbatim, however deep —
        // the library refuses it loudly on publish if it does not fit, which is the point.
        let pinned: SignalConfig = serde_json::from_value(json!({
            "id": "raw-stock", "dataItemId": "stock",
            "channel": "a/deliberately/deep/hand/written/path"
        }))
        .unwrap();
        let set = served_set(&[pinned], Some(&s), Some(&m));
        assert_eq!(
            find(&set, "raw-stock").signal.channel.as_deref(),
            Some("a/deliberately/deep/hand/written/path")
        );
        assert_eq!(set.channel_truncated, 2, "the derived half is still shaped");

        // An explicit entry that omits `channel` takes the derived one, shaped to the same budget.
        let set = served_set(&[explicit("raw-stock", "stock")], Some(&s), Some(&m));
        assert_eq!(
            find(&set, "raw-stock").signal.channel.as_deref(),
            Some("materials-materials/stock-stock/raw-stock"),
            "the shaped channel carries the EXPLICIT id"
        );
    }

    #[test]
    fn shaping_a_channel_changes_nothing_else_about_the_served_signal() {
        // Provenance, ids, names, bindings and publish policies are untouched by the budget: only
        // the channel is shaped, and the full path stays in the model behind `sb/browse`.
        let m = deep_model();
        let wide = {
            let mut s = sel(json!({ "mode": "all" }));
            s.channel_budget = budget(9, 256);
            served_set(&[], Some(&s), Some(&m))
        };
        let narrow = {
            let mut s = sel(json!({ "mode": "all" }));
            s.channel_budget = budget(2, 60);
            served_set(&[], Some(&s), Some(&m))
        };
        assert_eq!(wide.signals.len(), narrow.signals.len());
        for (w, n) in wide.signals.iter().zip(&narrow.signals) {
            assert_eq!(w.provenance, n.provenance);
            assert_eq!(w.signal.id, n.signal.id);
            assert_eq!(w.signal.name, n.signal.name);
            assert_eq!(w.signal.data_item_id, n.signal.data_item_id);
            assert_eq!(w.signal.condition_binding, n.signal.condition_binding);
            assert_eq!(w.signal.publish, n.signal.publish);
        }
        assert_eq!(wide.derived_matched, narrow.derived_matched);
        assert_eq!(wide.channel_truncated, 0);

        // The untruncated component path is still what the model serves as `signal.address`.
        let address = m.address_of("line-a-agent", "stock").expect("an address");
        assert_eq!(
            address["componentPath"],
            json!(MAZAK_STOCK_PATH),
            "nothing is lost"
        );
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
        assert_eq!(
            x.signal.name.as_deref(),
            Some("Xabs"),
            "unset name takes the derived one"
        );
        assert_eq!(
            x.signal.channel.as_deref(),
            Some("axes/linear-x/x-position"),
            "the derived channel carries the EXPLICIT id"
        );
        assert_eq!(
            x.signal.condition_bindings(),
            ["Xtravel"],
            "unset binding takes the auto one"
        );
        assert_eq!(x.signal.publish_policy().mode, PublishMode::OnChange);
        // ... and no second entry serves Xabs.
        assert_eq!(
            set.signals
                .iter()
                .filter(|s| s.signal.data_item_id == "Xabs")
                .count(),
            1
        );

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
        assert!(
            x.signal.condition_bindings().is_empty(),
            "conditionBinding: [] explicitly clears the auto binding"
        );
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
            assert_eq!(
                set.signals[0].signal, bare[0],
                "no enrichment without a selection"
            );
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
        assert_eq!(
            ids(&set),
            vec!["avail", "asset-changed", "xabs"],
            "the FIRST three, tree order"
        );
        assert_eq!(set.derived_matched, 14);
        assert_eq!(
            set.derived_truncated, 11,
            "the cut is counted, never silent"
        );

        // Explicit signals do not count against the cap.
        let explicit = vec![explicit("spindle", "Sspeed"), explicit("prog", "program")];
        let set = served_set(&explicit, Some(&s), Some(&m));
        assert_eq!(set.signals.len(), 5, "2 explicit + 3 derived");
        assert_eq!(
            set.signals
                .iter()
                .filter(|s| s.provenance == Provenance::Discovered)
                .count(),
            3
        );
        // ... and the explicitly-bound items were never derived candidates at all.
        assert_eq!(set.derived_matched, 12);
    }
}
