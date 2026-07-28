//! # Configuration types (LLD §2 / §8)
//!
//! The shapes `component.global.agents[]` and each `component.instances[]` entry deserialize into.
//! Two rules shape this module:
//!
//! * **Secrets are references, never values.** [`AuthRef`]/[`TlsRef`] name vault secrets; the
//!   *resolved* material ([`AgentCredentials`]) is handed in from the `device.rs` boundary, which is
//!   the only place that knows the EdgeCommons credential service exists.
//! * **Structure is validated here, semantics too.** [`parse_agents`] rejects a malformed agent
//!   list, and [`validate_bindings`] rejects a device pointing at an agent that is not configured,
//!   two devices claiming the same uuid on one agent, or a signal whose condition binding names its
//!   own data item.

use std::collections::{BTreeSet, HashMap};

use serde::Deserialize;
use serde_json::Value;
use url::Url;

use super::error::MtcError;

/// The standard's default agent heartbeat, in milliseconds (Part 1 §5.1.3.1.1).
pub const DEFAULT_HEARTBEAT_MS: u32 = 10_000;
/// Default `/current` cadence for the polling path.
pub const DEFAULT_POLL_INTERVAL_MS: u32 = 1_000;
/// Default one-shot request timeout.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u32 = 10_000;
/// Default response/part size cap: 16 MiB.
pub const DEFAULT_MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;

// =================================================================================================
// Agents
// =================================================================================================

/// One entry of `component.global.agents[]` — an MTConnect agent, declared once and shared by every
/// device instance that names it (D-MTC-3).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentConfig {
    /// Stable, lower-kebab, unique across `agents[]`.
    pub id: String,
    /// The agent's base URL. `http`/`https` only, and never with embedded userinfo — credentials
    /// come from the vault.
    #[serde(deserialize_with = "de_base_url")]
    pub url: Url,
    /// HTTP authentication, by reference.
    #[serde(default)]
    pub auth: Option<AuthRef>,
    /// TLS material, by reference.
    #[serde(default)]
    pub tls: Option<TlsRef>,
    /// The `heartbeat` query parameter for streaming requests, and the liveness window.
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_ms: u32,
    /// Whether to stream (`prefer`) or poll only.
    #[serde(default)]
    pub streaming: StreamPolicy,
    /// `/current` cadence for the polling path (the fallback, and the only path under
    /// [`StreamPolicy::PollOnly`]).
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u32,
    /// One-shot (`/probe`, `/current`, windowed `/sample`) request timeout.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u32,
    /// Response/part size cap. A document larger than this is refused rather than buffered.
    #[serde(default = "default_max_document_bytes")]
    pub max_document_bytes: usize,
    /// Reconnect bounds for this agent's acquisition task.
    #[serde(default)]
    pub reconnect: ReconnectCfg,
}

/// Whether the agent runtime streams or polls (HLD §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamPolicy {
    /// Stream `/sample?interval=…`, falling back to `/current` polling when the stream cannot be
    /// established.
    #[default]
    Prefer,
    /// Never stream: `/current` at `pollIntervalMs`.
    PollOnly,
}

/// Reconnect bounds (capped exponential with full jitter, applied by the supervisor).
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReconnectCfg {
    pub initial_ms: u64,
    pub max_ms: u64,
}

impl Default for ReconnectCfg {
    fn default() -> Self {
        Self { initial_ms: 1_000, max_ms: 60_000 }
    }
}

/// HTTP authentication **by reference**: the value lives in the EdgeCommons vault, never in config.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AuthRef {
    /// HTTP Basic: a plain username plus a vault reference for the password.
    #[serde(rename_all = "camelCase")]
    Basic { username: String, secret_ref: String },
    /// A bearer token held in the vault.
    #[serde(rename_all = "camelCase")]
    Bearer { secret_ref: String },
}

/// TLS material **by reference** — a private CA to trust, and optionally a client identity.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TlsRef {
    /// A PEM CA bundle to trust in addition to the platform roots.
    #[serde(default)]
    pub ca_secret_ref: Option<String>,
    /// A PEM client certificate (mutual TLS). Requires [`Self::key_secret_ref`].
    #[serde(default)]
    pub cert_secret_ref: Option<String>,
    /// The PEM private key for [`Self::cert_secret_ref`].
    #[serde(default)]
    pub key_secret_ref: Option<String>,
}

// =================================================================================================
// Resolved credential material (handed in from device.rs; this module never resolves it)
// =================================================================================================

/// Resolved HTTP authentication — the value, not the reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMaterial {
    Basic { username: String, password: String },
    Bearer { token: String },
}

/// Resolved TLS material — PEM text, not references.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsMaterial {
    pub ca_pem: Option<String>,
    pub client_cert_pem: Option<String>,
    pub client_key_pem: Option<String>,
}

/// Everything an agent's HTTP client needs that came out of the vault. Constructed at the
/// `device.rs` boundary and passed down; `src/mtconnect/**` never learns where it came from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentCredentials {
    pub auth: Option<AuthMaterial>,
    pub tls: Option<TlsMaterial>,
}

// =================================================================================================
// Devices and signals
// =================================================================================================

/// One `component.instances[]` entry, in the client's own vocabulary: which agent serves it, which
/// MTConnect device it is, and what to publish from it.
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceConfig {
    /// The EdgeCommons instance id (the `{instance}` UNS token).
    pub id: String,
    /// The `agents[]` entry serving this device.
    pub agent_id: String,
    /// The MTConnect `Device/@uuid` this instance represents.
    pub device_uuid: String,
    /// The configured signal set. Empty means "publish nothing" — signals are explicit.
    pub signals: Vec<SignalConfig>,
}

/// One configured signal: a stable EdgeCommons identity bound to one MTConnect `dataItemId`
/// (HLD §5.3, D-MTC-5).
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignalConfig {
    /// The stable EdgeCommons id — lower-kebab, and the `signal.id` on the wire.
    pub id: String,
    /// A human label.
    #[serde(default)]
    pub name: Option<String>,
    /// An explicit `signal_path` override (else the id is used).
    #[serde(default)]
    pub channel: Option<String>,
    /// The binding key: `DataItem/@id` within this device.
    pub data_item_id: String,
    /// Condition data items whose state degrades this signal's quality (HLD §6, D-MTC-8).
    #[serde(default)]
    pub condition_binding: Vec<String>,
    /// How this signal is published.
    #[serde(default)]
    pub publish: PublishCfg,
}

/// Per-signal publish policy.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublishCfg {
    /// `on-change` (the default) or `interval`.
    #[serde(default)]
    pub mode: PublishMode,
    /// Coalescing window for `interval` mode, in milliseconds.
    #[serde(default)]
    pub batch_ms: u32,
    /// Absolute deadband — SAMPLE-category signals only. A change smaller than this is suppressed.
    #[serde(default)]
    pub deadband: Option<f64>,
}

impl Default for PublishCfg {
    fn default() -> Self {
        Self { mode: PublishMode::OnChange, batch_ms: 0, deadband: None }
    }
}

/// When a signal is published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PublishMode {
    /// Publish each accepted observation as it arrives.
    #[default]
    OnChange,
    /// Publish the latest value on a fixed cadence.
    Interval,
}

// =================================================================================================
// Parsing + cross-invariant validation
// =================================================================================================

/// Parse `component.global.agents[]`.
///
/// # Errors
/// [`MtcError::Config`] when the key is missing/not an array, when an entry is malformed, when the
/// list is empty, or when two entries share an id or a URL.
pub fn parse_agents(global: &Value) -> Result<Vec<AgentConfig>, MtcError> {
    let raw = global
        .get("agents")
        .ok_or_else(|| MtcError::Config("component.global.agents[] is required".into()))?;
    let arr = raw
        .as_array()
        .ok_or_else(|| MtcError::Config("component.global.agents must be an array".into()))?;
    if arr.is_empty() {
        return Err(MtcError::Config("component.global.agents[] must not be empty".into()));
    }

    let mut agents = Vec::with_capacity(arr.len());
    for entry in arr {
        let a: AgentConfig = serde_json::from_value(entry.clone())
            .map_err(|e| MtcError::Config(format!("malformed agent entry: {e}")))?;
        validate_agent(&a)?;
        agents.push(a);
    }

    let mut ids = BTreeSet::new();
    let mut urls = BTreeSet::new();
    for a in &agents {
        if !ids.insert(a.id.clone()) {
            return Err(MtcError::Config(format!("duplicate agent id `{}`", a.id)));
        }
        if !urls.insert(a.url.as_str().to_string()) {
            return Err(MtcError::Config(format!("duplicate agent url `{}`", a.url)));
        }
    }
    Ok(agents)
}

/// Per-agent structural checks that serde cannot express.
///
/// # Errors
/// [`MtcError::Config`] for a non-kebab id, a non-positive interval, a zero size cap, or TLS
/// material that names a certificate without its key.
pub fn validate_agent(a: &AgentConfig) -> Result<(), MtcError> {
    if !is_lower_kebab(&a.id) {
        return Err(MtcError::Config(format!("agent id `{}` must be lower-kebab", a.id)));
    }
    if a.heartbeat_ms == 0 || a.poll_interval_ms == 0 || a.request_timeout_ms == 0 {
        return Err(MtcError::Config(format!(
            "agent `{}`: heartbeatMs/pollIntervalMs/requestTimeoutMs must be > 0",
            a.id
        )));
    }
    if a.max_document_bytes == 0 {
        return Err(MtcError::Config(format!("agent `{}`: maxDocumentBytes must be > 0", a.id)));
    }
    if a.reconnect.initial_ms == 0 || a.reconnect.max_ms < a.reconnect.initial_ms {
        return Err(MtcError::Config(format!(
            "agent `{}`: reconnect.initialMs must be > 0 and <= reconnect.maxMs",
            a.id
        )));
    }
    if let Some(tls) = &a.tls {
        if tls.cert_secret_ref.is_some() != tls.key_secret_ref.is_some() {
            return Err(MtcError::Config(format!(
                "agent `{}`: tls.certSecretRef and tls.keySecretRef must be set together",
                a.id
            )));
        }
    }
    Ok(())
}

/// The cross-object invariants of LLD §8: every device names a configured agent, device uuids are
/// unique per agent, signal ids are unique per device, and a signal never binds its own data item
/// as a condition.
///
/// # Errors
/// [`MtcError::Config`] naming the offending device/signal.
pub fn validate_bindings(agents: &[AgentConfig], devices: &[DeviceConfig]) -> Result<(), MtcError> {
    let known: BTreeSet<&str> = agents.iter().map(|a| a.id.as_str()).collect();
    let mut per_agent_uuids: HashMap<&str, BTreeSet<&str>> = HashMap::new();

    for d in devices {
        if !known.contains(d.agent_id.as_str()) {
            return Err(MtcError::Config(format!(
                "device `{}` references unknown agent `{}`",
                d.id, d.agent_id
            )));
        }
        if d.device_uuid.trim().is_empty() {
            return Err(MtcError::Config(format!("device `{}`: deviceUuid must not be empty", d.id)));
        }
        if !per_agent_uuids.entry(&d.agent_id).or_default().insert(&d.device_uuid) {
            return Err(MtcError::Config(format!(
                "agent `{}` has two devices with uuid `{}`",
                d.agent_id, d.device_uuid
            )));
        }

        let mut signal_ids = BTreeSet::new();
        for s in &d.signals {
            if !signal_ids.insert(s.id.as_str()) {
                return Err(MtcError::Config(format!(
                    "device `{}` has two signals with id `{}`",
                    d.id, s.id
                )));
            }
            if s.data_item_id.trim().is_empty() {
                return Err(MtcError::Config(format!(
                    "device `{}` signal `{}`: dataItemId must not be empty",
                    d.id, s.id
                )));
            }
            if s.condition_binding.iter().any(|c| c == &s.data_item_id) {
                return Err(MtcError::Config(format!(
                    "device `{}` signal `{}`: conditionBinding must not name its own dataItemId",
                    d.id, s.id
                )));
            }
        }
    }
    Ok(())
}

/// Whether a token is lower-kebab (`^[a-z0-9]+(-[a-z0-9]+)*$`) — the UNS token rule.
#[must_use]
pub fn is_lower_kebab(s: &str) -> bool {
    !s.is_empty()
        && !s.starts_with('-')
        && !s.ends_with('-')
        && !s.contains("--")
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

fn de_base_url<'de, D>(d: D) -> Result<Url, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;
    let raw = String::deserialize(d)?;
    let url = Url::parse(&raw).map_err(|e| D::Error::custom(format!("invalid url `{raw}`: {e}")))?;
    match url.scheme() {
        "http" | "https" => {}
        other => return Err(D::Error::custom(format!("unsupported url scheme `{other}`"))),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(D::Error::custom(
            "agent url must not embed userinfo - use auth.secretRef instead",
        ));
    }
    Ok(url)
}

fn default_heartbeat_ms() -> u32 {
    DEFAULT_HEARTBEAT_MS
}
fn default_poll_interval_ms() -> u32 {
    DEFAULT_POLL_INTERVAL_MS
}
fn default_request_timeout_ms() -> u32 {
    DEFAULT_REQUEST_TIMEOUT_MS
}
fn default_max_document_bytes() -> usize {
    DEFAULT_MAX_DOCUMENT_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn one_agent() -> Value {
        json!({ "agents": [ { "id": "line-a-agent", "url": "http://agent:5000" } ] })
    }

    #[test]
    fn an_agent_entry_defaults_to_the_standards_own_values() {
        let agents = parse_agents(&one_agent()).unwrap();
        assert_eq!(agents.len(), 1);
        let a = &agents[0];
        assert_eq!(a.id, "line-a-agent");
        assert_eq!(a.url.as_str(), "http://agent:5000/");
        assert_eq!(a.heartbeat_ms, DEFAULT_HEARTBEAT_MS, "the standard's 10 s default");
        assert_eq!(a.poll_interval_ms, DEFAULT_POLL_INTERVAL_MS);
        assert_eq!(a.request_timeout_ms, DEFAULT_REQUEST_TIMEOUT_MS);
        assert_eq!(a.max_document_bytes, DEFAULT_MAX_DOCUMENT_BYTES);
        assert_eq!(a.streaming, StreamPolicy::Prefer, "streaming is the primary acquisition");
        assert_eq!(a.reconnect, ReconnectCfg { initial_ms: 1_000, max_ms: 60_000 });
        assert!(a.auth.is_none() && a.tls.is_none());
    }

    #[test]
    fn auth_and_tls_are_references_never_values() {
        let agents = parse_agents(&json!({ "agents": [{
            "id": "a", "url": "https://agent:5001",
            "auth": { "type": "basic", "username": "reader", "secretRef": "mtc/agent-a" },
            "tls": { "caSecretRef": "mtc/ca", "certSecretRef": "mtc/cert", "keySecretRef": "mtc/key" },
            "streaming": "poll-only", "pollIntervalMs": 500, "maxDocumentBytes": 1024
        }] }))
        .unwrap();
        let a = &agents[0];
        assert_eq!(
            a.auth,
            Some(AuthRef::Basic { username: "reader".into(), secret_ref: "mtc/agent-a".into() })
        );
        assert_eq!(a.tls.as_ref().unwrap().ca_secret_ref.as_deref(), Some("mtc/ca"));
        assert_eq!(a.streaming, StreamPolicy::PollOnly);
        assert_eq!(a.poll_interval_ms, 500);
        assert_eq!(a.max_document_bytes, 1024);

        // A bearer token is a reference too.
        let agents = parse_agents(&json!({ "agents": [{
            "id": "a", "url": "https://agent:5001",
            "auth": { "type": "bearer", "secretRef": "mtc/token" }
        }] }))
        .unwrap();
        assert_eq!(agents[0].auth, Some(AuthRef::Bearer { secret_ref: "mtc/token".into() }));
    }

    #[test]
    fn a_url_with_embedded_credentials_or_a_foreign_scheme_is_refused() {
        for bad in [
            "ftp://agent:5000",
            "mqtt://agent",
            "http://user:pass@agent:5000",
            "not a url",
        ] {
            let err = parse_agents(&json!({ "agents": [{ "id": "a", "url": bad }] })).unwrap_err();
            assert!(matches!(err, MtcError::Config(_)), "`{bad}` must be refused");
        }
        // A username alone is still userinfo.
        assert!(parse_agents(&json!({ "agents": [{ "id": "a", "url": "http://user@agent" }] })).is_err());
    }

    #[test]
    fn a_missing_empty_or_non_array_agent_list_is_a_configuration_error() {
        assert!(parse_agents(&json!({})).is_err());
        assert!(parse_agents(&json!({ "agents": {} })).is_err());
        assert!(parse_agents(&json!({ "agents": [] })).is_err());
    }

    #[test]
    fn agent_ids_and_urls_are_unique_and_ids_are_uns_tokens() {
        let dup_id = json!({ "agents": [
            { "id": "a", "url": "http://one:5000" },
            { "id": "a", "url": "http://two:5000" }
        ] });
        assert!(parse_agents(&dup_id).is_err());

        let dup_url = json!({ "agents": [
            { "id": "a", "url": "http://one:5000" },
            { "id": "b", "url": "http://one:5000" }
        ] });
        assert!(parse_agents(&dup_url).is_err());

        let bad_token = json!({ "agents": [{ "id": "Line_A", "url": "http://one:5000" }] });
        assert!(parse_agents(&bad_token).is_err());
    }

    #[test]
    fn an_unknown_agent_key_is_rejected_rather_than_ignored() {
        let bad = json!({ "agents": [{ "id": "a", "url": "http://x:5000", "pollIntervalMS": 10 }] });
        assert!(parse_agents(&bad).is_err(), "a typo'd key is a mistake, not a no-op");
    }

    #[test]
    fn non_positive_timings_and_half_specified_tls_are_refused() {
        let zero = json!({ "agents": [{ "id": "a", "url": "http://x:5000", "heartbeatMs": 0 }] });
        assert!(parse_agents(&zero).is_err());
        let zero_cap = json!({ "agents": [{ "id": "a", "url": "http://x:5000", "maxDocumentBytes": 0 }] });
        assert!(parse_agents(&zero_cap).is_err());
        let bad_backoff = json!({ "agents": [{
            "id": "a", "url": "http://x:5000", "reconnect": { "initialMs": 5000, "maxMs": 1000 }
        }] });
        assert!(parse_agents(&bad_backoff).is_err());
        let half_tls = json!({ "agents": [{
            "id": "a", "url": "http://x:5000", "tls": { "certSecretRef": "c" }
        }] });
        assert!(parse_agents(&half_tls).is_err());
    }

    fn signal(id: &str, item: &str) -> SignalConfig {
        SignalConfig {
            id: id.into(),
            name: None,
            channel: None,
            data_item_id: item.into(),
            condition_binding: Vec::new(),
            publish: PublishCfg::default(),
        }
    }

    fn device(id: &str, agent: &str, uuid: &str, signals: Vec<SignalConfig>) -> DeviceConfig {
        DeviceConfig { id: id.into(), agent_id: agent.into(), device_uuid: uuid.into(), signals }
    }

    #[test]
    fn a_signal_deserializes_with_its_binding_and_publish_policy() {
        let s: SignalConfig = serde_json::from_value(json!({
            "id": "x-position",
            "name": "X actual position",
            "channel": "axes/x",
            "dataItemId": "dcbc0570",
            "conditionBinding": ["e086dd60"],
            "publish": { "mode": "interval", "batchMs": 250, "deadband": 0.5 }
        }))
        .unwrap();
        assert_eq!(s.data_item_id, "dcbc0570");
        assert_eq!(s.condition_binding, vec!["e086dd60".to_string()]);
        assert_eq!(s.publish.mode, PublishMode::Interval);
        assert_eq!(s.publish.batch_ms, 250);
        assert_eq!(s.publish.deadband, Some(0.5));

        // Defaults: publish on change, no batching, no deadband.
        let s = signal("a", "d1");
        assert_eq!(s.publish, PublishCfg { mode: PublishMode::OnChange, batch_ms: 0, deadband: None });
    }

    #[test]
    fn the_cross_invariants_of_lld_8_are_enforced() {
        let agents = parse_agents(&one_agent()).unwrap();
        let ok = vec![device("cnc-1", "line-a-agent", "OKUMA.1", vec![signal("a", "d1")])];
        validate_bindings(&agents, &ok).unwrap();

        // Unknown agent.
        let bad = vec![device("cnc-1", "nope", "OKUMA.1", vec![])];
        assert!(validate_bindings(&agents, &bad).is_err());

        // Two devices claiming one uuid on the same agent.
        let bad = vec![
            device("cnc-1", "line-a-agent", "OKUMA.1", vec![]),
            device("cnc-2", "line-a-agent", "OKUMA.1", vec![]),
        ];
        assert!(validate_bindings(&agents, &bad).is_err());

        // Empty uuid.
        let bad = vec![device("cnc-1", "line-a-agent", "  ", vec![])];
        assert!(validate_bindings(&agents, &bad).is_err());

        // Duplicate signal id within a device.
        let bad = vec![device(
            "cnc-1",
            "line-a-agent",
            "OKUMA.1",
            vec![signal("a", "d1"), signal("a", "d2")],
        )];
        assert!(validate_bindings(&agents, &bad).is_err());

        // Empty dataItemId.
        let bad = vec![device("cnc-1", "line-a-agent", "OKUMA.1", vec![signal("a", "")])];
        assert!(validate_bindings(&agents, &bad).is_err());

        // A signal binding its own data item as a condition.
        let mut s = signal("a", "d1");
        s.condition_binding = vec!["d1".into()];
        let bad = vec![device("cnc-1", "line-a-agent", "OKUMA.1", vec![s])];
        assert!(validate_bindings(&agents, &bad).is_err());
    }

    #[test]
    fn lower_kebab_is_the_uns_token_rule() {
        assert!(is_lower_kebab("line-a-agent"));
        assert!(is_lower_kebab("a1"));
        assert!(!is_lower_kebab(""));
        assert!(!is_lower_kebab("-a"));
        assert!(!is_lower_kebab("a-"));
        assert!(!is_lower_kebab("a--b"));
        assert!(!is_lower_kebab("Line"));
        assert!(!is_lower_kebab("line_a"));
    }
}
