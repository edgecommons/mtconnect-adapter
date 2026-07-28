//! # The device seam: what a *protocol adapter* talks to
//!
//! [`DeviceSession`] is one live connection to one device. Implement it once per protocol —
//! Modbus, OPC UA, whatever you are bridging — and everything above it (the connection lifecycle,
//! backoff, publishing, health) is written against the trait and never learns your protocol.
//!
//! **The boundary rule, and it is worth enforcing in review:** a backend knows protocols. It does
//! **not** know EdgeCommons topics, the UNS, message envelopes, or metrics. If your `impl
//! DeviceSession` imports `edgecommons::uns`, the seam has leaked.
//!
//! ## Signals, not tags
//!
//! A **signal** is one data point — a measured value with identity, quality, and timestamps.
//! (OPC UA calls it a "tag"; Modbus calls it a "register".) The word "tag" is reserved in
//! EdgeCommons for the envelope's *business metadata*, which is a different thing entirely.
//!
//! ## Quality is not optional
//!
//! Every sample carries a `quality` normalized to `GOOD | BAD | UNCERTAIN`, plus the native code
//! in `qualityRaw` for diagnosis. This is what lets a consumer gate on quality without knowing
//! your protocol — and it is why a read failure must be published as a `BAD` sample rather than
//! swallowed. A signal that silently stops updating is indistinguishable from one that is simply
//! not changing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use url::Url;

/// One reading from the device.
///
/// The three trailing timestamp slots realize the four-slot model of `docs/SOUTHBOUND.md` §2 (the
/// fourth slot — the publish timestamp — is the envelope header's, stamped by the library). All
/// are optional ISO-8601 UTC strings, and none is ever synthesized from another: a backend sets
/// what its protocol actually knows and leaves the rest `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    /// The canonical, stable id the rest of the fleet keys on (e.g. `ns=3;i=1001`).
    pub signal_id: String,
    /// A human label.
    pub name: Option<String>,
    /// The value, or `None` when the protocol said there is none. `None` is published as an
    /// explicit JSON `null` beside a BAD quality (an MTConnect `UNAVAILABLE`), never as `0` or `""`
    /// — "I have no value" and "the value is zero" are different facts.
    pub value: Option<serde_json::Value>,
    pub quality: Quality,
    /// The protocol-native status code, kept verbatim for diagnosis.
    pub quality_raw: Option<String>,
    /// The **machine** timestamp: device/field-authored time, set only when the protocol supplied
    /// it. Never synthesized.
    pub source_ts: Option<String>,
    /// The **capture** timestamp: the moment the protocol read the value — a mediating server's
    /// stamp (an OPC UA server, an MTConnect agent). A direct-client protocol leaves it `None`:
    /// its receive moment IS the capture moment.
    pub capture_ts: Option<String>,
    /// The **adapter receive** timestamp. The worker auto-stamps it at read completion for every
    /// reading lacking it, so a backend only sets it when it has a better (earlier) receive stamp.
    pub received_ts: Option<String>,
    /// Additive protocol-specific fields ("extras", `docs/SOUTHBOUND.md` §2) copied onto the
    /// published sample: MTConnect rides its `sequence` here on every sample, plus
    /// `resetTriggered`/`duration`/`nativeCode` when the agent sent them.
    pub extra: Option<serde_json::Map<String, serde_json::Value>>,
    /// An explicit `signal_path` (channel) for this signal, when its configuration overrides the
    /// default. `None` publishes on the signal's own id.
    pub channel: Option<String>,
}

impl Reading {
    /// A reading that carries a value.
    #[must_use]
    pub fn good(signal_id: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            signal_id: signal_id.into(),
            name: None,
            value: Some(value),
            quality: Quality::Good,
            quality_raw: None,
            source_ts: None,
            capture_ts: None,
            received_ts: None,
            extra: None,
            channel: None,
        }
    }

    /// A reading that carries no value, with the protocol's own reason. Published as an explicit
    /// null with BAD quality — because a signal that silently vanishes is indistinguishable from
    /// one that is not changing.
    #[must_use]
    pub fn bad(signal_id: impl Into<String>, quality_raw: impl Into<String>) -> Self {
        Self {
            signal_id: signal_id.into(),
            name: None,
            value: None,
            quality: Quality::Bad,
            quality_raw: Some(quality_raw.into()),
            source_ts: None,
            capture_ts: None,
            received_ts: None,
            extra: None,
            channel: None,
        }
    }

    /// Attach one extra field (fluent).
    #[must_use]
    pub fn with_extra(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.extra.get_or_insert_with(serde_json::Map::new).insert(key.into(), value);
        self
    }
}

/// Normalized quality. The protocol's own status code goes in `quality_raw`.
///
/// `Uncertain` is unused by the simulated backend and used constantly by real ones: a stale
/// cached read, a value outside its calibrated range, a sensor that answered but warned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // `Uncertain` is for your backend, not the simulator's
pub enum Quality {
    Good,
    Bad,
    Uncertain,
}

/// Why talking to the device failed — and whether reconnecting could help.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)] // the simulator never fails transiently; a real device does, constantly
pub enum DeviceError {
    /// The link is down, or the device is busy. Reconnect and retry.
    #[error("transient: {0}")]
    Transient(#[source] anyhow::Error),
    /// Misconfiguration: a bad endpoint, a rejected credential, an address that does not exist.
    /// Reconnecting will fail identically, so the supervisor backs off hard rather than hammering.
    #[error("permanent: {0}")]
    Permanent(#[source] anyhow::Error),
}

impl DeviceError {
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

pub type Result<T> = std::result::Result<T, DeviceError>;

/// One signal in the adapter's inventory — its stable id and human label, known from config/backend
/// **without a device round-trip**. Backs the `sb/signals` command.
#[derive(Debug, Clone, PartialEq)]
pub struct SignalInfo {
    /// The canonical, stable id (the `sb/read`/`sb/write` `signalId`).
    pub id: String,
    /// A human label, when the backend has one.
    pub name: Option<String>,
}

/// One entry discovered by [`DeviceSession::browse`] — a signal the device *offers*, whether or not
/// it is configured. Backs the `sb/browse` diagnostics surface.
#[derive(Debug, Clone, PartialEq)]
pub struct BrowsedSignal {
    /// The stable id a consumer would configure or read.
    pub id: String,
    /// A human label, when the device provides one.
    pub name: Option<String>,
    /// The device-native type, kept verbatim for diagnosis (`"REAL"`, `"holding/uint16"`, …).
    pub type_name: String,
}

/// One page of a [`DeviceSession::browse`] enumeration. Browsing is **paged** because a device's
/// address space can be large; `next_cursor` is `Some` while more pages remain.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BrowsePage {
    pub entries: Vec<BrowsedSignal>,
    /// Opaque continuation token; pass it back as the next `cursor`. `None` on the last page.
    pub next_cursor: Option<String>,
}

/// Why a `sb/browse` could not answer. Kept distinct from [`DeviceError`] because "this protocol has
/// no discovery" is a permanent, honest capability limit — not a link failure.
#[derive(Debug)]
#[allow(dead_code)] // `Failed` is for a real backend whose discovery can fail mid-enumeration
pub enum BrowseError {
    /// The protocol has no discovery service. The default seam impl returns this, so an adapter that
    /// cannot browse stays honest (the command maps it to `BROWSE_UNSUPPORTED`).
    Unsupported,
    /// A mid-browse failure (a link error, a malformed reply). Maps to `BROWSE_FAILED`.
    Failed(String),
}

/// A live connection to one device. **This is the trait you implement.**
#[async_trait]
pub trait DeviceSession: Send + Sync {
    /// Read the configured signals once.
    ///
    /// A read that fails for *one* signal should return that signal with [`Quality::Bad`] rather
    /// than failing the whole call — one dead register must not blind you to the other ninety-nine.
    /// Return `Err` only when the *connection* is broken.
    async fn read_signals(&mut self) -> Result<Vec<Reading>>;

    /// Read a named subset **now** (backs `sb/read`). The default reads everything and filters, which
    /// is correct for any backend; override it when your protocol can read a subset more cheaply.
    ///
    /// # Errors
    ///
    /// Only when the *connection* is broken (same contract as [`read_signals`](Self::read_signals)).
    async fn read_named(&mut self, ids: &[String]) -> Result<Vec<Reading>> {
        let all = self.read_signals().await?;
        Ok(all.into_iter().filter(|r| ids.iter().any(|id| id == &r.signal_id)).collect())
    }

    /// Write a value back to the device.
    ///
    /// # Errors
    ///
    /// If the write is rejected, or the link is down.
    async fn write_signal(&mut self, signal_id: &str, value: &serde_json::Value) -> Result<()>;

    /// Enumerate the device's address space, one page at a time (backs `sb/browse`).
    ///
    /// The default returns [`BrowseError::Unsupported`] — a protocol with no discovery (Modbus, a
    /// fixed register map) is honest to leave it unimplemented. Override it when your protocol can
    /// enumerate (OPC UA browse, an EtherNet/IP tag list).
    ///
    /// # Errors
    ///
    /// [`BrowseError::Unsupported`] when the protocol has no discovery; [`BrowseError::Failed`] on a
    /// mid-browse link/protocol error.
    async fn browse(
        &mut self,
        _cursor: Option<String>,
        _max: usize,
    ) -> std::result::Result<BrowsePage, BrowseError> {
        Err(BrowseError::Unsupported)
    }

    /// Force a **fresh** read of the whole configured signal set, bypassing any cache or dedupe
    /// (backs `repoll`). The default is [`read_signals`](Self::read_signals), which is right for a
    /// backend whose read already goes to the device; override it when reads are served from a
    /// cache that a `repoll` is meant to refill.
    ///
    /// # Errors
    ///
    /// Only when the *connection* is broken (same contract as [`read_signals`](Self::read_signals)).
    async fn snapshot_now(&mut self) -> Result<Vec<Reading>> {
        self.read_signals().await
    }

    /// Drain the protocol notices that accumulated since the last call. These become UNS events
    /// (`crate::supervisor` holds the `events()` facade); the mapping from protocol fact to notice
    /// lives here, where it is unit-testable.
    fn take_notices(&mut self) -> Vec<Notice> {
        Vec::new()
    }

    /// How many configured signals this session is actually **delivering** right now — the truthful
    /// `southbound_health.signalsSubscribed` gauge. `None` means "the configured inventory size",
    /// which is what a backend with no compile step serves.
    fn served_signals(&self) -> Option<u64> {
        None
    }

    /// Close the connection. Must be safe to call twice.
    async fn close(&mut self) {}
}

// =================================================================================================
// Protocol notices -> UNS events (HLD §9)
// =================================================================================================

/// How loud a [`Notice`] is. The supervisor maps this onto the library's `Severity` when it emits;
/// keeping the seam's own vocabulary here means the mapping — the part worth testing — does not
/// need the `events()` facade to exercise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeLevel {
    Info,
    Warning,
    Critical,
}

/// One protocol fact worth telling an operator about: an agent that came or went, observations that
/// were provably lost, a device model that changed under us, a machine alarm that latched.
///
/// `event_type` is the UNS `evt` type — the family an edge-console panel subscribes to — and the
/// context is the event's *payload*: sequence numbers, uuids and data-item ids belong here, never in
/// a metric dimension (HLD §9).
#[derive(Debug, Clone, PartialEq)]
pub struct Notice {
    pub event_type: &'static str,
    pub level: NoticeLevel,
    pub message: String,
    pub context: Value,
}

/// The agent lifecycle family: reachable, unreachable, degraded to polling (HLD §9).
pub const EVENT_AGENT: &str = "MtconnectAgentEvent";
/// Observations the agent's buffer overran before we read them (HLD §9, ladder 2).
pub const EVENT_DATA_LOSS: &str = "MtconnectDataLossEvent";
/// The device's probe model changed under a running instance (HLD §9, D-MTC-5).
pub const EVENT_MODEL_DRIFT: &str = "MtconnectModelDriftEvent";
/// A CONDITION data item that latched into `Fault` (HLD §9, rate-limited).
pub const EVENT_CONDITION: &str = "MtconnectConditionEvent";

/// A `Fault` that keeps re-asserting must not become a firehose: one condition event per data item
/// per minute (HLD §9). The state itself is always published as the signal's value — this limit
/// bounds only the *event*.
pub const CONDITION_EVENT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Opens sessions. One factory per protocol.
#[async_trait]
pub trait DeviceBackend: Send + Sync {
    /// The protocol's name, as it appears in config and in the published `device.adapter` field.
    fn kind(&self) -> &'static str;

    /// The signal inventory this backend exposes for a device, **without connecting** — read from
    /// config in a real adapter. Backs `sb/signals` (a config view, no device round-trip). The
    /// simulator returns a fixed pair so the command has something to show.
    fn inventory(&self, _cfg: &ConnectionConfig) -> Vec<SignalInfo> {
        Vec::new()
    }

    /// Connect to one device.
    ///
    /// # Errors
    ///
    /// If the device is unreachable ([`DeviceError::Transient`]) or the configuration is wrong
    /// ([`DeviceError::Permanent`]).
    async fn connect(&self, cfg: &ConnectionConfig) -> Result<Box<dyn DeviceSession>>;
}

/// How to reach one device. Deliberately open (`additionalProperties` in the schema): every
/// protocol needs different keys, and this is the one place the adapter should not be strict.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionConfig {
    /// The endpoint, in whatever form the protocol uses. Published in `device.endpoint`. A
    /// protocol whose endpoint is **derived** rather than configured (MTConnect derives
    /// `mtconnect://<host>/<uuid>` from its agent and device uuid, HLD §5.1) leaves it out of
    /// configuration and fills it in at startup.
    #[serde(default)]
    pub endpoint: String,
    /// Everything else the protocol needs: a unit id, a security policy, a slave address.
    /// The simulator reads none of it; yours will.
    #[serde(flatten)]
    #[allow(dead_code)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

// --- The simulated backend -------------------------------------------------------------------
//
// A real adapter replaces this with its protocol. It ships so that `cargo run` works with no
// hardware, and so the tests have something to talk to — and a backend you can run on a laptop is
// worth more than one you can only run next to a PLC.

pub struct SimBackend;

/// The signals the simulator exposes — the ids it reads and the one it fails. A real backend derives
/// this from config; the simulator hard-codes it so `sb/signals` and `sb/browse` have content.
const SIM_SIGNALS: [(&str, &str, &str); 2] = [
    ("temperature-1", "Ambient temperature", "REAL"),
    ("pressure-1", "Line pressure", "REAL"),
];

#[async_trait]
impl DeviceBackend for SimBackend {
    fn kind(&self) -> &'static str {
        "sim"
    }

    fn inventory(&self, _cfg: &ConnectionConfig) -> Vec<SignalInfo> {
        SIM_SIGNALS
            .iter()
            .map(|(id, name, _)| SignalInfo { id: (*id).to_string(), name: Some((*name).to_string()) })
            .collect()
    }

    async fn connect(&self, cfg: &ConnectionConfig) -> Result<Box<dyn DeviceSession>> {
        if cfg.endpoint.is_empty() {
            // A missing endpoint will never fix itself: permanent, so the supervisor does not
            // spend the next hour reconnecting to nothing.
            return Err(DeviceError::Permanent(anyhow::anyhow!("no endpoint configured")));
        }
        Ok(Box::new(SimSession { tick: 0 }))
    }
}

pub struct SimSession {
    tick: u64,
}

#[async_trait]
impl DeviceSession for SimSession {
    async fn read_signals(&mut self) -> Result<Vec<Reading>> {
        self.tick += 1;
        let value = 20.0 + 5.0 * ((self.tick as f64) / 10.0).sin();
        // The sim sets none of the timestamp slots: it has no device clock, and inventing one
        // would be dishonest. The worker stamps `received_ts` at read completion, and that
        // receive moment becomes the published `serverTs` (a direct client's receive IS capture).
        Ok(vec![
            Reading {
                name: Some("Ambient temperature".into()),
                quality_raw: Some("OK".into()),
                ..Reading::good("temperature-1", serde_json::json!(value))
            },
            // A signal the simulated device cannot currently read. It is published as BAD rather
            // than omitted, because "I could not read this" is information and silence is not.
            Reading {
                name: Some("Line pressure".into()),
                ..Reading::bad("pressure-1", "SENSOR_FAULT")
            },
        ])
    }

    async fn write_signal(&mut self, signal_id: &str, value: &serde_json::Value) -> Result<()> {
        tracing::info!(signal_id, ?value, "sim: write accepted");
        Ok(())
    }

    /// A one-page browse of the simulator's inventory. A real backend pages a large address space and
    /// returns a `next_cursor`; the simulator has two signals, so the first page is the last page.
    async fn browse(
        &mut self,
        cursor: Option<String>,
        _max: usize,
    ) -> std::result::Result<BrowsePage, BrowseError> {
        // A cursor means "the page after the last one" — the sim has nothing more.
        if cursor.is_some() {
            return Ok(BrowsePage::default());
        }
        let entries = SIM_SIGNALS
            .iter()
            .map(|(id, name, ty)| BrowsedSignal {
                id: (*id).to_string(),
                name: Some((*name).to_string()),
                type_name: (*ty).to_string(),
            })
            .collect();
        Ok(BrowsePage { entries, next_cursor: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(endpoint: &str) -> ConnectionConfig {
        ConnectionConfig { endpoint: endpoint.into(), extra: serde_json::Map::new() }
    }

    #[tokio::test]
    async fn the_sim_backend_connects_and_reads() {
        let mut s = SimBackend.connect(&conn("sim://device")).await.unwrap();
        let readings = s.read_signals().await.unwrap();
        assert_eq!(readings.len(), 2);
        assert_eq!(readings[0].signal_id, "temperature-1");
        assert_eq!(readings[0].quality, Quality::Good);
        // No device clock -> no timestamp slots. The worker stamps `received_ts` at read
        // completion; a backend only fills what its protocol actually knows.
        assert!(readings[0].source_ts.is_none());
        assert!(readings[0].capture_ts.is_none());
        assert!(readings[0].received_ts.is_none());
    }

    #[tokio::test]
    async fn a_failed_read_is_published_as_bad_quality_not_omitted() {
        // The signal is still reported — with BAD quality and the native code — because a signal
        // that silently vanishes is indistinguishable from one that is not changing.
        let mut s = SimBackend.connect(&conn("sim://device")).await.unwrap();
        let readings = s.read_signals().await.unwrap();
        let bad = readings.iter().find(|r| r.signal_id == "pressure-1").unwrap();
        assert_eq!(bad.quality, Quality::Bad);
        assert_eq!(bad.quality_raw.as_deref(), Some("SENSOR_FAULT"));
    }

    #[tokio::test]
    async fn a_misconfiguration_is_permanent_so_the_supervisor_does_not_hammer_it() {
        // `unwrap_err` is not available here: a `Box<dyn DeviceSession>` is not `Debug`, so the
        // Ok-type cannot be printed. Match instead.
        let Err(e) = SimBackend.connect(&conn("")).await else {
            panic!("connecting with no endpoint must fail");
        };
        assert!(!e.is_transient(), "a missing endpoint will never fix itself by retrying");
    }

    #[tokio::test]
    async fn readings_advance() {
        let mut s = SimBackend.connect(&conn("sim://device")).await.unwrap();
        let a = s.read_signals().await.unwrap()[0].value.clone();
        let b = s.read_signals().await.unwrap()[0].value.clone();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn read_named_returns_only_the_requested_signals() {
        // The default `read_named` reads all and filters — override it only if your protocol reads a
        // subset more cheaply.
        let mut s = SimBackend.connect(&conn("sim://device")).await.unwrap();
        let got = s.read_named(&["temperature-1".to_string()]).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].signal_id, "temperature-1");
        // An unknown id resolves to nothing (the command layer reports it as a BAD/no-data entry).
        assert!(s.read_named(&["nope".to_string()]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_sim_browses_one_page_and_stops() {
        let mut s = SimBackend.connect(&conn("sim://device")).await.unwrap();
        let page = s.browse(None, 100).await.unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].id, "temperature-1");
        assert!(page.next_cursor.is_none(), "the sim's first page is its last");
        // A cursor asks for the page after the last — empty.
        let page2 = s.browse(Some("x".into()), 100).await.unwrap();
        assert!(page2.entries.is_empty());
    }

    #[test]
    fn the_sim_advertises_its_inventory_without_connecting() {
        // `sb/signals` reads this — a config view, no device round-trip.
        let inv = SimBackend.inventory(&conn("sim://device"));
        assert_eq!(inv.len(), 2);
        assert_eq!(inv[0].id, "temperature-1");
        assert_eq!(inv[0].name.as_deref(), Some("Ambient temperature"));
    }

    #[tokio::test]
    async fn browse_is_unsupported_by_default() {
        // A protocol with no discovery keeps the default — honest, not a fake empty page.
        struct NoBrowse;
        #[async_trait]
        impl DeviceSession for NoBrowse {
            async fn read_signals(&mut self) -> Result<Vec<Reading>> {
                Ok(vec![])
            }
            async fn write_signal(&mut self, _: &str, _: &serde_json::Value) -> Result<()> {
                Ok(())
            }
        }
        let mut s = NoBrowse;
        assert!(matches!(s.browse(None, 10).await, Err(BrowseError::Unsupported)));
    }
}

// =================================================================================================
// The MTConnect backend — the EdgeCommons side of the device seam (LLD §4)
// =================================================================================================
// # The MTConnect side of the device seam (LLD §4)
//
// This is the **only** place where the owned MTConnect client meets EdgeCommons vocabulary. Below
// it, `src/mtconnect/**` knows agents, documents and observations and nothing else; above it, the
// generated supervisor knows [`Reading`]s, qualities and topics and nothing about MTConnect.
//
// ## What one instance is
//
// One `component.instances[]` entry is one MTConnect **device** (`Device/@uuid`) served by a
// configured agent (D-MTC-3). [`MtcSession`] therefore owns no socket: it holds a queue that the
// agent's shared acquisition task feeds, and its `read_signals` drains what has arrived since the
// last call. The connection lifecycle the supervisor drives is still real — `connect` verifies the
// device is in the agent's probe and compiles the configured signals against the model, and an
// agent that goes away surfaces as a transient error so the supervisor reconnects.
//
// ## The mapping, in one place
//
// [`reading_from_observation`] is the whole HLD §5.3/§6 contract: value shape, quality (including
// the `conditionBinding` degradation), the agent's capture timestamp, and the `sequence` extra
// that gives a consumer exact once-only ordering across reconnects.

use crate::mtconnect::config::{DeviceConfig as MtcDeviceConfig, SignalConfig};
use crate::mtconnect::model::ProbeModel;
use crate::mtconnect::observations::{CondState, ObsValue, Observation};
use crate::mtconnect::{AgentRuntime, InstanceEvent, MtcError};
use crate::reload::{SignalRegistry, SignalSlot};

/// The `adapter` value that selects this backend.
pub const KIND: &str = "mtconnect";

/// `qualityRaw` for a healthy MTConnect observation.
pub const QUALITY_OK: &str = "MTC_OK";
/// `qualityRaw` for an agent that has no value for a data item.
pub const QUALITY_UNAVAILABLE: &str = "UNAVAILABLE";
/// `qualityRaw` for a configured signal whose `dataItemId` is not in the device model.
pub const QUALITY_NO_SUCH_DATAITEM: &str = "MTC_NO_SUCH_DATAITEM";

/// The refusal every write path answers with — MTConnect's API is read-only by specification
/// (Part 1 Fundamentals §5.1), so this is a capability statement, not a policy setting.
pub const READ_ONLY_MESSAGE: &str = "MTConnect is read-only (Part 1 Fundamentals §5.1)";

// =================================================================================================
// Config plumbing
// =================================================================================================

/// The `(agentId, deviceUuid)` pair out of an instance's open `connection` object.
///
/// # Errors
/// A message naming the missing/mistyped key, which the caller turns into a permanent
/// configuration failure.
pub fn connection_binding(cfg: &ConnectionConfig) -> std::result::Result<(String, String), String> {
    let field = |key: &str| -> std::result::Result<String, String> {
        match cfg.extra.get(key).and_then(Value::as_str) {
            Some(v) if !v.trim().is_empty() => Ok(v.to_string()),
            Some(_) => Err(format!("connection.{key} must not be empty")),
            None => Err(format!("connection.{key} is required for the `{KIND}` adapter")),
        }
    };
    Ok((field("agentId")?, field("deviceUuid")?))
}

/// The derived, non-secret endpoint published in `device.endpoint` and shown by `sb/status`
/// (HLD §5.1): `mtconnect://<host>[:<port>]/<percent-encoded-device-uuid>`.
#[must_use]
pub fn endpoint_description(base: &Url, device_uuid: &str) -> String {
    let host = base.host_str().unwrap_or("unknown-host");
    let authority = match base.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    format!("mtconnect://{authority}/{}", percent_encode(device_uuid))
}

/// Percent-encode everything outside the RFC 3986 unreserved set, so a uuid with a slash, a space,
/// or a colon cannot forge a different endpoint.
fn percent_encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char);
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// The key one device binding is stored under: an agent id plus a device uuid.
fn binding_key(agent_id: &str, device_uuid: &str) -> String {
    format!("{agent_id}\u{1f}{device_uuid}")
}

// =================================================================================================
// The backend
// =================================================================================================

/// The MTConnect [`DeviceBackend`]: it opens no connections of its own — the agent runtimes were
/// built and started before any instance supervisor ran (ADP-3 activation ordering).
pub struct MtcBackend {
    agents: HashMap<String, Arc<AgentRuntime>>,
    devices: HashMap<String, MtcDeviceConfig>,
    /// The live, reloadable signal set of each instance (LLD §8). A session reads its slot rather
    /// than the startup configuration, so a reload reaches acquisition without a reconnect.
    signals: Arc<SignalRegistry>,
}

impl MtcBackend {
    /// Wire the configured agents to the configured devices.
    #[must_use]
    pub fn new(
        agents: HashMap<String, Arc<AgentRuntime>>,
        devices: Vec<MtcDeviceConfig>,
    ) -> Self {
        let signals = Arc::new(SignalRegistry::new(&devices));
        let devices = devices
            .into_iter()
            .map(|d| (binding_key(&d.agent_id, &d.device_uuid), d))
            .collect();
        Self { agents, devices, signals }
    }

    /// The live signal registry a reload swaps into (LLD §8) — the same slots the sessions read.
    #[must_use]
    pub fn signals(&self) -> Arc<SignalRegistry> {
        Arc::clone(&self.signals)
    }

    fn binding(&self, cfg: &ConnectionConfig) -> std::result::Result<&MtcDeviceConfig, DeviceError> {
        let (agent_id, uuid) =
            connection_binding(cfg).map_err(|e| DeviceError::Permanent(anyhow::anyhow!(e)))?;
        self.devices.get(&binding_key(&agent_id, &uuid)).ok_or_else(|| {
            DeviceError::Permanent(anyhow::anyhow!(
                "no configured device `{uuid}` on agent `{agent_id}`"
            ))
        })
    }
}

#[async_trait]
impl DeviceBackend for MtcBackend {
    fn kind(&self) -> &'static str {
        KIND
    }

    /// The configured inventory — read straight from configuration, with no device round-trip, so
    /// `sb/signals` answers while the agent is unreachable.
    fn inventory(&self, cfg: &ConnectionConfig) -> Vec<SignalInfo> {
        match self.binding(cfg) {
            // The LIVE set, not the startup one: after a reload the inventory is what is published
            // now (LLD §8).
            Ok(device) => match self.signals.slot(&device.id) {
                Some(slot) => slot
                    .load()
                    .signals
                    .iter()
                    .map(|s| SignalInfo { id: s.id.clone(), name: s.name.clone() })
                    .collect(),
                None => Vec::new(),
            },
            Err(_) => Vec::new(),
        }
    }

    async fn connect(&self, cfg: &ConnectionConfig) -> Result<Box<dyn DeviceSession>> {
        let device = self.binding(cfg)?.clone();
        let agent = self
            .agents
            .get(&device.agent_id)
            .ok_or_else(|| {
                DeviceError::Permanent(anyhow::anyhow!(
                    "agent `{}` is not configured in component.global.agents[]",
                    device.agent_id
                ))
            })?
            .clone();

        // "Connected" means the agent answered AND this device is really in its probe.
        let model = agent.ensure_model(&device.device_uuid).await.map_err(to_device_error)?;
        let slot = self.signals.slot(&device.id).ok_or_else(|| {
            DeviceError::Permanent(anyhow::anyhow!(
                "instance `{}` has no signal slot; it was not in the compiled configuration",
                device.id
            ))
        })?;
        let handle = agent.attach(&device.device_uuid);
        Ok(Box::new(MtcSession::with_slot(agent, device, model, handle.rx, slot)))
    }
}

/// Transient/permanent classification for the supervisor: a link problem is worth retrying, a
/// configuration or credential problem is not (retrying it hammers the agent for nothing).
fn to_device_error(e: MtcError) -> DeviceError {
    if e.is_transient() {
        DeviceError::Transient(anyhow::anyhow!(e))
    } else {
        DeviceError::Permanent(anyhow::anyhow!(e))
    }
}

// =================================================================================================
// Credential resolution — the ONLY place a vault reference becomes a value
// =================================================================================================

/// Resolve an agent's `auth`/`tls` **references** into material the client can use.
///
/// This is the boundary the isolation rule draws: `src/mtconnect/**` receives values and cannot
/// reach the credential service, so a secret can never be read from — or logged by — the protocol
/// client. A reference with no credential service configured is a *configuration* failure, not a
/// silent fallback to an unauthenticated request.
///
/// # Errors
/// A message naming the unresolved reference (never its value).
pub fn resolve_agent_credentials(
    cfg: &crate::mtconnect::config::AgentConfig,
    creds: Option<&dyn edgecommons::credentials::CredentialService>,
) -> std::result::Result<crate::mtconnect::config::AgentCredentials, String> {
    use crate::mtconnect::config::{AgentCredentials, AuthMaterial, AuthRef, TlsMaterial};

    if cfg.auth.is_none() && cfg.tls.is_none() {
        return Ok(AgentCredentials::default());
    }
    let creds = creds.ok_or_else(|| {
        format!(
            "agent `{}` references vault secrets but the component has no `credentials` section",
            cfg.id
        )
    })?;
    let fetch = |secret_ref: &str| -> std::result::Result<String, String> {
        match creds.get_string(secret_ref) {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err(format!("secret `{secret_ref}` is not in the vault")),
            Err(e) => Err(format!("secret `{secret_ref}` could not be read: {e}")),
        }
    };

    let auth = match &cfg.auth {
        None => None,
        Some(AuthRef::Basic { username, secret_ref }) => Some(AuthMaterial::Basic {
            username: username.clone(),
            password: fetch(secret_ref)?,
        }),
        Some(AuthRef::Bearer { secret_ref }) => {
            Some(AuthMaterial::Bearer { token: fetch(secret_ref)? })
        }
    };

    let tls = match &cfg.tls {
        None => None,
        Some(tls) => Some(TlsMaterial {
            ca_pem: tls.ca_secret_ref.as_deref().map(fetch).transpose()?,
            client_cert_pem: tls.cert_secret_ref.as_deref().map(fetch).transpose()?,
            client_key_pem: tls.key_secret_ref.as_deref().map(fetch).transpose()?,
        }),
    };
    Ok(AgentCredentials { auth, tls })
}

// =================================================================================================
// The session
// =================================================================================================

/// One device instance's view of its agent: a queue, the compiled signal set, and the condition
/// states that degrade it.
pub struct MtcSession {
    agent: Arc<AgentRuntime>,
    device: MtcDeviceConfig,
    /// The live signal set, swapped by a reload (LLD §8). `None` for a session built without a
    /// registry, which then keeps the set it was constructed with.
    slot: Option<Arc<SignalSlot>>,
    /// The generation currently compiled — compared against the slot to notice a reload.
    generation: String,
    model: Arc<ProbeModel>,
    rx: mpsc::Receiver<InstanceEvent>,
    /// The latest state of every CONDITION data item this device published, with its native code —
    /// what `conditionBinding` degrades against.
    conditions: HashMap<String, (CondState, Option<String>)>,
    /// Configured signals whose `dataItemId` the model does not have: reported BAD every cycle,
    /// never silently dropped.
    unbound: Vec<String>,
    /// Protocol notices awaiting a drain into UNS events.
    notices: Vec<Notice>,
    /// When each condition data item last raised a `MtconnectConditionEvent` — the 1/min limiter.
    last_condition_event: HashMap<String, Instant>,
    closed: bool,
}

impl MtcSession {
    fn new(
        agent: Arc<AgentRuntime>,
        device: MtcDeviceConfig,
        model: Arc<ProbeModel>,
        rx: mpsc::Receiver<InstanceEvent>,
    ) -> Self {
        let generation = crate::reload::generation_of(&device.signals);
        let mut session = Self {
            agent,
            device,
            slot: None,
            generation,
            model,
            rx,
            conditions: HashMap::new(),
            unbound: Vec::new(),
            notices: Vec::new(),
            last_condition_event: HashMap::new(),
            closed: false,
        };
        session.recompile();
        session
    }

    /// The production constructor: the signal set comes from the instance's reloadable slot.
    fn with_slot(
        agent: Arc<AgentRuntime>,
        device: MtcDeviceConfig,
        model: Arc<ProbeModel>,
        rx: mpsc::Receiver<InstanceEvent>,
        slot: Arc<SignalSlot>,
    ) -> Self {
        let mut session = Self::new(agent, device, model, rx);
        session.slot = Some(slot);
        session.adopt_current_signals();
        session
    }

    /// Pick up a reloaded signal set, if one has been swapped in. Recompiling costs a walk of the
    /// configured signals against the **cached** model — no agent round-trip, so a reload lands
    /// even while the agent is unreachable (LLD §8).
    fn adopt_current_signals(&mut self) {
        let Some(slot) = &self.slot else { return };
        let live = slot.load();
        if live.generation == self.generation {
            return;
        }
        tracing::info!(
            instance = %self.device.id,
            from = %self.generation,
            to = %live.generation,
            signals = live.signals.len(),
            "signal set reloaded; recompiling against the cached probe model"
        );
        self.device.signals.clone_from(&live.signals);
        self.generation = live.generation.clone();
        self.recompile();
    }

    /// The configured data item ids of this device — the scope of a forced `/current` snapshot.
    fn configured_data_items(&self) -> Vec<String> {
        let mut ids: Vec<String> =
            self.device.signals.iter().map(|s| s.data_item_id.clone()).collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Whether this data item may raise a condition event now (one per minute, HLD §9).
    fn allow_condition_event(&mut self, data_item_id: &str, now: Instant) -> bool {
        match self.last_condition_event.get(data_item_id) {
            Some(last) if now.saturating_duration_since(*last) < CONDITION_EVENT_INTERVAL => false,
            _ => {
                self.last_condition_event.insert(data_item_id.to_string(), now);
                true
            }
        }
    }

    /// Turn one runtime event into the operator-facing notice it deserves. The agent's own
    /// published state supplies the sequence window a data-loss event needs.
    fn note(&mut self, event: &InstanceEvent) {
        let info = self.agent.info();
        let notice = match event {
            InstanceEvent::AgentUp(up) => Notice {
                event_type: EVENT_AGENT,
                level: NoticeLevel::Info,
                message: format!("agent `{}` is reachable", up.agent_id),
                context: json!({
                    "instance": self.device.id,
                    "agentId": up.agent_id,
                    "state": "up",
                    "mode": up.mode,
                    "instanceId": up.instance_id,
                    "agentVersion": up.agent_version,
                    "standardVersion": up.standard_version,
                }),
            },
            InstanceEvent::AgentDown(reason) => Notice {
                event_type: EVENT_AGENT,
                level: NoticeLevel::Critical,
                message: format!("agent `{}` is unreachable", info.agent_id),
                context: json!({
                    "instance": self.device.id,
                    "agentId": info.agent_id,
                    "state": "down",
                    "reason": reason,
                }),
            },
            InstanceEvent::StreamDegraded { failures } => Notice {
                event_type: EVENT_AGENT,
                level: NoticeLevel::Warning,
                message: format!(
                    "streaming could not be established after {failures} attempts; acquisition degraded to polling"
                ),
                context: json!({
                    "instance": self.device.id,
                    "agentId": info.agent_id,
                    "state": "degraded",
                    "mode": "poll",
                    "failures": failures,
                }),
            },
            InstanceEvent::DataLoss { skipped } => Notice {
                event_type: EVENT_DATA_LOSS,
                level: NoticeLevel::Warning,
                message: format!("{skipped} observations were lost: the agent's buffer overran our position"),
                context: json!({
                    "instance": self.device.id,
                    "agentId": info.agent_id,
                    "skipped": skipped,
                    "firstSequence": info.first_sequence,
                    "nextSequence": info.next_sequence,
                    "bufferSize": info.buffer_size,
                }),
            },
            InstanceEvent::ModelDrift { old, new } => Notice {
                event_type: EVENT_MODEL_DRIFT,
                level: NoticeLevel::Warning,
                message: "the device's probe model changed; signals were recompiled and browse cursors are void"
                    .to_string(),
                context: json!({
                    "instance": self.device.id,
                    "agentId": info.agent_id,
                    "deviceUuid": self.device.device_uuid,
                    "oldDigest": old,
                    "newDigest": new,
                }),
            },
            // Observations are data, not events: they publish as samples.
            InstanceEvent::Obs(_) | InstanceEvent::Snapshot(_) => return,
        };
        self.notices.push(notice);
    }

    /// Re-check every configured signal against the current model. A signal bound to a data item
    /// the device does not have is *named*, not dropped (D-MTC-5: drift is surfaced).
    fn recompile(&mut self) {
        self.unbound = self
            .device
            .signals
            .iter()
            .filter(|s| self.model.item(&s.data_item_id).is_none())
            .map(|s| s.id.clone())
            .collect();
        if !self.unbound.is_empty() {
            tracing::warn!(
                instance = %self.device.id,
                signals = ?self.unbound,
                "configured signals have no dataItem in the device model"
            );
        }
    }

    /// The signals bound to one data item (several signals MAY share one).
    fn signals_for<'a>(&'a self, data_item_id: &'a str) -> impl Iterator<Item = &'a SignalConfig> {
        self.device.signals.iter().filter(move |s| s.data_item_id == data_item_id)
    }

    /// Fold a batch of observations into readings: condition states are recorded **first**, so a
    /// value observed in the same batch as the fault that invalidates it is already degraded.
    fn map_batch(&mut self, observations: &[Observation]) -> Vec<Reading> {
        self.map_batch_at(observations, Instant::now())
    }

    /// [`Self::map_batch`] with an explicit clock, so the condition-event rate limit is testable.
    fn map_batch_at(&mut self, observations: &[Observation], now: Instant) -> Vec<Reading> {
        for obs in observations {
            if let Some(state) = obs.condition_state() {
                let code = obs.extra("nativeCode").and_then(Value::as_str).map(str::to_string);
                let previous = self
                    .conditions
                    .insert(obs.data_item_id.clone(), (state, code.clone()))
                    .map(|(s, _)| s);
                // A latching Fault is the event an operator wants; a Fault that is merely still
                // asserted is not, so only the TRANSITION into Fault raises one (rate-limited).
                if state == CondState::Fault
                    && previous != Some(CondState::Fault)
                    && self.allow_condition_event(&obs.data_item_id, now)
                {
                    self.notices.push(Notice {
                        event_type: EVENT_CONDITION,
                        level: NoticeLevel::Critical,
                        message: format!("condition `{}` went to Fault", obs.data_item_id),
                        context: json!({
                            "instance": self.device.id,
                            "dataItemId": obs.data_item_id,
                            "state": "FAULT",
                            "previousState": previous.map(condition_state_name),
                            "nativeCode": code,
                            "timestamp": obs.timestamp,
                        }),
                    });
                }
            }
        }
        let mut out = Vec::new();
        for obs in observations {
            for sig in self.signals_for(&obs.data_item_id) {
                out.push(reading_from_observation(obs, sig, &self.model, &self.conditions));
            }
        }
        out
    }

    /// The BAD readings for signals with no data item in the model.
    fn unbound_readings(&self) -> Vec<Reading> {
        self.unbound
            .iter()
            .map(|id| Reading::bad(id.clone(), QUALITY_NO_SUCH_DATAITEM))
            .collect()
    }

    /// Adopt a refreshed model (probe drift) and recompile the signal set against it.
    fn adopt_current_model(&mut self) {
        if let Some(model) = self.agent.model(&self.device.device_uuid) {
            self.model = model;
            self.recompile();
        }
    }
}

#[async_trait]
impl DeviceSession for MtcSession {
    /// Drain everything the agent's acquisition task has delivered since the last call. An empty
    /// answer is not a failure: `/current` deduplicates, so a machine that is not changing produces
    /// nothing, which is exactly what "on change" means.
    async fn read_signals(&mut self) -> Result<Vec<Reading>> {
        self.adopt_current_signals();
        let mut observations = Vec::new();
        let mut down: Option<String> = None;
        let mut drifted = false;

        while let Ok(event) = self.rx.try_recv() {
            self.note(&event);
            match event {
                InstanceEvent::Obs(obs) => observations.push(*obs),
                InstanceEvent::Snapshot(batch) => observations.extend(batch),
                InstanceEvent::ModelDrift { .. } => drifted = true,
                InstanceEvent::AgentDown(reason) => down = Some(reason),
                InstanceEvent::AgentUp(_)
                | InstanceEvent::DataLoss { .. }
                | InstanceEvent::StreamDegraded { .. } => {}
            }
        }
        if drifted {
            self.adopt_current_model();
        }
        // The agent went away: the supervisor's reconnect ladder owns what happens next.
        if let Some(reason) = down {
            return Err(DeviceError::Transient(anyhow::anyhow!("agent unreachable: {reason}")));
        }

        let mut readings = self.map_batch(&observations);
        readings.extend(self.unbound_readings());
        Ok(readings)
    }

    /// `sb/read`: a scoped `/current` read, serialized with acquisition through the agent's control
    /// channel. Always live — a read answers with what the agent has now.
    async fn read_named(&mut self, ids: &[String]) -> Result<Vec<Reading>> {
        self.adopt_current_signals();
        let wanted: Vec<String> = self
            .device
            .signals
            .iter()
            .filter(|s| ids.iter().any(|id| id == &s.id))
            .map(|s| s.data_item_id.clone())
            .collect();
        if wanted.is_empty() {
            return Ok(self
                .unbound_readings()
                .into_iter()
                .filter(|r| ids.iter().any(|id| id == &r.signal_id))
                .collect());
        }

        let observations = self
            .agent
            .request_snapshot(&self.device.device_uuid, &wanted)
            .await
            .map_err(to_device_error)?;
        let mut readings: Vec<Reading> = self
            .map_batch(&observations)
            .into_iter()
            .filter(|r| ids.iter().any(|id| id == &r.signal_id))
            .collect();
        readings.extend(
            self.unbound_readings().into_iter().filter(|r| ids.iter().any(|id| id == &r.signal_id)),
        );
        Ok(readings)
    }

    /// Always refused: MTConnect's API is read-only by specification (D-MTC-7). The command layer
    /// refuses first, and this exists so no code path can ever reach a write.
    async fn write_signal(&mut self, _signal_id: &str, _value: &Value) -> Result<()> {
        Err(DeviceError::Permanent(anyhow::anyhow!(READ_ONLY_MESSAGE)))
    }

    /// `repoll`: a **forced, fresh** `/current` for this device, scoped to its configured data items
    /// and serialized with acquisition through the agent's control channel (LLD §7). It is not a
    /// drain of what happened to have arrived — a repoll exists precisely to say the current truth
    /// again, so every configured signal answers, `UNAVAILABLE` ones included.
    async fn snapshot_now(&mut self) -> Result<Vec<Reading>> {
        self.adopt_current_signals();
        let wanted = self.configured_data_items();
        let observations = if wanted.is_empty() {
            Vec::new()
        } else {
            self.agent
                .request_snapshot(&self.device.device_uuid, &wanted)
                .await
                .map_err(to_device_error)?
        };
        let mut readings = self.map_batch(&observations);
        readings.extend(self.unbound_readings());
        Ok(readings)
    }

    fn take_notices(&mut self) -> Vec<Notice> {
        std::mem::take(&mut self.notices)
    }

    /// The signals this session is really delivering: the configured set minus the ones whose data
    /// item the current device model does not have (those publish a permanent BAD instead). Same
    /// number whether acquisition is streaming or polling — the compiled set is what is served, and
    /// the mode only decides how it arrives.
    fn served_signals(&self) -> Option<u64> {
        Some(self.device.signals.len().saturating_sub(self.unbound.len()) as u64)
    }

    /// `sb/browse`: the probe tree, paged out of the cached model — so the address space stays
    /// browsable while the agent is unreachable.
    async fn browse(
        &mut self,
        cursor: Option<String>,
        max: usize,
    ) -> std::result::Result<BrowsePage, BrowseError> {
        let start = match cursor {
            None => 0usize,
            Some(c) => c
                .parse::<usize>()
                .map_err(|_| BrowseError::Failed(format!("invalid cursor `{c}`")))?,
        };
        let max = max.max(1);
        let entries: Vec<BrowsedSignal> = self
            .model
            .tree
            .iter()
            .skip(start)
            .take(max)
            .map(|node| BrowsedSignal {
                id: node.id.clone(),
                name: node.name.clone(),
                type_name: node.type_name.clone(),
            })
            .collect();
        let next = start + entries.len();
        Ok(BrowsePage {
            entries,
            next_cursor: (next < self.model.tree.len()).then(|| next.to_string()),
        })
    }

    async fn close(&mut self) {
        if !self.closed {
            self.agent.detach(&self.device.device_uuid);
            self.closed = true;
        }
    }
}

// =================================================================================================
// Observation -> Reading (HLD §5.3 and §6)
// =================================================================================================

/// The wire name of a condition state, for a notice's `previousState`.
#[must_use]
pub fn condition_state_name(state: CondState) -> &'static str {
    match state {
        CondState::Normal => "NORMAL",
        CondState::Warning => "WARNING",
        CondState::Fault => "FAULT",
        CondState::Unavailable => "UNAVAILABLE",
    }
}

/// Map one observation onto one configured signal.
///
/// * **Samples** publish numbers (or arrays for `TIME_SERIES`/`*_3D`), **Events** their string or
///   enum verbatim, and a **Condition** publishes its state as the value with the native code in
///   the extras.
/// * **`UNAVAILABLE`** is a BAD reading with no value — an explicit null on the wire, never a `0`.
/// * A bound condition degrades the signal: `Warning` → `UNCERTAIN`, `Fault` → `BAD`, with the
///   native code in `qualityRaw` so an operator sees *which* alarm did it.
/// * The observation timestamp is the agent's **capture** stamp (→ `serverTs`); `sourceTs` stays
///   absent (MTConnect does not distinguish a device-authored time), and the adapter's receive
///   moment is stamped by the worker.
/// * The `sequence` always rides as an extra: exact once-only ordering across reconnects.
#[must_use]
pub fn reading_from_observation(
    obs: &Observation,
    sig: &SignalConfig,
    model: &ProbeModel,
    conditions: &HashMap<String, (CondState, Option<String>)>,
) -> Reading {
    let (value, mut quality, mut quality_raw) = match &obs.value {
        ObsValue::Unavailable => (None, Quality::Bad, QUALITY_UNAVAILABLE.to_string()),
        ObsValue::Condition(CondState::Unavailable) => {
            (None, Quality::Bad, QUALITY_UNAVAILABLE.to_string())
        }
        ObsValue::Condition(state) => {
            let native = obs.extra("nativeCode").and_then(Value::as_str);
            let (q, raw) = condition_quality(*state, native);
            (Some(Value::String(state.as_str().to_string())), q, raw)
        }
        ObsValue::Scalar(v) => (Some(v.clone()), Quality::Good, QUALITY_OK.to_string()),
    };

    // A bound condition can only make a reading worse, never better.
    if quality != Quality::Bad {
        if let Some((state, native)) = worst_bound_condition(sig, conditions) {
            let (bound_quality, bound_raw) = condition_quality(state, native.as_deref());
            if severity_of(bound_quality) > severity_of(quality) {
                quality = bound_quality;
                quality_raw = bound_raw;
            }
        }
    }

    let mut extra = serde_json::Map::new();
    extra.insert("sequence".to_string(), Value::from(obs.sequence));
    for (key, value) in &obs.extras {
        extra.insert((*key).to_string(), value.clone());
    }

    Reading {
        signal_id: sig.id.clone(),
        name: sig
            .name
            .clone()
            .or_else(|| obs.name.clone())
            .or_else(|| model.item(&obs.data_item_id).and_then(|m| m.name.clone())),
        value,
        quality,
        quality_raw: Some(quality_raw),
        // MTConnect has no device-authored time: the agent's stamp is a CAPTURE stamp.
        source_ts: None,
        capture_ts: (!obs.timestamp.is_empty()).then(|| obs.timestamp.clone()),
        // The worker stamps the receive moment for the whole batch at read completion.
        received_ts: None,
        extra: Some(extra),
        channel: sig.channel.clone(),
    }
}

/// The quality one condition state implies, with the `qualityRaw` an operator can act on.
#[must_use]
pub fn condition_quality(state: CondState, native_code: Option<&str>) -> (Quality, String) {
    let with_code = |prefix: &str| match native_code {
        Some(code) if !code.is_empty() => format!("{prefix}:{code}"),
        _ => prefix.to_string(),
    };
    match state {
        CondState::Normal => (Quality::Good, format!("{QUALITY_OK}:NORMAL")),
        CondState::Warning => (Quality::Uncertain, with_code("MTC_CONDITION:WARNING")),
        CondState::Fault => (Quality::Bad, with_code("MTC_CONDITION:FAULT")),
        CondState::Unavailable => (Quality::Bad, QUALITY_UNAVAILABLE.to_string()),
    }
}

/// The worst state among the conditions a signal is bound to (D-MTC-8). `None` when the signal
/// binds no conditions, or none of them has been observed yet.
#[must_use]
pub fn worst_bound_condition(
    sig: &SignalConfig,
    conditions: &HashMap<String, (CondState, Option<String>)>,
) -> Option<(CondState, Option<String>)> {
    sig.condition_binding
        .iter()
        .filter_map(|id| conditions.get(id))
        .max_by_key(|(state, _)| state.severity())
        .cloned()
}

fn severity_of(q: Quality) -> u8 {
    match q {
        Quality::Good => 0,
        Quality::Uncertain => 1,
        Quality::Bad => 2,
    }
}

#[cfg(test)]
mod mtconnect_seam_tests {
    use super::*;
    use crate::mtconnect::config::{parse_agents, AgentCredentials, PublishCfg};
    use crate::mtconnect::model::Category;
    use crate::mtconnect::xml::{parse_devices, parse_streams};
    use serde_json::json;
    use std::time::Duration;

    const DEVICES_2_7: &str = include_str!("../tests/fixtures/devices_2.7.xml");
    const CURRENT_2_7: &str = include_str!("../tests/fixtures/current_2.7.xml");

    fn model() -> Arc<ProbeModel> {
        Arc::new(
            ProbeModel::from_devices(&parse_devices(DEVICES_2_7).unwrap(), "OKUMA.123456").unwrap(),
        )
    }

    fn observations() -> Vec<Observation> {
        let doc = parse_streams(CURRENT_2_7).unwrap();
        let m = model();
        doc.device_streams
            .iter()
            .find(|d| d.uuid == "OKUMA.123456")
            .unwrap()
            .entries
            .iter()
            .filter_map(|e| {
                let id = e.elem.attr("dataItemId")?;
                crate::mtconnect::observations::decode(e, m.item(id))
            })
            .collect()
    }

    fn obs(id: &str) -> Observation {
        observations().into_iter().find(|o| o.data_item_id == id).expect("observation")
    }

    fn signal(id: &str, data_item_id: &str) -> SignalConfig {
        SignalConfig {
            id: id.into(),
            name: None,
            channel: None,
            data_item_id: data_item_id.into(),
            condition_binding: Vec::new(),
            publish: PublishCfg::default(),
        }
    }

    fn device(signals: Vec<SignalConfig>) -> MtcDeviceConfig {
        MtcDeviceConfig {
            id: "cnc-1".into(),
            agent_id: "line-a-agent".into(),
            device_uuid: "OKUMA.123456".into(),
            signals,
        }
    }

    fn runtime(url: &str) -> Arc<AgentRuntime> {
        let cfg = parse_agents(&json!({ "agents": [{ "id": "line-a-agent", "url": url,
            "requestTimeoutMs": 300 }] }))
        .unwrap()
        .remove(0);
        AgentRuntime::new(cfg, &AgentCredentials::default()).unwrap()
    }

    fn connection(agent: &str, uuid: &str) -> ConnectionConfig {
        let mut extra = serde_json::Map::new();
        extra.insert("agentId".into(), json!(agent));
        extra.insert("deviceUuid".into(), json!(uuid));
        ConnectionConfig { endpoint: String::new(), extra }
    }

    // --- the mapping ------------------------------------------------------------------------

    #[test]
    fn a_sample_maps_to_its_value_with_the_agents_capture_stamp_and_its_sequence() {
        let r = reading_from_observation(
            &obs("Xabs"),
            &signal("x-position", "Xabs"),
            &model(),
            &HashMap::new(),
        );
        assert_eq!(r.signal_id, "x-position");
        assert_eq!(r.value, Some(json!(123.456)));
        assert_eq!(r.quality, Quality::Good);
        assert_eq!(r.quality_raw.as_deref(), Some("MTC_OK"));
        // The observation timestamp is the AGENT's capture stamp -> serverTs (crate::app maps it).
        assert_eq!(r.capture_ts.as_deref(), Some("2026-07-27T10:00:04.250000Z"));
        assert!(r.source_ts.is_none(), "MTConnect distinguishes no device-authored time");
        assert!(r.received_ts.is_none(), "the worker stamps the receive moment");
        assert_eq!(r.extra.as_ref().unwrap()["sequence"], json!(37));
        // The label falls back to the agent's own name for the data item.
        assert_eq!(r.name.as_deref(), Some("Xabs"));

        // The published sample carries all of it (docs/SOUTHBOUND.md §2).
        let s = crate::app::build_sample(&r);
        assert_eq!(s.server_ts.as_deref(), Some("2026-07-27T10:00:04.250000Z"));
        assert_eq!(s.extra.unwrap()["sequence"], json!(37));
    }

    #[test]
    fn unavailable_maps_to_a_bad_explicit_null() {
        let r = reading_from_observation(
            &obs("Xload"),
            &signal("x-load", "Xload"),
            &model(),
            &HashMap::new(),
        );
        assert_eq!(r.value, None, "no value - not a zero, not an empty string");
        assert_eq!(r.quality, Quality::Bad);
        assert_eq!(r.quality_raw.as_deref(), Some("UNAVAILABLE"));
        let s = crate::app::build_sample(&r);
        assert!(s.explicit_null, "published as an explicit null");
        assert_eq!(s.quality, Some(edgecommons::facades::Quality::Bad));
    }

    #[test]
    fn a_condition_publishes_its_state_as_the_value() {
        let r = reading_from_observation(
            &obs("Xtravel"),
            &signal("x-travel", "Xtravel"),
            &model(),
            &HashMap::new(),
        );
        assert_eq!(r.value, Some(json!("FAULT")));
        assert_eq!(r.quality, Quality::Bad);
        assert_eq!(r.quality_raw.as_deref(), Some("MTC_CONDITION:FAULT:ALM-1041"));
        let extra = r.extra.unwrap();
        assert_eq!(extra["nativeCode"], json!("ALM-1041"));
        assert_eq!(extra["conditionText"], json!("X axis travel limit exceeded"));

        // A Normal condition is GOOD, and says which state it is.
        let r = reading_from_observation(
            &obs("logic"),
            &signal("logic", "logic"),
            &model(),
            &HashMap::new(),
        );
        assert_eq!(r.value, Some(json!("NORMAL")));
        assert_eq!(r.quality, Quality::Good);
        assert_eq!(r.quality_raw.as_deref(), Some("MTC_OK:NORMAL"));
    }

    #[test]
    fn a_bound_condition_degrades_the_signal_it_guards() {
        let mut sig = signal("x-position", "Xabs");
        sig.condition_binding = vec!["Xtravel".into()];
        let mut conditions = HashMap::new();

        // Nothing observed yet: the value stands on its own.
        let r = reading_from_observation(&obs("Xabs"), &sig, &model(), &conditions);
        assert_eq!(r.quality, Quality::Good);

        // A warning makes the value UNCERTAIN — it is still a value, and the alarm says why.
        conditions.insert("Xtravel".to_string(), (CondState::Warning, Some("ALM-7".into())));
        let r = reading_from_observation(&obs("Xabs"), &sig, &model(), &conditions);
        assert_eq!(r.quality, Quality::Uncertain);
        assert_eq!(r.quality_raw.as_deref(), Some("MTC_CONDITION:WARNING:ALM-7"));
        assert_eq!(r.value, Some(json!(123.456)), "a warned value is still published");

        // A fault makes it BAD.
        conditions.insert("Xtravel".to_string(), (CondState::Fault, Some("ALM-1041".into())));
        let r = reading_from_observation(&obs("Xabs"), &sig, &model(), &conditions);
        assert_eq!(r.quality, Quality::Bad);
        assert_eq!(r.quality_raw.as_deref(), Some("MTC_CONDITION:FAULT:ALM-1041"));

        // An UNAVAILABLE value stays UNAVAILABLE: a binding cannot make a reading look better.
        let r = reading_from_observation(&obs("Xload"), &{
            let mut s = signal("x-load", "Xload");
            s.condition_binding = vec!["Xtravel".into()];
            s
        }, &model(), &conditions);
        assert_eq!(r.quality_raw.as_deref(), Some("UNAVAILABLE"));

        // An unbound condition affects only its own signal.
        let r = reading_from_observation(
            &obs("Xabs"),
            &signal("x-position", "Xabs"),
            &model(),
            &conditions,
        );
        assert_eq!(r.quality, Quality::Good);
    }

    #[test]
    fn the_worst_bound_condition_wins() {
        let mut sig = signal("s", "Xabs");
        sig.condition_binding = vec!["c1".into(), "c2".into(), "c3".into()];
        let mut conditions = HashMap::new();
        assert!(worst_bound_condition(&sig, &conditions).is_none());

        conditions.insert("c1".to_string(), (CondState::Normal, None));
        conditions.insert("c2".to_string(), (CondState::Warning, Some("W".into())));
        assert_eq!(
            worst_bound_condition(&sig, &conditions),
            Some((CondState::Warning, Some("W".into())))
        );
        conditions.insert("c3".to_string(), (CondState::Fault, Some("F".into())));
        assert_eq!(
            worst_bound_condition(&sig, &conditions),
            Some((CondState::Fault, Some("F".into())))
        );
        // A condition nobody bound is invisible here.
        conditions.insert("other".to_string(), (CondState::Fault, Some("X".into())));
        assert_eq!(
            worst_bound_condition(&signal("s", "Xabs"), &conditions),
            None
        );
    }

    #[test]
    fn condition_quality_names_the_alarm_when_the_agent_gave_one() {
        assert_eq!(
            condition_quality(CondState::Warning, Some("ALM-9")),
            (Quality::Uncertain, "MTC_CONDITION:WARNING:ALM-9".to_string())
        );
        assert_eq!(
            condition_quality(CondState::Warning, None),
            (Quality::Uncertain, "MTC_CONDITION:WARNING".to_string())
        );
        assert_eq!(
            condition_quality(CondState::Fault, Some("")),
            (Quality::Bad, "MTC_CONDITION:FAULT".to_string()),
            "an empty native code is not a code"
        );
        assert_eq!(
            condition_quality(CondState::Unavailable, Some("X")),
            (Quality::Bad, "UNAVAILABLE".to_string())
        );
    }

    #[test]
    fn several_signals_may_share_one_data_item() {
        let device = device(vec![signal("x-a", "Xabs"), signal("x-b", "Xabs")]);
        let agent = runtime("http://127.0.0.1:9");
        let (_tx, rx) = mpsc::channel(4);
        let mut session = MtcSession::new(agent, device, model(), rx);
        let readings = session.map_batch(&[obs("Xabs")]);
        assert_eq!(readings.len(), 2);
        assert_eq!(readings[0].signal_id, "x-a");
        assert_eq!(readings[1].signal_id, "x-b");
    }

    #[test]
    fn a_configured_channel_overrides_the_published_signal_path() {
        let mut sig = signal("x-position", "Xabs");
        sig.channel = Some("axes/x".into());
        let r = reading_from_observation(&obs("Xabs"), &sig, &model(), &HashMap::new());
        assert_eq!(r.channel.as_deref(), Some("axes/x"));
    }

    // --- config plumbing ----------------------------------------------------------------------

    #[test]
    fn the_binding_is_read_off_the_open_connection_object() {
        let cfg = connection("line-a-agent", "OKUMA.123456");
        assert_eq!(
            connection_binding(&cfg).unwrap(),
            ("line-a-agent".to_string(), "OKUMA.123456".to_string())
        );

        let mut missing = connection("line-a-agent", "X");
        missing.extra.remove("deviceUuid");
        assert!(connection_binding(&missing).unwrap_err().contains("deviceUuid"));

        let blank = connection("line-a-agent", "   ");
        assert!(connection_binding(&blank).unwrap_err().contains("must not be empty"));
    }

    #[test]
    fn the_endpoint_is_derived_and_non_secret() {
        let base = Url::parse("http://agent.plant.local:5000/").unwrap();
        assert_eq!(
            endpoint_description(&base, "OKUMA.123456"),
            "mtconnect://agent.plant.local:5000/OKUMA.123456"
        );
        // The default port is not invented, and a hostile uuid cannot forge a path.
        let base = Url::parse("https://agent.plant.local/plant-a/").unwrap();
        assert_eq!(
            endpoint_description(&base, "a/b c"),
            "mtconnect://agent.plant.local/a%2Fb%20c"
        );
    }

    // --- the session --------------------------------------------------------------------------

    #[tokio::test]
    async fn a_missing_binding_is_a_permanent_configuration_failure() {
        let backend = MtcBackend::new(HashMap::new(), vec![device(vec![])]);
        // An instance whose connection names nothing.
        let mut bare = connection("line-a-agent", "OKUMA.123456");
        bare.extra.remove("agentId");
        let Err(e) = backend.connect(&bare).await else { panic!("must fail") };
        assert!(!e.is_transient());

        // A device that is not configured on that agent.
        let Err(e) = backend.connect(&connection("line-a-agent", "OTHER")).await else {
            panic!("must fail")
        };
        assert!(!e.is_transient());

        // A device whose agent runtime does not exist.
        let Err(e) = backend.connect(&connection("line-a-agent", "OKUMA.123456")).await else {
            panic!("must fail")
        };
        assert!(!e.is_transient(), "an unconfigured agent will not appear by retrying");
        assert_eq!(backend.kind(), KIND);
    }

    #[test]
    fn the_inventory_is_a_config_view_with_no_device_round_trip() {
        let backend = MtcBackend::new(
            HashMap::new(),
            vec![device(vec![
                SignalConfig { name: Some("X position".into()), ..signal("x-position", "Xabs") },
                signal("x-load", "Xload"),
            ])],
        );
        let inv = backend.inventory(&connection("line-a-agent", "OKUMA.123456"));
        assert_eq!(inv.len(), 2);
        assert_eq!(inv[0].id, "x-position");
        assert_eq!(inv[0].name.as_deref(), Some("X position"));
        assert_eq!(inv[1].name, None);
        // An unknown device has no inventory, and asking is not an error.
        assert!(backend.inventory(&connection("line-a-agent", "NOPE")).is_empty());
    }

    #[tokio::test]
    async fn reading_drains_what_the_agent_delivered_and_says_nothing_when_nothing_changed() {
        let agent = runtime("http://127.0.0.1:9");
        let handle = agent.attach("OKUMA.123456");
        let device = device(vec![signal("x-position", "Xabs"), signal("x-load", "Xload")]);
        let mut session = MtcSession::new(Arc::clone(&agent), device, model(), handle.rx);

        // Nothing delivered yet: an empty read, not a failure.
        assert!(session.read_signals().await.unwrap().is_empty());

        agent.ingest_streams(CURRENT_2_7, false).unwrap();
        let readings = session.read_signals().await.unwrap();
        assert_eq!(readings.len(), 2, "both configured signals, and nothing else");
        let x = readings.iter().find(|r| r.signal_id == "x-position").unwrap();
        assert_eq!(x.value, Some(json!(123.456)));
        let load = readings.iter().find(|r| r.signal_id == "x-load").unwrap();
        assert_eq!(load.quality, Quality::Bad);

        // The same document again publishes nothing: `/current` repeats itself, an update does not.
        agent.ingest_streams(CURRENT_2_7, false).unwrap();
        assert!(session.read_signals().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unbound_signal_is_reported_bad_every_cycle_rather_than_dropped() {
        let agent = runtime("http://127.0.0.1:9");
        let handle = agent.attach("OKUMA.123456");
        let device = device(vec![signal("ghost", "NOT-IN-MODEL")]);
        let mut session = MtcSession::new(agent, device, model(), handle.rx);

        let readings = session.read_signals().await.unwrap();
        assert_eq!(readings.len(), 1);
        assert_eq!(readings[0].signal_id, "ghost");
        assert_eq!(readings[0].quality, Quality::Bad);
        assert_eq!(readings[0].quality_raw.as_deref(), Some("MTC_NO_SUCH_DATAITEM"));
        assert_eq!(readings[0].value, None);
    }

    #[tokio::test]
    async fn a_lost_agent_is_a_transient_error_so_the_supervisor_reconnects() {
        let agent = runtime("http://127.0.0.1:9");
        let handle = agent.attach("OKUMA.123456");
        let mut session =
            MtcSession::new(Arc::clone(&agent), device(vec![signal("x", "Xabs")]), model(), handle.rx);

        // Bring it up, then knock it down.
        agent.ingest_streams(CURRENT_2_7, false).unwrap();
        session.read_signals().await.unwrap();
        assert!(agent.poll_once().await.is_err(), "the agent is not listening");

        let Err(e) = session.read_signals().await else { panic!("a lost agent must surface") };
        assert!(e.is_transient(), "reconnecting is exactly the right response");
    }

    // --- protocol notices -> UNS events (HLD §9) ------------------------------------------------

    /// A session fed from a channel the test owns, so every runtime event can be delivered
    /// deliberately — including the ones a fake agent could not be made to produce on demand.
    fn scripted_session(
        signals: Vec<SignalConfig>,
    ) -> (MtcSession, mpsc::Sender<InstanceEvent>) {
        let agent = runtime("http://127.0.0.1:9");
        let (tx, rx) = mpsc::channel(32);
        (MtcSession::new(agent, device(signals), model(), rx), tx)
    }

    #[tokio::test]
    async fn every_runtime_event_becomes_the_operator_event_it_deserves() {
        let (mut session, tx) = scripted_session(vec![signal("x", "Xabs")]);

        let info = Arc::new(crate::mtconnect::AgentInfo {
            agent_id: "line-a-agent".into(),
            mode: "stream",
            instance_id: Some(1_700_000_000),
            agent_version: Some("2.7.0.12".into()),
            ..Default::default()
        });
        tx.send(InstanceEvent::AgentUp(info)).await.unwrap();
        tx.send(InstanceEvent::DataLoss { skipped: 12 }).await.unwrap();
        tx.send(InstanceEvent::ModelDrift { old: "aa".into(), new: "bb".into() }).await.unwrap();
        tx.send(InstanceEvent::StreamDegraded { failures: 3 }).await.unwrap();
        session.read_signals().await.expect("drain");

        let notices = session.take_notices();
        assert_eq!(notices.len(), 4);
        assert!(session.take_notices().is_empty(), "a drain empties the queue");

        let up = &notices[0];
        assert_eq!(up.event_type, EVENT_AGENT);
        assert_eq!(up.level, NoticeLevel::Info);
        assert_eq!(up.context["state"], json!("up"));
        assert_eq!(up.context["agentId"], json!("line-a-agent"));
        assert_eq!(up.context["instanceId"], json!(1_700_000_000u64));
        assert_eq!(up.context["mode"], json!("stream"));

        let loss = &notices[1];
        assert_eq!(loss.event_type, EVENT_DATA_LOSS);
        assert_eq!(loss.level, NoticeLevel::Warning);
        // The count and the sequence window are event FIELDS - never metric dimensions (HLD §9).
        assert_eq!(loss.context["skipped"], json!(12));
        assert!(loss.context.get("firstSequence").is_some());
        assert!(loss.context.get("nextSequence").is_some());

        let drift = &notices[2];
        assert_eq!(drift.event_type, EVENT_MODEL_DRIFT);
        assert_eq!(drift.context["oldDigest"], json!("aa"));
        assert_eq!(drift.context["newDigest"], json!("bb"));
        assert_eq!(drift.context["deviceUuid"], json!("OKUMA.123456"));

        let degraded = &notices[3];
        assert_eq!(degraded.event_type, EVENT_AGENT);
        assert_eq!(degraded.level, NoticeLevel::Warning);
        assert_eq!(degraded.context["state"], json!("degraded"));
        assert_eq!(degraded.context["failures"], json!(3));

        // No notice carries anything that came out of a vault.
        for n in &notices {
            let rendered = n.context.to_string();
            assert!(!rendered.contains("secret") && !rendered.contains("password"), "{rendered}");
        }
    }

    #[tokio::test]
    async fn a_lost_agent_still_reports_its_event_even_though_the_read_fails() {
        let (mut session, tx) = scripted_session(vec![signal("x", "Xabs")]);
        tx.send(InstanceEvent::AgentDown("connect refused".into())).await.unwrap();
        assert!(session.read_signals().await.is_err(), "the supervisor must reconnect");

        let notices = session.take_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].event_type, EVENT_AGENT);
        assert_eq!(notices[0].level, NoticeLevel::Critical);
        assert_eq!(notices[0].context["state"], json!("down"));
        assert_eq!(notices[0].context["reason"], json!("connect refused"));
    }

    #[test]
    fn only_the_transition_into_fault_raises_a_condition_event_and_at_most_once_a_minute() {
        let agent = runtime("http://127.0.0.1:9");
        let (_tx, rx) = mpsc::channel(4);
        let mut session =
            MtcSession::new(agent, device(vec![signal("x", "Xabs")]), model(), rx);

        let cond = |state: CondState, seq: u64| Observation {
            data_item_id: "Xtravel".into(),
            sequence: seq,
            timestamp: "2026-07-27T10:00:00Z".into(),
            name: None,
            value: ObsValue::Condition(state),
            extras: smallvec::smallvec![("nativeCode", json!("ALM-2"))],
            element: "Fault".into(),
            sub_type: None,
            category: Category::Condition,
        };

        let t0 = Instant::now();
        session.map_batch_at(&[cond(CondState::Normal, 1)], t0);
        assert!(session.take_notices().is_empty(), "a healthy condition is not an event");

        session.map_batch_at(&[cond(CondState::Fault, 2)], t0);
        let raised = session.take_notices();
        assert_eq!(raised.len(), 1);
        assert_eq!(raised[0].event_type, EVENT_CONDITION);
        assert_eq!(raised[0].level, NoticeLevel::Critical);
        assert_eq!(raised[0].context["dataItemId"], json!("Xtravel"));
        assert_eq!(raised[0].context["state"], json!("FAULT"));
        assert_eq!(raised[0].context["previousState"], json!("NORMAL"));
        assert_eq!(raised[0].context["nativeCode"], json!("ALM-2"));

        // Still faulted: the state keeps publishing as the signal's value, the EVENT does not repeat.
        session.map_batch_at(&[cond(CondState::Fault, 3)], t0 + Duration::from_secs(1));
        assert!(session.take_notices().is_empty(), "a still-asserted fault is not a new event");

        // A fault that clears and re-latches inside the window is still rate-limited.
        session.map_batch_at(&[cond(CondState::Normal, 4)], t0 + Duration::from_secs(2));
        session.map_batch_at(&[cond(CondState::Fault, 5)], t0 + Duration::from_secs(3));
        assert!(session.take_notices().is_empty(), "one condition event per data item per minute");

        // Past the window, a re-latch is news again.
        session.map_batch_at(&[cond(CondState::Normal, 6)], t0 + Duration::from_secs(61));
        session.map_batch_at(&[cond(CondState::Fault, 7)], t0 + Duration::from_secs(62));
        assert_eq!(session.take_notices().len(), 1);
    }

    #[tokio::test]
    async fn signals_subscribed_counts_what_is_served_not_what_was_configured() {
        // Two signals bind real data items, one binds a data item the device does not have: the
        // gauge must report the two that are really delivered, in stream mode and poll mode alike
        // (the compiled set is what is served; the mode only decides how it arrives).
        let (session, _tx) = scripted_session(vec![
            signal("x-position", "Xabs"),
            signal("x-load", "Xload"),
            signal("ghost", "NOT-IN-MODEL"),
        ]);
        assert_eq!(session.served_signals(), Some(2));

        let (empty, _tx) = scripted_session(vec![]);
        assert_eq!(empty.served_signals(), Some(0));

        // The template simulator compiles nothing, so it keeps the configured inventory size.
        let sim_conn: ConnectionConfig =
            serde_json::from_value(json!({ "endpoint": "sim://plc-1" })).unwrap();
        let sim = SimBackend.connect(&sim_conn).await.unwrap();
        assert_eq!(sim.served_signals(), None);
    }

    #[tokio::test]
    async fn writes_are_refused_at_the_seam_itself() {
        let agent = runtime("http://127.0.0.1:9");
        let handle = agent.attach("OKUMA.123456");
        let mut session = MtcSession::new(agent, device(vec![]), model(), handle.rx);
        let Err(e) = session.write_signal("x-position", &json!(1.0)).await else {
            panic!("MTConnect has no write path")
        };
        assert!(!e.is_transient());
        assert!(e.to_string().contains("read-only"));
    }

    #[tokio::test]
    async fn browse_pages_the_cached_probe_tree_and_works_offline() {
        let agent = runtime("http://127.0.0.1:9");
        let handle = agent.attach("OKUMA.123456");
        let mut session = MtcSession::new(agent, device(vec![]), model(), handle.rx);

        let page = session.browse(None, 3).await.unwrap();
        assert_eq!(page.entries.len(), 3);
        assert_eq!(page.entries[0].id, "mtc:/component/");
        assert_eq!(page.entries[0].type_name, "Device");
        assert_eq!(page.entries[1].id, "mtc:/item/avail");
        assert_eq!(page.next_cursor.as_deref(), Some("3"));

        let page = session.browse(page.next_cursor, 100).await.unwrap();
        assert_eq!(page.entries[0].id, "mtc:/component/Axes");
        assert!(page.next_cursor.is_none(), "the last page ends the enumeration");

        assert!(matches!(
            session.browse(Some("not-a-number".into()), 10).await,
            Err(BrowseError::Failed(_))
        ));
    }

    #[tokio::test]
    async fn closing_detaches_the_instance_and_is_safe_twice() {
        let agent = runtime("http://127.0.0.1:9");
        let handle = agent.attach("OKUMA.123456");
        let mut session = MtcSession::new(Arc::clone(&agent), device(vec![]), model(), handle.rx);
        assert_eq!(agent.attached(), vec!["OKUMA.123456".to_string()]);
        session.close().await;
        session.close().await;
        assert!(agent.attached().is_empty());
    }

    // --- credential resolution ----------------------------------------------------------------

    /// A vault stand-in. Only `get_string` is used by the resolver, which is the whole surface the
    /// boundary needs.
    struct FakeVault(HashMap<String, String>);

    impl edgecommons::credentials::CredentialService for FakeVault {
        fn get(&self, _name: &str) -> edgecommons::Result<Option<edgecommons::credentials::Secret>> {
            Ok(None)
        }
        fn get_version(
            &self,
            _name: &str,
            _version: &str,
        ) -> edgecommons::Result<Option<edgecommons::credentials::Secret>> {
            Ok(None)
        }
        fn exists(&self, name: &str) -> edgecommons::Result<bool> {
            Ok(self.0.contains_key(name))
        }
        fn list(
            &self,
            _prefix: &str,
        ) -> edgecommons::Result<Vec<edgecommons::credentials::SecretMeta>> {
            Ok(Vec::new())
        }
        fn versions(&self, _name: &str) -> edgecommons::Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn put(
            &self,
            _name: &str,
            _value: &[u8],
            _opts: edgecommons::credentials::PutOptions,
        ) -> edgecommons::Result<String> {
            Ok("v1".into())
        }
        fn delete(&self, _name: &str) -> edgecommons::Result<bool> {
            Ok(false)
        }
        fn get_string(&self, name: &str) -> edgecommons::Result<Option<String>> {
            Ok(self.0.get(name).cloned())
        }
    }

    fn agent_with(extra: serde_json::Value) -> crate::mtconnect::config::AgentConfig {
        let mut entry = json!({ "id": "line-a-agent", "url": "https://agent:5001" });
        for (k, v) in extra.as_object().cloned().unwrap_or_default() {
            entry[k] = v;
        }
        parse_agents(&json!({ "agents": [entry] })).unwrap().remove(0)
    }

    #[test]
    fn an_agent_with_no_references_needs_no_vault() {
        let creds = resolve_agent_credentials(&agent_with(json!({})), None).unwrap();
        assert_eq!(creds, crate::mtconnect::config::AgentCredentials::default());
    }

    #[test]
    fn references_resolve_into_material_the_client_can_use() {
        let vault = FakeVault(HashMap::from([
            ("mtc/agent-pw".to_string(), "s3cret".to_string()),
            ("mtc/ca".to_string(), "-----BEGIN CERTIFICATE-----".to_string()),
        ]));
        let cfg = agent_with(json!({
            "auth": { "type": "basic", "username": "reader", "secretRef": "mtc/agent-pw" },
            "tls": { "caSecretRef": "mtc/ca" }
        }));
        let creds = resolve_agent_credentials(&cfg, Some(&vault)).unwrap();
        assert_eq!(
            creds.auth,
            Some(crate::mtconnect::config::AuthMaterial::Basic {
                username: "reader".into(),
                password: "s3cret".into()
            })
        );
        assert_eq!(
            creds.tls.unwrap().ca_pem.as_deref(),
            Some("-----BEGIN CERTIFICATE-----")
        );

        // The reference itself never carries the value, and the config never held it.
        assert!(!format!("{cfg:?}").contains("s3cret"));
    }

    #[test]
    fn an_unresolvable_reference_fails_loudly_instead_of_going_unauthenticated() {
        let vault = FakeVault(HashMap::new());
        let cfg = agent_with(json!({ "auth": { "type": "bearer", "secretRef": "mtc/token" } }));
        let err = resolve_agent_credentials(&cfg, Some(&vault)).unwrap_err();
        assert!(err.contains("mtc/token"), "{err}");

        // No credential service at all, with references configured, is a configuration error.
        let err = resolve_agent_credentials(&cfg, None).unwrap_err();
        assert!(err.contains("credentials"), "{err}");
    }

    #[tokio::test]
    async fn read_named_of_an_unknown_signal_asks_the_agent_for_nothing() {
        let agent = runtime("http://127.0.0.1:9");
        let handle = agent.attach("OKUMA.123456");
        let mut session =
            MtcSession::new(agent, device(vec![signal("x-position", "Xabs")]), model(), handle.rx);
        // No configured signal matches, so there is nothing to read and no request to make.
        assert!(session.read_named(&["nope".to_string()]).await.unwrap().is_empty());
    }
}
