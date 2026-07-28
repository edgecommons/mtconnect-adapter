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

use async_trait::async_trait;
use serde::Deserialize;

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
    pub value: serde_json::Value,
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

    /// Close the connection. Must be safe to call twice.
    async fn close(&mut self) {}
}

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
    /// The endpoint, in whatever form the protocol uses. Published in `device.endpoint`.
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
                signal_id: "temperature-1".into(),
                name: Some("Ambient temperature".into()),
                value: serde_json::json!(value),
                quality: Quality::Good,
                quality_raw: Some("OK".into()),
                source_ts: None,
                capture_ts: None,
                received_ts: None,
            },
            // A signal the simulated device cannot currently read. It is published as BAD rather
            // than omitted, because "I could not read this" is information and silence is not.
            Reading {
                signal_id: "pressure-1".into(),
                name: Some("Line pressure".into()),
                value: serde_json::Value::Null,
                quality: Quality::Bad,
                quality_raw: Some("SENSOR_FAULT".into()),
                source_ts: None,
                capture_ts: None,
                received_ts: None,
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
