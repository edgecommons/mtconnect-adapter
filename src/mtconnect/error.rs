//! # `MtcError` — the client's error taxonomy (LLD §9)
//!
//! One enum for everything the owned MTConnect client can fail at, classified so the callers above
//! the seam can make two decisions without knowing HTTP or XML: **is this transient** (reconnect)
//! and **what code does an operator see** (`sb/read` per-entry codes, `qualityRaw`).
//!
//! Nothing in this module — or anywhere under `src/mtconnect/` — imports `edgecommons`. The
//! translation into `DeviceError`/`CommandError` happens at the `device.rs` boundary.

use std::fmt;

/// Everything the MTConnect client can fail at.
#[derive(Debug, thiserror::Error)]
pub enum MtcError {
    /// The agent answered with a non-2xx status.
    #[error("agent returned HTTP {status}")]
    Http { status: u16 },
    /// The request did not complete in `requestTimeoutMs`.
    #[error("request timed out after {ms} ms")]
    Timeout { ms: u64 },
    /// The transport failed (DNS, connect, reset, body read).
    #[error("transport error: {0}")]
    Transport(String),
    /// TLS setup or negotiation failed (bad CA/identity material, handshake refused).
    #[error("TLS error: {0}")]
    Tls(String),
    /// Authentication material is missing, unresolvable, or was rejected (401/403).
    #[error("authentication error: {0}")]
    Auth(String),
    /// A document (or a multipart part) exceeded `maxDocumentBytes`.
    #[error("document exceeds maxDocumentBytes ({limit} bytes)")]
    TooLarge { limit: usize },
    /// The multipart framing was malformed (boundary, part headers, truncation).
    #[error("multipart error: {0}")]
    Multipart(String),
    /// The XML was malformed, exceeded a structural cap, or was not the expected document type.
    #[error("xml error: {0}")]
    Xml(String),
    /// The agent's namespace version is outside the supported 1.3–2.7 window.
    #[error("unsupported MTConnect version `{0}` (supported: 1.3-2.7)")]
    UnsupportedVersion(String),
    /// The requested `from` fell outside the agent's buffer — recovery ladder 2.
    #[error("sequence out of range (agent firstSequence {first})")]
    OutOfRange { first: u64 },
    /// The agent's `instanceId` changed (it restarted) — recovery ladder 3.
    #[error("agent instanceId changed {old} -> {new}")]
    InstanceChanged { old: u64, new: u64 },
    /// The probe does not contain the configured `deviceUuid`.
    #[error("no device with uuid `{0}` in the agent's probe")]
    NoSuchDevice(String),
    /// A configured signal binds a `dataItemId` the device model does not have.
    #[error("no dataItem `{0}` in the device model")]
    NoSuchDataItem(String),
    /// An `MTConnectErrors` document that is not `OUT_OF_RANGE`.
    #[error("agent error {code}: {message}")]
    AgentError { code: String, message: String },
    /// The configuration is structurally or semantically invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
}

impl MtcError {
    /// Whether retrying the same operation could plausibly succeed. Drives the supervisor's
    /// transient/permanent split (a permanent failure backs off to the ceiling instead of
    /// hammering an agent that will keep refusing).
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            // 5xx and 408/429 are worth retrying; other 4xx are a client-side mistake.
            Self::Http { status } => *status >= 500 || *status == 408 || *status == 429,
            Self::Timeout { .. }
            | Self::Transport(_)
            | Self::TooLarge { .. }
            | Self::Multipart(_)
            | Self::Xml(_)
            | Self::OutOfRange { .. }
            | Self::InstanceChanged { .. }
            | Self::AgentError { .. } => true,
            Self::Tls(_)
            | Self::Auth(_)
            | Self::UnsupportedVersion(_)
            | Self::NoSuchDevice(_)
            | Self::NoSuchDataItem(_)
            | Self::Config(_) => false,
        }
    }

    /// The short, stable code an operator sees: the `<c>` of `MTC_AGENT_ERROR:<c>`, and the
    /// `qualityRaw` suffix for a failed read. Low-cardinality by construction (never an error
    /// string, never a status line).
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::Http { .. } => "HTTP",
            Self::Timeout { .. } => "TIMEOUT",
            Self::Transport(_) => "UNREACHABLE",
            Self::Tls(_) => "TLS",
            Self::Auth(_) => "AUTH",
            Self::TooLarge { .. } => "TOO_LARGE",
            Self::Multipart(_) => "MULTIPART",
            Self::Xml(_) => "PARSE",
            Self::UnsupportedVersion(_) => "UNSUPPORTED_VERSION",
            Self::OutOfRange { .. } => "OUT_OF_RANGE",
            Self::InstanceChanged { .. } => "INSTANCE_CHANGED",
            Self::NoSuchDevice(_) => "NO_SUCH_DEVICE",
            Self::NoSuchDataItem(_) => "NO_SUCH_DATAITEM",
            Self::AgentError { code, .. } => code,
            Self::Config(_) => "CONFIG",
        }
    }
}

/// A parse-side counter bag the runtime folds into the `MtconnectParse` metric family: how many
/// documents parsed, how many failed, how many elements the parser did not recognize (the
/// forward-compatibility signal — a 2.8 agent talking to a 2.7-aware parser shows up here rather
/// than as silent data loss), and how many observation elements were refused because a required
/// MTConnect field was missing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParseCounters {
    pub documents_parsed: u64,
    pub parse_errors: u64,
    pub unknown_elements: u64,
    /// Observation elements refused for a missing required field (`dataItemId`, `sequence`,
    /// `timestamp`) — data the agent sent and this client would not publish. Its own measure
    /// (D-R11) because it is neither a parse failure (the document parsed) nor an unknown element
    /// (the element was understood), and silently defaulting it is what C-3 was.
    pub rejected_observations: u64,
}

impl ParseCounters {
    pub fn record_ok(&mut self, unknown_elements: u64) {
        self.documents_parsed += 1;
        self.unknown_elements += unknown_elements;
    }

    pub fn record_err(&mut self) {
        self.parse_errors += 1;
    }

    /// One observation element refused (see [`super::observations::DecodeReject`]).
    pub fn record_rejected(&mut self) {
        self.rejected_observations += 1;
    }
}

impl fmt::Display for ParseCounters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "parsed={} errors={} unknown={} rejected={}",
            self.documents_parsed,
            self.parse_errors,
            self.unknown_elements,
            self.rejected_observations
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transience_splits_retryable_failures_from_client_side_mistakes() {
        assert!(MtcError::Http { status: 503 }.is_transient());
        assert!(MtcError::Http { status: 429 }.is_transient());
        assert!(!MtcError::Http { status: 404 }.is_transient());
        assert!(MtcError::Timeout { ms: 10 }.is_transient());
        assert!(MtcError::Transport("reset".into()).is_transient());
        // A bad credential or a missing device fails identically forever.
        assert!(!MtcError::Auth("no secret".into()).is_transient());
        assert!(!MtcError::NoSuchDevice("X".into()).is_transient());
        assert!(!MtcError::Config("no agents".into()).is_transient());
        assert!(!MtcError::Tls("bad ca".into()).is_transient());
        assert!(!MtcError::UnsupportedVersion("3.0".into()).is_transient());
        assert!(!MtcError::NoSuchDataItem("x".into()).is_transient());
    }

    #[test]
    fn every_variant_carries_a_low_cardinality_code() {
        assert_eq!(MtcError::Http { status: 500 }.code(), "HTTP");
        assert_eq!(MtcError::Timeout { ms: 1 }.code(), "TIMEOUT");
        assert_eq!(MtcError::Transport("x".into()).code(), "UNREACHABLE");
        assert_eq!(MtcError::Tls("x".into()).code(), "TLS");
        assert_eq!(MtcError::Auth("x".into()).code(), "AUTH");
        assert_eq!(MtcError::TooLarge { limit: 1 }.code(), "TOO_LARGE");
        assert_eq!(MtcError::Multipart("x".into()).code(), "MULTIPART");
        assert_eq!(MtcError::Xml("x".into()).code(), "PARSE");
        assert_eq!(
            MtcError::UnsupportedVersion("9".into()).code(),
            "UNSUPPORTED_VERSION"
        );
        assert_eq!(MtcError::OutOfRange { first: 3 }.code(), "OUT_OF_RANGE");
        assert_eq!(
            MtcError::InstanceChanged { old: 1, new: 2 }.code(),
            "INSTANCE_CHANGED"
        );
        assert_eq!(MtcError::NoSuchDevice("d".into()).code(), "NO_SUCH_DEVICE");
        assert_eq!(
            MtcError::NoSuchDataItem("d".into()).code(),
            "NO_SUCH_DATAITEM"
        );
        assert_eq!(MtcError::Config("c".into()).code(), "CONFIG");
        // An agent-reported code rides through verbatim: it IS the operator's code.
        assert_eq!(
            MtcError::AgentError {
                code: "UNAUTHORIZED".into(),
                message: "no".into()
            }
            .code(),
            "UNAUTHORIZED"
        );
    }

    #[test]
    fn errors_render_without_leaking_more_than_their_code() {
        assert_eq!(
            MtcError::Http { status: 401 }.to_string(),
            "agent returned HTTP 401"
        );
        assert_eq!(
            MtcError::InstanceChanged { old: 1, new: 2 }.to_string(),
            "agent instanceId changed 1 -> 2"
        );
    }

    #[test]
    fn parse_counters_accumulate_documents_errors_unknown_elements_and_rejects() {
        let mut c = ParseCounters::default();
        c.record_ok(2);
        c.record_ok(0);
        c.record_err();
        c.record_rejected();
        c.record_rejected();
        c.record_rejected();
        assert_eq!(
            c,
            ParseCounters {
                documents_parsed: 2,
                parse_errors: 1,
                unknown_elements: 2,
                rejected_observations: 3,
            }
        );
        // A refused observation is its own count: the document still parsed.
        assert_eq!(c.to_string(), "parsed=2 errors=1 unknown=2 rejected=3");
    }
}
