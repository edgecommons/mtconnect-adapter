//! # The HTTP client: `/probe`, `/current`, `/sample` (LLD §6)
//!
//! One `reqwest` client per agent, built once from that agent's configuration and resolved
//! credentials. The deliberate choices, each of which is a safety property rather than a preference:
//!
//! * **rustls, no redirects.** An agent that answers a redirect is not the agent that was
//!   configured; following one would send credentials somewhere the operator never named.
//! * **`Accept: application/xml`.** XML is the interoperable floor across 1.3–2.7; the optional 2.x
//!   JSON representation is a separate follow-on capability (D-MTC-10), not something to receive by
//!   accident.
//! * **Every response is size-capped** at `maxDocumentBytes`, checked against `Content-Length`
//!   *and* enforced while reading (a chunked body advertises nothing).
//! * **Credentials are values handed in, never references resolved here.** This module cannot reach
//!   the EdgeCommons vault, by construction.
//!
//! One-shots return the document *text*; parsing is [`super::xml`]'s job. The streaming seam
//! ([`MtcClient::open_sample_stream`]) hands back an unread body for [`super::stream`] to consume.

use std::time::Duration;

use base64::Engine as _;
use reqwest::redirect::Policy;
use url::Url;

use super::config::{AgentConfig, AgentCredentials, AuthMaterial, TlsMaterial};
use super::error::MtcError;

/// The one media type this client accepts (D-MTC-10: XML only in R1).
pub const ACCEPT: &str = "application/xml";

/// One agent's HTTP client.
#[derive(Debug)]
pub struct MtcClient {
    http: reqwest::Client,
    base: Url,
    auth: Option<AuthMaterial>,
    max_document_bytes: usize,
    request_timeout: Duration,
}

/// What [`MtcClient::open_sample_stream`] asks the agent for — the streaming request the M3
/// acquisition state machine issues (`interval` + `heartbeat` + `from`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamRequest {
    /// Resume point; `None` starts at the agent's current position.
    pub from: Option<u64>,
    /// The `interval` query parameter: how often the agent may send a part.
    pub interval_ms: u32,
    /// The `heartbeat` query parameter: how often it MUST send one, empty if need be.
    pub heartbeat_ms: u32,
    /// An optional XPath (`path=`) server-side filter.
    pub path: Option<String>,
}

/// An open multipart response body. The framing lives in [`super::multipart`]; this type is just
/// the transport: a content type and a bounded chunk source.
#[derive(Debug)]
pub struct StreamResponse {
    content_type: String,
    response: reqwest::Response,
    max_document_bytes: usize,
}

impl StreamResponse {
    /// The response's `Content-Type` header verbatim — the boundary lives in its parameters, and
    /// cppagent sends `multipart/mixed` where the standard says `multipart/x-mixed-replace`.
    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// The part cap this stream inherits from its agent's configuration.
    #[must_use]
    pub fn max_document_bytes(&self) -> usize {
        self.max_document_bytes
    }

    /// The next chunk of the body, or `None` at end of stream.
    ///
    /// # Errors
    /// [`MtcError::Transport`] on a read failure.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, MtcError> {
        match self.response.chunk().await {
            Ok(Some(bytes)) => Ok(Some(bytes.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(map_transport_error(&e)),
        }
    }
}

/// The live implementation of the streaming state machine's transport seam.
impl super::stream::ChunkSource for StreamResponse {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, MtcError> {
        StreamResponse::next_chunk(self).await
    }
}

impl MtcClient {
    /// Build the client for one agent.
    ///
    /// # Errors
    /// [`MtcError::Tls`] when the supplied CA/identity material is unusable, and
    /// [`MtcError::Transport`] when the HTTP client itself cannot be constructed.
    pub fn new(cfg: &AgentConfig, creds: &AgentCredentials) -> Result<Self, MtcError> {
        let mut builder = reqwest::Client::builder()
            .use_rustls_tls()
            // An agent that redirects is not the agent that was configured.
            .redirect(Policy::none())
            .user_agent(concat!("mtconnect-adapter/", env!("CARGO_PKG_VERSION")));

        if let Some(tls) = &creds.tls {
            builder = apply_tls(builder, tls)?;
        }

        // A builder failure with TLS material in hand is a TLS problem: reqwest validates the
        // certificate/identity when it assembles the client, not when `from_pem` returns.
        let http = builder.build().map_err(|e| {
            if creds.tls.is_some() {
                MtcError::Tls(e.to_string())
            } else {
                MtcError::Transport(e.to_string())
            }
        })?;
        Ok(Self {
            http,
            base: cfg.url.clone(),
            auth: creds.auth.clone(),
            max_document_bytes: cfg.max_document_bytes,
            request_timeout: Duration::from_millis(u64::from(cfg.request_timeout_ms)),
        })
    }

    /// The configured base URL.
    #[must_use]
    pub fn base(&self) -> &Url {
        &self.base
    }

    /// The configured response cap.
    #[must_use]
    pub fn max_document_bytes(&self) -> usize {
        self.max_document_bytes
    }

    /// `GET /probe` — the agent's device model.
    ///
    /// # Errors
    /// See [`MtcClient::get_text`].
    pub async fn probe(&self) -> Result<String, MtcError> {
        self.get_text(self.url_for("probe", &[])?).await
    }

    /// `GET /current[?path=…]` — a snapshot of the latest observation for every data item.
    ///
    /// # Errors
    /// See [`MtcClient::get_text`].
    pub async fn current(&self, path: Option<&str>) -> Result<String, MtcError> {
        let mut query = Vec::new();
        if let Some(p) = path {
            query.push(("path", p.to_string()));
        }
        self.get_text(self.url_for("current", &query)?).await
    }

    /// `GET /sample?from=…&count=…` — a bounded window of the agent's buffer (the one-shot form;
    /// the streaming form is [`Self::open_sample_stream`]).
    ///
    /// # Errors
    /// See [`MtcClient::get_text`].
    pub async fn sample(
        &self,
        from: Option<u64>,
        count: Option<u32>,
        path: Option<&str>,
    ) -> Result<String, MtcError> {
        let mut query = Vec::new();
        if let Some(f) = from {
            query.push(("from", f.to_string()));
        }
        if let Some(c) = count {
            query.push(("count", c.to_string()));
        }
        if let Some(p) = path {
            query.push(("path", p.to_string()));
        }
        self.get_text(self.url_for("sample", &query)?).await
    }

    /// Open the multipart `/sample?interval=…&heartbeat=…` stream — **the seam the M3 acquisition
    /// state machine consumes**. The response is returned unread: no timeout is applied (a stream is
    /// supposed to stay open; liveness is the heartbeat's job, not the transport's).
    ///
    /// # Errors
    /// [`MtcError::Http`]/[`MtcError::Auth`] for a refused request, [`MtcError::Transport`] when the
    /// connection fails.
    pub async fn open_sample_stream(
        &self,
        req: &StreamRequest,
    ) -> Result<StreamResponse, MtcError> {
        let mut query = vec![
            ("interval", req.interval_ms.to_string()),
            ("heartbeat", req.heartbeat_ms.to_string()),
        ];
        if let Some(f) = req.from {
            query.push(("from", f.to_string()));
        }
        if let Some(p) = &req.path {
            query.push(("path", p.clone()));
        }
        let url = self.url_for("sample", &query)?;

        let response = self
            .authorize(self.http.get(url))
            .send()
            .await
            .map_err(|e| map_transport_error(&e))?;
        let response = check_status(response)?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        Ok(StreamResponse {
            content_type,
            response,
            max_document_bytes: self.max_document_bytes,
        })
    }

    /// Resolve an endpoint against the base URL, appending the query. The base's own path is
    /// preserved, so an agent published under `https://host/plant-a/` works.
    ///
    /// # Errors
    /// [`MtcError::Config`] when the resulting URL is not well-formed.
    pub fn url_for(&self, endpoint: &str, query: &[(&str, String)]) -> Result<Url, MtcError> {
        let base_path = self.base.path().trim_end_matches('/');
        let mut url = self.base.clone();
        url.set_path(&format!("{base_path}/{endpoint}"));
        url.set_query(None);
        if !query.is_empty() {
            let mut pairs = url.query_pairs_mut();
            for (k, v) in query {
                pairs.append_pair(k, v);
            }
        }
        Ok(url)
    }

    /// One bounded GET returning the document text.
    ///
    /// # Errors
    /// [`MtcError::Timeout`] past `requestTimeoutMs`; [`MtcError::Auth`] on 401/403;
    /// [`MtcError::Http`] on any other non-2xx; [`MtcError::TooLarge`] past `maxDocumentBytes`;
    /// [`MtcError::Transport`] otherwise.
    async fn get_text(&self, url: Url) -> Result<String, MtcError> {
        let response = self
            .authorize(self.http.get(url))
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(|e| map_transport_error(&e))?;
        let mut response = check_status(response)?;

        // A cooperative agent declares its size; a hostile or broken one does not, so the read
        // itself is bounded too.
        if let Some(len) = response.content_length() {
            if len > self.max_document_bytes as u64 {
                return Err(MtcError::TooLarge {
                    limit: self.max_document_bytes,
                });
            }
        }

        let mut body: Vec<u8> = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    if body.len() + chunk.len() > self.max_document_bytes {
                        return Err(MtcError::TooLarge {
                            limit: self.max_document_bytes,
                        });
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                Err(e) => return Err(map_transport_error(&e)),
            }
        }
        String::from_utf8(body)
            .map_err(|_| MtcError::Xml("response body is not valid UTF-8".into()))
    }

    fn authorize(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let rb = rb.header(reqwest::header::ACCEPT, ACCEPT);
        match &self.auth {
            Some(auth) => rb.header(reqwest::header::AUTHORIZATION, authorization_header(auth)),
            None => rb,
        }
    }
}

/// The `Authorization` header value for resolved credentials.
#[must_use]
pub fn authorization_header(auth: &AuthMaterial) -> String {
    match auth {
        AuthMaterial::Basic { username, password } => {
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
            format!("Basic {encoded}")
        }
        AuthMaterial::Bearer { token } => format!("Bearer {token}"),
    }
}

fn apply_tls(
    mut builder: reqwest::ClientBuilder,
    tls: &TlsMaterial,
) -> Result<reqwest::ClientBuilder, MtcError> {
    if let Some(ca) = &tls.ca_pem {
        for cert in reqwest::Certificate::from_pem_bundle(ca.as_bytes())
            .map_err(|e| MtcError::Tls(format!("ca bundle: {e}")))?
        {
            builder = builder.add_root_certificate(cert);
        }
    }
    if let (Some(cert), Some(key)) = (&tls.client_cert_pem, &tls.client_key_pem) {
        let mut pem = cert.clone();
        if !pem.ends_with('\n') {
            pem.push('\n');
        }
        pem.push_str(key);
        let identity = reqwest::Identity::from_pem(pem.as_bytes())
            .map_err(|e| MtcError::Tls(format!("client identity: {e}")))?;
        builder = builder.identity(identity);
    }
    Ok(builder)
}

/// Turn a non-2xx answer into the error an operator can act on: 401/403 is a credential problem,
/// everything else is the status.
fn check_status(response: reqwest::Response) -> Result<reqwest::Response, MtcError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    match status.as_u16() {
        401 | 403 => Err(MtcError::Auth(format!(
            "agent refused the credentials (HTTP {status})"
        ))),
        code => Err(MtcError::Http { status: code }),
    }
}

fn map_transport_error(e: &reqwest::Error) -> MtcError {
    if e.is_timeout() {
        return MtcError::Timeout { ms: 0 };
    }
    if e.is_connect() {
        return MtcError::Transport(format!("connect: {e}"));
    }
    MtcError::Transport(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mtconnect::config::parse_agents;
    use serde_json::json;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// A one-connection-at-a-time HTTP/1.1 agent stand-in: it records each request head and answers
    /// with whatever the responder returns. Raw sockets, so the test asserts the bytes on the wire
    /// (headers included) rather than a mock's idea of them.
    struct TestAgent {
        addr: SocketAddr,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl TestAgent {
        fn url(&self) -> String {
            format!("http://{}", self.addr)
        }
        fn last_request(&self) -> String {
            self.requests
                .lock()
                .unwrap()
                .last()
                .cloned()
                .unwrap_or_default()
        }
        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    async fn spawn_agent<F>(responder: F) -> TestAgent
    where
        F: Fn(&str) -> Option<Vec<u8>> + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let responder = Arc::new(responder);

        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let seen = Arc::clone(&seen);
                let responder = Arc::clone(&responder);
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match sock.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => head.push(byte[0]),
                        }
                    }
                    let head = String::from_utf8_lossy(&head).into_owned();
                    seen.lock().unwrap().push(head.clone());
                    match responder(&head) {
                        // `None` = never answer: the request must hit its own timeout.
                        None => tokio::time::sleep(Duration::from_secs(30)).await,
                        Some(bytes) => {
                            let _ = sock.write_all(&bytes).await;
                            let _ = sock.flush().await;
                        }
                    }
                });
            }
        });

        TestAgent { addr, requests }
    }

    fn http_ok(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn client_for(url: &str, extra: serde_json::Value) -> MtcClient {
        let mut entry = json!({ "id": "test-agent", "url": url });
        for (k, v) in extra.as_object().cloned().unwrap_or_default() {
            entry[k] = v;
        }
        let agents = parse_agents(&json!({ "agents": [entry] })).unwrap();
        MtcClient::new(&agents[0], &AgentCredentials::default()).unwrap()
    }

    #[test]
    fn urls_are_built_against_the_configured_base_path() {
        let c = client_for("http://agent:5000", json!({}));
        assert_eq!(
            c.url_for("probe", &[]).unwrap().as_str(),
            "http://agent:5000/probe"
        );
        assert_eq!(
            c.url_for("current", &[("path", "//Axes".to_string())])
                .unwrap()
                .as_str(),
            "http://agent:5000/current?path=%2F%2FAxes"
        );
        assert_eq!(
            c.url_for("sample", &[("from", "42".into()), ("count", "100".into())])
                .unwrap()
                .as_str(),
            "http://agent:5000/sample?from=42&count=100"
        );

        // An agent published under a path prefix keeps it.
        let c = client_for("https://gw.example.com/plant-a/", json!({}));
        assert_eq!(
            c.url_for("probe", &[]).unwrap().as_str(),
            "https://gw.example.com/plant-a/probe"
        );
        assert_eq!(c.base().as_str(), "https://gw.example.com/plant-a/");
        assert_eq!(
            c.max_document_bytes(),
            super::super::config::DEFAULT_MAX_DOCUMENT_BYTES
        );
    }

    #[test]
    fn the_authorization_header_is_built_from_resolved_material_only() {
        assert_eq!(
            authorization_header(&AuthMaterial::Basic {
                username: "reader".into(),
                password: "s3cret".into()
            }),
            // base64("reader:s3cret")
            "Basic cmVhZGVyOnMzY3JldA=="
        );
        assert_eq!(
            authorization_header(&AuthMaterial::Bearer {
                token: "abc.def".into()
            }),
            "Bearer abc.def"
        );
    }

    #[tokio::test]
    async fn a_probe_sends_accept_xml_and_returns_the_document() {
        let agent = spawn_agent(|_| Some(http_ok("<MTConnectDevices/>"))).await;
        let c = client_for(&agent.url(), json!({}));
        assert_eq!(c.probe().await.unwrap(), "<MTConnectDevices/>");

        let head = agent.last_request();
        assert!(head.starts_with("GET /probe HTTP/1.1"), "{head}");
        assert!(head.contains("accept: application/xml"), "{head}");
        assert!(
            !head.to_lowercase().contains("authorization:"),
            "no auth configured, none sent"
        );
    }

    #[tokio::test]
    async fn credentials_ride_every_request_when_configured() {
        let agent = spawn_agent(|_| Some(http_ok("<MTConnectStreams/>"))).await;
        let agents =
            parse_agents(&json!({ "agents": [{ "id": "a", "url": agent.url() }] })).unwrap();
        let creds = AgentCredentials {
            auth: Some(AuthMaterial::Bearer {
                token: "tok-123".into(),
            }),
            tls: None,
        };
        let c = MtcClient::new(&agents[0], &creds).unwrap();
        c.current(None).await.unwrap();
        assert!(agent
            .last_request()
            .contains("authorization: Bearer tok-123"));
    }

    #[tokio::test]
    async fn current_and_sample_carry_their_query_parameters() {
        let agent = spawn_agent(|_| Some(http_ok("<MTConnectStreams/>"))).await;
        let c = client_for(&agent.url(), json!({}));

        c.current(Some("//Axes//DataItem[@category='SAMPLE']"))
            .await
            .unwrap();
        assert!(agent.last_request().starts_with("GET /current?path="));

        c.sample(Some(42), Some(100), None).await.unwrap();
        assert!(agent
            .last_request()
            .starts_with("GET /sample?from=42&count=100"));

        c.sample(None, None, None).await.unwrap();
        assert!(agent.last_request().starts_with("GET /sample HTTP/1.1"));
    }

    #[tokio::test]
    async fn the_streaming_seam_asks_for_interval_heartbeat_and_from() {
        let body = "--x\r\nContent-type: text/xml\r\nContent-length: 2\r\n\r\nhi\r\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: multipart/x-mixed-replace;boundary=x\r\nConnection: close\r\n\r\n{body}"
        );
        let agent = spawn_agent(move |_| Some(response.clone().into_bytes())).await;
        let c = client_for(&agent.url(), json!({}));

        let mut stream = c
            .open_sample_stream(&StreamRequest {
                from: Some(42),
                interval_ms: 250,
                heartbeat_ms: 10_000,
                path: Some("//Axes".into()),
            })
            .await
            .unwrap();

        let head = agent.last_request();
        assert!(head.contains("interval=250"), "{head}");
        assert!(head.contains("heartbeat=10000"), "{head}");
        assert!(head.contains("from=42"), "{head}");
        assert!(head.contains("path=%2F%2FAxes"), "{head}");

        assert_eq!(
            stream.content_type(),
            "multipart/x-mixed-replace;boundary=x"
        );
        assert_eq!(
            stream.max_document_bytes(),
            super::super::config::DEFAULT_MAX_DOCUMENT_BYTES
        );
        let mut seen = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.unwrap() {
            seen.extend_from_slice(&chunk);
        }
        assert_eq!(
            String::from_utf8(seen).unwrap(),
            body,
            "the body arrives unparsed"
        );
    }

    #[tokio::test]
    async fn a_declared_oversize_document_is_refused_before_it_is_read() {
        let big = "<MTConnectDevices>".to_string() + &"x".repeat(4096) + "</MTConnectDevices>";
        let agent = spawn_agent(move |_| Some(http_ok(&big))).await;
        let c = client_for(&agent.url(), json!({ "maxDocumentBytes": 512 }));
        assert!(matches!(
            c.probe().await,
            Err(MtcError::TooLarge { limit: 512 })
        ));
    }

    #[tokio::test]
    async fn an_undeclared_oversize_body_is_refused_while_it_is_read() {
        // No Content-Length: the cap has to hold during the read, or a hostile agent could stream
        // an unbounded body into memory.
        let agent = spawn_agent(move |_| {
            let body = "y".repeat(4096);
            Some(
                format!("HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n{body}")
                    .into_bytes(),
            )
        })
        .await;
        let c = client_for(&agent.url(), json!({ "maxDocumentBytes": 512 }));
        assert!(matches!(
            c.probe().await,
            Err(MtcError::TooLarge { limit: 512 })
        ));
    }

    #[tokio::test]
    async fn a_document_at_the_cap_is_accepted() {
        let body = "<a/>".to_string();
        let agent = spawn_agent(move |_| Some(http_ok(&body))).await;
        let c = client_for(&agent.url(), json!({ "maxDocumentBytes": 4 }));
        assert_eq!(c.probe().await.unwrap(), "<a/>");
    }

    #[tokio::test]
    async fn refused_credentials_are_an_auth_error_and_other_statuses_are_not() {
        let agent = spawn_agent(|head: &str| {
            let status = if head.contains("/probe") {
                "401 Unauthorized"
            } else {
                "503 Service Unavailable"
            };
            Some(
                format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .into_bytes(),
            )
        })
        .await;
        let c = client_for(&agent.url(), json!({}));

        let err = c.probe().await.unwrap_err();
        assert!(matches!(err, MtcError::Auth(_)), "{err:?}");
        assert!(
            !err.is_transient(),
            "a rejected credential will not fix itself"
        );

        let err = c.current(None).await.unwrap_err();
        assert!(matches!(err, MtcError::Http { status: 503 }), "{err:?}");
        assert!(err.is_transient());
    }

    #[tokio::test]
    async fn a_silent_agent_times_out_rather_than_hanging_forever() {
        let agent = spawn_agent(|_| None).await;
        let c = client_for(&agent.url(), json!({ "requestTimeoutMs": 150 }));
        let err = c.probe().await.unwrap_err();
        assert!(matches!(err, MtcError::Timeout { .. }), "{err:?}");
        assert!(err.is_transient());
        assert_eq!(
            agent.request_count(),
            1,
            "the client does not retry on its own"
        );
    }

    #[tokio::test]
    async fn an_unreachable_agent_is_a_transport_error() {
        // Bind and drop, so the port is (almost certainly) closed.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let c = client_for(
            &format!("http://{addr}"),
            json!({ "requestTimeoutMs": 500 }),
        );
        let err = c.probe().await.unwrap_err();
        // Refused or unanswered - both are the link, and both are worth reconnecting to.
        assert!(
            matches!(err, MtcError::Transport(_) | MtcError::Timeout { .. }),
            "{err:?}"
        );
        assert!(err.is_transient(), "a down agent is worth reconnecting to");
    }

    #[tokio::test]
    async fn a_redirect_is_never_followed() {
        // Following one would send the configured credentials to a host nobody configured.
        let agent = spawn_agent(|_| {
            Some(
                "HTTP/1.1 302 Found\r\nLocation: http://evil.example.com/probe\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string()
                    .into_bytes(),
            )
        })
        .await;
        let c = client_for(&agent.url(), json!({}));
        assert!(matches!(
            c.probe().await,
            Err(MtcError::Http { status: 302 })
        ));
        assert_eq!(agent.request_count(), 1);
    }

    #[tokio::test]
    async fn a_non_utf8_body_is_a_parse_error_not_a_panic() {
        let agent = spawn_agent(|_| {
            let mut out =
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n".to_vec();
            out.extend_from_slice(&[0xff, 0xfe, 0xfd, 0xfc]);
            Some(out)
        })
        .await;
        let c = client_for(&agent.url(), json!({}));
        assert!(matches!(c.probe().await, Err(MtcError::Xml(_))));
    }

    #[test]
    fn tls_material_is_validated_when_the_client_is_built() {
        let agents =
            parse_agents(&json!({ "agents": [{ "id": "a", "url": "https://agent:5001" }] }))
                .unwrap();
        let creds = AgentCredentials {
            auth: None,
            tls: Some(TlsMaterial {
                ca_pem: Some(
                    "-----BEGIN CERTIFICATE-----\nnot base64 at all\n-----END CERTIFICATE-----\n"
                        .into(),
                ),
                ..TlsMaterial::default()
            }),
        };
        let err = MtcClient::new(&agents[0], &creds).unwrap_err();
        assert!(matches!(err, MtcError::Tls(_)), "{err:?}");
        assert!(!err.is_transient());

        // Half an identity is refused by config validation; a broken one here is a TLS error.
        let creds = AgentCredentials {
            auth: None,
            tls: Some(TlsMaterial {
                ca_pem: None,
                client_cert_pem: Some(
                    "-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----".into(),
                ),
                client_key_pem: Some(
                    "-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n".into(),
                ),
            }),
        };
        assert!(matches!(
            MtcClient::new(&agents[0], &creds),
            Err(MtcError::Tls(_))
        ));
    }
}
