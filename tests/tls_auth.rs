//! # The transport-security leg: rustls + HTTP authentication against a real TLS agent
//!
//! An MTConnect agent on a plant network is routinely published over HTTPS behind a **private** CA,
//! sometimes demanding a client certificate, and usually behind Basic or bearer authentication. All
//! of that is configuration the operator supplies as **vault references**, resolved once at the
//! `device.rs` boundary and handed to the client as material (`AgentCredentials`).
//!
//! This suite exercises that path end to end against a TLS server minted in-process: a throwaway
//! private CA issues the server's certificate (and, for the mutual-TLS case, a client identity), the
//! real [`MtcClient`] connects to it, and the assertions are made on the bytes that arrive — so what
//! is proved is that the trust decision, the handshake and the `Authorization` header really work,
//! not that a mock said they did.
//!
//! Nothing here needs a network, a fixture file, or Docker. The canonical cppagent container's own
//! TLS variant is a separate, live-infra leg (`tests/compose.mtconnect-agent.yaml`); this suite is
//! what makes the TLS/auth behaviour a *unit-testable* property of the client.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use mtconnect_adapter::mtconnect::config::{
    AgentCredentials, AuthMaterial, TlsMaterial, parse_agents,
};
use mtconnect_adapter::mtconnect::{MtcClient, MtcError};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const PROBE_DOC: &str = "<MTConnectDevices/>";

/// A throwaway certificate authority and the identities it issues.
struct Pki {
    ca_pem: String,
    ca_cert: rcgen::Certificate,
    ca_key: KeyPair,
}

/// One issued identity: its PEM chain, its PEM key, and the DER rustls wants.
struct Identity {
    cert_pem: String,
    key_pem: String,
    cert_der: CertificateDer<'static>,
    key_der: PrivateKeyDer<'static>,
}

impl Pki {
    fn new() -> Self {
        let ca_key = KeyPair::generate().expect("ca key");
        let mut params = CertificateParams::new(Vec::new()).expect("ca params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        params
            .distinguished_name
            .push(DnType::CommonName, "mtconnect-test-ca");
        let ca_cert = params.self_signed(&ca_key).expect("ca cert");
        Self {
            ca_pem: ca_cert.pem(),
            ca_cert,
            ca_key,
        }
    }

    /// Issue an identity for `names` (SANs). An empty list gives a client identity.
    fn issue(&self, names: Vec<String>, common_name: &str) -> Identity {
        let key = KeyPair::generate().expect("key");
        let mut params = CertificateParams::new(names).expect("params");
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        let cert = params
            .signed_by(&key, &self.ca_cert, &self.ca_key)
            .expect("issue");
        Identity {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            cert_der: cert.der().clone(),
            key_der: PrivateKeyDer::try_from(key.serialize_der()).expect("pkcs8 key"),
        }
    }

    fn root_store(&self) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots
            .add(self.ca_cert.der().clone())
            .expect("trust our own CA");
        roots
    }
}

/// An MTConnect agent that speaks TLS: it answers `/probe` and records every request head it saw.
struct TlsAgent {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
}

impl TlsAgent {
    /// Start the agent. `require_client_cert` turns on mutual TLS against the same CA.
    async fn start(pki: &Pki, require_client_cert: bool) -> Self {
        let server = pki.issue(vec!["localhost".to_string()], "mtconnect-test-agent");
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());

        let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("protocol versions");
        let config = if require_client_cert {
            let verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(pki.root_store()), provider)
                    .build()
                    .expect("client verifier");
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        }
        .with_single_cert(vec![server.cert_der], server.key_der)
        .expect("server cert");

        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let requests = Arc::new(Mutex::new(Vec::new()));

        let seen = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                let acceptor = acceptor.clone();
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    // A refused handshake (no client certificate, an untrusted client) ends here —
                    // which is exactly the failure the client under test has to report.
                    let Ok(mut tls) = acceptor.accept(sock).await else {
                        return;
                    };
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match tls.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => head.push(byte[0]),
                        }
                    }
                    seen.lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&head).into_owned());
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{PROBE_DOC}",
                        PROBE_DOC.len()
                    );
                    let _ = tls.write_all(response.as_bytes()).await;
                    let _ = tls.shutdown().await;
                });
            }
        });

        Self { addr, requests }
    }

    /// The agent's base URL. `localhost` (not the literal IP) so the server certificate's SAN is
    /// what the client actually checks.
    fn url(&self) -> String {
        format!("https://localhost:{}", self.addr.port())
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

/// A client for one agent, with the credential material an operator's vault would have resolved.
fn client(url: &str, creds: AgentCredentials) -> MtcClient {
    let agents = parse_agents(&json!({ "agents": [{
        "id": "line-a-agent",
        "url": url,
        "requestTimeoutMs": 4000
    }] }))
    .unwrap();
    MtcClient::new(&agents[0], &creds).expect("client")
}

fn trusting(pki: &Pki) -> AgentCredentials {
    AgentCredentials {
        auth: None,
        tls: Some(TlsMaterial {
            ca_pem: Some(pki.ca_pem.clone()),
            ..TlsMaterial::default()
        }),
    }
}

#[tokio::test]
async fn a_private_ca_is_trusted_and_the_agent_answers_over_tls() {
    let pki = Pki::new();
    let agent = TlsAgent::start(&pki, false).await;

    let document = client(&agent.url(), trusting(&pki))
        .probe()
        .await
        .expect("probe over TLS");
    assert_eq!(document, PROBE_DOC);

    // The same request discipline as plain HTTP — the transport changed, the contract did not.
    let head = agent.last_request();
    assert!(head.starts_with("GET /probe HTTP/1.1"), "{head}");
    assert!(head.contains("accept: application/xml"), "{head}");
    assert!(
        !head.to_lowercase().contains("authorization:"),
        "no auth configured, none sent"
    );
}

#[tokio::test]
async fn an_agent_signed_by_an_unknown_authority_is_refused_rather_than_trusted() {
    let pki = Pki::new();
    let agent = TlsAgent::start(&pki, false).await;

    // No CA configured: the private CA is not in the platform trust store, so the handshake fails
    // and the request never happens. Trusting it silently would be the whole point of TLS gone.
    let err = client(&agent.url(), AgentCredentials::default())
        .probe()
        .await
        .unwrap_err();
    assert!(
        matches!(err, MtcError::Transport(_) | MtcError::Tls(_)),
        "{err:?}"
    );
    assert_eq!(
        agent.request_count(),
        0,
        "nothing was sent to an agent that could not be verified"
    );

    // A CA that is real but not this agent's is refused just the same.
    let other = Pki::new();
    let err = client(&agent.url(), trusting(&other))
        .probe()
        .await
        .unwrap_err();
    assert!(
        matches!(err, MtcError::Transport(_) | MtcError::Tls(_)),
        "{err:?}"
    );
    assert_eq!(agent.request_count(), 0);
}

#[tokio::test]
async fn basic_and_bearer_credentials_ride_the_encrypted_request() {
    let pki = Pki::new();
    let agent = TlsAgent::start(&pki, false).await;

    let mut creds = trusting(&pki);
    creds.auth = Some(AuthMaterial::Basic {
        username: "reader".into(),
        password: "s3cret".into(),
    });
    client(&agent.url(), creds).probe().await.expect("probe");
    // base64("reader:s3cret") — the credential the vault resolved, on the wire, inside TLS.
    assert!(
        agent
            .last_request()
            .contains("authorization: Basic cmVhZGVyOnMzY3JldA=="),
        "{}",
        agent.last_request()
    );

    let mut creds = trusting(&pki);
    creds.auth = Some(AuthMaterial::Bearer {
        token: "tok-123".into(),
    });
    client(&agent.url(), creds).probe().await.expect("probe");
    assert!(
        agent
            .last_request()
            .contains("authorization: Bearer tok-123")
    );
}

#[tokio::test]
async fn a_client_identity_satisfies_an_agent_that_demands_mutual_tls() {
    let pki = Pki::new();
    let agent = TlsAgent::start(&pki, true).await;
    let identity = pki.issue(Vec::new(), "mtconnect-adapter-client");

    // Trusting the agent is not enough when the agent also wants to know who we are.
    let err = client(&agent.url(), trusting(&pki))
        .probe()
        .await
        .unwrap_err();
    assert!(
        matches!(err, MtcError::Transport(_) | MtcError::Tls(_)),
        "{err:?}"
    );

    let creds = AgentCredentials {
        auth: None,
        tls: Some(TlsMaterial {
            ca_pem: Some(pki.ca_pem.clone()),
            client_cert_pem: Some(identity.cert_pem.clone()),
            client_key_pem: Some(identity.key_pem.clone()),
        }),
    };
    let document = client(&agent.url(), creds)
        .probe()
        .await
        .expect("mutual TLS probe");
    assert_eq!(document, PROBE_DOC);
    assert_eq!(
        agent.request_count(),
        1,
        "only the authenticated client got through"
    );
}

#[tokio::test]
async fn a_streaming_request_negotiates_the_same_trust_as_a_one_shot() {
    // The stream is the acquisition path that stays open for days; it must not be reachable under
    // weaker trust than a one-shot probe.
    let pki = Pki::new();
    let agent = TlsAgent::start(&pki, false).await;
    let request = mtconnect_adapter::mtconnect::StreamRequest {
        from: Some(1),
        interval_ms: 250,
        heartbeat_ms: 10_000,
        path: None,
    };

    let err = client(&agent.url(), AgentCredentials::default())
        .open_sample_stream(&request)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MtcError::Transport(_) | MtcError::Tls(_)),
        "{err:?}"
    );

    let mut creds = trusting(&pki);
    creds.auth = Some(AuthMaterial::Bearer {
        token: "tok-123".into(),
    });
    client(&agent.url(), creds)
        .open_sample_stream(&request)
        .await
        .expect("stream opens");
    let head = agent.last_request();
    assert!(
        head.contains("interval=250") && head.contains("heartbeat=10000"),
        "{head}"
    );
    assert!(
        head.contains("authorization: Bearer tok-123"),
        "credentials ride the stream too"
    );
}
