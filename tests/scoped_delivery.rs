//! # The `sb/*` surface driven END TO END through a REAL `CommandInbox`
//!
//! The delivery topic, the envelope, the library scope enforcement, the handler, and the reply —
//! over a recording messaging seam (no broker, no device).
//!
//! The unit tests in `src/commands.rs` call the `Commander` directly, so they can only prove what
//! this component does with an **already resolved** instance. These tests prove the half the
//! library now owns:
//!
//! * a command lands on **both** cmd wildcards — addressed to the component (`.../cmd/{verb}`) and
//!   addressed to a device (`.../{instance}/cmd/{verb}`);
//! * the **topic** selects the device even when nothing would resolve by default (two configured);
//! * a body `instance` conflicting with the topic token is refused with `BAD_ARGS` **before
//!   dispatch** — the handler never runs, so the device is never touched;
//! * `describe` advertises every verb as `"scope": "instance"`.
//!
//! Do not weaken these: they are the contract that lets `src/commands.rs` stay free of any
//! topic/body addressing logic of its own.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use edgecommons::commands::{CommandInbox, InstanceConnectivitySource, ReloadAction};
use edgecommons::config::model::Config;
use edgecommons::messaging::{
    Message, MessageBuilder, MessageHandler, MessagingService, Qos, ReplyFuture, topic_matches,
};
use edgecommons::metrics::{Metric, MetricService};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use mtconnect_adapter::app::{DeviceConfig, DeviceControl, Health, LinkState, set_paused};
use mtconnect_adapter::commands::{DeviceHandle, VERBS, register_all};
use mtconnect_adapter::device::SignalInfo;
use mtconnect_adapter::metrics::DeviceMetrics;

const DEVICE: &str = "test-thing";
const COMPONENT: &str = "TestComponent";
const REPLY_TO: &str = "edgecommons/reply-scoped-test";

// =================================================================================================
// Fakes: a recording messaging seam + a no-op metric service
// =================================================================================================

/// Records the subscriptions and replies of the inbox, and delivers a concrete topic to every
/// subscription whose filter matches it through the MQTT matcher of the library — so the two
/// `cmd/#` wildcards receive concrete verb topics exactly as a broker would deliver them.
#[derive(Default)]
struct FakeMessaging {
    handlers: Mutex<HashMap<String, Arc<dyn MessageHandler>>>,
    replies: Mutex<Vec<(String, Message)>>,
    connected: AtomicBool,
}

impl FakeMessaging {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn subscribed(&self) -> HashSet<String> {
        self.handlers.lock().unwrap().keys().cloned().collect()
    }

    async fn simulate(&self, topic: &str, message: Message) {
        let matched: Vec<Arc<dyn MessageHandler>> = {
            let handlers = self.handlers.lock().unwrap();
            handlers
                .iter()
                .filter(|(filter, _)| topic_matches(filter, topic))
                .map(|(_, handler)| handler.clone())
                .collect()
        };
        for handler in matched {
            handler.handle(topic.to_string(), message.clone()).await;
        }
    }

    /// The single recorded reply body, pinned to the reply_to topic of the request.
    fn take_reply(&self) -> Value {
        let mut replies = self.replies.lock().unwrap();
        assert_eq!(replies.len(), 1, "exactly one reply expected");
        let (topic, message) = replies.pop().expect("a reply");
        assert_eq!(
            topic, REPLY_TO,
            "the reply must go to the reply_to of the request"
        );
        message.body
    }
}

#[async_trait]
impl MessagingService for FakeMessaging {
    async fn publish(&self, _topic: &str, _msg: &Message) -> edgecommons::Result<()> {
        Ok(())
    }
    async fn publish_northbound(
        &self,
        _topic: &str,
        _msg: &Message,
        _qos: Qos,
    ) -> edgecommons::Result<()> {
        Ok(())
    }
    async fn publish_raw(&self, _topic: &str, _payload: &Value) -> edgecommons::Result<()> {
        Ok(())
    }
    async fn publish_northbound_raw(
        &self,
        _topic: &str,
        _payload: &Value,
        _qos: Qos,
    ) -> edgecommons::Result<()> {
        Ok(())
    }

    async fn subscribe(
        &self,
        filter: &str,
        handler: Arc<dyn MessageHandler>,
        _max_messages: usize,
        _max_concurrency: usize,
    ) -> edgecommons::Result<()> {
        self.handlers
            .lock()
            .unwrap()
            .insert(filter.to_string(), handler);
        Ok(())
    }

    /// The inbox subscribes its two wildcards through the acknowledged path.
    async fn subscribe_acknowledged(
        &self,
        filter: &str,
        handler: Arc<dyn MessageHandler>,
        max_messages: usize,
        max_concurrency: usize,
        _timeout: Duration,
    ) -> edgecommons::Result<()> {
        self.subscribe(filter, handler, max_messages, max_concurrency)
            .await
    }

    async fn subscribe_northbound(
        &self,
        filter: &str,
        handler: Arc<dyn MessageHandler>,
        _qos: Qos,
        max_messages: usize,
        max_concurrency: usize,
    ) -> edgecommons::Result<()> {
        self.subscribe(filter, handler, max_messages, max_concurrency)
            .await
    }

    async fn unsubscribe(&self, filter: &str) -> edgecommons::Result<()> {
        self.handlers.lock().unwrap().remove(filter);
        Ok(())
    }
    async fn unsubscribe_northbound(&self, filter: &str) -> edgecommons::Result<()> {
        self.unsubscribe(filter).await
    }

    async fn request(&self, _topic: &str, _msg: Message) -> edgecommons::Result<ReplyFuture> {
        unimplemented!("the command inbox never sends requests")
    }
    async fn request_northbound(
        &self,
        _topic: &str,
        _msg: Message,
    ) -> edgecommons::Result<ReplyFuture> {
        unimplemented!("the command inbox never sends requests")
    }
    async fn request_with_timeout(
        &self,
        _topic: &str,
        _msg: Message,
        _timeout: Option<Duration>,
    ) -> edgecommons::Result<ReplyFuture> {
        unimplemented!("the command inbox never sends requests")
    }
    async fn request_northbound_with_timeout(
        &self,
        _topic: &str,
        _msg: Message,
        _timeout: Option<Duration>,
    ) -> edgecommons::Result<ReplyFuture> {
        unimplemented!("the command inbox never sends requests")
    }

    async fn reply(&self, request: &Message, mut reply: Message) -> edgecommons::Result<()> {
        reply.header.correlation_id = request.header.correlation_id.clone();
        let reply_to = request.header.reply_to.clone().unwrap_or_default();
        self.replies.lock().unwrap().push((reply_to, reply));
        Ok(())
    }
    async fn reply_northbound(&self, request: &Message, reply: Message) -> edgecommons::Result<()> {
        self.reply(request, reply).await
    }

    fn cancel_request(&self, _reply_future: ReplyFuture) {}
    fn cancel_request_northbound(&self, _reply_future: ReplyFuture) {}

    fn connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct NoopMetrics;

#[async_trait]
impl MetricService for NoopMetrics {
    fn define_metric(&self, _metric: Metric) {}
    fn is_metric_defined(&self, _name: &str) -> bool {
        true
    }
    async fn emit_metric(
        &self,
        _name: &str,
        _values: HashMap<String, f64>,
    ) -> edgecommons::Result<()> {
        Ok(())
    }
    async fn emit_metric_now(
        &self,
        _name: &str,
        _values: HashMap<String, f64>,
    ) -> edgecommons::Result<()> {
        Ok(())
    }
    async fn flush_metrics(&self) -> edgecommons::Result<()> {
        Ok(())
    }
    async fn shutdown(&self) {}
}

// =================================================================================================
// The harness: a live inbox carrying the nine verbs of this component over N devices
// =================================================================================================

fn config() -> Arc<Config> {
    Arc::new(Config::from_value(COMPONENT, DEVICE, json!({})).unwrap())
}

fn device_config(id: &str) -> DeviceConfig {
    serde_json::from_value(json!({
        "id": id,
        "adapter": "sim",
        "connection": { "endpoint": format!("sim://{id}") },
        "writes": { "allow": ["setpoint-1"] }
    }))
    .unwrap()
}

/// Every control message that REACHED a device task — empty proves no handler ran.
type ControlLog = Arc<Mutex<Vec<String>>>;

struct Harness {
    messaging: Arc<FakeMessaging>,
    inbox: Arc<CommandInbox>,
    control_log: ControlLog,
    healths: Vec<Arc<Health>>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Harness {
    /// One live inbox, wired by `register_all` over one device per id, each served by a mock task
    /// that records what reaches it.
    async fn start(ids: &[&str]) -> Self {
        let messaging = FakeMessaging::new();
        let control_log: ControlLog = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        let mut healths = Vec::new();
        let mut tasks = Vec::new();

        for id in ids {
            let cfg = device_config(id);
            let (tx, mut rx) = mpsc::channel::<DeviceControl>(8);
            let health = Arc::new(Health::default());
            health.set_link(LinkState::Online);
            let dm = Arc::new(DeviceMetrics::new(
                Arc::new(NoopMetrics),
                config(),
                cfg.id.clone(),
                Arc::clone(&health),
                30,
                None,
            ));

            let log = Arc::clone(&control_log);
            let task_health = Arc::clone(&health);
            tasks.push(tokio::spawn(async move {
                while let Some(control) = rx.recv().await {
                    match control {
                        DeviceControl::Pause { reply } => {
                            log.lock().unwrap().push("pause".to_string());
                            let _ = reply.send(set_paused(&task_health, true));
                        }
                        DeviceControl::Resume { reply } => {
                            log.lock().unwrap().push("resume".to_string());
                            let _ = reply.send(set_paused(&task_health, false));
                        }
                        _ => log.lock().unwrap().push("other".to_string()),
                    }
                }
            }));

            handles.push(DeviceHandle {
                cfg,
                control: tx,
                health: Arc::clone(&health),
                dm,
                signals: vec![SignalInfo {
                    id: "setpoint-1".into(),
                    name: Some("Setpoint".into()),
                }],
                // These devices are the template simulator: no MTConnect agent behind them, so
                // there is no published protocol view to read (the addressing contract this suite
                // proves is protocol-independent).
                protocol: None,
            });
            healths.push(health);
        }

        let uptime: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(|| 1);
        let reload: ReloadAction = Arc::new(|| Box::pin(async { true }));
        let redacted: Arc<dyn Fn() -> Option<Value> + Send + Sync> = Arc::new(|| None);
        let instances: InstanceConnectivitySource = Arc::new(Vec::new);
        let inbox = CommandInbox::new(
            Arc::clone(&messaging) as Arc<dyn MessagingService>,
            config(),
            uptime,
            reload,
            redacted,
            instances,
        );
        register_all(&inbox, handles).expect("the nine verbs register");
        Arc::clone(&inbox).start().await;

        Self {
            messaging,
            inbox,
            control_log,
            healths,
            _tasks: tasks,
        }
    }

    /// Deliver `verb` on the component-scope topic (`instance` = `None`) or on the instance-scope
    /// topic of one device, and return the reply body.
    async fn send(&self, verb: &str, instance: Option<&str>, body: Value) -> Value {
        let topic = match instance {
            Some(id) => format!("ecv1/{DEVICE}/{COMPONENT}/{id}/cmd/{verb}"),
            None => format!("ecv1/{DEVICE}/{COMPONENT}/cmd/{verb}"),
        };
        let request = MessageBuilder::new(verb, "1.0")
            .payload(body)
            .reply_to(REPLY_TO)
            .build();
        self.messaging.simulate(&topic, request).await;
        self.messaging.take_reply()
    }

    fn control_calls(&self) -> Vec<String> {
        self.control_log.lock().unwrap().clone()
    }
}

// =================================================================================================
// Tests
// =================================================================================================

#[tokio::test]
async fn the_inbox_subscribes_both_cmd_wildcards() {
    // D-U28: a command must land whether it is addressed to the component or to one device.
    let h = Harness::start(&["plc-1"]).await;
    assert_eq!(
        h.messaging.subscribed(),
        HashSet::from([
            format!("ecv1/{DEVICE}/{COMPONENT}/cmd/#"),
            format!("ecv1/{DEVICE}/{COMPONENT}/+/cmd/#"),
        ])
    );
    h.inbox.stop().await;
}

#[tokio::test]
async fn a_component_addressed_and_an_instance_addressed_request_both_reach_the_verb() {
    // One device configured: the component-addressed delivery names no instance and resolves to it;
    // the instance-addressed delivery carries the token on the topic.
    let h = Harness::start(&["plc-1"]).await;

    let body = h.send("sb/status", None, json!({})).await;
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["result"]["id"], json!("plc-1"));

    let body = h.send("sb/status", Some("plc-1"), json!({})).await;
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["result"]["id"], json!("plc-1"));
    h.inbox.stop().await;
}

#[tokio::test]
async fn the_topic_instance_token_selects_the_device_among_several() {
    let h = Harness::start(&["plc-1", "plc-2"]).await;

    // Nothing would resolve by default here — the DELIVERY TOPIC is what addresses the device.
    let body = h.send("sb/status", Some("plc-2"), json!({})).await;
    assert_eq!(body["result"]["id"], json!("plc-2"));

    // No instance anywhere, two devices: only this component can answer that — BAD_ARGS from it.
    let body = h.send("sb/status", None, json!({})).await;
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("BAD_ARGS"));

    // An instance this component has no device for is NO_SUCH_INSTANCE, also from it.
    let body = h.send("sb/status", Some("ghost"), json!({})).await;
    assert_eq!(body["error"]["code"], json!("NO_SUCH_INSTANCE"));
    h.inbox.stop().await;
}

#[tokio::test]
async fn a_body_instance_alone_still_addresses_the_verb() {
    // A component-addressed delivery whose body names a device: the library folds the body into
    // `addressed_instance`, so the handler routes on it without ever reading the body.
    let h = Harness::start(&["plc-1", "plc-2"]).await;
    let body = h
        .send("sb/status", None, json!({ "instance": "plc-2" }))
        .await;
    assert_eq!(body["result"]["id"], json!("plc-2"));
    h.inbox.stop().await;
}

#[tokio::test]
async fn a_body_instance_conflicting_with_the_topic_is_refused_before_dispatch() {
    // The conflict is a LIBRARY decision, made before dispatch: the reply is BAD_ARGS and the
    // handler never ran — nothing reached a device task and neither device was paused.
    let h = Harness::start(&["plc-1", "plc-2"]).await;
    let body = h
        .send("sb/pause", Some("plc-1"), json!({ "instance": "plc-2" }))
        .await;
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("BAD_ARGS"));
    assert!(
        h.control_calls().is_empty(),
        "no handler may run on an addressing error - nothing may reach a device"
    );
    for health in &h.healths {
        assert!(
            !health.is_paused(),
            "no device may be paused by a refused command"
        );
    }

    // The same verb, correctly addressed, does reach the device — the refusal above was about the
    // addressing, not about the verb.
    let body = h.send("sb/pause", Some("plc-1"), json!({})).await;
    assert_eq!(body["result"]["paused"], json!(true));
    assert_eq!(h.control_calls(), vec!["pause".to_string()]);
    h.inbox.stop().await;
}

#[tokio::test]
async fn describe_advertises_sb_write_as_permanently_unsupported() {
    // D-MTC-7: `sb/write` is registered (so a request gets a real, standard refusal) and advertised
    // `unsupported` through the command-availability surface, so a console disables the surface
    // instead of offering a write that can never work. Registration order matters: the availability
    // is set after the verbs and before the panels, inside the pre-activation window.
    let h = Harness::start(&["plc-1"]).await;
    let body = h.send("describe", None, json!({})).await;
    let commands = body["result"]["commands"]
        .as_array()
        .expect("a commands array");

    let write = commands
        .iter()
        .find(|c| c["verb"] == json!("sb/write"))
        .expect("sb/write is registered");
    assert_eq!(write["availability"]["state"], json!("unsupported"));
    assert_eq!(
        write["availability"]["reason"],
        json!("MTConnect is read-only")
    );

    // Every other verb is plainly available — availability is a capability statement about this
    // protocol, not a blanket disablement.
    for verb in VERBS.iter().filter(|v| **v != "sb/write") {
        let entry = commands
            .iter()
            .find(|c| c["verb"] == json!(verb))
            .expect("registered");
        assert!(entry.get("availability").is_none(), "{verb} is available");
    }

    // And the wire refusal matches what the manifest says.
    let reply = h
        .send(
            "sb/write",
            Some("plc-1"),
            json!({ "signalId": "setpoint-1", "value": 1 }),
        )
        .await;
    assert_eq!(reply["ok"], json!(false));
    assert_eq!(reply["error"]["code"], json!("WRITE_NOT_ALLOWED"));
    assert!(
        h.control_calls().is_empty(),
        "a refused write never reaches a device task"
    );

    // The five HLD §8 views are on the same descriptor, in order, and none of them is a write
    // surface.
    let panels = &body["result"]["panels"];
    assert_eq!(panels["schemaVersion"], json!("edgecommons.panels.v2"));
    assert_eq!(panels["defaultView"], json!("overview"));
    let views = panels["views"].as_array().expect("a views array");
    let ids: Vec<&str> = views.iter().map(|p| p["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        vec![
            "overview",
            "device-structure",
            "signals",
            "conditions",
            "diagnostics"
        ]
    );
    assert!(!serde_json::to_string(views).unwrap().contains("writeVerb"));
    h.inbox.stop().await;
}

#[tokio::test]
async fn describe_advertises_every_verb_as_instance_scoped() {
    // The console derives its UI from the advertised scope: `instance` means "show an instance
    // selector". Every verb this adapter registers says so on the wire.
    let h = Harness::start(&["plc-1"]).await;
    let body = h.send("describe", None, json!({})).await;
    let commands = body["result"]["commands"]
        .as_array()
        .expect("a commands array");
    for verb in VERBS {
        let entry = commands
            .iter()
            .find(|c| c["verb"] == json!(verb))
            .unwrap_or_else(|| panic!("{verb} is advertised"));
        assert_eq!(
            entry["scope"],
            json!("instance"),
            "{verb} must advertise instance scope"
        );
    }
    h.inbox.stop().await;
}
