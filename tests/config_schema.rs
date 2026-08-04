//! # `config.schema.json` — validated against the shipped configurations, and against the code
//!
//! Two halves, because a schema can be right about a document and still wrong about the software:
//!
//! 1. every shipped `test-configs/*.json` validates against the schema (global object + each
//!    instance against `#/$defs/instance`), and the documents the schema must refuse really are
//!    refused;
//! 2. the same documents parse into the types the adapter actually uses, and through the semantic
//!    validator — the cross-object invariants (an agent that resolves, uuids unique per agent) that
//!    JSON Schema cannot express.

use std::path::Path;

use mtconnect_adapter::app::{ChannelBudgets, DeviceConfig, PublishDefaults, compile_mtconnect};
use mtconnect_adapter::mtconnect::config::{PublishMode, parse_agents};
use serde_json::{Value, json};

fn schema() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.schema.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("read schema"))
        .expect("parse schema")
}

fn config(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test-configs")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).expect("read config"))
        .expect("parse config")
}

/// Validate one `component.global` object.
fn validate_global(instance: &Value) -> Result<(), String> {
    let validator = jsonschema::validator_for(&schema()).expect("compile schema");
    validator.validate(instance).map_err(|e| e.to_string())
}

/// Validate one `component.instances[]` entry against `#/$defs/instance`.
fn validate_instance(instance: &Value) -> Result<(), String> {
    let mut schema = schema();
    // Point the root at the instance definition, keeping `$defs` reachable for the `$ref`s.
    let defs = schema["$defs"].clone();
    schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": "#/$defs/device",
        "$defs": defs
    });
    let validator = jsonschema::validator_for(&schema).expect("compile schema");
    validator.validate(instance).map_err(|e| e.to_string())
}

#[test]
fn every_shipped_configuration_validates_against_the_schema() {
    for name in ["config.json", "mtconnect.json"] {
        let cfg = config(name);
        let component = &cfg["component"];
        validate_global(&component["global"])
            .unwrap_or_else(|e| panic!("{name}: component.global is invalid: {e}"));
        for instance in component["instances"].as_array().expect("instances") {
            validate_instance(instance)
                .unwrap_or_else(|e| panic!("{name}: instance is invalid: {e}"));
        }
    }
}

#[test]
fn the_shipped_mtconnect_configuration_compiles_through_the_semantic_validator() {
    let cfg = config("mtconnect.json");
    let agents = parse_agents(&cfg["component"]["global"]).expect("agents");
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].id, "line-a-agent");

    let mut devices: Vec<DeviceConfig> = cfg["component"]["instances"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| serde_json::from_value(v.clone()).expect("instance"))
        .collect();
    let compiled = compile_mtconnect(
        &mut devices,
        &agents,
        PublishDefaults::default(),
        &ChannelBudgets::default(),
    )
    .expect("bindings resolve");

    assert_eq!(compiled.len(), 1);
    assert_eq!(compiled[0].device_uuid, "OKUMA.123456");
    assert_eq!(compiled[0].signals.len(), 5);
    // The endpoint is derived from the agent + uuid, not configured.
    assert_eq!(
        devices[0].connection.endpoint,
        "mtconnect://127.0.0.1:5000/OKUMA.123456"
    );
}

#[test]
fn the_shipped_simulator_configuration_still_works() {
    // The simulator stays the deterministic backend that needs no agent at all.
    let cfg = config("config.json");
    let mut devices: Vec<DeviceConfig> = cfg["component"]["instances"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| serde_json::from_value(v.clone()).expect("instance"))
        .collect();
    assert_eq!(devices[0].adapter, "sim");
    assert_eq!(devices[0].connection.endpoint, "sim://device-1");
    // No MTConnect instances, so no agent is needed.
    assert!(
        compile_mtconnect(
            &mut devices,
            &[],
            PublishDefaults::default(),
            &ChannelBudgets::default()
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn an_mtconnect_instance_must_name_an_agent_and_a_device() {
    let ok = json!({
        "id": "cnc-1",
        "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.1" }
    });
    validate_instance(&ok).expect("the minimum valid instance");

    // No binding at all.
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "connection": {}
        }))
        .is_err()
    );
    // Half a binding.
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "connection": { "agentId": "line-a-agent" }
        }))
        .is_err()
    );
    // A sim instance needs its endpoint instead.
    validate_instance(&json!({
        "id": "cnc-1", "adapter": "sim", "connection": { "endpoint": "sim://cnc-1" }
    }))
    .expect("the simulator's own shape");
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "adapter": "sim", "connection": { "agentId": "a", "deviceUuid": "u" }
        }))
        .is_err()
    );
    // An unknown adapter is not a backend this component has.
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "adapter": "opcua", "connection": { "agentId": "a", "deviceUuid": "u" }
        }))
        .is_err()
    );
}

#[test]
fn the_write_allow_list_is_pinned_empty_because_mtconnect_has_no_write_path() {
    let with_writes = json!({
        "id": "cnc-1",
        "connection": { "agentId": "a", "deviceUuid": "u" },
        "writes": { "allow": ["x-position"] }
    });
    assert!(
        validate_instance(&with_writes).is_err(),
        "the schema itself must refuse a writable signal (D-MTC-7)"
    );
    validate_instance(&json!({
        "id": "cnc-1",
        "connection": { "agentId": "a", "deviceUuid": "u" },
        "writes": { "allow": [] }
    }))
    .expect("an empty allow-list is the only valid one");
}

#[test]
fn a_signal_must_bind_a_data_item_and_may_declare_its_conditions() {
    let instance = json!({
        "id": "cnc-1",
        "connection": { "agentId": "a", "deviceUuid": "u" },
        "signals": [{
            "id": "x-position",
            "name": "X position",
            "channel": "axes/x",
            "dataItemId": "Xabs",
            "conditionBinding": ["Xtravel"],
            "publish": { "mode": "interval", "batchMs": 250, "deadband": 0.5 }
        }]
    });
    validate_instance(&instance).expect("the full signal shape");

    // A signal with no binding is not a signal.
    assert!(
        validate_instance(&json!({
            "id": "cnc-1",
            "connection": { "agentId": "a", "deviceUuid": "u" },
            "signals": [{ "id": "x-position" }]
        }))
        .is_err()
    );
    // A typo'd key is a mistake, not a no-op.
    assert!(
        validate_instance(&json!({
            "id": "cnc-1",
            "connection": { "agentId": "a", "deviceUuid": "u" },
            "signals": [{ "id": "x", "dataItemID": "Xabs" }]
        }))
        .is_err()
    );
    // Signal ids are UNS tokens.
    assert!(
        validate_instance(&json!({
            "id": "cnc-1",
            "connection": { "agentId": "a", "deviceUuid": "u" },
            "signals": [{ "id": "X_Position", "dataItemId": "Xabs" }]
        }))
        .is_err()
    );
}

#[test]
fn the_selection_block_is_additive_closed_and_validated() {
    // The soak-style minimal instance: nothing but an identity, a binding, and "publish it all".
    let minimal = json!({
        "id": "cnc-1",
        "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.1" },
        "selection": { "mode": "all" }
    });
    validate_instance(&minimal).expect("the minimal selection instance");
    // ... and it compiles through the semantic validator too.
    let agents =
        parse_agents(&json!({ "agents": [{ "id": "line-a-agent", "url": "http://a:5000" }] }))
            .unwrap();
    let mut devices: Vec<DeviceConfig> = vec![serde_json::from_value(minimal).unwrap()];
    let compiled = compile_mtconnect(
        &mut devices,
        &agents,
        PublishDefaults {
            batch_ms: 250,
            publish_mode: PublishMode::Interval,
        },
        &ChannelBudgets::default(),
    )
    .expect("compiles");
    let selection = compiled[0]
        .selection
        .as_ref()
        .expect("the selection rides the compile");
    assert_eq!(
        selection.max_signals, 500,
        "the derived-set cap defaults to 500"
    );
    assert!(
        selection.auto_condition_binding,
        "auto conditionBinding defaults on"
    );
    assert_eq!(
        selection.default_batch_ms, 250,
        "defaults.batchMs is stamped in at compile"
    );
    assert_eq!(
        selection.default_publish_mode,
        PublishMode::Interval,
        "defaults.publishMode is stamped in at compile"
    );

    // The full shape.
    validate_instance(&json!({
        "id": "cnc-1",
        "connection": { "agentId": "a", "deviceUuid": "u" },
        "signals": [{ "id": "x-position", "dataItemId": "Xabs" }],
        "selection": {
            "mode": "include",
            "include": [{ "category": "SAMPLE", "type": "POSITION|LOAD", "subType": "ACTUAL",
                          "idMatch": "X.*", "path": "Axes/**" }],
            "exclude": [{ "idMatch": "Xfreq" }],
            "maxSignals": 100,
            "autoConditionBinding": false
        }
    }))
    .expect("the full selection shape");

    // Closed objects: a typo'd key is a mistake, not a no-op.
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "connection": { "agentId": "a", "deviceUuid": "u" },
            "selection": { "mode": "all", "includes": [] }
        }))
        .is_err()
    );
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "connection": { "agentId": "a", "deviceUuid": "u" },
            "selection": { "mode": "include", "include": [{ "types": "POSITION" }] }
        }))
        .is_err()
    );
    // An unknown mode, category, or a zero cap is refused by the schema itself.
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "connection": { "agentId": "a", "deviceUuid": "u" },
            "selection": { "mode": "everything" }
        }))
        .is_err()
    );
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "connection": { "agentId": "a", "deviceUuid": "u" },
            "selection": { "mode": "include", "include": [{ "category": "sample" }] }
        }))
        .is_err()
    );
    assert!(
        validate_instance(&json!({
            "id": "cnc-1", "connection": { "agentId": "a", "deviceUuid": "u" },
            "selection": { "mode": "all", "maxSignals": 0 }
        }))
        .is_err()
    );

    // A regex the schema cannot judge is refused by the side-effect-free semantic validator.
    let mut bad: Vec<DeviceConfig> = vec![
        serde_json::from_value(json!({
            "id": "cnc-1",
            "connection": { "agentId": "line-a-agent", "deviceUuid": "OKUMA.1" },
            "selection": { "mode": "include", "include": [{ "type": "(" }] }
        }))
        .unwrap(),
    ];
    assert!(
        compile_mtconnect(
            &mut bad,
            &agents,
            PublishDefaults::default(),
            &ChannelBudgets::default()
        )
        .is_err(),
        "a bad pattern never commits"
    );

    // A `sim` instance has no probe to derive from: selection is refused there.
    let mut sim: Vec<DeviceConfig> = vec![
        serde_json::from_value(json!({
            "id": "plc-1", "adapter": "sim",
            "connection": { "endpoint": "sim://plc-1" },
            "selection": { "mode": "all" }
        }))
        .unwrap(),
    ];
    assert!(
        compile_mtconnect(
            &mut sim,
            &[],
            PublishDefaults::default(),
            &ChannelBudgets::default()
        )
        .is_err()
    );
}

#[test]
fn an_agent_declares_a_reachable_url_and_references_its_secrets() {
    validate_global(&json!({ "agents": [{
        "id": "line-a-agent",
        "url": "https://agent:5001",
        "auth": { "type": "basic", "username": "reader", "secretRef": "mtc/agent-pw" },
        "tls": { "caSecretRef": "mtc/ca", "certSecretRef": "mtc/cert", "keySecretRef": "mtc/key" },
        "streaming": "poll-only",
        "heartbeatMs": 10000,
        "pollIntervalMs": 500,
        "requestTimeoutMs": 10000,
        "maxDocumentBytes": 1048576,
        "reconnect": { "initialMs": 1000, "maxMs": 60000 }
    }] }))
    .expect("the full agent shape");

    // An empty agent list, a foreign scheme, and a missing url are all refused.
    assert!(validate_global(&json!({ "agents": [] })).is_err());
    assert!(validate_global(&json!({ "agents": [{ "id": "a", "url": "mqtt://agent" }] })).is_err());
    assert!(validate_global(&json!({ "agents": [{ "id": "a" }] })).is_err());
    // An id that is not a UNS token cannot be a metric dimension.
    assert!(
        validate_global(&json!({ "agents": [{ "id": "Line_A", "url": "http://a:5000" }] }))
            .is_err()
    );
    // A password INLINE instead of by reference.
    assert!(
        validate_global(&json!({ "agents": [{
        "id": "a", "url": "http://a:5000",
        "auth": { "type": "basic", "username": "u", "password": "hunter2" }
    }] }))
        .is_err(),
        "secrets are references, never values"
    );
    // Half a client identity.
    assert!(
        validate_global(&json!({ "agents": [{
        "id": "a", "url": "http://a:5000", "tls": { "certSecretRef": "c" }
    }] }))
        .is_err()
    );
    // An unknown global key is a mistake, not a no-op.
    assert!(
        validate_global(&json!({ "agents": [{ "id": "a", "url": "http://a:5000" }], "nope": 1 }))
            .is_err()
    );
}

#[test]
fn the_schema_keeps_every_object_closed() {
    fn walk(node: &Value, path: &str, seen: &mut Vec<String>) {
        if let Some(map) = node.as_object() {
            let is_schema_object = map.get("type").and_then(Value::as_str) == Some("object");
            if is_schema_object && map.get("additionalProperties") != Some(&json!(false)) {
                seen.push(path.to_string());
            }
            for (key, value) in map {
                walk(value, &format!("{path}/{key}"), seen);
            }
        } else if let Some(items) = node.as_array() {
            for (i, value) in items.iter().enumerate() {
                walk(value, &format!("{path}/{i}"), seen);
            }
        }
    }
    let mut open = Vec::new();
    walk(&schema(), "", &mut open);
    assert!(
        open.is_empty(),
        "these objects accept unknown keys: {open:?}"
    );
}
