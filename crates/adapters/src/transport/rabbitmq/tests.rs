use super::{RabbitmqInputEndpoint, RabbitmqOutputEndpoint};
use feldera_types::transport::rabbitmq::{RabbitmqInputConfig, RabbitmqOutputConfig};

#[test]
fn input_endpoint_accepts_valid_config() {
    let json = r#"{
        "host": "localhost",
        "username": "guest",
        "password": "guest",
        "queue": "events",
        "offset": { "policy": "next" }
    }"#;
    let cfg: RabbitmqInputConfig = serde_json::from_str(json).unwrap();
    assert!(cfg.is_stream());
    assert!(RabbitmqInputEndpoint::new(cfg).is_ok());
}

#[test]
fn input_endpoint_rejects_empty_queue() {
    let json = r#"{
        "host": "localhost",
        "username": "guest",
        "password": "guest",
        "queue": ""
    }"#;
    let cfg: RabbitmqInputConfig = serde_json::from_str(json).unwrap();
    assert!(RabbitmqInputEndpoint::new(cfg).is_err());
}

#[test]
fn output_endpoint_accepts_valid_config() {
    let json = r#"{
        "host": "localhost",
        "username": "guest",
        "password": "guest",
        "exchange": "analytics",
        "routing_key": "results.v1"
    }"#;
    let cfg: RabbitmqOutputConfig = serde_json::from_str(json).unwrap();
    assert!(RabbitmqOutputEndpoint::new(cfg).is_ok());
}

#[test]
fn output_endpoint_rejects_empty_routing_key() {
    let json = r#"{
        "host": "localhost",
        "username": "guest",
        "password": "guest",
        "exchange": "analytics",
        "routing_key": ""
    }"#;
    let cfg: RabbitmqOutputConfig = serde_json::from_str(json).unwrap();
    assert!(RabbitmqOutputEndpoint::new(cfg).is_err());
}
