//! Tests of the datagen connector's configuration. The connector tests in
//! `transport::datagen_tests` cover what the connector generates.

use feldera_types::config::{InputEndpointConfig, TransportConfig};
use feldera_types::transport::datagen::GenerationPlan;
use serde_json::json;

#[test]
fn missing_config_does_something_sane() {
    let config: InputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "workers": 3
            }
        }
    }))
    .unwrap();

    if let TransportConfig::Datagen(dtg) = config.connector_config.transport {
        assert_eq!(dtg.plan.len(), 1);
        assert_eq!(dtg.plan[0], GenerationPlan::default());
    }
}

#[test]
fn test_invalid_configs() {
    let config: Result<InputEndpointConfig, _> = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "unknown": 1,
                "plan": [
                    {
                        "limit": 3,
                        "fields": {
                            "ts": {
                                "range": [
                                    1724803200000u64,
                                    1724803200002u64
                                ]
                            },
                            "dt": {
                                "range": [
                                    19963,
                                    19965
                                ]
                            },
                            "t": {
                                "range": [
                                    5,
                                    7
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }));
    assert!(config.is_err());

    let config: Result<InputEndpointConfig, _> = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limitx": 1
                    }
                ]
            }
        }
    }));
    assert!(config.is_err());

    let config: Result<InputEndpointConfig, _> = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "fields": {
                            "ts": {
                                "unknown": "abc",
                                "range": [
                                    1724803200000u64,
                                    1724803200002u64
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }));
    assert!(config.is_err());

    let config: Result<InputEndpointConfig, _> = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "fields": {
                            "ts": {
                                "strategy": "unknown"
                            }
                        }
                    }
                ]
            }
        }
    }));
    assert!(config.is_err());
}
