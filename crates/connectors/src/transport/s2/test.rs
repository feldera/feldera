#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use feldera_types::config::{ConnectorConfig, FormatConfig, TransportConfig};
    use feldera_types::transport::s2::{S2InputConfig, S2OutputConfig, S2StartFrom};
    use serde_json::{self, json};

    #[test]
    fn config_serialization_roundtrip() {
        let config = S2InputConfig {
            basin: "my-basin".to_string(),
            stream: "my-stream".to_string(),
            auth_token: "tok_test123".to_string(),
            endpoint: None,
            start_from: S2StartFrom::SeqNum(42),
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: S2InputConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn config_default_start_from() {
        let json = r#"{"basin":"b","stream":"s","auth_token":"t"}"#;
        let config: S2InputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.start_from, S2StartFrom::Beginning);
    }

    #[test]
    fn config_all_start_from_variants() {
        for (variant, expected) in [
            (r#"{"SeqNum":10}"#, S2StartFrom::SeqNum(10)),
            (r#"{"Timestamp":1000}"#, S2StartFrom::Timestamp(1000)),
            (r#"{"TailOffset":5}"#, S2StartFrom::TailOffset(5)),
            (r#""Beginning""#, S2StartFrom::Beginning),
            (r#""Tail""#, S2StartFrom::Tail),
        ] {
            let json =
                format!(r#"{{"basin":"b","stream":"s","auth_token":"t","start_from":{variant}}}"#);
            let config: S2InputConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(config.start_from, expected);
        }
    }

    #[test]
    fn metadata_checkpoint_roundtrip() {
        use crate::transport::s2::S2Metadata as Metadata;

        // Empty range (no messages processed)
        let meta = Metadata {
            seq_num_range: 0..0,
            position_resolved: true,
        };
        let json = serde_json::to_value(&meta).unwrap();
        let restored = Metadata::from_resume_info(Some(json)).unwrap();
        assert_eq!(restored.seq_num_range, 0..0);
        assert!(restored.position_resolved);

        // Non-empty range
        let meta = Metadata {
            seq_num_range: 6..10,
            position_resolved: true,
        };
        let json = serde_json::to_value(&meta).unwrap();
        let restored = Metadata::from_resume_info(Some(json)).unwrap();
        assert_eq!(restored.seq_num_range, 6..10);
        assert!(restored.position_resolved);

        // None resume info -> start from the configured position.
        let restored = Metadata::from_resume_info(None).unwrap();
        assert_eq!(restored.seq_num_range, 0..0);
        assert!(!restored.position_resolved);
    }

    #[test]
    fn legacy_metadata_defaults_to_unresolved_position() {
        use crate::transport::s2::S2Metadata as Metadata;

        let restored = Metadata::from_resume_info(Some(json!({
            "seq_num_range": { "start": 0, "end": 0 }
        })))
        .unwrap();
        assert!(!restored.position_resolved);
    }

    #[test]
    fn replay_read_is_bounded_to_checkpoint_range() {
        use crate::transport::s2::make_replay_read_input;
        use s2_sdk::types::ReadFrom;

        let input = make_replay_read_input(&(6..10));
        assert!(matches!(input.start.from, ReadFrom::SeqNum(6)));
        assert_eq!(input.stop.limits.count, Some(4));
    }

    #[test]
    fn transport_config_name() {
        use feldera_types::config::TransportConfig;
        let config = TransportConfig::S2Input(S2InputConfig {
            basin: "b".to_string(),
            stream: "s".to_string(),
            auth_token: "t".to_string(),
            endpoint: None,
            start_from: S2StartFrom::default(),
        });
        assert_eq!(config.name(), "s2_input");
    }

    #[test]
    fn transport_config_serde_roundtrip() {
        use feldera_types::config::TransportConfig;
        let config = TransportConfig::S2Input(S2InputConfig {
            basin: "test-basin".to_string(),
            stream: "test-stream".to_string(),
            auth_token: "tok_abc".to_string(),
            endpoint: None,
            start_from: S2StartFrom::Tail,
        });
        let json = serde_json::to_value(&config).unwrap();
        let deserialized: TransportConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn config_with_endpoint() {
        let config = S2InputConfig {
            basin: "b".to_string(),
            stream: "s".to_string(),
            auth_token: "t".to_string(),
            endpoint: Some("http://localhost:8080".to_string()),
            start_from: S2StartFrom::default(),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("localhost:8080"));
        let deserialized: S2InputConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn config_endpoint_omitted_when_none() {
        let config = S2InputConfig {
            basin: "b".to_string(),
            stream: "s".to_string(),
            auth_token: "t".to_string(),
            endpoint: None,
            start_from: S2StartFrom::default(),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert!(!json.contains("endpoint"));
    }

    #[test]
    fn connector_config_insert_delete_format_roundtrip() {
        let config = ConnectorConfig::new(
            TransportConfig::S2Input(S2InputConfig {
                basin: "test-basin".to_string(),
                stream: "test-stream".to_string(),
                auth_token: "tok_test123".to_string(),
                endpoint: Some("http://localhost:8080".to_string()),
                start_from: S2StartFrom::Beginning,
            }),
            Some(FormatConfig {
                name: Cow::from("json"),
                config: json!({
                    "update_format": "insert_delete"
                }),
            }),
        );

        let serialized = serde_json::to_value(&config).unwrap();
        let deserialized: ConnectorConfig = serde_json::from_value(serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    // --- S2 Output Config tests ---

    #[test]
    fn config_output_serialization_roundtrip() {
        let config = S2OutputConfig {
            basin: "my-basin".to_string(),
            stream: "my-stream".to_string(),
            auth_token: "tok_test123".to_string(),
            endpoint: None,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: S2OutputConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn transport_config_s2_output_name() {
        let config = TransportConfig::S2Output(S2OutputConfig {
            basin: "b".to_string(),
            stream: "s".to_string(),
            auth_token: "t".to_string(),
            endpoint: None,
        });
        assert_eq!(config.name(), "s2_output");
    }

    #[test]
    fn transport_config_s2_output_serde_roundtrip() {
        let config = TransportConfig::S2Output(S2OutputConfig {
            basin: "test-basin".to_string(),
            stream: "test-stream".to_string(),
            auth_token: "tok_abc".to_string(),
            endpoint: None,
        });
        let json = serde_json::to_value(&config).unwrap();
        let deserialized: TransportConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn config_output_with_endpoint() {
        let config = S2OutputConfig {
            basin: "b".to_string(),
            stream: "s".to_string(),
            auth_token: "t".to_string(),
            endpoint: Some("http://localhost:8080".to_string()),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("localhost:8080"));
        let deserialized: S2OutputConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn config_output_endpoint_omitted_when_none() {
        let config = S2OutputConfig {
            basin: "b".to_string(),
            stream: "s".to_string(),
            auth_token: "t".to_string(),
            endpoint: None,
        };
        let json = serde_json::to_string(&config).unwrap();
        assert!(!json.contains("endpoint"));
    }
}
