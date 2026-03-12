#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use feldera_types::config::{
        ConnectorConfig, FormatConfig, OutputBufferConfig, TransportConfig,
        default_max_queued_records,
    };
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
        };
        let json = serde_json::to_value(&meta).unwrap();
        let restored = Metadata::from_resume_info(Some(json)).unwrap();
        assert_eq!(restored.seq_num_range, 0..0);

        // Non-empty range
        let meta = Metadata {
            seq_num_range: 6..10,
        };
        let json = serde_json::to_value(&meta).unwrap();
        let restored = Metadata::from_resume_info(Some(json)).unwrap();
        assert_eq!(restored.seq_num_range, 6..10);

        // None resume info -> start from beginning
        let restored = Metadata::from_resume_info(None).unwrap();
        assert_eq!(restored.seq_num_range, 0..0);
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
        let config = ConnectorConfig {
            transport: TransportConfig::S2Input(S2InputConfig {
                basin: "test-basin".to_string(),
                stream: "test-stream".to_string(),
                auth_token: "tok_test123".to_string(),
                endpoint: Some("http://localhost:8080".to_string()),
                start_from: S2StartFrom::Beginning,
            }),
            format: Some(FormatConfig {
                name: Cow::from("json"),
                config: json!({
                    "update_format": "insert_delete"
                }),
            }),
            index: None,
            output_buffer_config: OutputBufferConfig::default(),
            max_batch_size: None,
            max_worker_batch_size: None,
            max_queued_records: default_max_queued_records(),
            paused: false,
            labels: Vec::new(),
            start_after: None,
        };

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
