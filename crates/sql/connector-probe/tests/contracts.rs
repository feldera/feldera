use feldera_connector_probe::{Request, Validator};
use serde_json::{json, Value};

fn validate(class: &str, config: Value) -> Value {
    let request: Request = serde_json::from_value(json!({
        "id": "test", "config_class": class,
        "json": json!([{"transport": {"config": config}}]).to_string(),
        "pointer": "/0/transport/config"
    }))
    .unwrap();
    serde_json::to_value(Validator::new().unwrap().validate(request).unwrap()).unwrap()
}

#[test]
fn nulls_produce_diagnostics_instead_of_panics() {
    assert_eq!(
        validate("PostgresReaderConfig", json!({"uri": null}))["warnings"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(validate(
        "KafkaInputConfig",
        json!({"topic": null, "topics": ["events"]})
    )["warnings"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        validate(
            "AvroParserConfig",
            json!({"schema": "schema", "registry_urls": null})
        )["warnings"][0]["message"],
        "\"registry_urls\" cannot be null; remove it to use the default"
    );
}

#[test]
fn flattened_options_are_not_mistaken_for_strict_fields() {
    assert!(validate(
        "KafkaInputConfig",
        json!({"topic": "events", "bootstrap.servers": [1, {}]})
    )["warnings"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        validate("HttpOutputConfig", json!({"backpresure": true}))["warnings"][0]["message"],
        "unknown field \"backpresure\""
    );
}

#[test]
fn decoding_stops_before_semantics_and_preserves_input_order() {
    let first = validate(
        "PostgresReaderConfig",
        json!({"unknown": 0, "uri": [], "query": ""}),
    );
    assert_eq!(first["warnings"].as_array().unwrap().len(), 1);
    assert_eq!(first["warnings"][0]["message"], "unknown field \"unknown\"");
}

#[test]
fn jackson_coercions_and_whitespace_differ_from_rust_defaults() {
    assert!(
        validate("PostgresReaderConfig", json!({"uri": false, "query": 1}))["warnings"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        validate("AvroEncoderConfig", json!({"threads": 1.5}))["warnings"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        !validate("HttpOutputConfig", json!({"backpressure": 1.5}))["warnings"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        !validate("AvroEncoderConfig", json!({"key_mode": ""}))["warnings"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        !validate("HttpOutputConfig", json!({"backpressure": "\u{2003}"}))["warnings"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(validate(
        "PostgresReaderConfig",
        json!({"uri": "\u{00a0}", "query": "q"})
    )["warnings"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn explicit_null_aliases_still_conflict() {
    let result = validate(
        "AvroEncoderConfig",
        json!({"registry_timeout": null, "registry_timeout_secs": null}),
    );
    assert_eq!(result["warnings"].as_array().unwrap().len(), 1);
    assert!(result["warnings"][0]["message"]
        .as_str()
        .unwrap()
        .contains("write one of the two"));
}

#[test]
fn source_locations_use_utf16_and_nested_map_fallback() {
    let raw = r#"[{"name":"🦀","transport":{"config":{"registry_headers":{"a/b":{}}}}}]"#;
    let request: Request = serde_json::from_value(json!({"id":"x", "config_class":"AvroParserConfig", "json":raw, "pointer":"/0/transport/config"})).unwrap();
    let result = Validator::new().unwrap().validate(request).unwrap();
    let config_start = raw.find("{\"registry_headers").unwrap();
    assert_eq!(
        result.warnings[0].position.start_column,
        raw[..config_start].encode_utf16().count() + 1
    );
}

#[test]
fn rejects_unsupported_and_malformed_requests() {
    for class in ["Unknown", "S3InputConfig"] {
        let request: Request = serde_json::from_value(
            json!({"id":"x", "config_class":class, "json":"{}", "pointer":""}),
        )
        .unwrap();
        assert!(Validator::new().unwrap().validate(request).is_err());
    }
    let request: Request = serde_json::from_value(
        json!({"id":"x", "config_class":"HttpOutputConfig", "json":"{", "pointer":""}),
    )
    .unwrap();
    assert!(Validator::new().unwrap().validate(request).is_err());
}
