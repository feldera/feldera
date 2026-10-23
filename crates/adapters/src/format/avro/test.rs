//! Unit tests of the Avro schema validation that need no engine. The connector tests in
//! `parser_tests` and `encoder_tests` cover the format with an engine.

use apache_avro::{Schema as AvroSchema, schema::ResolvedSchema};
use feldera_types::program_schema::{ColumnType, Field};

/// A Debezium temporal type mapped to an incompatible SQL column must be
/// rejected during schema validation.
#[test]
fn test_debezium_temporal_type_mismatch() {
    use super::schema::validate_struct_schema;

    let value_schema = r#"{
        "type": "record",
        "name": "Temporal",
        "connect.name": "test_namespace.Temporal",
        "fields": [
            { "name": "id", "type": { "type": "long", "connect.name": "io.debezium.time.Timestamp" } }
        ]
    }"#;

    let hoisted = super::coercion::hoist_coercible_types(value_schema).unwrap();
    let schema = AvroSchema::parse_str(&hoisted).unwrap();
    let resolved = ResolvedSchema::try_from(&schema).unwrap();
    let refs = resolved
        .get_names()
        .iter()
        .map(|(name, schema)| (name.clone(), (*schema).clone()))
        .collect();

    // The `id` column is BIGINT, but the Avro field is a Debezium timestamp.
    let fields = vec![Field::new("id".into(), ColumnType::bigint(false))];
    let err = validate_struct_schema(&schema, &refs, &fields).unwrap_err();
    assert!(
        err.contains("io.debezium.time.Timestamp") && err.contains("TIMESTAMP"),
        "unexpected error message: {err}"
    );
}
