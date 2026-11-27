use feldera_sql_rel_model::{LiteralLimits, ObjectLimits, ValidationLimits, program::Program};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/statement-program.json")).unwrap()
}
fn check(wire: Value) -> Result<(), String> {
    let program: Program = serde_json::from_value(wire).map_err(|e| e.to_string())?;
    program
        .validate(
            ValidationLimits::default(),
            LiteralLimits::default(),
            ObjectLimits::default(),
        )
        .map_err(|e| e.to_string())
}

#[test]
fn java_program_roundtrip_retains_lowering_order_dml_and_catalog_ownership() {
    let wire = fixture();
    check(wire.clone()).unwrap();
    let program: Program = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(&program).unwrap(), wire);
    assert_eq!(program.statements.len(), 11);
    assert_eq!(program.producers.roots.len(), 4);
    assert!(program.statements.iter().any(|s| !s.source().visible));
}

#[test]
fn malformed_statement_references_and_ownership_are_rejected() {
    for (path, replacement, expected) in [
        ("/statements/0/values", json!(99), "SET requires"),
        ("/statements/1/ty", json!(999), "reference"),
        ("/statements/2/function", json!(999), "requires a function"),
        ("/statements/6/schema", json!(999), "schema outside"),
        ("/statements/6/name/name", json!("other"), "names differ"),
        ("/statements/7/relation", json!(999), "relation outside"),
        (
            "/statements/9/root/fields/0/index",
            json!(999),
            "projection outside",
        ),
        ("/statements/9/emit_final", json!(999), "emit_final"),
        ("/statements/10/name_position/line", json!(999), "source"),
        (
            "/statements/0/source/kind",
            json!("java.lang.Runtime"),
            "source kind",
        ),
    ] {
        let mut invalid = fixture();
        *invalid.pointer_mut(path).unwrap() = replacement;
        let error = check(invalid).unwrap_err();
        assert!(error.contains(expected), "{path}: {error}");
    }
    let mut invalid = fixture();
    invalid["statements"].as_array_mut().unwrap().remove(0);
    invalid["diagnostics"]["before"]
        .as_array_mut()
        .unwrap()
        .remove(0);
    assert!(
        check(invalid)
            .unwrap_err()
            .contains("no statement or catalog owner")
    );
    let mut invalid = fixture();
    invalid["producers"]["roots"]
        .as_array_mut()
        .unwrap()
        .reverse();
    assert!(check(invalid).unwrap_err().contains("roots differ"));
    let mut invalid = fixture();
    let boundary = invalid["diagnostics"]["before"][6].clone();
    invalid["diagnostics"]["before"]
        .as_array_mut()
        .unwrap()
        .push(boundary);
    let duplicate = invalid["statements"][6].clone();
    invalid["statements"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    assert!(
        check(invalid)
            .unwrap_err()
            .contains("multiple declarations")
    );
}

#[test]
fn required_nullable_fields_and_unknown_statement_fields_are_strict() {
    for (object, field) in [
        ("/statements/0/source", "position_internal"),
        ("/statements/1", "attributes"),
        ("/statements/1/attributes/0", "nullable"),
        ("/statements/1/attributes/0", "named_type"),
        ("/statements/7", "columns"),
        ("/statements/7", "relation"),
    ] {
        let mut invalid = fixture();
        invalid
            .pointer_mut(object)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            serde_json::from_value::<Program>(invalid).is_err(),
            "{object}/{field}"
        );
    }
    let mut invalid = fixture();
    invalid["statements"][0]["unmodeled"] = json!(true);
    assert!(serde_json::from_value::<Program>(invalid).is_err());
}

#[test]
fn optimizer_selection_survives_replay_of_new_and_old_programs() {
    for selected in [true, false] {
        let mut wire = fixture();
        wire["context"]["options"]["ioOptions"]["rustOptimizerPasses"] = json!(selected);
        let program: Program = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(program).unwrap(), wire);
    }
    let mut invalid = fixture();
    invalid["context"]["options"]["ioOptions"]["rustOptimizerPasses"] = Value::Null;
    assert!(serde_json::from_value::<Program>(invalid).is_err());
}

#[test]
fn duplicate_logging_keys_are_rejected_before_map_construction() {
    // Construct the raw bytes: parsing into Value first would lose the duplicate.
    let wire = serde_json::to_string(&fixture()).unwrap();
    for duplicate in [
        r#"{"example":"1","example":"2"}"#,
        r#"{"example":"1","example":"1"}"#,
        r#"{"example":"1","\u0065xample":"2"}"#,
    ] {
        let invalid = wire.replace(
            "\"loggingLevel\":{}",
            &format!("\"loggingLevel\":{duplicate}"),
        );
        assert_ne!(wire, invalid);
        let error = serde_json::from_str::<Program>(&invalid).unwrap_err();
        assert!(error.to_string().contains("duplicate logging level key"));
    }

    let mut valid = fixture();
    valid["context"]["options"]["ioOptions"]["loggingLevel"] = json!({"first": "1", "second": "2"});
    check(valid.clone()).unwrap();
    let program: Program = serde_json::from_value(valid.clone()).unwrap();
    assert_eq!(serde_json::to_value(program).unwrap(), valid);
}

#[test]
fn catalog_column_indices_match_their_ordered_positions() {
    for index in [1, 999, i32::MAX as u32, i32::MAX as u32 + 1, u32::MAX] {
        let mut invalid = fixture();
        invalid["catalog"]["schemas"][0]["columns"][0]["index"] = json!(index);
        assert!(check(invalid).unwrap_err().contains("column index differs"));
    }
}

#[test]
fn program_schema_and_limits_match_the_typed_contract() {
    let generated = serde_json::to_value(schemars::schema_for!(Program)).unwrap();
    let checked: Value = serde_json::from_str(include_str!("../program-schema.json")).unwrap();
    assert_eq!(generated, checked);
    assert_eq!(
        generated["definitions"]["StatementSource"]["properties"]["position_internal"]["type"],
        json!(["boolean", "null"])
    );
    let program: Program = serde_json::from_value(fixture()).unwrap();
    for limits in [
        ValidationLimits {
            source_bytes: 1,
            ..ValidationLimits::default()
        },
        ValidationLimits {
            references: 1,
            ..ValidationLimits::default()
        },
    ] {
        assert!(
            program
                .validate(limits, LiteralLimits::default(), ObjectLimits::default())
                .is_err()
        );
    }
}

#[test]
fn generated_and_user_sources_cannot_be_substituted() {
    for (path, replacement, expected) in [
        (
            "/statements/3/source/source",
            json!(0),
            "visibility differs",
        ),
        (
            "/statements/0/source/source",
            json!(1),
            "visibility differs",
        ),
        ("/statements/0/source/source", json!(999), "source outside"),
        ("/catalog/schemas/0/source", json!(999), "source outside"),
        (
            "/catalog/schemas/0/source",
            json!(0),
            "value source differs",
        ),
    ] {
        let mut invalid = fixture();
        *invalid.pointer_mut(path).unwrap() = replacement;
        let error = check(invalid).unwrap_err();
        assert!(error.contains(expected), "{path}: {error}");
    }
}

#[test]
fn aggregate_declarations_and_calls_use_the_shared_operator_arena() {
    let wire: Value =
        serde_json::from_str(include_str!("fixtures/aggregate-program.json")).unwrap();
    check(wire.clone()).unwrap();
    let program: Program = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(&program).unwrap(), wire);
    assert_eq!(wire["properties"]["operators"], json!({"kind":"shared"}));
    let declared = &wire["statements"][0]["function"];
    let calls: Vec<_> = wire["properties"]["relations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|relation| relation["fields"]["kind"] == "aggregate")
        .flat_map(|relation| relation["fields"]["calls"].as_array().unwrap())
        .collect();
    assert!(calls.iter().any(|call| &call["operator"] == declared));
    for (path, replacement, expected) in [
        (
            "/statements/0/function",
            json!(999),
            "requires an aggregate",
        ),
        (
            "/expressions/operator_objects/0/return_type",
            json!(999),
            "reference",
        ),
        (
            "/expressions/operator_objects/0/name/names",
            json!(["schema", "function"]),
            "simple name",
        ),
        (
            "/properties/operators",
            json!({"kind":"owned", "objects":[]}),
            "outside arena",
        ),
    ] {
        let mut invalid = wire.clone();
        *invalid.pointer_mut(path).unwrap() = replacement;
        let error = check(invalid).unwrap_err();
        assert!(error.contains(expected), "{path}: {error}");
    }
}

#[test]
fn frontend_diagnostic_timeline_is_typed_and_bounded() {
    let mut wire = fixture();
    wire["diagnostics"]["before"][0]["messages"] = json!([{
        "context":{"kind":"absent"}, "range":{"kind":"absent"},
        "continuation":false,"type":"Unused declaration","message":"Unused view"}]);
    check(wire.clone()).unwrap();
    for (path, replacement) in [
        ("/diagnostics/before", json!([])),
        (
            "/diagnostics/before/0/messages/0/continuation",
            json!("false"),
        ),
        (
            "/diagnostics/before/0/messages/0/range",
            json!({"kind":"source","source":999,
            "start":{"line":1,"column":1},"end":{"line":1,"column":1}}),
        ),
        (
            "/diagnostics/after/context",
            json!({"kind":"source","source":0,
            "start":{"line":999999,"column":1},"end":{"line":999999,"column":1}}),
        ),
    ] {
        let mut invalid = wire.clone();
        *invalid.pointer_mut(path).unwrap() = replacement;
        assert!(check(invalid).is_err(), "accepted {path}");
    }
    let mut invalid = wire.clone();
    invalid["diagnostics"]["before"][0]["messages"][0]["warning"] = json!(false);
    assert!(check(invalid).is_err());
    let mut program: Program = serde_json::from_value(wire).unwrap();
    program.diagnostics.before[0].messages[0].message = "x".repeat(256 * 1024);
    let limits = ValidationLimits {
        source_bytes: 128 * 1024,
        ..ValidationLimits::default()
    };
    assert!(
        program
            .validate(limits, LiteralLimits::default(), ObjectLimits::default())
            .unwrap_err()
            .message
            .contains("diagnostic text")
    );
}

#[test]
fn compilation_context_rejects_lost_options_and_source_boundaries() {
    let wire = fixture();
    for (path, value) in [
        ("/context/sources/lines", json!(["different submission"])),
        ("/context/sources/file", json!("different.sql")),
        ("/context/input_source", json!("Unknown")),
        (
            "/context/options/languageOptions/optimizationLevel",
            json!(true),
        ),
        (
            "/context/unresolved_view_lateness",
            json!([{"view":"missing","range":{"kind":"source","source":999,
            "start":{"line":1,"column":1},"end":{"line":1,"column":1}}}]),
        ),
    ] {
        let mut invalid = wire.clone();
        *invalid.pointer_mut(path).unwrap() = value;
        assert!(check(invalid).is_err(), "accepted {path}");
    }
    for (path, field) in [
        ("/context/options/ioOptions", "inputFile"),
        ("/context/options/languageOptions", "cteViews"),
        ("/context/sources", "file"),
    ] {
        let mut invalid = wire.clone();
        invalid
            .pointer_mut(path)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(check(invalid).is_err(), "defaulted missing {field}");
    }
    let mut invalid = wire;
    invalid["context"]["options"]["ioOptions"]["newUnmodeledOption"] = json!(true);
    assert!(check(invalid).is_err());
}

#[test]
fn table_scan_order_preserves_independent_identity_order_and_rejects_invalid_members() {
    let wire = fixture();
    for replacement in [json!([]), json!([999]), json!([0, 0])] {
        let mut invalid = wire.clone();
        invalid["table_scan_order"] = replacement;
        assert!(check(invalid).is_err());
    }
    let mut invalid = wire.clone();
    let order = invalid["table_scan_order"].as_array_mut().unwrap();
    order[1] = order[0].clone();
    assert!(check(invalid).unwrap_err().contains("repeated"));
    let mut invalid = wire;
    invalid["table_scan_order"]
        .as_array_mut()
        .unwrap()
        .reverse();
    check(invalid).unwrap();
}
