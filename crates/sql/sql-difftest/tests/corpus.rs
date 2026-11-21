use feldera_sql_difftest::{
    compare_expressions,
    corpus::{compare_zsets, portable_output_zset, validate_case},
    parse_json,
};
use serde_json::{json, Value};
use std::{fs, process::Command};

fn case() -> Value {
    let zset = json!({
        "class": "DBSPZSetExpression", "id": 0,
        "elementType": {"class": "DBSPTypeDecimal", "id": 2, "mayBeNull": false,
            "precision": 38, "scale": 10},
        "type": {"class": "DBSPTypeZSet", "id": 3, "elementType": {"node": 2}},
        "data": [{"class": "DBSPDecimalLiteral", "id": 1, "type": {"node": 2}, "value": "2"}],
        "weights": [-2]
    });
    json!({
        "schema_version": 1, "source_test": "Example#test", "case_index": 0,
        "name": "decimal", "producer_commit": "compiler", "calcite_commit": "calcite",
        "runtime_commit": "runtime", "program": "CREATE VIEW v AS SELECT 1",
        "options": {"ioOptions": {"gen2": false}, "languageOptions": {"incrementalize": true}},
        "schema": {}, "diagnostics": [], "engine_mode": "classic", "runtime_verified": false,
        "status": "exported", "expected_runtime_error": null, "step_semantics": "delta",
        "input_order": [], "output_order": [], "circuit_inputs": [], "circuit_outputs": [],
        "circuit": {"class": "DBSPCircuit", "id": 0, "allOperators": [], "declarations": []},
        "commands": [{"kind": "step", "inputs": [], "outputs": [{
            "name": {"text": "v", "quoted": false}, "primary_keys": [],
            "zset_ir": zset
        }]}, {"kind": "compact"}]
    })
}

#[test]
fn validates_oracle_graphs_and_preserves_step_semantics() {
    let baseline = case();
    let summary = validate_case(&baseline).unwrap();
    assert!(summary.exported);
    assert_eq!(summary.steps, 1);
    assert!(!summary.expected_error);
    for (path, bad) in [
        ("/schema_version", json!(2)),
        ("/runtime_verified", json!(true)),
        ("/step_semantics", json!("whole_relation")),
        ("/engine_mode", json!("gen2")),
        ("/commands/0/kind", json!("unknown")),
        ("/commands/0/outputs/0/zset_ir/data/0", json!({"node": 12})),
        ("/commands/0/outputs/0/zset_ir/elementType", json!(5)),
        (
            "/commands/0/outputs/0/zset_ir/type/elementType",
            json!({"id": 200, "class": "DBSPTypeBool"}),
        ),
        ("/commands/0/outputs/0/zset_ir/weights", json!([])),
        ("/commands/0/outputs/0/zset_ir/weights/0", json!(false)),
        ("/commands/0/outputs/0/zset_ir/weights/0", json!(1.5)),
        (
            "/commands/0/outputs/0/zset_ir/weights/0",
            json!(9223372036854775808_u64),
        ),
    ] {
        let mut changed = baseline.clone();
        *changed.pointer_mut(path).unwrap() = bad;
        assert!(
            validate_case(&changed).is_err(),
            "accepted mutation at {path}"
        );
    }
    let mut skipped = baseline;
    skipped["status"] = json!("skipped");
    skipped["reason"] = json!("test body failed");
    skipped.as_object_mut().unwrap().remove("circuit");
    skipped.as_object_mut().unwrap().remove("commands");
    assert!(!validate_case(&skipped).unwrap().exported);
}

#[test]
fn distinguishes_test_steps_from_effective_compilation_mode() {
    let mut record = case();
    record["options"]["languageOptions"]["incrementalize"] = json!(false);
    record["step_semantics"] = json!("whole_relation");
    record["compilation_options"] = record["options"].clone();
    record["compilation_options"]["languageOptions"]["incrementalize"] = json!(true);
    record["circuit"]["incremental"] = json!(true);
    validate_case(&record).unwrap();
    for (path, bad) in [
        ("/circuit/incremental", json!(false)),
        ("/compilation_options/ioOptions/gen2", json!(true)),
        (
            "/compilation_options/languageOptions/incrementalize",
            json!(null),
        ),
        ("/step_semantics", json!("delta")),
    ] {
        let mut changed = record.clone();
        *changed.pointer_mut(path).unwrap() = bad;
        assert!(
            validate_case(&changed).is_err(),
            "accepted mutation at {path}"
        );
    }
}

#[test]
fn validates_submission_boundaries_without_rewriting_sql() {
    let mut record = case();
    record["submissions"] = json!([
        {"sql": "CREATE TABLE t(i INT)", "many": false, "batch": 0},
        {"sql": "CREATE VIEW v AS SELECT * FROM t", "many": true, "batch": 1}
    ]);
    validate_case(&record).unwrap();
    for (field, value) in [
        ("sql", json!(null)),
        ("many", json!(0)),
        ("batch", json!(-1)),
    ] {
        let mut bad = record.clone();
        bad["submissions"][0][field] = value;
        assert!(validate_case(&bad).is_err());
    }
    record["submissions"][0]["batch"] = json!(2);
    assert!(validate_case(&record).is_err());
}

#[test]
fn expression_comparison_keeps_exact_decimal_values_and_sharing() {
    let left = parse_json(br#"{"class":"DBSPZSetExpression","id":0,"values":[{"class":"DBSPDecimalLiteral","id":1,"value":1234567890123456789012345678.1234567890},{"node":1}]}"#).unwrap();
    let mut right = left.clone();
    right["id"] = json!(100);
    right["values"][0]["id"] = json!(200);
    right["values"][1]["node"] = json!(200);
    compare_expressions(&left, &right).unwrap();
    right["values"][0]["value"] = parse_json(b"1234567890123456789012345678.1234567891").unwrap();
    assert!(compare_expressions(&left, &right).is_err());
    right = left.clone();
    right["values"][1] = right["values"][0].clone();
    right["values"][1]["id"] = json!(2);
    assert!(compare_expressions(&left, &right).is_err());
}

#[test]
fn zsets_compare_as_signed_bags_of_exact_typed_rows() {
    let mut expected = case()["commands"][0]["outputs"][0]["zset_ir"].clone();
    expected["data"].as_array_mut().unwrap().push(json!({
        "class": "DBSPDecimalLiteral", "id": 4, "type": {"node": 2},
        "value": "1234567890123456789012345678.1234567890"
    }));
    expected["weights"].as_array_mut().unwrap().push(json!(3));
    let mut actual = expected.clone();
    actual["data"].as_array_mut().unwrap().reverse();
    actual["weights"] = json!([3, -1, -1]);
    actual["data"][1]["id"] = json!(100);
    actual["data"]
        .as_array_mut()
        .unwrap()
        .push(json!({"node": 100}));
    compare_zsets(&expected, &actual).unwrap();

    let mut changed = actual.clone();
    changed["data"][0]["value"] = json!("1234567890123456789012345678.1234567891");
    assert!(compare_zsets(&expected, &changed).is_err());
    changed = actual.clone();
    changed["weights"][1] = json!(-2);
    assert!(compare_zsets(&expected, &changed).is_err());
    changed = actual;
    changed["weights"] = json!([3, i64::MAX, 1]);
    let error = compare_zsets(&expected, &changed).unwrap_err();
    assert!(error.to_string().contains("consolidated weight exceeds"));
}

#[test]
fn portable_outputs_normalize_exact_decimal_values_and_keep_signed_weights() {
    let mut bag = case()["commands"][0]["outputs"][0]["zset_ir"].clone();
    bag["data"][0]["value"] = json!("1234567890123456789012345678.1234567890");
    bag["weights"] = json!([i64::MIN]);
    let expected = json!({
        "schema_version": 1,
        "row_type": {"kind": "decimal", "nullable": false, "precision": 38, "scale": 10},
        "rows": [{
            "row": {"coefficient": "1234567890123456789012345678123456789", "exponent": -9},
            "weight": i64::MIN.to_string()
        }]
    });
    let encode = |bag: &Value| serde_json::to_value(portable_output_zset(bag).unwrap()).unwrap();
    assert_eq!(encode(&bag), expected);
    bag["data"][0]["id"] = json!(40);
    bag["data"][0]["value"] = json!("1234567890123456789012345678123456789E-9");
    assert_eq!(encode(&bag), expected);

    bag["data"][0]["value"] = json!("NaN");
    assert!(portable_output_zset(&bag).is_err());
    bag["data"][0]["class"] = json!("DBSPApplyExpression");
    assert!(portable_output_zset(&bag).is_err());
}

#[test]
fn directory_validation_rejects_missing_cases_and_false_counts() {
    let root = std::env::temp_dir().join(format!("sql-corpus-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
            .arg("--validate-corpus")
            .arg(&root)
            .output()
            .unwrap()
    };
    assert_ne!(run().status.code(), Some(0));
    let test = root.join("test");
    fs::create_dir(&test).unwrap();
    let manifest = json!({"schema_version": 1, "source_test": "Example#test",
        "case_count": 1, "exported": 1, "skipped": 0, "test_body_status": "passed"});
    fs::write(test.join("complete.json"), manifest.to_string()).unwrap();
    assert_ne!(run().status.code(), Some(0));
    fs::write(test.join("case-0000.json"), case().to_string()).unwrap();
    assert_eq!(run().status.code(), Some(0));
    let mut wrong = manifest.clone();
    wrong["exported"] = json!(2);
    fs::write(test.join("complete.json"), wrong.to_string()).unwrap();
    assert_eq!(run().status.code(), Some(1));
    fs::write(test.join("complete.json"), manifest.to_string()).unwrap();
    fs::write(test.join("unexpected.json"), "{}").unwrap();
    assert_ne!(run().status.code(), Some(0));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn probe_tables_cannot_certify_partial_runs_or_compiler_bugs() {
    let root = std::env::temp_dir().join(format!("sql-probes-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
            .arg("--validate-probes")
            .arg(&root)
            .output()
            .unwrap()
    };
    let mut manifest = json!({"schema_version": 1, "oracle_ready": true,
        "runtime_verified": false, "compiler_bugs": 0, "probes": 1, "accepted": 1,
        "rejected": 0, "expected_probes": 1, "producer_commit": "compiler", "calcite_commit": "calcite",
        "runtime_commit": "runtime"});
    let mut row = json!({"schema_version": 1, "compiler_bug": false,
        "probe": {"schema_version": 1, "id": 0, "program": "CREATE VIEW v AS SELECT 1"},
        "schema": {}, "diagnostics": [], "accepted": true,
        "options": {"ioOptions": {"inputCircuit": true}, "languageOptions": {"optimizationLevel": 0}},
        "s2": {"class": "DBSPCircuit", "id": 0, "allOperators": [], "declarations": []}});
    fs::write(root.join("golden.jsonl"), row.to_string()).unwrap();
    assert_ne!(run().status.code(), Some(0));
    fs::write(root.join("complete.json"), manifest.to_string()).unwrap();
    assert_eq!(run().status.code(), Some(0));
    row["s2"]["allOperators"] = json!([{"node": 19}]);
    fs::write(root.join("golden.jsonl"), row.to_string()).unwrap();
    assert_ne!(run().status.code(), Some(0));
    manifest["compiler_bugs"] = json!(1);
    manifest["oracle_ready"] = json!(false);
    fs::write(root.join("complete.json"), manifest.to_string()).unwrap();
    assert_ne!(run().status.code(), Some(0));
    fs::remove_dir_all(root).unwrap();
}
