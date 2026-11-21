use feldera_sql_difftest::{canonical_dump_bytes, compare_dumps, parse_json};
use serde_json::{json, Value};

fn dump(mode: &str) -> Value {
    json!({
        "schema_version": 1,
        "producer_commit": "java-test-revision",
        "runtime_abi": "test-runtime-build",
        "engine_mode": mode,
        "options": {"languageOptions": {"incrementalize": true}},
        "observation": {"stage": "S2", "sequence": 0, "path": []},
        "replayable": false,
        "circuit": {"class": "DBSPCircuit", "id": 1, "declarations": [], "allOperators": []}
    })
}

#[test]
fn compares_both_modes_across_revisions_and_allocation_ids() {
    for mode in ["classic", "gen2"] {
        let expected = dump(mode);
        let mut actual = expected.clone();
        actual["producer_commit"] = json!("rust-test-revision");
        actual["circuit"]["id"] = json!(500);
        compare_dumps(&expected, &actual).unwrap();
    }
}

#[test]
fn rejects_missing_unknown_and_incompatible_headers_before_circuit_decoding() {
    let expected = dump("classic");
    for field in expected.as_object().unwrap().keys() {
        let mut actual = expected.clone();
        actual.as_object_mut().unwrap().remove(field);
        assert!(compare_dumps(&actual, &actual).is_err(), "missing {field}");
    }
    for (field, value) in [
        ("schema_version", json!(2)),
        ("schema_version", json!("1")),
        ("runtime_abi", json!("")),
        ("producer_commit", json!("  ")),
        ("engine_mode", json!("native")),
        ("options", json!([])),
        ("replayable", json!(true)),
        ("replayable", json!(null)),
        ("unexpected", json!(0)),
    ] {
        let mut actual = expected.clone();
        actual[field] = value;
        actual["circuit"] = Value::Null;
        let error = compare_dumps(&actual, &actual).unwrap_err();
        assert_eq!(error.path, format!("/{field}"));
    }
    for (field, value) in [
        ("runtime_abi", json!("another-runtime")),
        ("engine_mode", json!("gen2")),
        (
            "options",
            json!({"languageOptions": {"incrementalize": false}}),
        ),
        (
            "observation",
            json!({"stage": "S3", "sequence": 1, "path": []}),
        ),
    ] {
        let mut actual = expected.clone();
        actual[field] = value;
        actual["circuit"] = Value::Null;
        assert_eq!(
            compare_dumps(&expected, &actual).unwrap_err().path,
            format!("/{field}")
        );
    }
}

#[test]
fn validates_nested_pass_and_iteration_identity() {
    let mut expected = dump("gen2");
    expected["observation"] = json!({
        "stage": "S4", "sequence": 8, "path": [
            {"container": "Optimizer", "pass": "Simplify", "index": 4, "iteration": 0},
            {"container": "Repeat", "pass": "Simplify", "index": 0, "iteration": 2}
        ]
    });
    compare_dumps(&expected, &expected).unwrap();
    let mut actual = expected.clone();
    actual["observation"]["path"][1]["iteration"] = json!(3);
    assert_eq!(
        compare_dumps(&expected, &actual).unwrap_err().path,
        "/observation"
    );
    for (field, value) in [
        ("container", json!("")),
        ("pass", json!(null)),
        ("index", json!(-1)),
        ("iteration", json!(1.5)),
    ] {
        let mut invalid = expected.clone();
        invalid["observation"]["path"][1][field] = value;
        assert!(compare_dumps(&invalid, &invalid).is_err());
    }
}

#[test]
fn rejects_impossible_observation_boundaries() {
    for observation in [
        json!({"stage": "S2", "sequence": 1, "path": []}),
        json!({"stage": "S4", "sequence": 1, "path": []}),
        json!({"stage": "S5", "sequence": 1, "path": []}),
        json!({"stage": "S3", "sequence": -1, "path": []}),
        json!({"stage": "S3", "sequence": 1, "path": null}),
        json!({"stage": "S3", "sequence": 1, "path": [], "extra": 1}),
    ] {
        let mut invalid = dump("classic");
        invalid["observation"] = observation;
        assert!(compare_dumps(&invalid, &invalid).is_err());
    }
}

#[test]
fn reports_payload_errors_under_circuit() {
    let expected = dump("classic");
    let mut actual = expected.clone();
    actual["circuit"]["allOperators"] = json!([{"node": 999}]);
    assert!(compare_dumps(&expected, &actual)
        .unwrap_err()
        .path
        .starts_with("/circuit/"));
}

#[test]
fn canonical_bytes_have_the_same_equality_contract_as_comparisons() {
    let expected = dump("classic");
    let mut renamed = expected.clone();
    renamed["producer_commit"] = json!("another-build");
    renamed["circuit"]["id"] = json!(990);
    assert_eq!(
        canonical_dump_bytes(&expected).unwrap(),
        canonical_dump_bytes(&renamed).unwrap()
    );
    for (field, value) in [
        ("metadata", json!({"class": "UserData", "id": 7})),
        (
            "annotations",
            json!([{"id": 7, "class": "Hash", "value": "abc"}]),
        ),
        ("incremental", json!(true)),
    ] {
        let mut changed = expected.clone();
        changed["circuit"][field] = value;
        assert!(compare_dumps(&expected, &changed).is_err());
        assert_ne!(
            canonical_dump_bytes(&expected).unwrap(),
            canonical_dump_bytes(&changed).unwrap()
        );
    }
    // Plain metadata is sorted recursively, but its ids are never renamed.
    let mut a = expected.clone();
    let mut b = expected.clone();
    a["circuit"]["metadata"] = parse_json(br#"{"b":{"z":2,"a":1},"id":7}"#).unwrap();
    b["circuit"]["metadata"] = parse_json(br#"{"id":7,"b":{"a":1,"z":2}}"#).unwrap();
    assert_eq!(
        canonical_dump_bytes(&a).unwrap(),
        canonical_dump_bytes(&b).unwrap()
    );
    b["circuit"]["metadata"]["id"] = json!(8);
    assert_ne!(
        canonical_dump_bytes(&a).unwrap(),
        canonical_dump_bytes(&b).unwrap()
    );
    b["replayable"] = json!(true);
    assert!(canonical_dump_bytes(&b).is_err());
}

#[test]
fn final_s3_comparison_ignores_only_local_sequence_numbers() {
    use feldera_sql_difftest::compare_final_s3;
    let mut expected = dump("classic");
    expected["observation"] = json!({"stage": "S3", "sequence": 900, "path": []});
    let mut actual = expected.clone();
    actual["observation"]["sequence"] = json!(0);
    compare_final_s3(&expected, &actual).unwrap();
    assert!(compare_dumps(&expected, &actual).is_err());
    for (field, value) in [
        ("runtime_abi", json!("other")),
        ("options", json!({"changed": true})),
        (
            "observation",
            json!({"stage": "S2", "sequence": 0, "path": []}),
        ),
        (
            "circuit",
            json!({"class": "DBSPCircuit", "id": 1, "declarations": [],
                          "allOperators": [], "metadata": {"changed": true}}),
        ),
    ] {
        let mut changed = actual.clone();
        changed[field] = value;
        assert!(compare_final_s3(&expected, &changed).is_err(), "{field}");
    }
}
