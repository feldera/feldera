use feldera_sql_difftest::compare_circuits;
use serde_json::{json, Value};

// A small dump in the shape emitted by ToJsonOuterVisitor/ToJsonInnerVisitor.
// The inner type deliberately shares the source operator's numeric id.
fn circuit() -> Value {
    json!({
        "class": "DBSPCircuit", "id": 0, "incremental": true,
        "metadata": {"inputs": [], "outputs": [], "variables": {"warnings": true}},
        "declarations": [],
        "allOperators": [
            {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [],
             "annotations": [], "outputType": {"class": "DBSPTypeInteger", "id": 1,
                 "width": 32, "mayBeNull": false},
             "metadata": {"columnMetadata": [{"type": {"node": 1}}]}},
            {"class": "DBSPMapOperator", "id": 2, "annotations": [],
             "inputs": [{"operator": {"node": 1}, "outputNumber": 0}],
             "outputType": {"node": 1},
             "position": {"start_line_number": 1, "start_column": 2}},
            {"class": "DBSPSinkOperator", "id": 3, "annotations": [],
             "inputs": [{"operator": {"node": 2}, "outputNumber": 0}],
             "outputType": {"node": 1}}
        ]
    })
}

fn renumber(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if key == "id" || key == "node" {
                    *value = json!(value.as_u64().unwrap() + 1000);
                } else {
                    renumber(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(renumber),
        _ => {}
    }
}

#[test]
fn allocation_ids_and_object_key_order_do_not_matter() {
    let expected = circuit();
    let mut actual = expected.clone();
    renumber(&mut actual);
    compare_circuits(&expected, &actual).unwrap();
    // Exercise independently parsed JSON, not just cloned maps.
    compare_circuits(
        &expected,
        &serde_json::from_str(&actual.to_string()).unwrap(),
    )
    .unwrap();
}

#[test]
fn definitions_can_move_to_another_occurrence() {
    let expected = circuit();
    let mut actual = expected.clone();
    let definition = actual["allOperators"][0].clone();
    actual["allOperators"][0] = json!({"node": 1});
    actual["allOperators"][1]["inputs"][0]["operator"] = definition;
    compare_circuits(&expected, &actual).unwrap();
}

#[test]
fn rejects_missing_operator_tags_and_nonexistent_ports_in_both_inputs() {
    let mut value = circuit();
    value["allOperators"][1]["inputs"][0]["outputNumber"] = json!(1);
    let error = compare_circuits(&value, &value).unwrap_err();
    assert!(error.path.ends_with("/inputs/0/outputNumber"));
    value["allOperators"][0]["class"] = json!("DBSPControlledKeyFilterOperator");
    compare_circuits(&value, &value).unwrap();
    value["allOperators"][1]["inputs"][0]["outputNumber"] = json!(2);
    assert!(compare_circuits(&value, &value).is_err());
    value["allOperators"][0]["class"] = json!("DBSPInputMapWithWaterlineOperator");
    compare_circuits(&value, &value).unwrap();
    value["allOperators"][0]
        .as_object_mut()
        .unwrap()
        .remove("class");
    let error = compare_circuits(&value, &value).unwrap_err();
    assert_eq!(error.path, "/allOperators/0");
}

#[test]
fn a_pruned_nested_output_is_not_addressable() {
    let mut value = circuit();
    let child = value["allOperators"][0].clone();
    value["allOperators"][0] = json!({
        "class": "DBSPNestedOperator", "id": 11, "inputs": [], "allOperators": [child],
        "internalOutputs": [null, {"outputNumber": 0, "operator": {"node": 1}}],
    });
    value["allOperators"][1]["inputs"][0]["operator"] = json!({"node": 11});
    assert!(compare_circuits(&value, &value).is_err());
    value["allOperators"][1]["inputs"][0]["outputNumber"] = json!(1);
    compare_circuits(&value, &value).unwrap();
}

#[test]
fn semantic_fields_are_never_discarded() {
    let expected = circuit();
    for (path, replacement) in [
        ("/metadata/variables/warnings", json!(false)),
        ("/incremental", json!(false)),
        ("/allOperators/0/outputType/mayBeNull", json!(true)),
        ("/allOperators/1/inputs/0/outputNumber", json!(1)),
        ("/allOperators/1/inputs/0/operator/node", json!(3)),
        ("/allOperators/1/position/start_column", json!(9)),
        ("/allOperators/1/class", json!("DBSPFilterOperator")),
        (
            "/allOperators/1/annotations",
            json!([{"class": "OperatorHash", "id": 7}]),
        ),
    ] {
        let mut actual = expected.clone();
        *actual.pointer_mut(path).unwrap() = replacement;
        let error = compare_circuits(&expected, &actual).expect_err(path);
        assert!(!error.message.contains("undefined"), "{path}: {error}");
    }
}

#[test]
fn opaque_ids_and_class_tags_are_not_ir_definitions() {
    let mut expected = circuit();
    expected["metadata"]["properties"] = json!({"class": "external", "id": 77, "node": 99});
    expected["allOperators"][0]["annotations"] = json!([
        {"class": "OperatorHash", "id": 77}, {"node": 999}
    ]);
    compare_circuits(&expected, &expected).unwrap();
    for path in [
        "/metadata/properties/id",
        "/allOperators/0/annotations/0/id",
    ] {
        let mut actual = expected.clone();
        *actual.pointer_mut(path).unwrap() = json!(78);
        assert!(compare_circuits(&expected, &actual).is_err());
    }
}

#[test]
fn expression_sharing_is_preserved() {
    let expected = circuit();
    let mut actual = expected.clone();
    let mut duplicate = actual["allOperators"][0]["outputType"].clone();
    duplicate["id"] = json!(50);
    actual["allOperators"][1]["outputType"] = duplicate;
    assert!(compare_circuits(&expected, &actual).is_err());
    assert!(compare_circuits(&actual, &expected).is_err());
}

#[test]
fn operator_sharing_is_preserved_even_for_identical_definitions() {
    let mut expected = circuit();
    let mut duplicate = expected["allOperators"][0].clone();
    // Keep the same number of nodes, changing only which one an edge uses.
    duplicate["id"] = json!(99);
    duplicate["outputType"] = json!({"node": 1});
    expected["allOperators"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    let mut actual = expected.clone();
    actual["allOperators"][1]["inputs"][0]["operator"] = json!({"node": 99});
    assert!(compare_circuits(&expected, &actual).is_err());
}

#[test]
fn edge_order_and_operator_order_are_preserved() {
    let mut expected = circuit();
    expected["allOperators"][2]["inputs"] = json!([
        {"operator": {"node": 1}, "outputNumber": 0},
        {"operator": {"node": 2}, "outputNumber": 0}
    ]);
    let mut actual = expected.clone();
    actual["allOperators"][2]["inputs"]
        .as_array_mut()
        .unwrap()
        .reverse();
    assert!(compare_circuits(&expected, &actual).is_err());
    actual = expected.clone();
    actual["allOperators"].as_array_mut().unwrap().swap(0, 1);
    assert!(compare_circuits(&expected, &actual).is_err());
}

#[test]
fn cycles_and_nested_circuits_keep_their_topology() {
    let mut expected = circuit();
    expected["allOperators"][0]["inputs"] = json!([
        {"operator": {"node": 2}, "outputNumber": 0}
    ]);
    let mut actual = expected.clone();
    renumber(&mut actual);
    compare_circuits(&expected, &actual).unwrap();
    actual["allOperators"][0]["inputs"][0]["operator"] = json!({"node": 1003});
    assert!(compare_circuits(&expected, &actual).is_err());

    let expected = json!({"class": "DBSPCircuit", "id": 100, "declarations": [],
        "allOperators": [{"class": "DBSPNestedOperator", "id": 200,
            "allOperators": circuit()["allOperators"],
            "internalOutputs": [{"operator": {"node": 3}, "outputNumber": 0}, null]}]});
    let mut actual = expected.clone();
    renumber(&mut actual);
    compare_circuits(&expected, &actual).unwrap();
}

#[test]
fn malformed_dumps_cannot_compare_equal_to_themselves() {
    let mut dangling = circuit();
    dangling["allOperators"][1]["inputs"][0]["operator"] = json!({"node": 999});
    let mut duplicate = circuit();
    duplicate["allOperators"][2]["id"] = json!(2);
    let mut negative = circuit();
    negative["allOperators"][2]["id"] = json!(-1);
    let mut invalid_class = circuit();
    invalid_class["allOperators"][2]["class"] = json!(42);
    let mut wrong_namespace = circuit();
    wrong_namespace["allOperators"][1]["outputType"] = json!({"node": 3});
    for invalid in [
        dangling,
        duplicate,
        negative,
        invalid_class,
        wrong_namespace,
        json!({}),
    ] {
        let error = compare_circuits(&invalid, &invalid).unwrap_err();
        assert!(error.message.starts_with("expected circuit:"));
        assert!(compare_circuits(&circuit(), &invalid)
            .unwrap_err()
            .message
            .starts_with("actual circuit:"));
    }
}

#[test]
fn long_reference_chains_do_not_recurse_or_expand() {
    let mut expected = circuit();
    expected["allOperators"] = Value::Array((1..10_000).map(|id| json!({
        "class": "DBSPMapOperator", "id": id,
        "inputs": [{"operator": {"node": if id == 9999 { 1 } else { id + 1 }}, "outputNumber": 0}]
    })).collect());
    let mut actual = expected.clone();
    renumber(&mut actual);
    compare_circuits(&expected, &actual).unwrap();
}

#[test]
fn arbitrary_precision_values_remain_distinct_and_diagnostics_hide_them() {
    let mut expected = circuit();
    expected["metadata"]["secret"] = json!("do-not-print");
    expected["allOperators"][1]["constant"] =
        serde_json::from_str("123456789012345678901234567890").unwrap();
    let mut actual = expected.clone();
    actual["allOperators"][1]["constant"] =
        serde_json::from_str("123456789012345678901234567891").unwrap();
    let error = compare_circuits(&expected, &actual)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("123456789"));
    assert!(!error.contains("do-not-print"));
}

#[test]
fn plain_object_ids_and_missing_fields_are_compared() {
    let mut expected = circuit();
    expected["allOperators"][1]["extra"] = json!({"id": 5});
    let mut actual = expected.clone();
    actual["allOperators"][1]["extra"]["id"] = json!(6);
    assert!(compare_circuits(&expected, &actual).is_err());
    actual["allOperators"][1]
        .as_object_mut()
        .unwrap()
        .remove("extra");
    assert!(compare_circuits(&expected, &actual).is_err());
}

#[test]
fn ordinary_json_cannot_impersonate_a_canonical_reference() {
    let expected = circuit();
    let mut actual = expected.clone();
    actual["allOperators"][1]["outputType"] = json!({"namespace": "inner", "node": 0});
    assert!(compare_circuits(&expected, &actual).is_err());
}

#[test]
fn object_field_order_is_irrelevant() {
    let a: Value = serde_json::from_str(r#"{"class":"DBSPCircuit","id":1,"allOperators":[],"declarations":[],"metadata":{"a":1,"b":2}}"#).unwrap();
    let b: Value = serde_json::from_str(r#"{"metadata":{"b":2,"a":1},"declarations":[],"allOperators":[],"id":9,"class":"DBSPCircuit"}"#).unwrap();
    compare_circuits(&a, &b).unwrap();
}
