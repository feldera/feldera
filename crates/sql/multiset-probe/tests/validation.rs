use feldera_multiset_probe::{
    analyze, ensure_distinct_outputs, insert_weight_validation, merge_sums,
    propagate_empty_sources, remove_all_view_operators, remove_dead_operators,
    remove_redundant_integrates, remove_unused_operators, remove_view_operators,
    select_error_consumers,
};
use serde_json::{json, Value};

fn circuit() -> Value {
    json!({"class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
        {"class": "DBSPSourceMapOperator", "id": 1, "inputs": []},
        {"class": "DBSPSinkOperator", "id": 2, "inputs": [{"operator": {"node": 1}, "outputNumber": 0}]}
    ]})
}

#[test]
fn error_consumer_selection_tracks_only_successors_of_the_error_source() {
    let mut wire = circuit();
    assert!(select_error_consumers(&wire).unwrap().is_empty());
    let operators = wire["allOperators"].as_array_mut().unwrap();
    operators.insert(
        0,
        json!({
            "class": "DBSPSourceMultisetOperator", "id": 3, "inputs": [],
            "tableName": {"name": "feldera_error_table", "isQuoted": false}
        }),
    );
    operators.push(json!({
        "class": "DBSPSumOperator", "id": 4, "inputs": [
            {"operator": {"node": 3}, "outputNumber": 0},
            {"operator": {"node": 1}, "outputNumber": 0}
        ]
    }));
    operators.push(json!({
        "class": "DBSPSinkOperator", "id": 5,
        "inputs": [{"operator": {"node": 4}, "outputNumber": 0}]
    }));
    assert_eq!(select_error_consumers(&wire).unwrap(), vec![3, 4, 5]);
}

#[test]
fn stream_kind_validation_rejects_mixed_sums_and_exempts_system_sinks() {
    let mut wire = json!({"class": "DBSPCircuit", "incremental": false, "allOperators": [
        {"class": "DBSPConstantOperator", "id": 1, "inputs": []},
        {"class": "DBSPDifferentiateOperator", "id": 2, "inputs": [
            {"operator": {"node": 1}, "outputNumber": 0}
        ]},
        {"class": "DBSPSumOperator", "id": 3, "inputs": [
            {"operator": {"node": 1}, "outputNumber": 0},
            {"operator": {"node": 2}, "outputNumber": 0}
        ]}
    ]});
    assert!(feldera_multiset_probe::stream_kinds(&wire)
        .unwrap_err()
        .to_string()
        .contains("input 1: expected [Collection], found Delta"));

    wire["allOperators"][2] = json!({
        "class": "DBSPSinkOperator", "id": 3, "metadata": {"system": false},
        "inputs": [{"operator": {"node": 2}, "outputNumber": 0}]
    });
    assert!(feldera_multiset_probe::stream_kinds(&wire).is_err());
    wire["allOperators"][2]["metadata"]["system"] = true.into();
    assert_eq!(
        feldera_multiset_probe::stream_kinds(&wire).unwrap(),
        json!([["COLLECTION"], ["DELTA"], ["DELTA"]])
    );
}

#[test]
fn nested_stream_kinds_use_the_region_clock_and_preserve_missing_ports() {
    let mut wire = json!({"incremental": false, "allOperators": [
        {"class": "DBSPConstantOperator", "id": 1, "inputs": []},
        {"class": "DBSPNestedOperator", "id": 2,
         "inputs": [{"operator": {"node": 1}, "outputNumber": 0}],
         "allOperators": [
            {"class": "DBSPDeltaOperator", "id": 3,
             "inputs": [{"operator": {"node": 1}, "outputNumber": 0}]},
            {"class": "DBSPViewDeclarationOperator", "id": 4, "inputs": []}
         ],
         "internalOutputs": [null, {"operator": {"node": 4}, "outputNumber": 0}],
         "outputViews": ["unused", "current"]},
        {"class": "DBSPNoopOperator", "id": 5,
         "inputs": [{"operator": {"node": 2}, "outputNumber": 1}]}
    ]});
    let stream_kinds = feldera_multiset_probe::stream_kinds;
    assert_eq!(
        stream_kinds(&wire).unwrap(),
        json!([
            ["COLLECTION"],
            ["DELTA"],
            ["DELTA"],
            [null, "COLLECTION"],
            ["COLLECTION"]
        ])
    );

    wire["allOperators"][2]["inputs"][0]["outputNumber"] = 0.into();
    assert!(stream_kinds(&wire)
        .unwrap_err()
        .to_string()
        .contains("unavailable port 2:0"));
    wire["allOperators"][2]["inputs"][0]["outputNumber"] = 1.into();
    wire["allOperators"][1]["allOperators"][0]["inputs"][0]["operator"]["node"] = 4.into();
    assert!(stream_kinds(&wire)
        .unwrap_err()
        .to_string()
        .contains("delta input does not match the nested input"));
}

#[test]
fn weight_validation_redirects_all_consumers_and_preserves_keyed_sources() {
    let mut wire = circuit();
    assert_eq!(insert_weight_validation(&wire).unwrap(), wire);
    wire["allOperators"][0] = json!({
        "class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [],
        "tableName": {"name": "Quoted table", "isQuoted": true},
        "outputType": {"class": "DBSPTypeZSet", "id": 5, "elementType": {
            "class": "DBSPTypeBool", "id": 6, "mayBeNull": false
        }}
    });
    wire["allOperators"].as_array_mut().unwrap().push(json!({
        "class": "DBSPSinkOperator", "id": 3,
        "inputs": [{"operator": {"node": 1}, "outputNumber": 0}]
    }));
    let result = insert_weight_validation(&wire).unwrap();
    let nodes = result["allOperators"].as_array().unwrap();
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes[1]["class"], "DBSPWeightValidatorOperator");
    assert_eq!(nodes[1]["message"], "Table Quoted table");
    assert_eq!(nodes[1]["inputs"][0]["operator"]["node"], 1);
    for sink in &nodes[2..] {
        assert_eq!(sink["inputs"][0]["operator"]["node"], nodes[1]["id"]);
    }
    assert_eq!(analyze(&result).unwrap(), vec![vec![true]; 4]);
}

#[test]
fn removing_the_error_table_also_removes_its_schema_and_checks_consumers() {
    let mut wire = circuit();
    wire["metadata"] =
        json!({"inputs": [{"name": "feldera_error_table"}, {"name": "data"}], "outputs": []});
    wire["allOperators"].as_array_mut().unwrap().insert(
        0,
        json!({
            "class": "DBSPSourceMultisetOperator", "id": 3, "inputs": [],
            "tableName": {"name": "feldera_error_table", "isQuoted": false}
        }),
    );
    let rewritten = feldera_multiset_probe::remove_error_table(&wire).unwrap();
    assert_eq!(rewritten["metadata"]["inputs"], json!([{"name": "data"}]));
    assert_eq!(rewritten["allOperators"].as_array().unwrap().len(), 2);
    wire["allOperators"][2]["inputs"][0]["operator"]["node"] = 3.into();
    assert!(feldera_multiset_probe::remove_error_table(&wire)
        .unwrap_err()
        .to_string()
        .contains("consumers"));
}

#[test]
fn output_distinct_requires_collections_and_preserves_existing_sets() {
    let mut wire = circuit();
    wire["incremental"] = true.into();
    assert!(ensure_distinct_outputs(&wire)
        .unwrap_err()
        .to_string()
        .contains("pre-incremental"));
    wire["incremental"] = false.into();
    assert_eq!(ensure_distinct_outputs(&wire).unwrap(), wire);
    wire["allOperators"][0]["class"] = "DBSPSourceMultisetOperator".into();
    wire["allOperators"][1]["provenance"] = json!({"ref": 0});
    assert!(ensure_distinct_outputs(&wire)
        .unwrap_err()
        .to_string()
        .contains("provenance"));
}

#[test]
fn late_view_removal_also_bypasses_the_preserved_error_view() {
    let mut wire = circuit();
    wire["allOperators"][0]["outputType"] = json!({
        "class": "DBSPTypeZSet", "id": 10, "elementType": {
            "class": "DBSPTypeBool", "id": 11, "mayBeNull": false
        }
    });
    wire["allOperators"][1]["inputs"][0]["operator"]["node"] = 4.into();
    let operators = wire["allOperators"].as_array_mut().unwrap();
    for (offset, name) in ["error_view", "ordinary"].into_iter().enumerate() {
        let id = offset + 3;
        let source = if offset == 0 { 1 } else { 3 };
        operators.insert(
            offset + 1,
            json!({
                "class": "DBSPViewOperator", "id": id,
                "inputs": [{"operator": {"node": source}, "outputNumber": 0}],
                "viewName": {"name": name}, "metadata": {"recursive": false, "columns": []},
            }),
        );
    }
    let early = remove_view_operators(&wire).unwrap();
    assert_eq!(early["allOperators"].as_array().unwrap().len(), 3);
    assert_eq!(early["allOperators"][1]["viewName"]["name"], "error_view");
    let late = remove_all_view_operators(&wire).unwrap();
    assert_eq!(late["allOperators"].as_array().unwrap().len(), 2);
    assert_eq!(late["allOperators"][1]["inputs"][0]["operator"]["node"], 1);
}

#[test]
fn pruning_relocates_provenance_definitions_to_the_first_surviving_use() {
    let mut wire = circuit();
    let position = json!({
        "start_line_number": 1, "start_column": 2,
        "end_line_number": 1, "end_column": 8,
    });
    let mut relation = json!({
        "id": 7, "relId": 42, "planId": 9, "tableScan": false,
        "digest": "test", "text": "test", "sql": "SELECT i", "statefulSqlName": "",
    });
    let dead = json!({
        "class": "DBSPStreamDistinctOperator", "id": 3,
        "inputs": [{"operator": {"node": 1}, "outputNumber": 0}],
        "provenance": {
            "id": 4, "kind": "LastRel", "position": position,
            "positions": [position], "relation": relation,
        },
    });
    wire["allOperators"].as_array_mut().unwrap().insert(1, dead);
    wire["allOperators"][2]["provenance"] = json!({
        "id": 5, "kind": "IntermediateRel", "position": position,
        "positions": [], "relation": {"ref": 7},
    });

    let pruned = remove_dead_operators(&wire).unwrap();
    assert_eq!(pruned["allOperators"].as_array().unwrap().len(), 2);
    relation["id"] = 0.into();
    assert_eq!(
        pruned["allOperators"][1]["provenance"],
        json!({
            "id": 0, "kind": "IntermediateRel", "position": position,
            "positions": [], "relation": relation,
        }),
    );
}

#[test]
fn trace_retention_roots_keep_their_waterline_chain() {
    let mut wire = circuit();
    wire["allOperators"][0]["class"] = "DBSPSourceMultisetOperator".into();
    wire["allOperators"].as_array_mut().unwrap().extend([
        json!({"class": "DBSPWaterlineOperator", "id": 3,
            "inputs": [{"operator": {"node": 1}, "outputNumber": 0}]}),
        json!({"class": "DBSPApplyOperator", "id": 4,
            "inputs": [{"operator": {"node": 3}, "outputNumber": 0}]}),
        json!({"class": "DBSPIntegrateTraceRetainKeysOperator", "id": 5,
            "inputs": [{"operator": {"node": 1}, "outputNumber": 0},
                       {"operator": {"node": 4}, "outputNumber": 0}]}),
    ]);
    let pruned = remove_dead_operators(&wire).unwrap();
    assert_eq!(pruned["allOperators"].as_array().unwrap().len(), 5);
    assert_eq!(
        analyze(&pruned).unwrap(),
        vec![vec![true], vec![true], vec![false], vec![false], vec![true]]
    );
}

#[test]
fn ignores_saved_annotations_but_checks_references_and_port_numbers() {
    let mut wire = circuit();
    wire["allOperators"][0]["annotations"] = json!([{"class": "Multiset", "value": true}]);
    assert_eq!(analyze(&wire).unwrap(), vec![vec![false], vec![false]]);
    wire["allOperators"][1]["inputs"][0]["outputNumber"] = json!(1);
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("output port"));
    wire["allOperators"][1]["inputs"][0]["operator"]["node"] = json!(99);
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("undefined node"));
}

#[test]
fn rejects_unknown_operators_bad_arity_and_cycles() {
    let mut wire = circuit();
    wire["allOperators"][0]["class"] = json!("DBSPNewOperator");
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("unsupported multiset operator"));
    wire = circuit();
    wire["allOperators"][1]["inputs"] = json!([]);
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("input count"));
    wire = circuit();
    wire["allOperators"][1]["inputs"][0]["operator"]["node"] = json!(2);
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("earlier operator"));
}

#[test]
fn rejects_duplicate_identity_even_if_unused() {
    let mut wire = circuit();
    wire["allOperators"][0]["unused"] = json!([
        {"class": "DBSPTypeBool", "id": 5},
        {"class": "DBSPTypeBool", "id": 5}
    ]);
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("duplicate node"));
}

#[test]
fn annotations_cannot_replace_inner_node_definitions() {
    let wire = json!({"class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
        {"class": "DBSPConstantOperator", "id": 1, "inputs": [], "function": {
            "class": "DBSPZSetExpression", "id": 10, "weights": [1], "data": [0],
            "annotations": [{"class": "DBSPZSetExpression", "id": 10, "weights": [-1], "data": [0]}]
        }},
        {"class": "DBSPConstantOperator", "id": 2, "inputs": [], "function": {"node": 10}}
    ]});
    assert_eq!(analyze(&wire).unwrap(), vec![vec![false], vec![false]]);
}

#[test]
fn only_empty_indexed_constants_have_an_encoding() {
    let mut wire = json!({"class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
        {"class": "DBSPConstantOperator", "id": 1, "inputs": [], "function": {
            "class": "DBSPIndexedZSetExpression", "id": 10
        }}
    ]});
    assert_eq!(analyze(&wire).unwrap(), vec![vec![false]]);
    wire["allOperators"][0]["function"]["weights"] = json!([1]);
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("empty encoding"));
}

#[test]
fn empty_sum_replacement_carries_replay_literal_provenance() {
    let wire = json!({"class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
        {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [], "outputType": {
            "class": "DBSPTypeZSet", "id": 10, "sourceEmpty": true, "elementType": {
                "class": "DBSPTypeInteger", "id": 11, "width": 32, "signed": true,
                "mayBeNull": false, "sourceEmpty": true
            }
        }},
        {"class": "DBSPNegateOperator", "id": 2, "inputs": [
            {"operator": {"node": 1}, "outputNumber": 0}
        ], "outputType": {"node": 10}},
        {"class": "DBSPSumOperator", "id": 3, "inputs": [
            {"operator": {"node": 1}, "outputNumber": 0},
            {"operator": {"node": 2}, "outputNumber": 0}
        ], "outputType": {"node": 10}}
    ]});

    let rewritten = merge_sums(&wire).unwrap();
    let replacement = &rewritten["allOperators"][2];
    assert_eq!(replacement["class"], "DBSPConstantOperator");
    assert_ne!(replacement["outputType"]["id"], 10);
    assert_eq!(
        replacement["function"]["type"]["node"],
        replacement["outputType"]["id"]
    );
    assert_eq!(replacement["function"]["sourceEmpty"], true);
    assert_eq!(analyze(&rewritten).unwrap()[2], vec![false]);
}

fn collection_conversions() -> Value {
    json!({"class": "DBSPCircuit", "id": 0, "incremental": false, "declarations": [], "allOperators": [
        {"class": "DBSPConstantOperator", "id": 1, "inputs": [], "outputType": {
            "class": "DBSPTypeZSet", "id": 10, "elementType": {
                "class": "DBSPTypeInteger", "id": 11, "width": 32, "signed": true, "mayBeNull": false
            }
        }, "function": {
            "class": "DBSPZSetExpression", "id": 12, "weights": [], "data": [], "type": {"node": 10}
        }},
        {"class": "DBSPDifferentiateOperator", "id": 2, "inputs": [
            {"operator": {"node": 1}, "outputNumber": 0}
        ], "outputType": {"node": 10}},
        {"class": "DBSPIntegrateOperator", "id": 3, "inputs": [
            {"operator": {"node": 2}, "outputNumber": 0}
        ], "outputType": {"node": 10}},
        {"class": "DBSPSourceMultisetOperator", "id": 4, "inputs": [], "outputType": {"node": 10}},
        {"class": "DBSPSumOperator", "id": 5, "inputs": [
            {"operator": {"node": 3}, "outputNumber": 0},
            {"operator": {"node": 4}, "outputNumber": 0}
        ], "outputType": {"node": 10}},
        {"class": "DBSPSinkOperator", "id": 6, "metadata": {"system": false}, "inputs": [
            {"operator": {"node": 5}, "outputNumber": 0}
        ]}
    ]})
}

#[test]
fn compact_stream_names_preserve_annotation_order_and_the_integral_check_fails_closed() {
    let mut wire = circuit();
    wire["allOperators"][0]["annotations"] = json!([{"class": "Other"}]);
    wire["allOperators"][1]["annotations"] = json!([]);
    let named = feldera_multiset_probe::compact_names(&wire).unwrap();
    assert_eq!(
        named["allOperators"][0]["annotations"],
        json!([
            {"class": "Other"}, {"class": "CompactName", "name": "s0"}
        ])
    );
    assert_eq!(
        named["allOperators"][1]["annotations"],
        json!([{"class": "CompactName", "name": "s1"}])
    );
    assert_eq!(
        feldera_multiset_probe::require_no_integrals(&named).unwrap(),
        named
    );
    assert!(
        feldera_multiset_probe::require_no_integrals(&collection_conversions())
            .unwrap_err()
            .to_string()
            .contains("integration operator")
    );
}

#[test]
fn constant_view_analysis_preserves_sink_order_and_excludes_the_error_view() {
    let mut wire = collection_conversions();
    let operators = wire["allOperators"].as_array_mut().unwrap();
    operators[5]["viewName"] = json!({"name": "uses_input"});
    for (id, name, producer) in [
        (7, "differentiated", 2),
        (8, "error_view", 1),
        (9, "literal", 1),
    ] {
        operators.push(json!({
            "class": "DBSPSinkOperator", "id": id, "viewName": {"name": name},
            "inputs": [{"operator": {"node": producer}, "outputNumber": 0}],
        }));
    }
    assert_eq!(
        feldera_multiset_probe::constant_view_sinks(&wire).unwrap(),
        [7, 9]
    );
}

#[test]
fn empty_sources_keep_collection_kind_across_differentiate_and_integrate() {
    let wire = collection_conversions();
    let rewritten = propagate_empty_sources(&wire).unwrap();
    let operators = rewritten["allOperators"].as_array().unwrap();
    assert_eq!(operators.len(), 5);
    assert_eq!(operators[1]["class"], "DBSPIncrementalConstantOperator");
    assert_eq!(operators[2]["class"], "DBSPConstantOperator");
    assert_eq!(operators[4]["inputs"][0]["operator"]["node"], 4);
    assert_eq!(
        analyze(&rewritten).unwrap(),
        vec![
            vec![false],
            vec![false],
            vec![false],
            vec![true],
            vec![true]
        ]
    );
}

#[test]
fn redundant_integrates_reuse_the_input_or_the_delta_constant_expression() {
    let mut wire = collection_conversions();
    let canceled = remove_redundant_integrates(&wire).unwrap();
    let operators = canceled["allOperators"].as_array().unwrap();
    assert_eq!(operators.len(), 5);
    assert_eq!(operators[3]["inputs"][0]["operator"]["node"], 1);

    wire["allOperators"][0]["class"] = "DBSPIncrementalConstantOperator".into();
    wire["allOperators"][0]["annotations"] = json!([{"class":"Multiset","value":false}]);
    wire["allOperators"].as_array_mut().unwrap().remove(1);
    wire["allOperators"][1]["inputs"][0]["operator"]["node"] = 1.into();
    let snapshotted = remove_redundant_integrates(&wire).unwrap();
    let snapshot = &snapshotted["allOperators"][1];
    assert_eq!(snapshot["class"], "DBSPConstantOperator");
    assert_eq!(snapshot["function"], json!({"node":12}));
    assert_eq!(
        snapshot["annotations"],
        wire["allOperators"][0]["annotations"]
    );
}

fn map_with_cyclic_type() -> Value {
    let mut wire = circuit();
    wire["allOperators"][1]["class"] = json!("DBSPMapOperator");
    wire["allOperators"][1]["function"] = json!({
        "class": "DBSPClosureExpression", "id": 30,
        "parameters": [{"class": "DBSPParameter", "id": 31, "name": "x", "type": {
            "class": "DBSPTypeRef", "id": 20, "mayBeNull": false, "type": {"node": 20}
        }}],
        "body": {"class": "DBSPVariablePath", "id": 32, "variable": "x"}
    });
    wire
}

#[test]
fn rejects_cyclic_types_and_unknown_lineage_forms() {
    let mut wire = map_with_cyclic_type();
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("nesting exceeds"));
    wire["allOperators"][1]["function"]["parameters"][0]["type"] = json!({
        "class": "DBSPTypeTuple", "id": 20, "mayBeNull": false, "tupFields": []
    });
    wire["allOperators"][1]["function"]["body"]["class"] = json!("DBSPNewExpression");
    assert!(analyze(&wire)
        .unwrap_err()
        .to_string()
        .contains("unsupported lineage expression"));
}

#[test]
fn pruning_retains_sources_and_relocates_shared_inner_definitions() {
    let wire = json!({"class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
        {"class": "DBSPConstantOperator", "id": 1, "inputs": [], "function": {
            "class": "DBSPZSetExpression", "id": 10, "weights": [1], "data": [{
                "class": "DBSPI32Literal", "id": 11, "value": "1", "type": {
                    "class": "DBSPTypeInteger", "id": 12, "width": 32, "signed": true, "mayBeNull": false
                }
            }]
        }},
        {"class": "DBSPSourceMapOperator", "id": 2, "inputs": [], "outputType": {
            "class": "DBSPTypeZSet", "id": 13, "elementType": {"node": 12}
        }},
        {"class": "DBSPSinkOperator", "id": 3, "inputs": [{"operator": {"node": 2}, "outputNumber": 0}]},
        {"class": "DBSPSourceMultisetOperator", "id": 4, "inputs": []}
    ]});
    let pruned = remove_dead_operators(&wire).unwrap();
    assert_eq!(pruned["allOperators"].as_array().unwrap().len(), 3);
    assert_eq!(
        pruned["allOperators"][0]["outputType"]["elementType"]["class"],
        "DBSPTypeInteger"
    );
    assert_eq!(
        analyze(&pruned).unwrap(),
        vec![vec![false], vec![false], vec![true]]
    );
    feldera_sql_difftest::compare_circuits(&pruned, &remove_dead_operators(&pruned).unwrap())
        .unwrap();
    let without_unused_sources = remove_unused_operators(&wire).unwrap();
    assert_eq!(
        without_unused_sources["allOperators"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        analyze(&without_unused_sources).unwrap(),
        vec![vec![false], vec![false]]
    );
}
