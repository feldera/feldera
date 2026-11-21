use feldera_sql_ir::{Circuit, Field, NodeId};
use serde_json::{json, Value};

fn recursive() -> Value {
    json!({
        "class": "DBSPCircuit", "id": 0, "declarations": [],
        "metadata": {"class": "catalog", "id": -1, "values": [{"node": 999}]},
        "allOperators": [
            {"node": 5},
            {"class": "DBSPNestedOperator", "id": 9, "inputs": [], "internalOutputs": [
                null, {"outputNumber": 0, "operator": {"node": 5}}
            ], "allOperators": []},
            {"class": "DBSPMapOperator", "id": 2, "inputs": [
                {"outputNumber": 1, "operator": {"node": 9}},
                {"outputNumber": 0, "operator": {
                    "class": "DBSPMapOperator", "id": 5, "inputs": [
                        {"outputNumber": 0, "operator":{"node": 2}}
                    ], "outputType": {"class":"DBSPTypeInteger", "id": 5}
                }}
            ], "outputType": {"node": 5}, "annotations": [{"class":"external", "id":5}],
            "provenance": {"children": [{"node": 999}]}}
        ]
    })
}

#[test]
fn preserves_shared_fields_ordered_ports_and_definition_sites() {
    let raw = recursive();
    let circuit = Circuit::from_json(raw.clone()).unwrap();
    let operators: Vec<_> = circuit.operators().map(|(id, _)| id).collect();
    assert_eq!(
        operators,
        [NodeId::Outer(5), NodeId::Outer(9), NodeId::Outer(2)]
    );
    let node = circuit.node(NodeId::Outer(2)).unwrap();
    assert!(matches!(
        node.field("outputType"),
        Some(Field::Reference(NodeId::Inner(5)))
    ));
    assert_eq!(
        circuit.node(NodeId::Inner(5)).unwrap().class(),
        "DBSPTypeInteger"
    );
    assert_eq!(circuit.to_json(), raw);
    assert!(circuit.matches_json(&raw));
    for (path, replacement) in [
        ("/allOperators/0", json!({"class":"DBSPMapOperator","id":5})),
        ("/allOperators/2/inputs/0/outputNumber", json!(0)),
        ("/metadata/values/0", json!({"node":0})),
        (
            "/allOperators/2/outputType",
            json!({"node":5, "unexpected":null}),
        ),
    ] {
        let mut changed = raw.clone();
        *changed.pointer_mut(path).unwrap() = replacement;
        assert!(!circuit.matches_json(&changed), "{path}");
    }
}

#[test]
fn cloned_graph_publishes_only_observable_edits() {
    let raw = recursive();
    let original = Circuit::from_json(raw.clone()).unwrap();
    let mut edited = original.clone();
    assert_eq!(edited.changed_graph(&original), None);
    assert_eq!(
        Circuit::from_json(raw.clone())
            .unwrap()
            .changed_graph(&original),
        None
    );

    edited.annotations_mut(NodeId::Outer(2)).unwrap().clear();
    let mut expected = raw.clone();
    expected["allOperators"][2]["annotations"] = json!([]);
    assert_eq!(edited.changed_graph(&original), Some(expected));
    assert_eq!(original.to_json(), raw);
}

#[test]
fn live_graph_retains_shared_inner_nodes_and_keeps_new_definitions() {
    let raw = recursive();
    let original = Circuit::from_json(raw.clone()).unwrap();
    let mut changed = raw.clone();
    let root = &mut changed["allOperators"][2];
    root["function"] = json!({"class":"DBSPVariablePath","id":6,"type":{"node":5},"variable":"v"});
    let graph = Circuit::from_json(changed).unwrap();
    let reply = graph.changed_live_graph(&original).unwrap();
    assert_eq!(reply["retained_inner"], json!([5]));
    let op = &reply["graph"]["allOperators"][2];
    assert_eq!(op["function"]["id"], 6);
    assert_eq!(op["inputs"][1]["operator"]["outputType"], json!({"node":5}));
    assert_eq!(op["function"]["type"], json!({"node":5}));
    assert_eq!(reply["graph"]["metadata"], raw["metadata"]);
}

#[test]
fn live_graph_replays_shared_inner_fields_that_refer_to_an_operator() {
    let mut raw = recursive();
    raw["allOperators"][2]["inputs"][1]["operator"]["outputType"]["ports"] =
        json!([{"outputNumber": 0, "operator": {"node": 5}}]);
    let original = Circuit::from_json(raw).unwrap();
    let mut changed = original.clone();
    changed.annotations_mut(NodeId::Outer(2)).unwrap().clear();
    assert_eq!(
        changed.changed_live_graph(&original),
        Some(changed.to_json())
    );
}

#[test]
fn live_graph_detects_a_changed_descendant_with_the_same_identity() {
    let mut before = recursive();
    before["allOperators"][2]["outputType"] = json!({
        "class":"DBSPTypeRef","id":6,"type":{"node":5}
    });
    let original = Circuit::from_json(before.clone()).unwrap();
    before["allOperators"][2]["inputs"][1]["operator"]["outputType"]["mayBeNull"] = json!(true);
    let changed = Circuit::from_json(before.clone()).unwrap();
    assert_eq!(changed.changed_live_graph(&original), Some(before));
}

#[test]
fn rejects_dangling_nodes_bad_ports_and_duplicate_definitions() {
    for (path, bad) in [
        ("/allOperators/0/node", json!(555)),
        ("/allOperators/2/inputs/0/outputNumber", json!(0)),
        ("/allOperators/2/inputs/0/outputNumber", json!(-1)),
        ("/allOperators/2/outputType", json!({"node": 9})),
        ("/allOperators/2/id", json!(5)),
        (
            "/allOperators/2/inputs/1/operator/outputType/id",
            json!("5"),
        ),
    ] {
        let mut raw = recursive();
        *raw.pointer_mut(path).unwrap() = bad;
        assert!(Circuit::from_json(raw).is_err(), "{path}");
    }
}

#[test]
fn preserves_schema_key_order_embedded_by_the_classic_rust_writer() {
    let schema = r#"{"name":"readings","case_sensitive":false,"fields":[],"foreign_keys":[]}"#;
    let raw: Value = serde_json::from_str(&format!(
        r#"{{"class":"DBSPCircuit","id":0,"metadata":{schema},"declarations":[],"allOperators":[]}}"#
    )).unwrap();
    let encoded = Circuit::from_json(raw).unwrap().to_json();
    assert_eq!(encoded["metadata"].to_string(), schema);
}

#[test]
fn gen2_writer_moves_a_shared_definition_to_its_first_public_use() {
    let raw = json!({
        "class": "DBSPCircuit", "id": 0, "declarations":[], "allOperators": [{
            "class": "DBSPConstantOperator", "id": 1, "inputs": [],
            "outputType": {
                "class": "DBSPTypeTuple", "id": 2,
                "originalStruct": {"class": "DBSPTypeStruct", "id": 3, "fields": [
                    {"class":"Field", "id":4, "type":{"class":"DBSPTypeInteger", "id":5, "width":32}}
                ]},
                "tupFields":[{"node":5}, {"node":5}]
            }
        }]
    });
    let circuit = Circuit::from_json(raw.clone()).unwrap();
    let public = circuit.to_gen2_json().unwrap();
    let ty = &public["allOperators"][0]["outputType"];
    assert!(ty.get("originalStruct").is_none());
    assert_eq!(ty["tupFields"][0]["class"], "DBSPTypeInteger");
    assert_eq!(ty["tupFields"][1], json!({"node":5}));
    assert_eq!(circuit.to_json(), raw);
}

#[test]
fn provenance_uses_separate_identities_for_origins_and_relations() {
    use feldera_sql_ir::provenance::OriginKind;
    let position =
        json!({"start_line_number":1,"start_column":1,"end_line_number":1,"end_column":8});
    let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
        {"class":"DBSPConstantOperator","id":5,"inputs":[],"provenance":{
            "id":2,"kind":"RelSequence","position":position,"positions":[],"children":[
                {"id":0,"kind":"LastRel","position":position,"positions":[position],"relation":{
                    "id":0,"relId":4,"planId":9,"tableScan":false,"text":"r","digest":"d","sql":"SELECT 1","statefulSqlName":null
                }},
                {"id":1,"kind":"IntermediateRel","position":position,"positions":[],"relation":{"ref":0}}
            ]
        }},
        {"class":"DBSPMapOperator","id":6,"inputs":[{"outputNumber":0,"operator":{"node":5}}],"provenance":{"ref":2}}
    ]});
    let circuit = Circuit::from_json(graph.clone()).unwrap();
    let origins = circuit.provenance().unwrap();
    let id = origins.operator_origin(NodeId::Outer(6)).unwrap();
    assert_eq!(Some(id), origins.operator_origin(NodeId::Outer(5)));
    let OriginKind::Sequence(children) = &origins.origin(id).unwrap().kind else {
        panic!("missing chain")
    };
    let OriginKind::Final(relation) = origins.origin(children[0]).unwrap().kind else {
        panic!("missing relation")
    };
    assert_eq!(
        origins.origin(children[1]).unwrap().kind,
        OriginKind::Intermediate(relation)
    );
    assert_eq!(origins.relation(relation).unwrap().plan_id, 9);
    let mut invalid = graph;
    invalid["allOperators"][0]["provenance"]["children"][1]["relation"]["ref"] = 1.into();
    assert!(Circuit::from_json(invalid).unwrap().provenance().is_err());
}

#[test]
fn collection_builders_preserve_key_and_element_definition_sites() {
    use feldera_sql_ir::build::{CollectionType, TypeUse};
    let key = json!({"class":"DBSPTypeInteger","id":8,"width":32,"signed":true,"code":"INT32","mayBeNull":false});
    let element = json!({"node":8});
    let collection = CollectionType::IndexedZSet {
        key: TypeUse::try_from(key.clone()).unwrap(),
        element: TypeUse::try_from(element.clone()).unwrap(),
    }
    .into_wire(12, None);
    assert_eq!(
        collection,
        json!({"class":"DBSPTypeIndexedZSet","id":12,"sourceEmpty":true,
        "keyType":key,"elementType":element,"name":"IndexedWSet","code":"INDEXED_ZSET","mayBeNull":false})
    );
    assert!(TypeUse::try_from(json!({"id":1,"class":"DBSPTupleExpression"})).is_err());
}

#[test]
fn edit_checks_new_edges_before_a_node_is_attached() {
    use feldera_sql_ir::{CircuitEditor, ExpressionKind, InnerKind};
    let mut edit = CircuitEditor::new(Circuit::from_json(recursive()).unwrap());
    let missing = NodeId::Inner(4096);
    assert!(edit
        .inner(
            InnerKind::Expression(ExpressionKind::Field),
            [("expression", Field::Array(vec![Field::Definition(missing)]))]
        )
        .is_err());
    assert!(edit
        .derive_inner(NodeId::Inner(5), [("type", Field::Reference(missing))])
        .is_err());
    assert!(edit
        .set_operator_field(NodeId::Outer(2), "outputType", Field::Reference(missing))
        .is_err());
    assert!(edit
        .set_operator_field(
            NodeId::Outer(2),
            "function",
            Field::Reference(NodeId::Inner(5))
        )
        .is_err());
    assert_eq!(edit.finish().unwrap().to_json(), recursive());
}

#[test]
fn coalescing_recursive_deltas_rebuilds_nested_inputs_and_consumer_types() {
    let port = |id| json!({"operator":{"node":id},"outputNumber":0});
    let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
        {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],
         "outputType":{"class":"DBSPTypeZSet","id":10}},
        {"class":"DBSPNestedOperator","id":2,"inputs":[port(1),port(1)],
         "allOperators":[
           {"class":"DBSPDeltaOperator","id":3,"inputs":[port(1)],"outputType":{"node":10}},
           {"class":"DBSPDeltaOperator","id":4,"inputs":[port(1)],
            "outputType":{"class":"DBSPTypeZSet","id":11}}],
         "internalOutputs":[port(4)]},
        {"class":"DBSPWeightValidatorOperator","id":5,"inputs":[port(2)],"outputType":{"node":11}}
    ]});
    let result = Circuit::from_json(graph)
        .unwrap()
        .coalesce_operators(&[(NodeId::Outer(4), NodeId::Outer(3))].into())
        .unwrap();
    let nested = result.node(NodeId::Outer(2)).unwrap();
    assert_eq!(nested.inputs().count(), 1);
    assert_eq!(
        result.to_json()["allOperators"][1]["internalOutputs"][0],
        port(3)
    );
    assert_eq!(
        result
            .node(NodeId::Outer(5))
            .unwrap()
            .edge("outputType")
            .unwrap(),
        NodeId::Inner(10)
    );
}

#[test]
fn new_filter_chain_inherits_the_rebuilt_windows_indexed_stream_type() {
    let port = |id| json!({"operator":{"node":id},"outputNumber":0});
    let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
        {"class":"DBSPMapIndexOperator","id":1,"inputs":[],
         "outputType":{"class":"DBSPTypeIndexedZSet","id":10}},
        {"class":"DBSPMapIndexOperator","id":2,"inputs":[],
         "outputType":{"class":"DBSPTypeIndexedZSet","id":11}},
        {"class":"DBSPWindowOperator","id":3,"inputs":[port(2),port(1)],"outputType":{"node":11}},
        {"class":"DBSPChainOperator","id":4,"inputs":[port(3)],"kinds":["Filter","Filter"],
         "inputType":null,"outputType":{"node":11}},
        {"class":"DBSPChainOperator","id":5,"inputs":[port(3)],"kinds":["Filter","Filter"],
         "inputType":{"node":11},"outputType":{"node":11}}
    ]});
    let result = Circuit::from_json(graph)
        .unwrap()
        .coalesce_operators(&[(NodeId::Outer(2), NodeId::Outer(1))].into())
        .unwrap();
    let ty = |op, field| result.node(NodeId::Outer(op)).unwrap().edge(field).unwrap();
    assert_eq!(ty(3, "outputType"), ty(1, "outputType"));
    assert_eq!(ty(4, "inputType"), ty(3, "outputType"));
    assert_eq!(ty(4, "outputType"), ty(4, "inputType"));
    // Extending an existing chain retains the type from its original prefix.
    assert_eq!(ty(5, "inputType"), NodeId::Inner(11));
}

#[test]
fn binary_result_defines_a_shared_operand_type_before_java_reads_it() {
    use feldera_sql_ir::{BinaryOp, CircuitEditor};

    let graph = Circuit::from_json(json!({
        "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[{
            "class":"DBSPConstantOperator","id":1,"inputs":[],"function":{
                "class":"DBSPVariablePath","id":2,"variable":"offset","type":{
                    "class":"DBSPTypeInteger","id":3,"code":"INT32","width":32,
                    "signed":true,"mayBeNull":false
                }
            }
        }]
    }))
    .unwrap();
    let mut editor = CircuitEditor::new(graph);
    let operand = editor.expression(NodeId::Inner(2)).unwrap();
    let ty = editor.expression_type(operand).unwrap();
    let binary = editor
        .binary_expression(operand.into(), ty, BinaryOp::Sub, operand, operand)
        .unwrap();
    editor
        .set_operator_field(
            NodeId::Outer(1),
            "function",
            Field::Reference(binary.into()),
        )
        .unwrap();
    let wire = editor.finish().unwrap().to_json();
    let binary = &wire["allOperators"][0]["function"];
    assert_eq!(binary["type"]["class"], "DBSPTypeInteger");
    assert_eq!(binary["left"]["type"]["node"], binary["type"]["id"]);
    assert_eq!(binary["right"]["node"], binary["left"]["id"]);
}

#[test]
fn edited_structured_data_obeys_wire_identity_and_depth_rules() {
    use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind, InnerKind};

    fn rebuild(data: Value) -> Result<Circuit, Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],
            "allOperators":[{"class":"DBSPConstantOperator","id":1,"inputs":[]}]
        }))?;
        let mut edit = CircuitEditor::new(graph);
        let expr = edit.inner(
            InnerKind::Expression(ExpressionKind::VariablePath),
            [
                ("position", Field::Data(data)),
                ("variable", Field::Data("x".into())),
            ],
        )?;
        edit.set_operator_field(NodeId::Outer(1), "function", Field::Reference(expr))?;
        edit.finish()
    }

    let structured = rebuild(json!({"range":[1,2]})).unwrap();
    let function = structured
        .node(NodeId::Outer(1))
        .unwrap()
        .edge("function")
        .unwrap();
    assert!(matches!(
        structured.node(function).unwrap().field("position"),
        Some(Field::Object(_))
    ));
    let decoded = Circuit::from_json(structured.to_json()).unwrap();
    assert!(structured.matches_json(&decoded.to_json()));
    let NodeId::Inner(id) = function else {
        panic!("expected an expression")
    };

    // A supplied data object cannot introduce another definition of its owner.
    assert!(rebuild(json!({"class":"DBSPVariablePath", "id":id})).is_err());
    assert!(rebuild(json!({"node":999})).is_err());
    let deep = (0..129).fold(Value::Null, |child, _| Value::Array(vec![child]));
    assert!(rebuild(deep).is_err());
}
