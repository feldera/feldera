//! Cross-check complete expression graphs exported by ExpressionBuilderOracle,
//! including sharing between a builder's input and output.

use feldera_sql_difftest::compare_circuits;
use feldera_sql_ir::{BinaryOp, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId};
use serde_json::{json, Value};

fn check(
    examples: &str,
    build: impl Fn(&mut CircuitEditor, NodeId, &str, &[NodeId]) -> Result<ExpressionId, Error>,
) {
    let cases: Value = serde_json::from_str(examples).unwrap();
    let mut differences = Vec::new();
    for (name, example) in cases.as_object().unwrap() {
        let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],
            "allOperators":[{"class":"DBSPMapOperator","id":1,"inputs":[],"function":example["before"]}]});
        let mut edit = CircuitEditor::new(Circuit::from_json(graph.clone()).unwrap());
        let op = NodeId::Outer(1);
        let wrapper = edit.node(op).unwrap().edge("function").unwrap();
        let mut fields = edit.node(wrapper).unwrap().edges("fields").unwrap();

        let origin = if name.starts_with("positioned_tuple/") {
            fields[0]
        } else {
            wrapper
        };
        let built = build(&mut edit, origin, name, &fields);
        if example.get("error").is_some() {
            let error = built.expect_err("Java rejected this input");
            let reason = match example["error"].as_str().unwrap() {
                "UnimplementedException" => "binary coercion is undefined for ADD_SATURATING",
                "InternalCompilerError" => "tuple expression field type mismatch",
                other => panic!("unhandled Java failure class {other}"),
            };
            assert_eq!(error.to_string(), reason, "{name}");
            continue;
        }
        let result = built.unwrap_or_else(|e| panic!("{name}: {e}"));
        fields.push(result.into());
        let wrapper_type = edit.node(wrapper).unwrap().edge("type").unwrap();
        let types = fields
            .iter()
            .map(|f| Field::Reference(edit.node(*f).unwrap().edge("type").unwrap()))
            .collect();
        let ty = edit
            .derive_inner(wrapper_type, [("tupFields", Field::Array(types))])
            .unwrap();
        let fields = Field::Array(fields.into_iter().map(Field::Reference).collect());
        let output = edit
            .derive_inner(
                wrapper,
                [("fields", fields), ("type", Field::Reference(ty))],
            )
            .unwrap();
        edit.set_operator_field(op, "function", Field::Reference(output))
            .unwrap();
        let built = edit.finish().unwrap().to_json();
        let mut expected = graph;
        expected["allOperators"][0]["function"] = example["after"].clone();
        if let Err(error) = compare_circuits(&expected, &built) {
            differences.push(format!("{name}: {error}"));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}

#[test]
fn expands_tuples_like_java() {
    check(
        include_str!("fixtures/expand_tuple.json"),
        |edit, origin, _, args| edit.expand_tuple(origin, edit.expression(args[0])?),
    );
}

#[test]
fn combines_binary_operands_like_java() {
    check(
        include_str!("fixtures/binary_expressions.json"),
        |edit, origin, name, args| {
            let op = BinaryOp::from_wire(name.split('/').nth(1).unwrap()).unwrap();
            let operands = edit
                .node(args[0])?
                .edges("fields")?
                .into_iter()
                .map(|n| edit.expression(n))
                .collect::<Result<Vec<_>, _>>()?;
            let ty = edit.type_id(edit.node(args[1])?.edge("type")?)?;
            edit.binary_expressions(origin, ty, op, &operands)
        },
    );
}

#[test]
fn combines_single_binary_and_star_aggregates_like_java() {
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/combine_aggregates.json")).unwrap();
    for (count, case) in cases.as_object().unwrap() {
        let circuit = Circuit::from_json(case["before"].clone()).unwrap();
        let body = circuit.root_id();
        let inputs = circuit.root().edges("allOperators").unwrap();
        let mut edit = CircuitEditor::new(circuit);
        let collection = edit.node(inputs[0]).unwrap().edge("outputType").unwrap();
        let key = edit
            .type_id(edit.node(collection).unwrap().edge("keyType").unwrap())
            .unwrap();
        let mut next = case["next_generated_name"].as_u64().unwrap();
        let output = edit
            .combine_aggregate_list(
                body,
                feldera_sql_ir::OperatorSite {
                    operator: inputs[0],
                    position: if count.ends_with("/observation") {
                        Some(serde_json::from_value(case["site_position"].clone()).unwrap())
                    } else {
                        None
                    },
                },
                key,
                &inputs,
                &mut next,
            )
            .unwrap();
        assert_eq!(next, case["next_generated_name_after"]);
        let result = edit.finish().unwrap();
        assert_eq!(
            Some(&output),
            result.root().edges("allOperators").unwrap().last()
        );
        compare_circuits(&case["after"], &result.to_json())
            .unwrap_or_else(|error| panic!("{count}: {error}"));
    }
}

#[test]
fn an_exhausted_aggregate_schedule_preserves_the_circuit_and_name_allocator() {
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/combine_aggregates.json")).unwrap();
    let mut graph = cases["3/checkpoint"]["before"].clone();
    graph["id"] = u64::MAX.into();
    let circuit = Circuit::from_json(graph.clone()).unwrap();
    let body = circuit.root_id();
    let inputs = circuit.root().edges("allOperators").unwrap();
    let mut edit = CircuitEditor::new(circuit);
    let indexed = edit.node(inputs[0]).unwrap().edge("outputType").unwrap();
    let key = edit
        .type_id(edit.node(indexed).unwrap().edge("keyType").unwrap())
        .unwrap();
    let site = feldera_sql_ir::OperatorSite {
        operator: inputs[0],
        position: None,
    };
    let mut next = 73;
    assert_eq!(
        edit.combine_aggregate_list(body, site, key, &inputs, &mut next)
            .unwrap_err()
            .to_string(),
        "outer node identity exhausted"
    );
    assert_eq!(next, 73);
    assert_eq!(edit.finish().unwrap().to_json(), graph);
}

#[test]
fn grouping_keys_compare_nested_types_independently_of_arena_identity() {
    use feldera_sql_ir::{InnerKind, TypeKind};
    let cases: Value = serde_json::from_str(include_str!("fixtures/expand_tuple.json")).unwrap();
    let expr = &cases["null_struct_field/checkpoint"]["before"];
    let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[{"class":"DBSPMapOperator","id":1,"inputs":[],"function":expr}]});
    let mut edit = CircuitEditor::new(Circuit::from_json(graph).unwrap());
    let wrapper = edit
        .node(NodeId::Outer(1))
        .unwrap()
        .edge("function")
        .unwrap();
    let tuple = edit.node(wrapper).unwrap().edges("fields").unwrap()[0];
    let ty = edit.node(tuple).unwrap().edge("type").unwrap();
    let named = edit.node(ty).unwrap().edges("tupFields").unwrap()[0];
    let duplicate = edit.derive_inner(named, []).unwrap();
    let first = edit.reference_type(edit.type_id(named).unwrap()).unwrap();
    let second = edit
        .reference_type(edit.type_id(duplicate).unwrap())
        .unwrap();
    assert!(edit.same_type(first, second).unwrap());
    let user = |edit: &mut CircuitEditor, param| {
        edit.empty_origin_node(
            named,
            InnerKind::Type(TypeKind::User),
            [
                ("code", Field::Data("USER".into())),
                ("name", Field::Data("Buffer".into())),
                ("mayBeNull", Field::Data(false.into())),
                ("typeArgs", Field::Array(vec![Field::Reference(param)])),
            ],
        )
        .unwrap()
    };
    let a = user(&mut edit, first.into());
    let b = user(&mut edit, second.into());
    assert!(edit
        .same_type(edit.type_id(a).unwrap(), edit.type_id(b).unwrap())
        .unwrap());
    let different = edit
        .derive_inner(
            duplicate,
            [("hashName", Field::Data("another_struct".into()))],
        )
        .unwrap();
    assert!(!edit
        .same_type(
            edit.type_id(named).unwrap(),
            edit.type_id(different).unwrap()
        )
        .unwrap());
}
