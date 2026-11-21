use feldera_sql_difftest::compare_circuits;
use feldera_sql_ir::{Circuit, CircuitEditor, OperatorKind, OutputPort};
use feldera_sql_opt::limiters::{minimum_bound, Bound};
use serde_json::Value;

#[test]
fn minimum_of_projected_bounds_matches_java() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/limiter_minimum.json")).unwrap();
    for (count, oracle) in cases.as_object().unwrap() {
        let circuit = Circuit::from_json(oracle["before"].clone()).unwrap();
        let body = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let bounds = edit
            .circuit()
            .operators()
            .filter(|(_, op)| op.operator_kind() == Some(OperatorKind::Waterline))
            .map(|(id, op)| Bound {
                port: OutputPort {
                    operator: id,
                    number: 0,
                },
                ty: edit.type_id(op.edge("outputType").unwrap()).unwrap(),
            })
            .collect::<Vec<_>>();
        let mut next = oracle["next"].as_u64().unwrap();
        minimum_bound(&mut edit, body, &bounds, &mut next).unwrap();
        assert_eq!(next, oracle["after_next"]);
        compare_circuits(&oracle["after"], &edit.finish().unwrap().to_json())
            .unwrap_or_else(|error| panic!("{count} bounds: {error}"));
    }
}

#[test]
fn window_consumes_deltas_and_restores_the_input_kind() {
    use feldera_sql_opt::limiters::{window, Stream, StreamKind, WindowBounds};
    let examples: Value =
        serde_json::from_str(include_str!("fixtures/limiter_window.json")).unwrap();
    for (name, case) in examples.as_object().unwrap() {
        let circuit = Circuit::from_json(case["before"].clone()).unwrap();
        let body = circuit.root_id();
        let ops = circuit.root().edges("allOperators").unwrap();
        let mut edit = CircuitEditor::new(circuit);
        let ty = |id| {
            edit.type_id(edit.node(id).unwrap().edge("outputType").unwrap())
                .unwrap()
        };
        let collection = name == "collection";
        let kind = if collection {
            StreamKind::Collection
        } else {
            StreamKind::Delta
        };
        let port = |operator| OutputPort {
            operator,
            number: 0,
        };
        let data = Stream {
            port: port(ops[0]),
            ty: ty(ops[0]),
            kind,
        };
        let control = Bound {
            port: port(ops[1]),
            ty: ty(ops[1]),
        };
        let (delta, output) = window(
            &mut edit,
            body,
            ops[0],
            data,
            control,
            WindowBounds {
                lower_inclusive: collection,
                upper_inclusive: !collection,
                lower_unbounded: collection,
            },
        )
        .unwrap();
        assert_eq!(delta.kind, StreamKind::Delta);
        assert_eq!(output.kind, kind);
        // Corresponding operator positions are stable even when allocator ids differ.
        let graph = edit.finish().unwrap();
        let expected = Circuit::from_json(case["after"].clone()).unwrap();
        let expected_ops = expected.root().edges("allOperators").unwrap();
        let actual_ops = graph.root().edges("allOperators").unwrap();
        for (key, actual) in [("input", delta), ("output", output)] {
            let index = expected_ops
                .iter()
                .position(|&n| n == feldera_sql_ir::NodeId::Outer(case[key].as_u64().unwrap()))
                .unwrap();
            assert_eq!(actual.port, port(actual_ops[index]));
        }
        compare_circuits(&case["after"], &graph.to_json()).unwrap();
    }
}

#[test]
fn rejects_limiter_facts_from_a_different_graph() {
    use feldera_sql_opt::limiters::insert_limiters;
    use serde_json::json;
    let circuit = Circuit::from_json(json!({
        "id": 10, "class": "DBSPCircuit", "declarations":[], "allOperators": [
            {"id":11,"class":"DBSPSourceMultisetOperator","inputs":[]}
        ]
    }))
    .unwrap();
    let state = serde_json::from_value(json!({"operators": {"10": {
        "position":{"start_line_number":0,"start_column":0,"end_line_number":0,"end_column":0},
        "reachable_from_error":false,"input_kinds":[]
    }}, "gen2":false,"next_generated_name":0,"singletons":[]}))
    .unwrap();
    let error = insert_limiters(circuit, state).expect_err("misaligned limiter facts");
    assert_eq!(error.to_string(), "missing limiter facts for Outer(11)");
}
