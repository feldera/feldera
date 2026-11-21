//! Execute the frozen stateful outer joins from their exported graphs.

use dbsp::{
    DBData, OrdZSet, RootCircuit, Runtime, Stream,
    utils::{Tup2, Tup3, Tup7},
};
use feldera_native_dbsp_probe::{
    FullJoinOutputRow, JoinOutputRow, JoinProjectedRow, build_full_join, build_left_join,
    full_join_projected, left_join_projected,
};
use feldera_native_probe::{ArchiveInteger, Error};
use feldera_sql_difftest::{compare_circuits, parse_json};
use feldera_sqllib::{F64, SqlString};
use serde_json::Value;
use std::{env, fs, sync::Arc};

mod circuit_fixture;
mod integer_corpus;
#[path = "circuit_fixture/numeric_input.rs"]
mod numeric_input;
use circuit_fixture::{bag, body, class, field, items, links, one};
use integer_corpus::input_row;
use numeric_input::NumericBatch;

fn index_inputs(project: &Value, left: &Value, right: &Value) -> usize {
    let ([parameter], project_body) = body(project) else {
        panic!("expected one projection parameter")
    };
    let projected: Vec<_> = items(&class(project_body, "DBSPTupleExpression")["fields"])
        .iter()
        .map(|column| field(column, parameter))
        .collect();
    assert_eq!(projected, [0, 2, 4]);

    let ([parameter], left_body) = body(left) else {
        panic!("expected one left-index parameter")
    };
    let [left_key, left_value] = items(&class(left_body, "DBSPRawTupleExpression")["fields"])
    else {
        panic!("expected left key and value")
    };
    assert_eq!(
        field(
            one(&class(left_key, "DBSPTupleExpression")["fields"]),
            parameter
        ),
        0
    );
    assert_eq!(
        field(
            one(&class(left_value, "DBSPTupleExpression")["fields"]),
            parameter
        ),
        1
    );

    let ([parameter], right_body) = body(right) else {
        panic!("expected one right-index parameter")
    };
    let block = class(right_body, "DBSPBlockExpression");
    let [guard, binding] = items(&block["contents"]) else {
        panic!("expected a NULL guard and an index value")
    };
    let guard = class(
        &class(guard, "DBSPExpressionStatement")["expression"],
        "DBSPIfExpression",
    );
    let condition = class(&guard["condition"], "DBSPIsNullExpression");
    let key_column = field(&condition["expression"], parameter);
    let skip = class(&guard["positive"], "DBSPReturnExpression");
    assert!(class(&skip["argument"], "DBSPRawTupleExpression")["fields"].is_null());
    let binding = class(binding, "DBSPLetStatement");
    let initializer = class(&binding["initializer"], "DBSPRawTupleExpression");
    let [right_key, right_value] = items(&initializer["fields"]) else {
        panic!("expected right key and value")
    };
    let unwrap = class(
        one(&class(right_key, "DBSPTupleExpression")["fields"]),
        "DBSPHandleErrorExpression",
    );
    assert_eq!(
        field(
            &class(&unwrap["source"], "DBSPUnwrapExpression")["expression"],
            parameter
        ),
        key_column
    );
    assert_eq!(
        field(
            one(&class(right_value, "DBSPTupleExpression")["fields"]),
            parameter
        ),
        1
    );
    let some = class(&block["lastExpression"], "DBSPSomeExpression");
    assert_eq!(
        class(&some["expression"], "DBSPVariablePath")["variable"],
        binding["variable"]
    );

    key_column
}

fn key_column(circuit: &Value) -> usize {
    compare_circuits(circuit, circuit).expect("strict exported circuit");
    let [source, project, left, right, join, output, constant, error] =
        items(&circuit["allOperators"])
    else {
        panic!("unexpected frozen left-join graph")
    };
    for (node, name) in [
        (source, "DBSPSourceMultisetOperator"),
        (project, "DBSPMapOperator"),
        (left, "DBSPMapIndexOperator"),
        (right, "DBSPFlatMapIndexOperator"),
        (join, "DBSPLeftJoinOperator"),
        (output, "DBSPSinkOperator"),
        (constant, "DBSPIncrementalConstantOperator"),
        (error, "DBSPSinkOperator"),
    ] {
        class(node, name);
    }
    links(source, &[]);
    links(project, &[source]);
    links(left, &[project]);
    links(right, &[project]);
    links(join, &[left, right]);
    links(output, &[join]);
    links(constant, &[]);
    links(error, &[constant]);
    assert_eq!(source["tableName"]["name"], "t");
    assert_eq!(output["viewName"]["name"], "v");
    assert_eq!(error["viewName"]["name"], "error_view");
    let empty = class(&constant["function"], "DBSPZSetExpression");
    assert!(items(&empty["data"]).is_empty() && items(&empty["weights"]).is_empty());

    let key_column = index_inputs(project, left, right);

    let ([_, left, right], join_body) = body(join) else {
        panic!("expected join key and two inputs")
    };
    let [left_value, right_value] = items(&class(join_body, "DBSPTupleExpression")["fields"])
    else {
        panic!("expected two output fields")
    };
    assert_eq!(field(left_value, left), 0);
    let extension = class(right_value, "DBSPIfExpression");
    let null = class(&extension["condition"], "DBSPIsNullExpression");
    let dereference = class(&null["expression"], "DBSPDerefExpression");
    assert_eq!(
        class(&dereference["expression"], "DBSPVariablePath")["variable"],
        right["name"]
    );
    assert!(class(&extension["positive"], "DBSPBoolLiteral")["value"].is_null());
    assert_eq!(field(&extension["negative"], right), 0);
    key_column
}

fn output_row(fields: &[Value]) -> JoinOutputRow {
    let [left, right] = fields else {
        panic!("expected two output fields")
    };
    Tup2(
        class(left, "DBSPBoolLiteral")["value"]
            .as_bool()
            .expect("required BOOLEAN"),
        class(right, "DBSPBoolLiteral")["value"].as_bool(),
    )
}

fn full_output_row(fields: &[Value]) -> FullJoinOutputRow {
    let [left, right] = fields else {
        panic!("expected two output fields")
    };
    Tup2(
        class(left, "DBSPBoolLiteral")["value"].as_bool(),
        class(right, "DBSPBoolLiteral")["value"].as_bool(),
    )
}

fn verify_owned_join<T: DBData>(
    case: &Value,
    key: usize,
    build: impl FnOnce(
        &Stream<RootCircuit, OrdZSet<JoinProjectedRow>>,
        usize,
    ) -> Result<Stream<RootCircuit, OrdZSet<T>>, Error>
    + Clone
    + Send
    + 'static,
    expected_row: impl Fn(&[Value]) -> T,
) {
    let (mut runtime, (input, output)) = Runtime::init_circuit(1, move |circuit| {
        let (projected, input) = numeric_input::source(circuit, |[id, flag, reference]| {
            Tup3(
                i32::try_from(id.unwrap()).unwrap(),
                flag.unwrap() == 1,
                reference.map(|value| i32::try_from(value).unwrap()),
            )
        });
        Ok((input, build(&projected, key)?.output()))
    })
    .unwrap();
    let mut owners = Vec::new();
    for step in items(&case["commands"]) {
        assert_eq!(step["kind"], "step");
        let rows = bag(one(&step["inputs"]), "T", input_row);
        let batch = Arc::new(NumericBatch::new(
            [
                (ArchiveInteger::I32, false),
                (ArchiveInteger::Bool, false),
                (ArchiveInteger::I32, true),
            ],
            rows.into_iter().map(|Tup2(row, weight)| {
                (
                    [
                        Some(row.0.into()),
                        Some(row.2.into()),
                        row.4.map(i128::from),
                    ],
                    weight,
                )
            }),
        ));
        owners.push(batch.owner());
        input.set_for_worker(0, Some(batch));
        runtime.transaction().unwrap();
        let expected = bag(one(&step["outputs"]), "V", &expected_row);
        assert_eq!(output.consolidate(), OrdZSet::from_keys((), expected));
    }
    runtime.kill().unwrap();
    drop(input);
    assert!(owners.iter().all(|owner| owner.upgrade().is_none()));
}

#[test]
#[ignore = "requires P0_LEFT_JOIN_CASE from the frozen native corpus archive"]
fn frozen_left_join_executes_from_ir() {
    let path = env::var_os("P0_LEFT_JOIN_CASE").expect("frozen corpus case path");
    let case = parse_json(&fs::read(path).unwrap()).expect("strict JSON case");
    assert_eq!(
        case["source_test"],
        "org.dbsp.sqlCompiler.compiler.sql.simple.OptimizedIncrementalTests#leftOuterJoinTest"
    );
    assert_eq!(case["case_index"], 0);
    assert_eq!(case["step_semantics"], "delta");
    let key = key_column(&case["circuit"]);
    verify_owned_join(&case, key, left_join_projected, output_row);
    let (mut runtime, ports) =
        Runtime::init_circuit(1, move |circuit| Ok(build_left_join(circuit, key)?))
            .expect("build native left join");
    let steps = items(&case["commands"]);
    assert_eq!(steps.len(), 3);
    for step in steps {
        assert_eq!(step["kind"], "step");
        let mut input = bag(one(&step["inputs"]), "T", input_row);
        let output = bag(one(&step["outputs"]), "V", output_row);
        ports.input.append(&mut input);
        runtime.transaction().expect("execute signed input");
        assert_eq!(ports.output.consolidate(), OrdZSet::from_keys((), output));
        assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    }

    // Exercise a matched key and its transition back to a left-only output.
    let left = Tup7(10, F64::new(0.0), true, SqlString::new(), None, None, None);
    let right = Tup7(
        20,
        F64::new(0.0),
        false,
        SqlString::new(),
        Some(10),
        None,
        None,
    );
    ports
        .input
        .append(&mut vec![Tup2(left, 1), Tup2(right.clone(), 1)]);
    runtime.transaction().unwrap();
    assert_eq!(
        ports.output.consolidate(),
        OrdZSet::from_keys(
            (),
            vec![Tup2(Tup2(true, Some(false)), 1), Tup2(Tup2(false, None), 1),]
        )
    );
    ports.input.append(&mut vec![Tup2(right, -1)]);
    runtime.transaction().unwrap();
    assert_eq!(
        ports.output.consolidate(),
        OrdZSet::from_keys(
            (),
            vec![
                Tup2(Tup2(true, Some(false)), -1),
                Tup2(Tup2(false, None), -1),
                Tup2(Tup2(true, None), 1),
            ]
        )
    );
}

fn padded_index_value(operator: &Value, present: usize) {
    let ([parameter], body) = body(operator) else {
        panic!("expected indexed value parameter")
    };
    let fields = items(&class(body, "DBSPTupleExpression")["fields"]);
    assert_eq!(fields.len(), 2);
    assert!(class(&fields[1 - present], "DBSPBoolLiteral")["value"].is_null());
    let some = class(&fields[present], "DBSPSomeExpression");
    let payload = class(&some["expression"], "DBSPFieldExpression");
    assert_eq!(payload["fieldNo"], 0);
    let deref = class(&payload["expression"], "DBSPDerefExpression");
    let indexed_value = class(&deref["expression"], "DBSPFieldExpression");
    assert_eq!(indexed_value["fieldNo"], 1);
    assert_eq!(
        class(&indexed_value["expression"], "DBSPVariablePath")["variable"],
        parameter["name"]
    );
}

fn check_null_branch(operator: &Value, key: usize) {
    let ([parameter], body) = body(operator) else {
        panic!("expected NULL-key branch parameter")
    };
    let block = class(body, "DBSPBlockExpression");
    let [guard, bind] = items(&block["contents"]) else {
        panic!("expected NULL-key guard and result")
    };
    let guard = class(
        &class(guard, "DBSPExpressionStatement")["expression"],
        "DBSPIfExpression",
    );
    let negation = class(&guard["condition"], "DBSPUnaryExpression");
    assert_eq!(negation["opcode"], "NOT");
    assert_eq!(
        field(
            &class(&negation["source"], "DBSPIsNullExpression")["expression"],
            parameter
        ),
        key
    );
    let discard = class(&guard["positive"], "DBSPReturnExpression");
    assert!(class(&discard["argument"], "DBSPTupleExpression")["fields"].is_null());
    let bind = class(bind, "DBSPLetStatement");
    let output = class(&bind["initializer"], "DBSPTupleExpression");
    let [left, right] = items(&output["fields"]) else {
        panic!("expected padded NULL-key row")
    };
    assert!(class(left, "DBSPBoolLiteral")["value"].is_null());
    assert_eq!(
        field(&class(right, "DBSPSomeExpression")["expression"], parameter),
        1
    );
    let keep = class(&block["lastExpression"], "DBSPSomeExpression");
    assert_eq!(
        class(&keep["expression"], "DBSPVariablePath")["variable"],
        bind["variable"]
    );
}

fn full_join_key(circuit: &Value) -> usize {
    compare_circuits(circuit, circuit).expect("strict exported circuit");
    let nodes = items(&circuit["allOperators"]);
    let expected: &[(&str, &[usize])] = &[
        ("DBSPSourceMultisetOperator", &[]),
        ("DBSPMapOperator", &[0]),
        ("DBSPMapIndexOperator", &[1]),
        ("DBSPFlatMapIndexOperator", &[1]),
        ("DBSPJoinOperator", &[2, 3]),
        ("DBSPAntiJoinOperator", &[2, 3]),
        ("DBSPMapOperator", &[5]),
        ("DBSPAntiJoinOperator", &[3, 2]),
        ("DBSPMapOperator", &[7]),
        ("DBSPFlatMapOperator", &[1]),
        ("DBSPSumOperator", &[4, 6, 8, 9]),
        ("DBSPSinkOperator", &[10]),
        ("DBSPIncrementalConstantOperator", &[]),
        ("DBSPSinkOperator", &[12]),
    ];
    assert_eq!(nodes.len(), expected.len());
    for (node, &(name, inputs)) in nodes.iter().zip(expected) {
        class(node, name);
        links(
            node,
            &inputs
                .iter()
                .map(|&index| &nodes[index])
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(nodes[0]["tableName"]["name"], "t");
    assert_eq!(nodes[11]["viewName"]["name"], "v");
    assert_eq!(nodes[13]["viewName"]["name"], "error_view");
    let constant = class(&nodes[12]["function"], "DBSPZSetExpression");
    assert!(items(&constant["data"]).is_empty() && items(&constant["weights"]).is_empty());
    let key = index_inputs(&nodes[1], &nodes[2], &nodes[3]);
    let ([_, left, right], join) = body(&nodes[4]) else {
        panic!("expected join parameters")
    };
    let [first, second] = items(&class(join, "DBSPTupleExpression")["fields"]) else {
        panic!("expected two matched fields")
    };
    assert_eq!(
        field(&class(first, "DBSPSomeExpression")["expression"], left),
        0
    );
    assert_eq!(
        field(&class(second, "DBSPSomeExpression")["expression"], right),
        0
    );
    padded_index_value(&nodes[6], 0);
    padded_index_value(&nodes[8], 1);
    check_null_branch(&nodes[9], key);
    key
}

#[test]
#[ignore = "requires P0_FULL_JOIN_CASE from the frozen native corpus archive"]
fn frozen_full_join_executes_from_ir() {
    let path = env::var_os("P0_FULL_JOIN_CASE").expect("frozen corpus case path");
    let case = parse_json(&fs::read(path).unwrap()).expect("strict JSON case");
    assert_eq!(
        case["source_test"],
        "org.dbsp.sqlCompiler.compiler.sql.simple.OptimizedIncrementalTests#fullOuterJoinTest"
    );
    assert_eq!(case["case_index"], 0);
    assert_eq!(case["step_semantics"], "delta");
    let key = full_join_key(&case["circuit"]);
    verify_owned_join(&case, key, full_join_projected, full_output_row);
    let (mut runtime, ports) =
        Runtime::init_circuit(1, move |circuit| Ok(build_full_join(circuit, key)?))
            .expect("build native full outer join");
    let steps = items(&case["commands"]);
    assert_eq!(steps.len(), 3);
    for step in steps {
        assert_eq!(step["kind"], "step");
        let mut input = bag(one(&step["inputs"]), "T", input_row);
        let output = bag(one(&step["outputs"]), "V", full_output_row);
        ports.input.append(&mut input);
        runtime.transaction().expect("execute signed input");
        assert_eq!(ports.output.consolidate(), OrdZSet::from_keys((), output));
        assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    }

    let left = Tup7(10, F64::new(0.0), true, SqlString::new(), None, None, None);
    let right = Tup7(
        20,
        F64::new(0.0),
        false,
        SqlString::new(),
        Some(10),
        None,
        None,
    );
    ports
        .input
        .append(&mut vec![Tup2(left, 1), Tup2(right.clone(), 1)]);
    runtime.transaction().unwrap();
    assert_eq!(
        ports.output.consolidate(),
        OrdZSet::from_keys(
            (),
            vec![
                Tup2(Tup2(Some(true), Some(false)), 1),
                Tup2(Tup2(Some(false), None), 1),
                Tup2(Tup2(None, Some(true)), 1),
            ]
        )
    );
    ports.input.append(&mut vec![Tup2(right, -1)]);
    runtime.transaction().unwrap();
    assert_eq!(
        ports.output.consolidate(),
        OrdZSet::from_keys(
            (),
            vec![
                Tup2(Tup2(Some(true), Some(false)), -1),
                Tup2(Tup2(Some(false), None), -1),
                Tup2(Tup2(Some(true), None), 1),
            ]
        )
    );
}
