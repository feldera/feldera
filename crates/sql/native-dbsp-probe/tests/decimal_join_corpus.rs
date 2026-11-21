//! Replay both Postgres DECIMAL arithmetic graphs, including positive mismatches.

use dbsp::{
    OrdZSet, Runtime,
    circuit::{CircuitConfig, StepSize},
    utils::{Tup2, Tup3, Tup4},
};
use feldera_native_dbsp_probe::{
    DecimalInputPort, DecimalOperation, ExpectedDecimalRow, NumericRow, build_decimal_join,
    decimal_join_projected,
};
use feldera_native_probe::ArchiveInteger;
use feldera_sql_difftest::{compare_circuits, parse_json};
use feldera_sqllib::integer_to_decimalN;
use serde_json::Value;
use std::{env, fs, sync::Arc};

mod circuit_fixture;
#[path = "circuit_fixture/numeric_input.rs"]
mod numeric_input;
use circuit_fixture::{bag, body, class, field, items, links, number, one};
use numeric_input::NumericBatch;

const INPUT_NAMES: [&str; 9] = [
    "num_data",
    "num_exp_add",
    "num_exp_sub",
    "num_exp_div",
    "num_exp_mul",
    "num_exp_sqrt",
    "num_exp_ln",
    "num_exp_log10",
    "num_exp_power_10_ln",
];

fn plan(case: &Value) -> (DecimalOperation, usize) {
    let circuit = &case["circuit"];
    compare_circuits(circuit, circuit).expect("strict exported circuit");
    let operators = items(&circuit["allOperators"]);
    assert_eq!(operators.len(), 18);
    for (source, name) in operators[..9].iter().zip(INPUT_NAMES) {
        class(source, "DBSPSourceMultisetOperator");
        links(source, &[]);
        assert_eq!(source["tableName"]["name"], name);
    }
    let right_input = &one(&operators[12]["inputs"])["operator"]["node"];
    let expected_source = operators[..9]
        .iter()
        .position(|source| source["id"] == *right_input)
        .expect("reference source index");
    for (index, name, inputs) in [
        (9, "DBSPMapIndexOperator", &[0][..]),
        (10, "DBSPStreamJoinOperator", &[9, 9]),
        (11, "DBSPFlatMapIndexOperator", &[10]),
        (12, "DBSPFlatMapIndexOperator", &[expected_source]),
        (13, "DBSPStreamJoinOperator", &[11, 12]),
        (14, "DBSPFlatMapOperator", &[13]),
        (15, "DBSPSinkOperator", &[14]),
        (16, "DBSPConstantOperator", &[]),
        (17, "DBSPSinkOperator", &[16]),
    ] {
        let sources: Vec<_> = inputs.iter().map(|&idx| &operators[idx]).collect();
        links(class(&operators[index], name), &sources);
    }
    assert_eq!(operators[15]["viewName"]["name"], "e");
    assert_eq!(operators[17]["viewName"]["name"], "error_view");
    let empty = class(&operators[16]["function"], "DBSPZSetExpression");
    assert!(items(&empty["data"]).is_empty() && items(&empty["weights"]).is_empty());

    let ([pair], index_expression) = body(&operators[11]) else {
        panic!("expected one arithmetic pair");
    };
    let [guard, expression] = items(&class(index_expression, "DBSPBlockExpression")["contents"])
    else {
        panic!("expected NULL guard and key/value binding");
    };
    let guard = class(
        &class(guard, "DBSPExpressionStatement")["expression"],
        "DBSPIfExpression",
    );
    let both_keys = class(&guard["condition"], "DBSPBinaryExpression");
    assert_eq!(both_keys["opcode"], "OR");
    for (branch, index) in [("left", 0), ("right", 2)] {
        let null = class(&both_keys[branch], "DBSPIsNullExpression");
        assert_eq!(field(&null["expression"], pair), index);
    }
    let indexed = class(
        &class(expression, "DBSPLetStatement")["initializer"],
        "DBSPRawTupleExpression",
    );
    let [_, result] = items(&indexed["fields"]) else {
        panic!("expected index key and arithmetic result");
    };
    let expression = one(&class(result, "DBSPTupleExpression")["fields"]);
    let (operation, arithmetic) = if expression["class"] == "DBSPHandleErrorExpression" {
        let cast = class(&expression["source"], "DBSPCastExpression");
        assert_eq!(cast["safe"], "RustCast");
        assert_eq!(cast["type"]["typeArgs"][0]["precision"], 38);
        assert_eq!(cast["type"]["typeArgs"][0]["scale"], 10);
        (DecimalOperation::Multiply, &cast["source"])
    } else {
        (DecimalOperation::Add, expression)
    };
    let arithmetic = class(arithmetic, "DBSPBinaryExpression");
    assert_eq!(field(&arithmetic["left"], pair), 1);
    assert_eq!(field(&arithmetic["right"], pair), 3);
    assert_eq!(arithmetic["type"]["precision"], 38);
    match operation {
        DecimalOperation::Add => {
            assert_eq!(arithmetic["opcode"], "ADD");
            assert_eq!(arithmetic["type"]["scale"], 10);
            assert_eq!(INPUT_NAMES[expected_source], "num_exp_add");
        }
        DecimalOperation::Multiply => {
            assert_eq!(arithmetic["opcode"], "MUL");
            assert_eq!(arithmetic["type"]["scale"], 20);
            assert_eq!(INPUT_NAMES[expected_source], "num_exp_mul");
        }
    }
    let ([row], mismatches) = body(&operators[14]) else {
        panic!("expected one compared row");
    };
    let [guard, _] = items(&class(mismatches, "DBSPBlockExpression")["contents"]) else {
        panic!("expected comparison guard and result");
    };
    let guard = class(
        &class(guard, "DBSPExpressionStatement")["expression"],
        "DBSPIfExpression",
    );
    let negated = class(&guard["condition"], "DBSPUnaryExpression");
    assert_eq!(negated["opcode"], "NOT");
    let null_as_false = class(&negated["source"], "DBSPUnaryExpression");
    assert_eq!(null_as_false["opcode"], "WRAP_BOOL");
    let comparison = class(&null_as_false["source"], "DBSPBinaryExpression");
    assert_eq!(comparison["opcode"], "NEQ");
    assert_eq!(field(&comparison["left"], row), 2);
    assert_eq!(field(&comparison["right"], row), 3);

    (operation, expected_source)
}

fn numeric_row(fields: &[Value]) -> NumericRow {
    let [id, value] = fields else {
        panic!("expected id and decimal")
    };
    Tup2(
        number(id, "DBSPI32Literal"),
        number(value, "DBSPDecimalLiteral"),
    )
}

fn expected_row(fields: &[Value]) -> ExpectedDecimalRow {
    let [first, second, value] = fields else {
        panic!("expected two ids and a decimal");
    };
    Tup3(
        number(first, "DBSPI32Literal"),
        number(second, "DBSPI32Literal"),
        number(value, "DBSPDecimalLiteral"),
    )
}

fn data_batch(rows: &[Tup2<NumericRow, i64>]) -> Arc<NumericBatch<2>> {
    Arc::new(NumericBatch::new(
        [(ArchiveInteger::I32, true), (ArchiveInteger::I128, true)],
        rows.iter().map(|Tup2(row, weight)| {
            (
                [row.0.map(i128::from), row.1.map(|value| value.mantissa())],
                *weight,
            )
        }),
    ))
}

fn reference_batch(rows: &[Tup2<ExpectedDecimalRow, i64>]) -> Arc<NumericBatch<3>> {
    Arc::new(NumericBatch::new(
        [
            (ArchiveInteger::I32, true),
            (ArchiveInteger::I32, true),
            (ArchiveInteger::I128, true),
        ],
        rows.iter().map(|Tup2(row, weight)| {
            (
                [
                    row.0.map(i128::from),
                    row.1.map(i128::from),
                    row.2.map(|value| value.mantissa()),
                ],
                *weight,
            )
        }),
    ))
}

fn check_case(variable: &str, method: &str, supplemental_result: &str) {
    let path = env::var_os(variable).expect("frozen corpus case path");
    let case = parse_json(&fs::read(path).unwrap()).expect("strict JSON case");
    assert_eq!(
        case["source_test"],
        format!("org.dbsp.sqlCompiler.compiler.sql.postgres.PostgresNumericTests#{method}"),
    );
    assert_eq!(case["case_index"], 0);
    assert_eq!(case["step_semantics"], "whole_relation");
    let (operation, expected_source) = plan(&case);
    let (mut owned, (data_input, reference_input, owned_output)) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
        move |circuit| {
            let key = |value: Option<i128>| value.map(|n| i32::try_from(n).unwrap());
            let (data, data_input) = numeric_input::source(circuit, move |[id, value]| {
                Tup2(key(id), integer_to_decimalN::<38, 10>(value))
            });
            let (reference, reference_input) =
                numeric_input::source(circuit, move |[a, b, value]| {
                    Tup3(key(a), key(b), integer_to_decimalN::<38, 10>(value))
                });
            Ok((
                data_input,
                reference_input,
                decimal_join_projected(&data, &reference, operation)?,
            ))
        },
    )
    .unwrap();
    let (mut runtime, ports) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
        move |circuit| Ok(build_decimal_join(circuit, operation, expected_source)?),
    )
    .unwrap();
    let case_step = one(&case["commands"]);
    assert_eq!(case_step["kind"], "step");
    let expected_mismatches = bag(one(&case_step["outputs"]), "E", |fields| {
        let [id1, id2, actual, expected] = fields else {
            panic!("expected four fields")
        };
        Tup4(
            number(id1, "DBSPI32Literal"),
            number(id2, "DBSPI32Literal"),
            number(actual, "DBSPDecimalLiteral"),
            number(expected, "DBSPDecimalLiteral"),
        )
    });
    let mut expected_comparisons = Vec::new();
    let inputs = items(&case_step["inputs"]);
    assert_eq!(inputs.len(), INPUT_NAMES.len());
    let data = data_batch(&bag(&inputs[0], INPUT_NAMES[0], numeric_row));
    let reference = reference_batch(&bag(
        &inputs[expected_source],
        INPUT_NAMES[expected_source],
        expected_row,
    ));
    let data_owner = data.owner();
    let reference_owner = reference.owner();
    data_input.set_for_worker(0, Some(data));
    reference_input.set_for_worker(0, Some(reference));
    for (index, (input, port)) in inputs.iter().zip(&ports.inputs).enumerate() {
        let name = INPUT_NAMES[index];
        match port {
            DecimalInputPort::SingleKey(port) => port.append(&mut bag(input, name, numeric_row)),
            DecimalInputPort::PairKey(port) => {
                let mut rows = bag(input, name, expected_row);
                if index == expected_source {
                    expected_comparisons.extend(
                        rows.iter().map(|Tup2(row, weight)| {
                            Tup2(Tup4(row.0, row.1, row.2, row.2), *weight)
                        }),
                    );
                }
                port.append(&mut rows);
            }
        }
    }
    assert_eq!(expected_comparisons.len(), 100);
    let comparisons = OrdZSet::from_keys((), expected_comparisons);
    let mismatches = OrdZSet::from_keys((), expected_mismatches);
    owned.transaction().unwrap();
    assert_eq!(owned_output.compared.consolidate(), comparisons);
    assert_eq!(owned_output.output.consolidate(), mismatches);
    assert_eq!(owned_output.errors.consolidate(), OrdZSet::empty());
    runtime.transaction().unwrap();
    assert_eq!(ports.compared.consolidate(), comparisons);
    assert_eq!(ports.output.consolidate(), mismatches);
    assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));

    let DecimalInputPort::SingleKey(source) = &ports.inputs[0] else {
        unreachable!()
    };
    let DecimalInputPort::PairKey(expected) = &ports.inputs[expected_source] else {
        unreachable!()
    };
    let calculated = supplemental_result.parse().unwrap();
    // The two reference differences change one mantissa word each.
    let low_diff = format!("{supplemental_result}.0000000001").parse().unwrap();
    let hi_diff = match supplemental_result {
        "6" => "1844674413.3709551616",
        "9" => "1844674416.3709551616",
        _ => unreachable!(),
    }
    .parse()
    .unwrap();
    let three = Some("3".parse().unwrap());
    let mut supplemental_data = vec![
        Tup2(Tup2(Some(1), three), 1),
        Tup2(Tup2(Some(2), three), 1),
        Tup2(Tup2(None, three), 1),
    ];
    let mut supplemental_reference = vec![
        Tup2(Tup3(Some(1), Some(1), Some(low_diff)), 1),
        Tup2(Tup3(Some(1), Some(2), Some(hi_diff)), 1),
        Tup2(Tup3(Some(2), Some(2), None), 1),
        Tup2(Tup3(None, Some(2), Some(low_diff)), 1),
    ];
    data_input.set_for_worker(0, Some(data_batch(&supplemental_data)));
    reference_input.set_for_worker(0, Some(reference_batch(&supplemental_reference)));
    source.append(&mut supplemental_data);
    expected.append(&mut supplemental_reference);
    runtime.transaction().unwrap();
    owned.transaction().unwrap();
    let mismatches = OrdZSet::from_keys(
        (),
        vec![
            Tup2(Tup4(Some(1), Some(1), Some(calculated), Some(low_diff)), 1),
            Tup2(Tup4(Some(1), Some(2), Some(calculated), Some(hi_diff)), 1),
        ],
    );
    assert_eq!(ports.output.consolidate(), mismatches);
    assert_eq!(owned_output.output.consolidate(), mismatches);
    assert_eq!(owned_output.errors.consolidate(), OrdZSet::empty());
    assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    owned.kill().unwrap();
    drop((data_input, reference_input));
    assert!(data_owner.upgrade().is_none() && reference_owner.upgrade().is_none());
}

#[test]
#[ignore = "requires P0_DECIMAL_ADD_CASE from the frozen native corpus archive"]
fn frozen_decimal_add_executes_from_ir() {
    check_case("P0_DECIMAL_ADD_CASE", "testAdd", "6");
}

#[test]
#[ignore = "requires P0_DECIMAL_MULTIPLY_CASE from the frozen native corpus archive"]
fn frozen_decimal_multiply_executes_from_ir() {
    check_case("P0_DECIMAL_MULTIPLY_CASE", "testMultiply", "9");
}
