//! Replay the selected global aggregates and exercise active group transitions.

use dbsp::{
    OrdZSet, Runtime,
    operator::time_series::{RelOffset, RelRange},
    utils::{Tup1, Tup2, Tup7},
};
use feldera_native_dbsp_probe::{
    IntegerCaseRow, build_count_distinct, build_nullable_sum, build_rolling_count,
    rolling_count_projected,
};
use feldera_native_probe::ArchiveInteger;
use feldera_sqllib::{F64, SqlString, integer_to_decimalN};
use serde_json::Value;
use std::sync::Arc;

#[path = "circuit_fixture/aggregates.rs"]
mod aggregate_fixture;
mod circuit_fixture;
mod integer_corpus;
#[path = "circuit_fixture/numeric_input.rs"]
mod numeric_input;
use aggregate_fixture::{
    case_from_file, constant_tuple, count_filter, operator_graph, sum_column, tuple,
};
use circuit_fixture::{bag, body, class, field, items, number, one};
use integer_corpus::input_row;
use numeric_input::NumericBatch;

fn row(id: i32, flag: bool, value: Option<i32>) -> IntegerCaseRow {
    Tup7(id, F64::new(0.0), flag, SqlString::new(), value, None, None)
}

#[test]
#[ignore = "requires P0_NULLABLE_SUM_CASE from the frozen native corpus archive"]
fn frozen_nullable_sum_executes_from_ir() {
    let case = case_from_file("P0_NULLABLE_SUM_CASE", "optionAggregateTest");
    let column = sum_column(&case);
    let (mut runtime, ports) =
        Runtime::init_circuit(1, move |circuit| Ok(build_nullable_sum(circuit, column)?)).unwrap();
    let mut step = |mut input, output| {
        ports.input.append(&mut input);
        runtime.transaction().unwrap();
        assert_eq!(ports.output.consolidate(), OrdZSet::from_keys((), output));
        assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    };
    for command in items(&case["commands"]) {
        assert_eq!(command["kind"], "step");
        let input = bag(one(&command["inputs"]), "T", input_row);
        let output = bag(one(&command["outputs"]), "V", |fields| {
            let [sum] = fields else {
                panic!("expected one sum")
            };
            Tup1(number(sum, "DBSPI32Literal"))
        });
        step(input, output);
    }
    let null = row(1, false, None);
    let positive = row(2, false, Some(5));
    let negative = row(3, false, Some(-5));
    step(vec![Tup2(null.clone(), 1)], vec![]);
    step(
        vec![Tup2(positive.clone(), 1), Tup2(negative.clone(), 1)],
        vec![Tup2(Tup1(None), -1), Tup2(Tup1(Some(0)), 1)],
    );
    step(
        vec![Tup2(positive, -1), Tup2(negative, -1)],
        vec![Tup2(Tup1(Some(0)), -1), Tup2(Tup1(None), 1)],
    );
    step(vec![Tup2(null, -1)], vec![]);
}

#[test]
#[ignore = "requires P0_COUNT_DISTINCT_CASE from the frozen native corpus archive"]
fn frozen_count_distinct_executes_from_ir() {
    let case = case_from_file("P0_COUNT_DISTINCT_CASE", "emptyAggregate");
    let (column, threshold) = count_filter(&case);
    let (mut runtime, ports) = Runtime::init_circuit(1, move |circuit| {
        Ok(build_count_distinct(circuit, column, threshold)?)
    })
    .unwrap();
    let mut step = |mut input, output| {
        ports.input.append(&mut input);
        runtime.transaction().unwrap();
        assert_eq!(ports.output.consolidate(), OrdZSet::from_keys((), output));
        assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    };
    for command in items(&case["commands"]) {
        assert_eq!(command["kind"], "step");
        let input = bag(one(&command["inputs"]), "T", input_row);
        let output = bag(one(&command["outputs"]), "V", |fields| {
            let [rows, keys] = fields else {
                panic!("expected two counts")
            };
            Tup2(
                number(rows, "DBSPI64Literal").unwrap(),
                number(keys, "DBSPI64Literal").unwrap(),
            )
        });
        step(input, output);
    }
    let boundary = row(threshold, false, None);
    let first = row(threshold + 1, false, None);
    let duplicate_key = row(threshold + 1, true, None);
    let unique = row(threshold + 2, false, None);
    step(
        vec![
            Tup2(boundary.clone(), 1),
            Tup2(first.clone(), 2),
            Tup2(duplicate_key.clone(), 1),
            Tup2(unique.clone(), 1),
        ],
        vec![Tup2(Tup2(0, 0), -1), Tup2(Tup2(4, 2), 1)],
    );
    step(
        vec![Tup2(first, -2)],
        vec![Tup2(Tup2(4, 2), -1), Tup2(Tup2(2, 2), 1)],
    );
    step(
        vec![
            Tup2(boundary, -1),
            Tup2(duplicate_key, -1),
            Tup2(unique, -1),
        ],
        vec![Tup2(Tup2(2, 2), -1), Tup2(Tup2(0, 0), 1)],
    );
}

fn window_bound(node: &Value) -> RelOffset<u64> {
    let bound = class(node, "DBSPWindowBoundExpression");
    let distance = number(&bound["representation"], "DBSPU64Literal").expect("window bound");
    if bound["isPreceding"].as_bool().expect("window direction") {
        RelOffset::Before(distance)
    } else {
        RelOffset::After(distance)
    }
}

fn rolling_spec(case: &Value) -> ([usize; 2], RelRange<u64>) {
    let operators = operator_graph(
        case,
        &[
            ("DBSPSourceMultisetOperator", &[]),
            ("DBSPMapOperator", &[0]),
            ("DBSPMapIndexOperator", &[1]),
            ("DBSPPartitionedRollingAggregateOperator", &[2]),
            ("DBSPMapIndexOperator", &[3]),
            ("DBSPMapIndexOperator", &[1]),
            ("DBSPJoinOperator", &[5, 4]),
            ("DBSPSinkOperator", &[6]),
            ("DBSPIncrementalConstantOperator", &[]),
            ("DBSPSinkOperator", &[8]),
        ],
    );
    let ([source], projected) = body(&operators[1]) else {
        panic!("expected one projection parameter");
    };
    let [id, decimal] = tuple(projected) else {
        panic!("expected id and decimal projection");
    };
    let columns = [field(id, source), field(decimal, source)];

    let ([input], index) = body(&operators[2]) else {
        panic!("expected one index parameter");
    };
    let [time, payload] = items(&class(index, "DBSPRawTupleExpression")["fields"]) else {
        panic!("expected timestamp and partitioned payload");
    };
    let wrap = class(time, "DBSPUnsignedWrapExpression");
    assert_eq!(wrap["ascending"], true);
    assert_eq!(wrap["nullsLast"], false);
    let cast = class(
        &class(&wrap["source"], "DBSPHandleErrorExpression")["source"],
        "DBSPCastExpression",
    );
    let mantissa = class(&cast["source"], "DBSPUnaryExpression");
    assert_eq!(mantissa["opcode"], "DECIMAL_TO_INTEGER");
    assert_eq!(field(&mantissa["source"], input), 1);
    let [partition, row_value] = tuple(payload) else {
        panic!("expected partition and projected row");
    };
    assert!(constant_tuple(partition).is_empty());
    let [id, decimal] = tuple(row_value) else {
        panic!("expected id and decimal payload");
    };
    assert_eq!([field(id, input), field(decimal, input)], [0, 1]);

    let rolling = &operators[3];
    let fold = class(&rolling["function"], "DBSPFold");
    let [zero] = tuple(&fold["zero"]) else {
        panic!("expected one count accumulator");
    };
    assert_eq!(number(zero, "DBSPI64Literal"), Some(0_i64));
    let increment = class(&fold["increment"], "DBSPClosureExpression");
    let [accumulator, _, weight] = items(&increment["parameters"]) else {
        panic!("expected accumulator, row and weight parameters");
    };
    let [statement] = items(&class(&increment["body"], "DBSPBlockExpression")["contents"]) else {
        panic!("expected count increment");
    };
    let assignment = class(
        &class(statement, "DBSPExpressionStatement")["expression"],
        "DBSPAssignmentExpression",
    );
    assert_eq!(field(&assignment["left"], accumulator), 0);
    let update = class(&assignment["right"], "DBSPConditionalIncrementExpression");
    assert_eq!(update["opcode"], "AGG_ADD");
    assert_eq!(field(&update["left"], accumulator), 0);
    let contribution = class(&update["right"], "DBSPBinaryExpression");
    assert_eq!(contribution["opcode"], "MUL_WEIGHT");
    assert_eq!(number(&contribution["left"], "DBSPI64Literal"), Some(1_i64));
    assert_eq!(
        class(&contribution["right"], "DBSPVariablePath")["variable"],
        weight["name"]
    );
    assert_eq!(operators[6]["balanced"], true);
    (
        columns,
        RelRange::new(
            window_bound(&rolling["lower"]),
            window_bound(&rolling["upper"]),
        ),
    )
}

#[test]
#[ignore = "requires P0_DECIMAL_ROLLING_CASE from the frozen native corpus archive"]
fn frozen_decimal_rolling_count_executes_from_ir() {
    let case = case_from_file("P0_DECIMAL_ROLLING_CASE", "overDecimalTest");
    let (columns, range) = rolling_spec(&case);
    let (mut owned, (owned_input, owned_output)) = Runtime::init_circuit(1, move |circuit| {
        let (source, input) = numeric_input::source(circuit, |[id, decimal]| {
            Tup2(
                i32::try_from(id.unwrap()).unwrap(),
                integer_to_decimalN::<6, 2>(decimal),
            )
        });
        Ok((
            input,
            rolling_count_projected(&source, columns, range)?.output(),
        ))
    })
    .unwrap();
    let mut owners = Vec::new();
    let (mut runtime, ports) = Runtime::init_circuit(1, move |circuit| {
        Ok(build_rolling_count(circuit, columns, range)?)
    })
    .unwrap();
    let mut step = |mut input: Vec<Tup2<IntegerCaseRow, i64>>, output| {
        let batch = Arc::new(NumericBatch::new(
            [(ArchiveInteger::I32, false), (ArchiveInteger::I128, true)],
            input.iter().map(|Tup2(row, weight)| {
                (
                    [Some(row.0.into()), row.5.map(|value| value.mantissa())],
                    *weight,
                )
            }),
        ));
        owners.push(batch.owner());
        owned_input.set_for_worker(0, Some(batch));
        owned.transaction().unwrap();
        let expected = OrdZSet::from_keys((), output);
        assert_eq!(owned_output.consolidate(), expected);
        ports.input.append(&mut input);
        runtime.transaction().unwrap();
        assert_eq!(ports.output.consolidate(), expected);
        assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    };
    for command in items(&case["commands"]) {
        assert_eq!(command["kind"], "step");
        let input = bag(one(&command["inputs"]), "T", input_row);
        let output = bag(one(&command["outputs"]), "V", |fields| {
            let [id, count] = fields else {
                panic!("expected id and count")
            };
            Tup2(
                number(id, "DBSPI32Literal").unwrap(),
                number(count, "DBSPI64Literal").unwrap(),
            )
        });
        step(input, output);
    }
    let mut last = row(11, false, None);
    last.5 = Some("1.00".parse().unwrap());
    let mut earlier = row(22, false, None);
    earlier.5 = Some("-0.25".parse().unwrap());
    let null = row(33, false, None);

    step(vec![Tup2(last, 1)], vec![Tup2(Tup2(11, 1), 1)]);
    step(
        vec![Tup2(earlier, 1)],
        vec![
            Tup2(Tup2(11, 1), -1),
            Tup2(Tup2(11, 2), 1),
            Tup2(Tup2(22, 1), 1),
        ],
    );
    step(
        vec![Tup2(null.clone(), 1)],
        vec![
            Tup2(Tup2(11, 2), -1),
            Tup2(Tup2(11, 3), 1),
            Tup2(Tup2(22, 1), -1),
            Tup2(Tup2(22, 2), 1),
            Tup2(Tup2(33, 1), 1),
        ],
    );
    step(
        vec![Tup2(null, -1)],
        vec![
            Tup2(Tup2(11, 3), -1),
            Tup2(Tup2(11, 2), 1),
            Tup2(Tup2(22, 2), -1),
            Tup2(Tup2(22, 1), 1),
            Tup2(Tup2(33, 1), -1),
        ],
    );
    owned.kill().unwrap();
    drop(owned_input);
    assert!(owners.iter().all(|owner| owner.upgrade().is_none()));
}
