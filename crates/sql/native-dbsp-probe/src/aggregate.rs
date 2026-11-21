//! Global aggregates for the selected INTEGER circuits.

use crate::{
    IntegerCasePorts, IntegerCaseRow,
    integer_input::{evaluate_columns, output_ports, output_ports_with_ids},
};
use dbsp::{
    Circuit, DBData, OrdIndexedZSet, OrdZSet, RootCircuit, Stream,
    operator::IncrementalConstantGenerator,
    utils::{Tup0, Tup1, Tup2, Tup3},
};
use feldera_native_probe::{Comparison, CompiledProgram, Error, Expr, Program};
use feldera_sqllib::{cast_to_i32N_i64, cast_to_i64_i128};

pub type SumOutputRow = Tup1<Option<i32>>;
pub type CountOutputRow = Tup2<i64, i64>;

pub fn build_nullable_sum(
    circuit: &RootCircuit,
    column: usize,
) -> Result<IntegerCasePorts<SumOutputRow>, Error> {
    if column != 4 {
        return Err(Error::InvalidProgram(
            "expected nullable INTEGER at field 4".into(),
        ));
    }
    let coalesce = CompiledProgram::compile(&Program::new(
        1,
        vec![
            Expr::Column(0),
            Expr::Literal(Some(0)),
            Expr::Coalesce(vec![0, 1]),
        ],
    )?)?;
    let (source, input) = circuit.add_input_zset::<IntegerCaseRow>();
    let values = source.map(|row| row.4);
    let contributions = evaluate_columns(&values, coalesce, |value| [value.map(i64::from)])
        .map_index(|Tup2(value, sum)| {
            (
                Tup0::new(),
                Tup3(
                    sum.expect("COALESCE is never NULL"),
                    i64::from(value.is_some()),
                    1,
                ),
            )
        });
    // A third accumulator distinguishes an absent group from a present
    // group containing only NULLs or values whose sum is zero.
    let aggregate = contributions.aggregate_linear_postprocess_persistent(
        None,
        |contribution| *contribution,
        |Tup3(sum, non_null, _)| {
            Tup1(if non_null == 0 {
                None
            } else {
                cast_to_i32N_i64(sum).expect("cast SUM to nullable INTEGER")
            })
        },
    );
    Ok(output_ports(
        circuit,
        input,
        with_empty_group(circuit, &aggregate, Tup1(None), None),
    ))
}

/// Persistent IDs for the stateful nodes of the selected native COUNT circuit.
#[derive(Clone, Copy)]
pub struct CountDistinctStateIds<'a> {
    pub per_key: &'a str,
    pub global: &'a str,
    pub empty_group: &'a str,
    pub errors: &'a str,
    pub output: &'a str,
    pub error_output: &'a str,
}

pub fn build_count_distinct(
    circuit: &RootCircuit,
    column: usize,
    threshold: i32,
) -> Result<IntegerCasePorts<CountOutputRow>, Error> {
    build_count_distinct_with_ids(circuit, column, threshold, None)
}

/// Build with the IDs from the native circuit descriptor. The caller supplies
/// distinct, stable IDs and must reuse them when restoring the same circuit.
pub fn build_count_distinct_persistent(
    circuit: &RootCircuit,
    column: usize,
    threshold: i32,
    ids: CountDistinctStateIds<'_>,
) -> Result<IntegerCasePorts<CountOutputRow>, Error> {
    build_count_distinct_with_ids(circuit, column, threshold, Some(ids))
}

fn build_count_distinct_with_ids(
    circuit: &RootCircuit,
    column: usize,
    threshold: i32,
    ids: Option<CountDistinctStateIds<'_>>,
) -> Result<IntegerCasePorts<CountOutputRow>, Error> {
    if column != 0 {
        return Err(Error::InvalidProgram("expected INTEGER at field 0".into()));
    }
    let predicate = CompiledProgram::compile(&Program::new(
        1,
        vec![
            Expr::Column(0),
            Expr::Literal(Some(i64::from(threshold))),
            Expr::Compare {
                op: Comparison::GreaterThan,
                left: 0,
                right: 1,
            },
        ],
    )?)?;
    let (source, input) = circuit.add_input_zset::<IntegerCaseRow>();
    let keys = source.map(|row| Some(row.0));
    let indexed = evaluate_columns(&keys, predicate, |value| [value.map(i64::from)])
        .flat_map_index(|Tup2(key, matches)| {
            (*matches == Some(1)).then(|| (Tup1(key.expect("required INTEGER")), Tup0::new()))
        });
    let per_key = indexed.aggregate_linear_postprocess_persistent(
        ids.map(|ids| ids.per_key),
        |_| 1_i64,
        Tup1,
    );
    let global = per_key
        .map_index(|(_, count)| (Tup0::new(), *count))
        .aggregate_linear_postprocess_persistent(
            ids.map(|ids| ids.global),
            |Tup1(count)| Tup2(Tup2(i128::from(*count), 1_i64), Tup2(1_i64, 1_i64)),
            |Tup2(Tup2(rows, _), Tup2(keys, _))| {
                Tup2(cast_to_i64_i128(rows).expect("cast COUNT to BIGINT"), keys)
            },
        );
    Ok(output_ports_with_ids(
        circuit,
        input,
        with_empty_group(circuit, &global, Tup2(0, 0), ids.map(|ids| ids.empty_group)),
        ids.map(|ids| ids.errors),
        ids.map(|ids| ids.output),
        ids.map(|ids| ids.error_output),
    ))
}

fn with_empty_group<R: DBData>(
    circuit: &RootCircuit,
    aggregate: &Stream<RootCircuit, OrdIndexedZSet<Tup0, R>>,
    empty: R,
    persistent_id: Option<&str>,
) -> Stream<RootCircuit, OrdZSet<R>> {
    let constant = circuit
        .add_source(IncrementalConstantGenerator::new(OrdZSet::from_keys(
            (),
            vec![Tup2(empty.clone(), 1)],
        )))
        .set_persistent_id(persistent_id);
    let remove_default = aggregate.map(move |_| empty.clone()).neg();
    let values = aggregate.map(|(_, value)| value.clone());
    circuit
        .accumulate_concat_zsets(&[(constant, true), (remove_default, true), (values, true)])
        .shard()
}
