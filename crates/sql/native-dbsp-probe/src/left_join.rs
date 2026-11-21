//! Stateful self-joins for the selected INTEGER/BOOLEAN circuits.

use crate::{IntegerCasePorts, IntegerCaseRow, integer_input::output_ports};
use dbsp::{
    OrdZSet, RootCircuit, Stream, ZSetHandle,
    typed_batch::IndexedZSetReader,
    utils::{Tup1, Tup2, Tup3},
};
use feldera_native_probe::{Batch, CompiledProgram, Error, Expr, Program};

pub type JoinOutputRow = Tup2<bool, Option<bool>>;
pub type FullJoinOutputRow = Tup2<Option<bool>, Option<bool>>;
pub type JoinProjectedRow = Tup3<i32, bool, Option<i32>>;

pub type LeftJoinPorts = IntegerCasePorts<JoinOutputRow>;

pub fn build_left_join(
    circuit: &RootCircuit,
    nullable_key_column: usize,
) -> Result<LeftJoinPorts, Error> {
    let (projected, input) = projected_input(circuit);
    let output = left_join_projected(&projected, nullable_key_column)?;
    Ok(output_ports(circuit, input, output))
}

pub fn build_full_join(
    circuit: &RootCircuit,
    nullable_key_column: usize,
) -> Result<IntegerCasePorts<FullJoinOutputRow>, Error> {
    let (projected, input) = projected_input(circuit);
    let output = full_join_projected(&projected, nullable_key_column)?;
    Ok(output_ports(circuit, input, output))
}

/// Joins the bounded projection of the original table.
pub fn left_join_projected(
    source: &Stream<RootCircuit, OrdZSet<JoinProjectedRow>>,
    nullable_key_column: usize,
) -> Result<Stream<RootCircuit, OrdZSet<JoinOutputRow>>, Error> {
    let projected = evaluate_keys(source, nullable_key_column)?;
    let left = projected.map_index(|row| (Tup1(row.0), Tup1(row.1)));
    let right = projected.flat_map_index(|row| row.2.map(|key| (Tup1(key), Some(Tup1(row.1)))));
    Ok(left.left_join(&right, |_, left, right| {
        Tup2(left.0, right.as_ref().map(|row| row.0))
    }))
}

/// Full outer join over the same bounded table projection.
pub fn full_join_projected(
    source: &Stream<RootCircuit, OrdZSet<JoinProjectedRow>>,
    nullable_key_column: usize,
) -> Result<Stream<RootCircuit, OrdZSet<FullJoinOutputRow>>, Error> {
    let projected = evaluate_keys(source, nullable_key_column)?;
    let left = projected.map_index(|row| (Tup1(row.0), Tup1(row.1)));
    let right = projected.flat_map_index(|row| row.2.map(|key| (Tup1(key), Tup1(row.1))));
    let matched = left.join(&right, |_, left, right| Tup2(Some(left.0), Some(right.0)));
    let left_only = left
        .antijoin(&right)
        .map(|(_, value)| Tup2(Some(value.0), None));
    let right_only = right
        .antijoin(&left)
        .map(|(_, value)| Tup2(None, Some(value.0)));
    let null_key = projected.flat_map(|row| row.2.is_none().then_some(Tup2(None, Some(row.1))));
    Ok(matched.sum([&left_only, &right_only, &null_key]))
}

type ProjectedInput = (
    Stream<RootCircuit, OrdZSet<JoinProjectedRow>>,
    ZSetHandle<IntegerCaseRow>,
);

fn projected_input(circuit: &RootCircuit) -> ProjectedInput {
    let (source, input) = circuit.add_input_zset::<IntegerCaseRow>();
    (source.map(|row| Tup3(row.0, row.2, row.4)), input)
}

fn evaluate_keys(
    source: &Stream<RootCircuit, OrdZSet<JoinProjectedRow>>,
    nullable_key_column: usize,
) -> Result<Stream<RootCircuit, OrdZSet<JoinProjectedRow>>, Error> {
    if nullable_key_column != 2 {
        return Err(Error::InvalidProgram(
            "expected the nullable INTEGER as the third projected field".into(),
        ));
    }
    let key = CompiledProgram::compile(&Program::new(3, vec![Expr::Column(nullable_key_column)])?)?;
    Ok(source.apply(move |batch| project_keys(batch, &key)))
}

fn project_keys(
    batch: &OrdZSet<JoinProjectedRow>,
    key: &CompiledProgram,
) -> OrdZSet<JoinProjectedRow> {
    let rows: Vec<_> = batch.iter().collect();
    let mut output = Vec::new();
    for chunk in rows.chunks(65_536) {
        let inputs: Vec<_> = chunk
            .iter()
            .map(|(row, (), _)| {
                vec![
                    Some(i64::from(row.0)),
                    Some(i64::from(row.1)),
                    row.2.map(i64::from),
                ]
            })
            .collect();
        let keys = key
            .evaluate(&Batch::from_rows(3, &inputs).expect("bounded key batch"))
            .expect("read nullable native key");
        output.extend(chunk.iter().zip(keys).map(|((row, (), weight), key)| {
            let key = key.map(|value| i32::try_from(value).expect("native key preserves INTEGER"));
            Tup2(Tup3(row.0, row.1, key), *weight)
        }));
    }
    OrdZSet::from_keys((), output)
}
