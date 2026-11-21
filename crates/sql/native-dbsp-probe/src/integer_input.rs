//! Shared table schema and output handles for the selected integer circuits.

use crate::ErrorRow;
use dbsp::{
    Circuit, DBData, OrdZSet, OutputHandle, RootCircuit, Stream, ZSetHandle,
    operator::IncrementalConstantGenerator, utils::Tup7,
};
use feldera_fxp::Fixed;
use feldera_sqllib::{F64, SqlString};

pub type IntegerCaseRow =
    Tup7<i32, F64, bool, SqlString, Option<i32>, Option<Fixed<6, 2>>, Option<u32>>;

pub struct IntegerCasePorts<R: DBData> {
    pub input: ZSetHandle<IntegerCaseRow>,
    pub output: OutputHandle<OrdZSet<R>>,
    pub errors: OutputHandle<OrdZSet<ErrorRow>>,
}

pub(crate) fn output_ports<R: DBData>(
    circuit: &RootCircuit,
    input: ZSetHandle<IntegerCaseRow>,
    output: Stream<RootCircuit, OrdZSet<R>>,
) -> IntegerCasePorts<R> {
    output_ports_with_ids(circuit, input, output, None, None, None)
}

pub(crate) fn output_ports_with_ids<R: DBData>(
    circuit: &RootCircuit,
    input: ZSetHandle<IntegerCaseRow>,
    output: Stream<RootCircuit, OrdZSet<R>>,
    errors_id: Option<&str>,
    output_id: Option<&str>,
    error_output_id: Option<&str>,
) -> IntegerCasePorts<R> {
    let errors = circuit
        .add_source(IncrementalConstantGenerator::new(
            OrdZSet::<ErrorRow>::from_keys((), vec![]),
        ))
        .set_persistent_id(errors_id)
        .output_persistent(error_output_id);
    IntegerCasePorts {
        input,
        output: output.output_persistent(output_id),
        errors,
    }
}

/// Evaluate a native expression and retain each weighted source row.
pub(crate) fn evaluate_columns<R: DBData, const N: usize>(
    input: &Stream<RootCircuit, OrdZSet<R>>,
    program: feldera_native_probe::CompiledProgram,
    columns: impl Fn(&R) -> [Option<i64>; N] + 'static,
) -> Stream<RootCircuit, OrdZSet<dbsp::utils::Tup2<R, Option<i64>>>> {
    use dbsp::{typed_batch::IndexedZSetReader, utils::Tup2};
    use feldera_native_probe::Batch;

    input.apply(move |batch| {
        let rows: Vec<_> = batch.iter().collect();
        let mut output = Vec::with_capacity(rows.len());
        let rows_per_chunk = 65_536.min(1_048_576 / N.max(1));
        for chunk in rows.chunks(rows_per_chunk) {
            let values: Vec<_> = chunk
                .iter()
                .map(|(row, (), _)| Vec::from(columns(row)))
                .collect();
            let results = program
                .evaluate(&Batch::from_rows(N, &values).expect("bounded numeric batch"))
                .expect("evaluate native expression");
            output.extend(
                chunk
                    .iter()
                    .zip(results)
                    .map(|((row, (), weight), result)| Tup2(Tup2(row.clone(), result), *weight)),
            );
        }
        OrdZSet::from_keys((), output)
    })
}
