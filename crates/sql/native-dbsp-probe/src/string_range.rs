//! A flat DBSP circuit for the validated UTF-8 range-filter IR slice.

use dbsp::{
    Circuit, OrdZSet, OutputHandle, RootCircuit, Stream, ZSetHandle,
    operator::ConstantGenerator,
    typed_batch::IndexedZSetReader,
    utils::{Tup1, Tup2, Tup3},
};
use feldera_native_probe::{CompiledStringRange, Error};
use feldera_sqllib::SqlString;

pub type StringRow = Tup1<Option<SqlString>>;
pub type ErrorRow = Tup3<SqlString, SqlString, SqlString>;

pub struct StringRangePorts {
    pub input: ZSetHandle<StringRow>,
    pub filtered: OutputHandle<OrdZSet<StringRow>>,
    pub errors: OutputHandle<OrdZSet<ErrorRow>>,
}

pub fn build_string_range(
    circuit: &RootCircuit,
    lower: &str,
    upper: &str,
) -> Result<StringRangePorts, Error> {
    let predicate = CompiledStringRange::compile(lower, upper)?;
    let (source, input) = circuit.add_input_zset::<StringRow>();
    let filtered = source
        .apply(move |batch| filter_batch(batch, &predicate))
        .output();
    Ok(StringRangePorts {
        input,
        filtered,
        errors: empty_error_stream(circuit).output(),
    })
}

pub(crate) fn empty_error_stream(circuit: &RootCircuit) -> Stream<RootCircuit, OrdZSet<ErrorRow>> {
    circuit.add_source(ConstantGenerator::new(OrdZSet::from_keys((), vec![])))
}

fn filter_batch(batch: &OrdZSet<StringRow>, predicate: &CompiledStringRange) -> OrdZSet<StringRow> {
    let rows: Vec<_> = batch.iter().collect();
    let mut output = Vec::new();
    for chunk in rows.chunks(65_536) {
        let values: Vec<_> = chunk
            .iter()
            .map(|(row, (), _)| row.0.as_ref().map(SqlString::str))
            .collect();
        let keep = predicate
            .filter(&values)
            .expect("bounded native filter input");
        output.extend(
            chunk
                .iter()
                .zip(keep)
                .filter(|(_, keep)| *keep != 0)
                .map(|((row, (), weight), _)| Tup2(row.clone(), *weight)),
        );
    }
    OrdZSet::from_keys((), output)
}
