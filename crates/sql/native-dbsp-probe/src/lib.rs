//! Bounded BIGINT batch projection on DBSP's existing dynamic-backed batches.
//!
//! Rows use one schema-independent Rust container. This is not the final native
//! row layout, a SQL-to-circuit compiler, or a production error-handling design.

use dbsp::{OrdZSet, typed_batch::IndexedZSetReader, utils::Tup2};
use feldera_native_probe::{Batch, CompiledProgram, Error, Program};

mod aggregate;
mod decimal_join;
mod integer_input;
mod json_address;
mod left_join;
mod rolling_count;
mod string_projection;
mod string_range;

pub use aggregate::{
    CountDistinctStateIds, CountOutputRow, SumOutputRow, build_count_distinct,
    build_count_distinct_persistent, build_nullable_sum,
};
pub use decimal_join::{
    DecimalInputPort, DecimalJoinOutputs, DecimalJoinPorts, DecimalMismatchRow, DecimalOperation,
    ExpectedDecimalRow, NumericRow, build_decimal_join, decimal_join_projected,
};
pub use integer_input::{IntegerCasePorts, IntegerCaseRow};
pub use json_address::{
    JsonAddressInput, JsonAddressPorts, JsonAddressProjection, JsonAddressRow, JsonAddressSpec,
    build_json_address,
};
pub use left_join::{
    FullJoinOutputRow, JoinOutputRow, JoinProjectedRow, LeftJoinPorts, build_full_join,
    build_left_join, full_join_projected, left_join_projected,
};
pub use rolling_count::{
    RollingCountRow, RollingProjectedRow, build_rolling_count, rolling_count_projected,
};
pub use string_projection::{
    ProjectedStringRow, StringProjection, StringProjectionPorts, StringProjectionSpec,
    StringProjectionStreams, build_string_projection, build_string_projection_streams,
};
pub use string_range::{ErrorRow, StringRangePorts, StringRow, build_string_range};

/// DBSP supplies `Data`, comparison, hashing and archive support for this type.
/// Row width belongs to the projection's descriptor, not to its Rust type.
pub type Row = Vec<Option<i64>>;

pub struct Projection {
    columns: usize,
    compiled: CompiledProgram,
}

impl Projection {
    pub fn compile(columns: usize, program: &Program) -> Result<Self, Error> {
        // Validate the separately supplied descriptor before code generation.
        // An empty batch still checks the program's declared column count.
        program.evaluate(&Batch::from_rows(columns, &[])?)?;
        Ok(Self {
            columns,
            compiled: CompiledProgram::compile(program)?,
        })
    }

    /// Append one native result column, preserving each row's signed weight.
    /// Consolidation belongs to DBSP, so equal projected rows combine normally.
    /// Calls cross the native ABI once per bounded chunk, never once per row.
    pub fn evaluate(&self, input: &OrdZSet<Row>) -> Result<OrdZSet<Row>, Error> {
        let tuples: Vec<_> = input.iter().collect();
        let mut output = Vec::with_capacity(tuples.len());
        // Match the native ABI's row and cell bounds even for a larger DBSP batch.
        let chunk_rows = 65_536.min(1_048_576 / self.columns.max(1));
        for (chunk_index, chunk) in tuples.chunks(chunk_rows).enumerate() {
            let rows: Vec<_> = chunk.iter().map(|(row, (), _)| row.clone()).collect();
            let batch = Batch::from_rows(self.columns, &rows)?;
            let values = self
                .compiled
                .evaluate(&batch)
                .map_err(|error| match error {
                    Error::Overflow {
                        row,
                        op,
                        left,
                        right,
                    } => Error::Overflow {
                        row: chunk_index * chunk_rows + row,
                        op,
                        left,
                        right,
                    },
                    error => error,
                })?;
            for ((row, (), weight), value) in chunk.iter().zip(values) {
                let mut projected = row.clone();
                projected.push(value);
                output.push(Tup2(projected, *weight));
            }
        }
        Ok(OrdZSet::from_keys((), output))
    }
}
