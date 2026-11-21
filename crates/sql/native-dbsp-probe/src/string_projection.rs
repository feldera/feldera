//! The five-field Unicode projection in the selected DBSP circuit.

use crate::{ErrorRow, StringRow, string_range::empty_error_stream};
use dbsp::{
    OrdZSet, OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    typed_batch::IndexedZSetReader,
    utils::{Tup2, Tup5},
};
use feldera_native_probe::{CompiledStringProjection, Error};
use feldera_sqllib::SqlString;

pub type ProjectedStringRow =
    Tup5<Option<SqlString>, SqlString, Option<i32>, i32, Option<SqlString>>;

#[derive(Clone)]
pub struct StringProjectionSpec {
    pub varchar_literal: String,
    pub padded_width: i32,
    pub padded_literal: String,
    pub trim_pattern: String,
}

pub struct StringProjectionPorts {
    pub input: ZSetHandle<StringRow>,
    pub output: OutputHandle<OrdZSet<ProjectedStringRow>>,
    pub errors: OutputHandle<OrdZSet<ErrorRow>>,
}

pub struct StringProjectionStreams {
    pub input: ZSetHandle<StringRow>,
    pub output: Stream<RootCircuit, OrdZSet<ProjectedStringRow>>,
    pub errors: Stream<RootCircuit, OrdZSet<ErrorRow>>,
}

pub fn build_string_projection(
    circuit: &RootCircuit,
    spec: &StringProjectionSpec,
) -> Result<StringProjectionPorts, Error> {
    let streams = build_string_projection_streams(circuit, spec)?;
    Ok(StringProjectionPorts {
        input: streams.input,
        output: streams.output.output(),
        errors: streams.errors.output(),
    })
}

pub fn build_string_projection_streams(
    circuit: &RootCircuit,
    spec: &StringProjectionSpec,
) -> Result<StringProjectionStreams, Error> {
    let projection = StringProjection::compile(spec)?;
    let (source, input) = circuit.add_input_zset::<StringRow>();
    let output = source.apply(move |batch| {
        projection.evaluate(batch.iter().map(|(row, (), weight)| (row.0, weight)))
    });
    Ok(StringProjectionStreams {
        input,
        output,
        errors: empty_error_stream(circuit),
    })
}

/// Compiled projection over signed input strings from any batch representation.
pub struct StringProjection {
    projection: CompiledStringProjection,
    literal: SqlString,
    padded_width: i32,
    literal_length: i32,
}

impl StringProjection {
    pub fn compile(spec: &StringProjectionSpec) -> Result<Self, Error> {
        if spec.padded_width < 1 {
            return Err(Error::InvalidProgram("CHAR width must be positive".into()));
        }
        let literal_length = i32::try_from(spec.padded_literal.chars().count()).map_err(|_| {
            Error::InvalidProgram("constant character length exceeds INTEGER".into())
        })?;
        Ok(Self {
            projection: CompiledStringProjection::compile(&spec.trim_pattern)?,
            literal: SqlString::from_ref(&spec.varchar_literal),
            padded_width: spec.padded_width,
            literal_length,
        })
    }

    pub fn evaluate(
        &self,
        rows: impl IntoIterator<Item = (Option<SqlString>, ZWeight)>,
    ) -> OrdZSet<ProjectedStringRow> {
        let rows: Vec<_> = rows.into_iter().collect();
        let mut output = Vec::with_capacity(rows.len());
        for chunk in rows.chunks(65_536) {
            let values: Vec<_> = chunk
                .iter()
                .map(|(text, _)| text.as_ref().map(SqlString::str))
                .collect();
            let slices = self
                .projection
                .project(&values)
                .expect("bounded native map input");
            output.extend(chunk.iter().zip(slices).map(|((text, weight), slices)| {
                let prefix = text.as_ref().map(|text| {
                    let kept = text
                        .str()
                        .get(..slices.first_end)
                        .expect("native UTF-8 prefix");
                    SqlString::maybe_reuse(kept, text)
                });
                let trimmed = text.as_ref().map(|text| {
                    let kept = text
                        .str()
                        .get(slices.trimmed_start..)
                        .expect("native UTF-8 suffix");
                    SqlString::maybe_reuse(kept, text)
                });
                Tup2(
                    Tup5(
                        prefix,
                        self.literal.clone(),
                        text.as_ref().map(|_| self.padded_width),
                        self.literal_length,
                        trimmed,
                    ),
                    *weight,
                )
            }));
        }
        OrdZSet::from_keys((), output)
    }
}
