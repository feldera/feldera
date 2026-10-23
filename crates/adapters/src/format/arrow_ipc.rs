//! The `arrow_ipc` format: weighted rows on the wire as Arrow IPC streams.
//!
//! A stream's columns are the relation's columns, flat, by SQL name, in the relation's
//! published Arrow types. An optional last column, [`WEIGHT_COLUMN`] (`Int64`), makes the
//! stream a Z-set: a positive weight inserts its row that many times and a negative weight
//! deletes it that many times. Without it every row is one insert. The encoder writes one
//! stream for each step, so a file holds the streams one after another, and the parser reads
//! every stream of a message.
//!
//! The format serves tests and is not documented for users.

use std::borrow::Cow;
use std::io::Cursor;
use std::iter::repeat_n;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::catalog::{ArrowStream, SerCursorFlattened};
use feldera_types::circuit::StagedBuffers;
use feldera_types::config::ConnectorConfig;
use feldera_types::program_schema::Relation;
use serde::{Deserialize, Serialize};

use super::parquet::{ArrowSchemaOptions, default_arrow_serde_config, relation_to_arrow_fields};
use super::{InputBuffer, SpongeSplitter};
use crate::catalog::{InputCollectionHandle, RecordFormat, SerBatchReader};
use crate::{
    ControllerError, OutputConsumer, SerCursor,
    format::{Encoder, InputFormat, OutputFormat, ParseError, Parser},
};

#[cfg(any(test, feature = "testing"))]
pub mod tests;

/// The column that carries the weight of each row of a stream.
pub const WEIGHT_COLUMN: &str = "__feldera_weight";

/// The format takes no options.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ArrowIpcConfig {}

/// The `arrow_ipc` input format.
pub struct ArrowIpcInputFormat;

impl InputFormat for ArrowIpcInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("arrow_ipc")
    }

    fn config_from_http_query(
        &self,
        _endpoint_name: &str,
        _query: &str,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(ArrowIpcConfig::default()))
    }

    fn new_parser(
        &self,
        endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        config: &serde_json::Value,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        if !config.is_null() {
            ArrowIpcConfig::deserialize(config).map_err(|e| {
                ControllerError::parser_config_parse_error(endpoint_name, &e, &config.to_string())
            })?;
        }
        let stream = input_stream
            .handle
            .configure_arrow_deserializer(default_arrow_serde_config().clone())
            .map_err(|e| {
                ControllerError::input_format_not_supported(
                    endpoint_name,
                    &format!("cannot configure an Arrow deserializer for this table: {e}"),
                )
            })?;
        Ok(Box::new(ArrowIpcParser {
            input_stream: stream,
        }))
    }
}

struct ArrowIpcParser {
    /// The stream that decoded rows are pushed to.
    input_stream: Box<dyn ArrowStream>,
}

impl Parser for ArrowIpcParser {
    fn parse(
        &mut self,
        data: &[u8],
        metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut errors = Vec::new();
        // The reader reads `data` without a buffer, so after a stream's end marker the cursor
        // stands at the start of the next stream.
        let mut cursor = Cursor::new(data);
        let mut index = 0;
        while (cursor.position() as usize) < data.len() {
            let reader = match StreamReader::try_new(&mut cursor, None) {
                Ok(reader) => reader,
                Err(e) => {
                    let error = format!("error parsing an Arrow IPC stream: {e}");
                    errors.push(ParseError::bin_envelope_error(error, &[], None));
                    break;
                }
            };
            for batch in reader {
                // A stream that does not decode cannot be trusted past the error.
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        let error = format!("error parsing arrow_ipc data (batch {index}): {e}");
                        errors.push(ParseError::bin_envelope_error(error, &[], None));
                        break;
                    }
                };
                let pushed = expand_weights(&batch).and_then(|(rows, polarities)| {
                    self.input_stream
                        .insert_with_polarities(&rows, &polarities, &metadata)
                        .map_err(|e| e.to_string())
                });
                if let Err(e) = pushed {
                    let error = format!("error parsing arrow_ipc data (batch {index}): {e}");
                    errors.push(ParseError::bin_envelope_error(error, &[], None));
                }
                index += 1;
            }
        }
        (self.input_stream.take_all(), errors)
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.input_stream.stage(buffers)
    }

    fn splitter(&self) -> Box<dyn super::Splitter> {
        Box::new(SpongeSplitter)
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self {
            input_stream: self.input_stream.fork(),
        })
    }
}

/// Returns `batch` without its weight column, each row repeated `|weight|` times, and
/// whether each repeated row is an insert. A zero weight drops its row; a batch without a
/// weight column is all inserts.
fn expand_weights(batch: &RecordBatch) -> Result<(RecordBatch, Vec<bool>), String> {
    let Ok(weight_index) = batch.schema().index_of(WEIGHT_COLUMN) else {
        return Ok((batch.clone(), vec![true; batch.num_rows()]));
    };
    let weights = batch
        .column(weight_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            format!(
                "column '{WEIGHT_COLUMN}' is {}, not Int64",
                batch.column(weight_index).data_type()
            )
        })?;
    if weights.null_count() > 0 {
        return Err(format!("column '{WEIGHT_COLUMN}' holds a null weight"));
    }

    // The rows are expanded in memory, so the total is capped before anything is allocated.
    // The format serves tests; a cap is simpler than pushing weights into the table.
    let total_copies = weights
        .values()
        .iter()
        .try_fold(0u64, |total, weight| {
            total.checked_add(weight.unsigned_abs())
        })
        .filter(|&total| total <= i32::MAX as u64)
        .ok_or_else(|| format!("the batch's weights expand to more than {} rows", i32::MAX))?;
    let mut indices = Vec::with_capacity(total_copies as usize);
    let mut polarities = Vec::with_capacity(total_copies as usize);
    for (row, &weight) in weights.values().iter().enumerate() {
        let copies = weight.unsigned_abs() as usize;
        indices.extend(repeat_n(row as u32, copies));
        polarities.extend(repeat_n(weight > 0, copies));
    }

    let columns = (0..batch.num_columns())
        .filter(|&index| index != weight_index)
        .collect::<Vec<_>>();
    let unweighted = batch.project(&columns).map_err(|e| e.to_string())?;
    let rows =
        take_record_batch(&unweighted, &UInt32Array::from(indices)).map_err(|e| e.to_string())?;
    Ok((rows, polarities))
}

/// The `arrow_ipc` output format.
pub struct ArrowIpcOutputFormat;

impl OutputFormat for ArrowIpcOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("arrow_ipc")
    }

    fn config_from_http_query(
        &self,
        _endpoint_name: &str,
        _query: &str,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(ArrowIpcConfig::default()))
    }

    fn new_encoder(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
        is_index: bool,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        // A stream is a flat table of weighted rows: it has no key half to write an
        // index's tombstone into.
        if is_index {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "'arrow_ipc' encoder cannot be attached to an index",
            ));
        }
        if let Some(format) = &config.format
            && !format.config.is_null()
        {
            ArrowIpcConfig::deserialize(&format.config).map_err(|e| {
                ControllerError::encoder_config_parse_error(
                    endpoint_name,
                    &e,
                    &serde_json::to_string(config).unwrap_or_default(),
                )
            })?;
        }
        let row_schema = Arc::new(Schema::new(relation_to_arrow_fields(
            &value_schema.fields,
            ArrowSchemaOptions::new(false),
        )));
        let mut fields = row_schema.fields().to_vec();
        fields.push(Arc::new(ArrowField::new(
            WEIGHT_COLUMN,
            DataType::Int64,
            false,
        )));
        Ok(Box::new(ArrowIpcEncoder {
            consumer,
            row_schema,
            stream_schema: Arc::new(Schema::new(fields)),
            input_is_indexed: key_schema.is_some(),
            buffer: Vec::new(),
        }))
    }
}

struct ArrowIpcEncoder {
    consumer: Box<dyn OutputConsumer>,
    /// The relation's published columns, which the cursor renders.
    row_schema: SchemaRef,
    /// The row columns and the weight.
    stream_schema: SchemaRef,
    /// The stream is an indexed Z-set (the connector is not an index): flatten the
    /// (key, value) cursor to one record level.
    input_is_indexed: bool,
    /// The stream being written, reused across steps.
    buffer: Vec<u8>,
}

impl Encoder for ArrowIpcEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.consumer.as_mut()
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let cursor = batch.cursor(RecordFormat::Parquet(default_arrow_serde_config().clone()))?;
        let mut cursor: Box<dyn SerCursor + '_> = if self.input_is_indexed {
            Box::new(SerCursorFlattened::new(cursor))
        } else {
            cursor
        };
        let mut rows = cursor.arrow_sink(self.row_schema.clone())?;
        let mut weights = Vec::new();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let weight = cursor.weight();
                if weight != 0 {
                    cursor.append_key_to_arrow(rows.as_mut())?;
                    weights.push(weight);
                }
                cursor.step_val();
            }
            cursor.step_key()?;
        }
        if weights.is_empty() {
            return Ok(());
        }

        let mut columns = rows.finish()?.columns().to_vec();
        columns.push(Arc::new(Int64Array::from(weights)) as ArrayRef);
        let stream = RecordBatch::try_new(self.stream_schema.clone(), columns)?;
        self.buffer.clear();
        let mut writer = StreamWriter::try_new(&mut self.buffer, &self.stream_schema)?;
        writer.write(&stream)?;
        writer.finish()?;
        drop(writer);
        self.consumer.push_buffer(&self.buffer, stream.num_rows());
        Ok(())
    }
}
