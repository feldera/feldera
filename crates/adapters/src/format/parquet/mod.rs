use std::io::Cursor;
use std::mem::take;
use std::{borrow::Cow, sync::Arc};

use actix_web::HttpRequest;
use anyhow::{bail, Result as AnyResult};
use arrow::datatypes::{
    DataType, Field as ArrowField, Fields, IntervalUnit as ArrowIntervalUnit, Schema, TimeUnit,
};
use bytes::Bytes;
use erased_serde::Serialize as ErasedSerialize;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Deserialize;
use serde_arrow::schema::SerdeArrowSchema;
use serde_arrow::ArrayBuilder;
use serde_urlencoded::Deserializer as UrlDeserializer;
use serde_yaml::Value as YamlValue;

use crate::catalog::{CursorWithPolarity, SerBatchReader};
use crate::format::MAX_DUPLICATES;
use crate::{
    catalog::{DeCollectionStream, InputCollectionHandle, RecordFormat},
    format::{Encoder, InputFormat, OutputFormat, ParseError, Parser},
    ControllerError, OutputConsumer, SerCursor,
};
use pipeline_types::format::json::JsonFlavor;
use pipeline_types::format::parquet::{ParquetEncoderConfig, ParquetParserConfig};
use pipeline_types::program_schema::{ColumnType, Field, IntervalUnit, Relation, SqlType};

#[cfg(test)]
pub mod test;

/// CSV format parser.
pub struct ParquetInputFormat;

impl InputFormat for ParquetInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("parquet")
    }

    /// Create a parser using configuration extracted from an HTTP request.
    // We could just rely on serde to deserialize the config from the
    // HTTP query, but a specialized method gives us more flexibility.
    fn config_from_http_request(
        &self,
        _endpoint_name: &str,
        _request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(ParquetParserConfig {}))
    }

    fn new_parser(
        &self,
        _endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        _config: &YamlValue,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let input_stream = input_stream
            .handle
            .configure_deserializer(RecordFormat::Json(JsonFlavor::ParquetConverter))?;
        Ok(Box::new(ParquetParser::new(input_stream)) as Box<dyn Parser>)
    }
}

struct ParquetParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionStream>,
    buf: Vec<u8>,
    last_event_number: u64,
}

impl ParquetParser {
    fn new(input_stream: Box<dyn DeCollectionStream>) -> Self {
        Self {
            input_stream,
            buf: Vec::with_capacity(4096),
            last_event_number: 0,
        }
    }
    fn parse(&mut self) -> (usize, Vec<ParseError>) {
        if self.buf.is_empty() {
            return (0, vec![]);
        }

        let bytes = Bytes::from(take(&mut self.buf));
        match SerializedFileReader::new(bytes) {
            Ok(reader) => {
                let (mut cnt, mut errors) = (0, vec![]);
                match reader.get_row_iter(None) {
                    Ok(iter) => {
                        for maybe_record in iter {
                            match maybe_record {
                                Ok(record) => {
                                    // TODO: this is a temporary solution (parquet->json->feldera) to avoid
                                    // the overhead of converting the record to JSON we can use serde_arrow
                                    // as well here.
                                    let record_json = record.to_json_value().to_string();
                                    match self.input_stream.insert(record_json.as_bytes()) {
                                        Ok(_) => cnt += 1,
                                        Err(e) => {
                                            errors.push(ParseError::bin_event_error(
                                                format!(
                                                    "Error parsing JSON record from parquet file: {}",
                                                    e
                                                ),
                                                self.last_event_number + 1,
                                                record_json.as_bytes(),
                                                None,
                                            ));
                                        }
                                    }
                                }
                                Err(e) => {
                                    errors.push(ParseError::bin_event_error(
                                        format!("Error reading a record from parquet file: {}", e),
                                        self.last_event_number + 1,
                                        &[],
                                        None,
                                    ));
                                }
                            }
                            self.last_event_number += 1;
                        }
                        self.input_stream.flush();
                        (cnt, errors)
                    }
                    Err(e) => (
                        0,
                        vec![ParseError::bin_envelope_error(
                            format!("Unable to iterate over parquet file: {}.", e),
                            &[],
                            None,
                        )],
                    ),
                }
            }
            Err(e) => (
                0,
                vec![ParseError::bin_envelope_error(
                    format!("Unable to read parquet file: {}.", e),
                    &[],
                    Some(Cow::from(
                        "Make sure the provided file is a valid parquet file.",
                    )),
                )],
            ),
        }
    }
}

impl Parser for ParquetParser {
    /// In the fragment case, we will wait until eoi() is called to parse any data.
    ///
    /// Happens for example with the file connector.
    fn input_fragment(&mut self, data: &[u8]) -> (usize, Vec<ParseError>) {
        self.buf.extend_from_slice(data);
        (0, vec![])
    }

    /// In the chunk case, we got an entire file in `data` and parse it immediately.
    fn input_chunk(&mut self, data: &[u8]) -> (usize, Vec<ParseError>) {
        self.buf.extend_from_slice(data);
        self.parse()
    }

    fn eoi(&mut self) -> (usize, Vec<ParseError>) {
        self.parse()
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(self.input_stream.fork()))
    }
}

/// CSV format encoder.
pub struct ParquetOutputFormat;

impl OutputFormat for ParquetOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("parquet")
    }

    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            ParquetEncoderConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
                request.query_string().as_bytes(),
            )))
            .map_err(|e| {
                ControllerError::encoder_config_parse_error(
                    endpoint_name,
                    &e,
                    request.query_string(),
                )
            })?,
        ))
    }

    fn new_encoder(
        &self,
        endpoint_name: &str,
        config: &YamlValue,
        schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        let config = ParquetEncoderConfig::deserialize(config).map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &e,
                &serde_yaml::to_string(&config).unwrap_or_default(),
            )
        })?;
        Ok(Box::new(ParquetEncoder::new(
            consumer,
            config,
            schema.clone(),
        )?))
    }
}

pub fn relation_to_arrow_fields(fields: &[Field], delta_lake: bool) -> Vec<ArrowField> {
    fn field_to_arrow_field(f: &Field, delta_lake: bool) -> ArrowField {
        ArrowField::new(
            &f.name,
            columntype_to_datatype(&f.columntype, delta_lake),
            // FIXME: Databricks refuses to understand the `nullable: false` constraint.
            delta_lake || f.columntype.nullable,
        )
    }

    fn struct_to_arrow_fields(fields: &[Field], delta_lake: bool) -> Fields {
        Fields::from(
            fields
                .iter()
                .map(|f| field_to_arrow_field(f, delta_lake))
                .collect::<Vec<ArrowField>>(),
        )
    }

    // The type conversion is chosen in accordance with our internal
    // data types (see sqllib). This may need to be adjusted in the future
    // or made configurable.
    fn columntype_to_datatype(c: &ColumnType, delta_lake: bool) -> DataType {
        match c.typ {
            SqlType::Boolean => DataType::Boolean,
            SqlType::TinyInt => DataType::Int8,
            SqlType::SmallInt => DataType::Int16,
            SqlType::Int => DataType::Int32,
            SqlType::BigInt => DataType::Int64,
            SqlType::Real => DataType::Float32,
            SqlType::Double => DataType::Float64,
            SqlType::Decimal => DataType::Decimal128(
                c.precision.unwrap_or(0).try_into().unwrap(),
                c.scale.unwrap_or(0).try_into().unwrap(),
            ),
            SqlType::Char | SqlType::Varchar => DataType::Utf8,
            SqlType::Time => DataType::Time64(TimeUnit::Nanosecond),
            // DeltaLake only supports microsecond-based timestamp encoding, so we just
            // hardwire that for now.  We can make it configurable in the future.
            // FIXME: Also, the timezone should be `None`, but that gets compiled into `timezone_ntz`
            // in the JSON schema, which Databricks doesn't fully support yet.
            SqlType::Timestamp => DataType::Timestamp(
                TimeUnit::Microsecond,
                if delta_lake { Some("UTC".into()) } else { None },
            ),
            SqlType::Date => DataType::Date32,
            SqlType::Null => DataType::Null,
            SqlType::Binary => DataType::LargeBinary,
            SqlType::Varbinary => DataType::LargeBinary,
            SqlType::Interval(
                IntervalUnit::YearToMonth | IntervalUnit::Year | IntervalUnit::Month,
            ) => DataType::Interval(ArrowIntervalUnit::YearMonth),
            SqlType::Interval(_) => DataType::Interval(ArrowIntervalUnit::DayTime),
            SqlType::Array => {
                // SqlType::Array implies c.component.is_some()
                let array_component = c.component.as_ref().unwrap();
                DataType::LargeList(Arc::new(ArrowField::new_list_field(
                    columntype_to_datatype(array_component, delta_lake),
                    // FIXME: Databricks refuses to understand the `nullable: false` constraint.
                    delta_lake || c.nullable,
                )))
            }
            SqlType::Struct => DataType::Struct(struct_to_arrow_fields(
                c.fields.as_ref().unwrap(),
                delta_lake,
            )),
        }
    }

    fields
        .iter()
        .map(|f| field_to_arrow_field(f, delta_lake))
        .collect::<Vec<ArrowField>>()
}

pub fn relation_to_parquet_schema(
    fields: &[Field],
    delta_lake: bool,
) -> Result<SerdeArrowSchema, ControllerError> {
    let fields = relation_to_arrow_fields(fields, delta_lake);

    SerdeArrowSchema::from_arrow_fields(&fields).map_err(|e| ControllerError::SchemaParseError {
        error: format!("Unable to convert schema to parquet/arrow: {e}"),
    })
}

struct ParquetEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,
    _relation: Relation,
    parquet_schema: SerdeArrowSchema,
    config: ParquetEncoderConfig,
    buffer: Vec<u8>,
    max_buffer_size: usize,
}

impl ParquetEncoder {
    fn new(
        output_consumer: Box<dyn OutputConsumer>,
        config: ParquetEncoderConfig,
        _relation: Relation,
    ) -> Result<Self, ControllerError> {
        let max_buffer_size = output_consumer.max_buffer_size_bytes();
        Ok(Self {
            output_consumer,
            config,
            parquet_schema: relation_to_parquet_schema(&_relation.fields, false)?,
            _relation,
            buffer: Vec::new(),
            max_buffer_size,
        })
    }
}

impl Encoder for ParquetEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let mut buffer = take(&mut self.buffer);
        let props = WriterProperties::builder().build();
        let schema = Arc::new(Schema::new(self.parquet_schema.to_arrow_fields()?));
        let mut builder = ArrayBuilder::new(self.parquet_schema.clone())?;

        let mut num_records = 0;
        let mut cursor = CursorWithPolarity::new(
            batch.cursor(RecordFormat::Parquet(self.parquet_schema.clone()))?,
        );
        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key();
                continue;
            }
            let mut w = cursor.weight();
            if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&w) {
                bail!("Unable to output record with very large weight {w}. Consider adjusting your SQL queries to avoid duplicate output records, e.g., using 'SELECT DISTINCT'.");
            }
            if w < 0 {
                // TODO: we don't support deletes in the parquet format yet.
                continue;
            }

            while w != 0 {
                let prev_len = buffer.len();
                cursor.serialize_key_to_arrow(&mut builder)?;

                // TODO: buffer.len() is always 0 here atm:
                let buffer_full = buffer.len() > self.max_buffer_size;
                if buffer_full {
                    if num_records == 0 {
                        // We should be able to fit at least one record in the buffer.
                        bail!("Parquet record exceeds maximum buffer size supported by the output transport. Max supported buffer size is {} bytes, but the record requires {} bytes.",
                                  self.max_buffer_size,
                                  buffer.len() - prev_len);
                    }
                    buffer.truncate(prev_len);
                } else {
                    if w > 0 {
                        w -= 1;
                    } else {
                        w += 1;
                    }
                    num_records += 1;
                }

                if num_records >= self.config.buffer_size_records || buffer_full {
                    let buffer_cursor = Cursor::new(&mut buffer);
                    let mut writer =
                        ArrowWriter::try_new(buffer_cursor, schema.clone(), Some(props.clone()))?;
                    let batch = builder.to_record_batch()?;
                    writer.write(&batch)?;
                    writer.close()?;

                    self.output_consumer.push_buffer(&buffer, num_records);
                    buffer.clear();

                    num_records = 0;
                }
            }
            cursor.step_key();
        }

        if num_records > 0 {
            let buffer_cursor = Cursor::new(&mut buffer);
            let mut writer =
                ArrowWriter::try_new(buffer_cursor, schema.clone(), Some(props.clone()))?;
            let batch = builder.to_record_batch()?;
            writer.write(&batch)?;
            writer.close()?;
            self.output_consumer.push_buffer(&buffer, num_records);
            buffer.clear();
        }

        self.buffer = buffer;
        Ok(())
    }
}
