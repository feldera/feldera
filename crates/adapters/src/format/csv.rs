use crate::{
    catalog::{CursorWithPolarity, DeCollectionStream, RecordFormat, SerCursor},
    format::{Encoder, InputFormat, OutputFormat, ParseError, Parser},
    util::{split_on_newline, truncate_ellipse},
    ControllerError, OutputConsumer,
};
use actix_web::HttpRequest;
use anyhow::{bail, Result as AnyResult};
use csv_core::{ReadRecordResult, Reader as CsvReader};
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::format::csv::{CsvEncoderConfig, CsvParserConfig};
use serde::Deserialize;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::{borrow::Cow, mem::take};

pub(crate) mod deserializer;
use crate::catalog::{InputCollectionHandle, SerBatchReader};
pub use deserializer::byte_record_deserializer;
pub use deserializer::string_record_deserializer;
use feldera_types::program_schema::Relation;
use serde_yaml::Value as YamlValue;

/// When including a long CSV record in an error message,
/// truncate it to `MAX_RECORD_LEN_IN_ERRMSG` bytes.
static MAX_RECORD_LEN_IN_ERRMSG: usize = 4096;

/// CSV format parser.
pub struct CsvInputFormat;

impl InputFormat for CsvInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("csv")
    }

    /// Create a parser using configuration extracted from an HTTP request.
    // We could just rely on serde to deserialize the config from the
    // HTTP query, but a specialized method gives us more flexibility.
    fn config_from_http_request(
        &self,
        _endpoint_name: &str,
        _request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(CsvParserConfig {}))
    }

    fn new_parser(
        &self,
        _endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        _config: &YamlValue,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let input_stream = input_stream
            .handle
            .configure_deserializer(RecordFormat::Csv)?;
        Ok(Box::new(CsvParser::new(input_stream)) as Box<dyn Parser>)
    }
}

struct CsvParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionStream>,

    /// Since we cannot assume that the input buffer ends on line end,
    /// we save the "leftover" part of the buffer after the last new-line
    /// character and prepend it to the next input buffer.
    leftover: Vec<u8>,

    last_event_number: u64,
}

impl CsvParser {
    fn new(input_stream: Box<dyn DeCollectionStream>) -> Self {
        Self {
            input_stream,
            leftover: Vec::new(),
            last_event_number: 0,
        }
    }

    fn parse_from_buffer(&mut self, mut buffer: &[u8]) -> (usize, Vec<ParseError>) {
        let mut errors = Vec::new();
        let mut num_records = 0;

        let mut csv_reader = CsvReader::new();

        // println!("parse_from_buffer:{}", std::str::from_utf8(buffer).unwrap());

        let mut output = vec![0u8; 1024];
        let mut ends = [0usize; 128];

        let mut total_bytes_read = 0;
        let mut record_buffer = buffer;
        loop {
            let (result, mut bytes_read, _, _) =
                csv_reader.read_record(buffer, &mut output, &mut ends);
            total_bytes_read += bytes_read;
            match result {
                ReadRecordResult::End => break,
                // `InputEmpty` status can be returned when there is no newline character in
                // the end of the last input record, which isn't really an error.  So we treat
                // it as success and leave it to the actual record parser to deal with possible
                // invalid CSV (our job here is simply to establish record boundaries).
                ReadRecordResult::Record | ReadRecordResult::InputEmpty => {
                    /*println!(
                        "result: {result:?}, record: '{}', bytes: {:?}",
                        std::str::from_utf8(&record_buffer[0..total_bytes_read])
                            .unwrap_or("invalid utf-8"),
                        &record_buffer[0..total_bytes_read],
                    );*/
                    match self
                        .input_stream
                        .insert(&record_buffer[0..total_bytes_read])
                    {
                        Err(e) => {
                            errors.push(ParseError::text_event_error(
                                "failed to deserialize CSV record",
                                e,
                                self.last_event_number + 1,
                                Some(
                                    &std::str::from_utf8(&record_buffer[0..total_bytes_read])
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|_| {
                                            format!("{:?}", &record_buffer[0..total_bytes_read])
                                        })
                                        .to_string(),
                                ),
                                None,
                            ));
                        }
                        Ok(()) => {
                            num_records += 1;
                        }
                    }
                    // Lines ending in "\r\n" get broken up after `\r` by the parser.
                    // Consume the remaining `\n`; otherwise it gets prepended to the
                    // next buffer.
                    if buffer.len() > bytes_read && buffer[bytes_read] == b'\n' {
                        bytes_read += 1;
                    }
                    record_buffer = &buffer[bytes_read..];
                    self.last_event_number += 1;
                    total_bytes_read = 0;
                    if result == ReadRecordResult::InputEmpty {
                        break;
                    }
                }
                ReadRecordResult::OutputFull | ReadRecordResult::OutputEndsFull => {}
            }

            buffer = &buffer[bytes_read..];
        }

        self.input_stream.flush();
        (num_records, errors)
    }
}

impl Parser for CsvParser {
    fn input_fragment(&mut self, data: &[u8]) -> (usize, Vec<ParseError>) {
        /*println!(
            "input bytes:{} data:\n{}",
            data.len(),
            std::str::from_utf8(data)
                .map(|s| s.to_string())
                .unwrap_or_else(|e| format!("invalid csv: {e}"))
        );*/

        let leftover = split_on_newline(data);

        // println!("leftover: {leftover}");

        if leftover == 0 {
            // `data` doesn't contain a new-line character; append it to
            // the `leftover` buffer so it gets processed with the next input
            // buffer.
            self.leftover.extend_from_slice(data);
            (0, Vec::new())
        } else {
            let mut leftover_buf = take(&mut self.leftover);
            leftover_buf.extend_from_slice(&data[0..leftover]);

            let res = self.parse_from_buffer(leftover_buf.as_slice());
            // println!("parse returned: {res:?}");

            leftover_buf.clear();
            leftover_buf.extend_from_slice(&data[leftover..]);
            self.leftover = leftover_buf;

            res
        }
    }

    fn eoi(&mut self) -> (usize, Vec<ParseError>) {
        if self.leftover.is_empty() {
            return (0, Vec::new());
        }

        // Try to interpret the leftover chunk as a complete CSV line.
        let mut leftover_buf = take(&mut self.leftover);
        let res = self.parse_from_buffer(leftover_buf.as_slice());
        leftover_buf.clear();
        self.leftover = leftover_buf;
        res
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(self.input_stream.fork()))
    }
}

/// CSV format encoder.
pub struct CsvOutputFormat;

impl OutputFormat for CsvOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("csv")
    }

    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            CsvEncoderConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
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
        _schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        let config = CsvEncoderConfig::deserialize(config).map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &e,
                &serde_yaml::to_string(&config).unwrap_or_default(),
            )
        })?;

        Ok(Box::new(CsvEncoder::new(consumer, config)))
    }
}

struct CsvEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,

    config: CsvEncoderConfig,
    buffer: Vec<u8>,
    max_buffer_size: usize,
}

impl CsvEncoder {
    fn new(output_consumer: Box<dyn OutputConsumer>, config: CsvEncoderConfig) -> Self {
        let max_buffer_size = output_consumer.max_buffer_size_bytes();

        Self {
            output_consumer,
            config,
            buffer: Vec::new(),
            max_buffer_size,
        }
    }
}

impl Encoder for CsvEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()> {
        let mut buffer = take(&mut self.buffer);
        //let mut writer = self.builder.from_writer(buffer);
        let mut num_records = 0;

        let mut cursor = CursorWithPolarity::new(batch.cursor(RecordFormat::Csv)?);

        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key();
                continue;
            }
            let prev_len = buffer.len();

            // `serialize_key_weight`
            cursor.serialize_key_weight(&mut buffer)?;

            // Drop the last encoded record if it exceeds max_buffer_size.
            // The record will be included in the next buffer.
            let new_len = buffer.len();
            let overflow = if new_len > self.max_buffer_size {
                if num_records == 0 {
                    let record =
                        std::str::from_utf8(&buffer[prev_len..new_len]).unwrap_or_default();
                    // We should be able to fit at least one record in the buffer.
                    bail!("CSV record exceeds maximum buffer size supported by the output transport. Max supported buffer size is {} bytes, but the following record requires {} bytes: '{}'.",
                              self.max_buffer_size,
                              new_len - prev_len,
                              truncate_ellipse(record, MAX_RECORD_LEN_IN_ERRMSG, "..."));
                }
                true
            } else {
                num_records += 1;
                false
            };

            if num_records >= self.config.buffer_size_records || overflow {
                if overflow {
                    buffer.truncate(prev_len);
                }
                // println!("push_buffer {}", buffer.len()
                // /*std::str::from_utf8(&buffer).unwrap()*/);
                self.output_consumer.push_buffer(&buffer, num_records);
                buffer.clear();
                num_records = 0;
            }

            if !overflow {
                cursor.step_key();
            }
        }

        if num_records > 0 {
            self.output_consumer.push_buffer(&buffer, num_records);
            buffer.clear();
        }

        self.buffer = buffer;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::format::string_record_deserializer;
    use feldera_types::deserialize_table_record;
    use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};

    #[derive(Debug, Eq, PartialEq)]
    #[allow(non_snake_case)]
    struct CaseSensitive {
        fIeLd1: bool,
        field2: String,
        field3: Option<u8>,
    }

    deserialize_table_record!(CaseSensitive["CaseSensitive", 3] {
    (fIeLd1, "fIeLd1", true, bool, None),
    (field2, "field2", true, String, None),
    (field3, "field3", false, Option<u8>, Some(None))
    });

    #[test]
    fn csv() {
        let data = r#"true,"foo",5
true,bar,buzz"#;
        let rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        let mut records = rdr.into_records();
        assert_eq!(
            CaseSensitive::deserialize_with_context(
                &mut string_record_deserializer(&records.next().unwrap().unwrap(), None),
                &SqlSerdeConfig::default()
            )
            .unwrap(),
            CaseSensitive {
                fIeLd1: true,
                field2: "foo".to_string(),
                field3: Some(5)
            }
        );
        assert_eq!(
            CaseSensitive::deserialize_with_context(
                &mut string_record_deserializer(&records.next().unwrap().unwrap(), None),
                &SqlSerdeConfig::default()
            )
            .map_err(|e| e.to_string()),
            Err(
                r#"{"field":"field3","description":"field 2: invalid digit found in string"}"#
                    .to_string()
            )
        );
    }
}
