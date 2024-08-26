use crate::{
    catalog::{CursorWithPolarity, DeCollectionStream, RecordFormat, SerCursor},
    format::{Encoder, InputFormat, OutputFormat, ParseError, Parser},
    util::truncate_ellipse,
    ControllerError, OutputConsumer,
};
use actix_web::HttpRequest;
use anyhow::{bail, Result as AnyResult};
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::{
    config::ConnectorConfig,
    format::csv::{CsvEncoderConfig, CsvParserConfig},
};
use serde::Deserialize;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::{borrow::Cow, mem::take};

pub(crate) mod deserializer;
use crate::catalog::{InputCollectionHandle, SerBatchReader};
pub use deserializer::byte_record_deserializer;
pub use deserializer::string_record_deserializer;
use feldera_types::program_schema::Relation;
use serde_yaml::Value as YamlValue;

use super::InputBuffer;

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

    /// Any bytes that haven't been found to be part of a full CSV record yet.
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

    fn parse_record(&mut self, record: &[u8], res: &mut (usize, Vec<ParseError>)) {
        match self.input_stream.insert(record).map_err(|e| {
            ParseError::text_event_error(
                "failed to deserialize CSV record",
                e,
                self.last_event_number + 1,
                Some(
                    &std::str::from_utf8(record)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| format!("{:?}", record))
                        .to_string(),
                ),
                None,
            )
        }) {
            Ok(()) => res.0 += 1,
            Err(error) => res.1.push(error),
        }
    }

    /// Tries to split `buffer` into a CSV record followed by any remaining text.
    ///
    /// A CSV record is usually one line that ends in a new-line, but it can
    /// span multiple lines if new-lines are enclosed in double quotes.
    fn split_record<'a>(&mut self, buffer: &'a [u8]) -> Option<(&'a [u8], &'a [u8])> {
        let mut quoted = false;
        for (offset, c) in buffer.iter().enumerate() {
            match c {
                b'"' => quoted = !quoted,
                b'\n' if !quoted => {
                    return Some(buffer.split_at(offset + 1));
                }
                _ => (),
            }
        }
        None
    }

    /// Parses `buffer` into a series of zero or more CSV records followed by
    /// any remaining text.  Returns the remainder and the results of parsing
    /// the records.
    fn parse_from_buffer<'a>(
        &mut self,
        mut buffer: &'a [u8],
    ) -> (&'a [u8], (usize, Vec<ParseError>)) {
        let mut res = (0, Vec::new());
        while let Some((record, rest)) = self.split_record(buffer) {
            self.parse_record(record, &mut res);
            self.last_event_number += 1;
            buffer = rest;
        }
        (buffer, res)
    }
}

impl Parser for CsvParser {
    fn input_fragment(&mut self, data: &[u8]) -> (usize, Vec<ParseError>) {
        if self.leftover.is_empty() {
            let (rest, res) = self.parse_from_buffer(data);
            self.leftover.extend(rest);
            res
        } else {
            self.leftover.extend_from_slice(data);
            let mut buffer = take(&mut self.leftover);
            let (rest, res) = self.parse_from_buffer(&buffer);
            buffer.drain(..buffer.len() - rest.len());
            self.leftover = buffer;
            res
        }
    }

    fn end_of_fragments(&mut self) -> (usize, Vec<ParseError>) {
        let mut res = (0, Vec::new());
        let leftover = take(&mut self.leftover);
        if !leftover.is_empty() {
            self.parse_record(leftover.as_slice(), &mut res);
        }
        res
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(self.input_stream.fork()))
    }
}

impl InputBuffer for CsvParser {
    fn flush(&mut self, n: usize) -> usize {
        self.input_stream.flush(n)
    }

    fn len(&self) -> usize {
        self.input_stream.len()
    }

    fn take(&mut self) -> Option<Box<dyn InputBuffer>> {
        self.input_stream.take()
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
        config: &ConnectorConfig,
        _schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        let config = CsvEncoderConfig::deserialize(&config.format.as_ref().unwrap().config)
            .map_err(|e| {
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
