use crate::{
    format::{Encoder, InputFormat, OutputFormat, Parser},
    util::split_on_newline,
    DeCollectionHandle, OutputConsumer, SerBatch,
};
#[cfg(feature = "server")]
use actix_web::HttpRequest;
use anyhow::{Error as AnyError, Result as AnyResult};
use csv::{
    Reader as CsvReader, ReaderBuilder as CsvReaderBuilder, WriterBuilder as CsvWriterBuilder,
};
use erased_serde::{Deserializer as ErasedDeserializer, Serialize as ErasedSerialize};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, io::Read, mem::take, sync::Arc};
use utoipa::ToSchema;

mod deserializer;
pub use deserializer::byte_record_deserializer;
pub use deserializer::string_record_deserializer;

/// CSV format parser.
pub struct CsvInputFormat;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct CsvParserConfig {}

impl InputFormat for CsvInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("csv")
    }

    #[cfg(feature = "server")]
    /// Create a parser using configuration extracted from an HTTP request.
    // We could just rely on serde to deserialize the config from the
    // HTTP query, but a specialized method gives us more flexibility.
    fn config_from_http_request(
        &self,
        _request: &HttpRequest,
    ) -> AnyResult<Box<dyn ErasedSerialize>> {
        Ok(Box::new(CsvParserConfig {}))
    }

    fn new_parser(
        &self,
        input_stream: &dyn DeCollectionHandle,
        _config: &mut dyn ErasedDeserializer,
    ) -> AnyResult<Box<dyn Parser>> {
        Ok(Box::new(CsvParser::new(input_stream)) as Box<dyn Parser>)
    }
}

struct CsvParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionHandle>,

    /// Since we cannot assume that the input buffer ends on line end,
    /// we save the "leftover" part of the buffer after the last new-line
    /// character and prepend it to the next input buffer.
    leftover: Vec<u8>,

    /// Builder used to create a new CSV reader for each received data
    /// buffer.
    builder: CsvReaderBuilder,
}

impl CsvParser {
    fn new(input_stream: &dyn DeCollectionHandle) -> Self {
        let mut builder = CsvReaderBuilder::new();
        builder.has_headers(false);

        Self {
            input_stream: input_stream.fork(),
            leftover: Vec::new(),
            builder,
        }
    }

    fn parse_from_reader<R>(
        input_stream: &mut dyn DeCollectionHandle,
        mut reader: CsvReader<R>,
    ) -> AnyResult<usize>
    where
        R: Read,
    {
        let mut num_records = 0;

        for record in reader.byte_records() {
            let record = record?;

            let mut deserializer = byte_record_deserializer(&record, None);
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            input_stream.insert(&mut deserializer).map_err(|e| {
                AnyError::msg(format!(
                    "failed to deserialize csv record '{record:?}': {e}"
                ))
            })?;
            num_records += 1;
        }

        Ok(num_records)
    }
}

impl Parser for CsvParser {
    fn input_fragment(&mut self, data: &[u8]) -> AnyResult<usize> {
        // println!("input {} bytes:\n{}\nself.leftover:\n{}", data.len(),
        //    std::str::from_utf8(data).map(|s| s.to_string()).unwrap_or_else(|e|
        // format!("invalid csv: {e}")),    std::str::from_utf8(&self.leftover).
        // map(|s| s.to_string()).unwrap_or_else(|e| format!("invalid csv: {e}")));

        let leftover = split_on_newline(data);

        // println!("leftover: {leftover}");

        if leftover == 0 {
            // `data` doesn't contain a new-line character; append it to
            // the `leftover` buffer so it gets processed with the next input
            // buffer.
            self.leftover.extend_from_slice(data);
            Ok(0)
        } else {
            let reader = self
                .builder
                .from_reader(Read::chain(&*self.leftover, &data[0..leftover]));

            let res = Self::parse_from_reader(&mut *self.input_stream, reader);
            // println!("parse returned: {res:?}");

            self.leftover.clear();
            self.leftover.extend_from_slice(&data[leftover..]);

            res
        }
    }

    fn eoi(&mut self) -> AnyResult<usize> {
        if self.leftover.is_empty() {
            return Ok(0);
        }

        // Try to interpret the leftover chunk as a complete CSV line.
        let reader = self.builder.from_reader(&*self.leftover);

        let res = Self::parse_from_reader(&mut *self.input_stream, reader);
        self.leftover.clear();
        res
    }

    fn flush(&mut self) {
        self.input_stream.flush();
    }

    fn clear(&mut self) {
        self.input_stream.clear_buffer();
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self::new(&*self.input_stream))
    }
}

/// CSV format encoder.
pub struct CsvOutputFormat;

const fn default_buffer_size_records() -> usize {
    10_000
}

#[derive(Deserialize, ToSchema)]
pub struct CsvEncoderConfig {
    #[serde(default = "default_buffer_size_records")]
    buffer_size_records: usize,
}

impl OutputFormat for CsvOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("csv")
    }

    fn new_encoder(
        &self,
        config: &mut dyn ErasedDeserializer,
        consumer: Box<dyn OutputConsumer>,
    ) -> AnyResult<Box<dyn Encoder>> {
        let config = CsvEncoderConfig::deserialize(config)?;

        Ok(Box::new(CsvEncoder::new(consumer, config)))
    }
}

struct CsvEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,

    /// Builder used to create a new CSV writer for each received data
    /// buffer.
    builder: CsvWriterBuilder,

    config: CsvEncoderConfig,

    buffer: Vec<u8>,
}

impl CsvEncoder {
    fn new(output_consumer: Box<dyn OutputConsumer>, config: CsvEncoderConfig) -> Self {
        let mut builder = CsvWriterBuilder::new();
        builder.has_headers(false);

        Self {
            output_consumer,
            builder,
            config,
            buffer: Vec::new(),
        }
    }
}

impl Encoder for CsvEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batches: &[Arc<dyn SerBatch>]) -> AnyResult<()> {
        let buffer = take(&mut self.buffer);
        let mut writer = self.builder.from_writer(buffer);
        let mut num_records = 0;

        for batch in batches.iter() {
            let mut cursor = batch.cursor();

            while cursor.key_valid() {
                let w = cursor.weight();
                writer.serialize((cursor.key(), w))?;
                num_records += 1;

                if num_records >= self.config.buffer_size_records {
                    let mut buffer = writer.into_inner()?;
                    // println!("push_buffer {}", std::str::from_utf8(&buffer).unwrap());
                    self.output_consumer.push_buffer(&buffer);
                    buffer.clear();
                    num_records = 0;
                    writer = self.builder.from_writer(buffer);
                }

                cursor.step_key();
            }
        }

        let mut buffer = writer.into_inner()?;

        if num_records > 0 {
            self.output_consumer.push_buffer(&buffer);
            buffer.clear();
        }

        self.buffer = buffer;

        Ok(())
    }
}
