use crate::{
    format::{InputFormat, Parser},
    Catalog, DeCollectionHandle,
};
use anyhow::{Error as AnyError, Result as AnyResult};
use csv::{byte_record_deserializer, Reader as CsvReader, ReaderBuilder as CsvReaderBuilder};
use erased_serde::Deserializer as ErasedDeserializer;
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    io::Read,
    sync::{Arc, Mutex},
};

/// CSV file format parser.
pub struct CsvInputFormat;

#[derive(Deserialize)]
struct CsvParserConfig {
    /// Input stream to feed parsed records to.
    input_stream: String,
}

impl InputFormat for CsvInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("csv")
    }

    fn new_parser(
        &self,
        config: &YamlValue,
        catalog: &Arc<Mutex<Catalog>>,
    ) -> AnyResult<Box<dyn Parser>> {
        let config = CsvParserConfig::deserialize(config)?;
        catalog
            .lock()
            .unwrap()
            .input_collection(&config.input_stream)
            .map(|stream| Box::new(CsvParser::new(stream)) as Box<dyn Parser>)
            .ok_or_else(|| AnyError::msg(format!("unknown stream '{}'", config.input_stream)))
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
            input_stream.insert(&mut deserializer)?;
            num_records += 1;
        }

        Ok(num_records)
    }

    /// Returns the index of the first character following the last newline
    /// in `data`.
    fn split_on_newline(data: &[u8]) -> usize {
        let data_len = data.len();
        let index = data
            .iter()
            .rev()
            .position(|&x| x == b'\n')
            .unwrap_or(data_len);

        data_len - index
    }
}

impl Parser for CsvParser {
    fn input(&mut self, data: &[u8]) -> AnyResult<usize> {
        // println!("input {} bytes: {}, self.leftover: {}", data.len(),
        // std::str::from_utf8(data).unwrap(),
        // std::str::from_utf8(&self.leftover).unwrap());

        let leftover = Self::split_on_newline(data);

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

    fn eof(&mut self) -> AnyResult<usize> {
        if self.leftover.is_empty() {
            return Ok(0);
        }

        // Try to interpret the leftover chunk as a complete CSV line.
        let reader = self.builder.from_reader(&*self.leftover);

        Self::parse_from_reader(&mut *self.input_stream, reader)
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
