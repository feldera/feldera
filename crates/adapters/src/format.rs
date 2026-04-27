use crate::format::parquet::{ParquetInputFormat, ParquetOutputFormat};
#[cfg(feature = "with-avro")]
use avro::input::AvroInputFormat;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;

#[cfg(feature = "with-avro")]
pub mod avro;
pub(crate) mod csv;
pub(crate) mod json;
pub mod parquet;
pub(crate) mod raw;

#[cfg(feature = "with-avro")]
use crate::format::avro::output::AvroOutputFormat;
pub use parquet::relation_to_parquet_schema;

pub use self::csv::{byte_record_deserializer, string_record_deserializer};
use self::{
    csv::{CsvInputFormat, CsvOutputFormat},
    json::{JsonInputFormat, JsonOutputFormat},
    raw::RawInputFormat,
};

pub use feldera_adapterlib::format::*;

/// Static map of supported input formats.
// TODO: support for registering new formats at runtime in order to allow
// external crates to implement new formats.
static INPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn InputFormat>>> = Lazy::new(|| {
    BTreeMap::from([
        ("csv", Box::new(CsvInputFormat) as Box<dyn InputFormat>),
        ("json", Box::new(JsonInputFormat) as Box<dyn InputFormat>),
        (
            "parquet",
            Box::new(ParquetInputFormat) as Box<dyn InputFormat>,
        ),
        #[cfg(feature = "with-avro")]
        ("avro", Box::new(AvroInputFormat) as Box<dyn InputFormat>),
        ("raw", Box::new(RawInputFormat) as Box<dyn InputFormat>),
    ])
});

pub fn get_input_format(name: &str) -> Option<&'static dyn InputFormat> {
    INPUT_FORMATS.get(name).map(|f| &**f)
}

/// Static map of supported output formats.
static OUTPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn OutputFormat>>> = Lazy::new(|| {
    BTreeMap::from([
        ("csv", Box::new(CsvOutputFormat) as Box<dyn OutputFormat>),
        ("json", Box::new(JsonOutputFormat) as Box<dyn OutputFormat>),
        (
            "parquet",
            Box::new(ParquetOutputFormat) as Box<dyn OutputFormat>,
        ),
        #[cfg(feature = "with-avro")]
        ("avro", Box::new(AvroOutputFormat) as Box<dyn OutputFormat>),
    ])
});

pub fn get_output_format(name: &str) -> Option<&'static dyn OutputFormat> {
    OUTPUT_FORMATS.get(name).map(|f| &**f)
}

/// A [Splitter] that never breaks data into records.
///
/// This supports [Parser]s that need all of a streaming data source to be read
/// in full before parsing.
pub struct SpongeSplitter;

impl Splitter for SpongeSplitter {
    fn input(&mut self, _data: &[u8]) -> Option<usize> {
        None
    }
    fn clear(&mut self) {}
}

/// A [Splitter] that breaks data at ASCII new-lines.
///
/// If the presented input data contains multiple complete lines, then this
/// splitter will group them all into one chunk.
pub struct LineSplitter;

impl Splitter for LineSplitter {
    fn input(&mut self, data: &[u8]) -> Option<usize> {
        // We search backward here to find as many complete lines as we can.
        data.iter()
            .rposition(|b| *b == b'\n')
            .map(|position| position + 1)
    }

    fn clear(&mut self) {}
}
