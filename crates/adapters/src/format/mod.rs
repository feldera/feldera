use crate::{DeCollectionHandle, SerBatch};
use anyhow::Result as AnyResult;
use once_cell::sync::Lazy;
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

mod csv;

pub use self::csv::{CsvEncoderConfig, CsvParserConfig};
use self::csv::{CsvInputFormat, CsvOutputFormat};

/// Static map of supported input formats.
// TODO: support for registering new formats at runtime in order to allow
// external crates to implement new formats.
static INPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn InputFormat>>> =
    Lazy::new(|| BTreeMap::from([("csv", Box::new(CsvInputFormat) as Box<dyn InputFormat>)]));

/// Static map of supported output formats.
static OUTPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn OutputFormat>>> =
    Lazy::new(|| BTreeMap::from([("csv", Box::new(CsvOutputFormat) as Box<dyn OutputFormat>)]));

/// Trait that represents a specific data format.
///
/// This is a factory trait that creates parsers for a specific data format.
pub trait InputFormat: Send + Sync {
    /// Unique name of the data format.
    fn name(&self) -> Cow<'static, str>;

    /// Create a new parser for the format.
    ///
    /// # Arguments
    ///
    /// * `input_stream` - Input stream of the circuit to push parsed data to.
    ///
    /// * `config` - Format-specific configuration.
    fn new_parser(
        &self,
        input_stream: &dyn DeCollectionHandle,
        config: &YamlValue,
    ) -> AnyResult<Box<dyn Parser>>;
}

impl dyn InputFormat {
    /// Lookup input format by name.
    pub fn get_format(name: &str) -> Option<&'static dyn InputFormat> {
        INPUT_FORMATS.get(name).map(|f| &**f)
    }
}

/// Parser that converts a raw byte stream into a stream of database records.
pub trait Parser: Send {
    /// Push a chunk of data to the parser.
    ///
    /// The parser breaks `data` up into records and pushes these records
    /// to the circuit using the
    /// [`DeCollectionHandle`](`crate::DeCollectionHandle`) API.
    ///
    /// The parser must not buffer any data, except for any incomplete records
    /// that cannot be fully parsed until more data or an end-of-file
    /// notification is received.
    ///
    /// Returns the number of records in the parsed representation or an error
    /// if parsing fails.
    fn input(&mut self, data: &[u8]) -> AnyResult<usize>;

    /// End-of-input-stream notification.
    ///
    /// No more data will be received from the stream.  The parser uses this
    /// notification to complete or discard any incompletely parsed records.
    ///
    /// Returns the number of additional records pushed to the circuit or an
    /// error if parsing fails.
    fn eoi(&mut self) -> AnyResult<usize>;

    /// Flush input handles.
    ///
    /// The implementation must call
    /// [`DeCollectionHandle::flush()`](`crate::DeCollectionHandle::flush`) on
    /// all input handles modified by this parser.
    fn flush(&mut self);

    /// Clear input handles.
    ///
    /// The implementation must call
    /// [`DeCollectionHandle::clear_buffer()`](`crate::DeCollectionHandle::clear_buffer`)
    /// on all input handles modified by this parser.
    fn clear(&mut self);

    /// Create a new parser with the same configuration as `self`.
    ///
    /// Used by multithreaded transport endpoints to create multiple parallel
    /// input pipelines.
    fn fork(&self) -> Box<dyn Parser>;
}

pub trait OutputFormat: Send + Sync {
    /// Unique name of the data format.
    fn name(&self) -> Cow<'static, str>;

    /// Create a new encoder for the format.
    ///
    /// # Arguments
    ///
    /// * `config` - Format-specific configuration.
    ///
    /// * `consumer` - Consumer to send encoded data batches to.
    fn new_encoder(
        &self,
        config: &YamlValue,
        consumer: Box<dyn OutputConsumer>,
    ) -> AnyResult<Box<dyn Encoder>>;
}

impl dyn OutputFormat {
    /// Lookup output format by name.
    pub fn get_format(name: &str) -> Option<&'static dyn OutputFormat> {
        OUTPUT_FORMATS.get(name).map(|f| &**f)
    }
}

pub trait Encoder: Send {
    fn encode(&mut self, batches: &[Arc<dyn SerBatch>]) -> AnyResult<()>;
}

pub trait OutputConsumer: Send {
    fn push_buffer(&mut self, buffer: &[u8]);
}
