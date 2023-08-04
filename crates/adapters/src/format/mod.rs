use crate::{DeCollectionHandle, SerBatch};
use actix_web::HttpRequest;
use anyhow::Result as AnyResult;
use erased_serde::{Deserializer as ErasedDeserializer, Serialize as ErasedSerialize};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

mod csv;
mod json;

pub use self::{
    csv::{
        byte_record_deserializer, string_record_deserializer, CsvEncoderConfig, CsvParserConfig,
    },
    json::{JsonParserConfig, JsonUpdateFormat},
};
use self::{
    csv::{CsvInputFormat, CsvOutputFormat},
    json::JsonInputFormat,
};

/// Different ways to encode table records in JSON.
// TODO: we currently only support the `Map` representation.
// There are other issues with this definition, discussed below.
#[doc(hidden)]
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub enum RecordFormat {
    // TODO: this is not useful as is.  The default parser will already
    // happily parse the record encoded as an array of columns (this is just
    // how serde handles structs, which can be encoded as either maps or
    // arrays).  To make this useful, the config should specify a subset
    // of columns in the input stream, encoded in the array.
    #[serde(rename = "array")]
    Array,
    // TODO: this really refers to serde's default way to encode a struct,
    // which can be an actual map in JSON, a list of values in CSV, etc.
    // So maybe "Default" would be a better name.
    #[serde(rename = "map")]
    Map,
    // Only applicable to single-column tables.  Input records contain
    // raw encoding of this column only.  This is particularly useful for
    // tables that store raw JSON or binary data to be parsed using SQL.
    #[serde(rename = "raw")]
    Raw,
}

impl Default for RecordFormat {
    fn default() -> Self {
        Self::Map
    }
}

/// Static map of supported input formats.
// TODO: support for registering new formats at runtime in order to allow
// external crates to implement new formats.
static INPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn InputFormat>>> = Lazy::new(|| {
    BTreeMap::from([
        ("csv", Box::new(CsvInputFormat) as Box<dyn InputFormat>),
        ("json", Box::new(JsonInputFormat) as Box<dyn InputFormat>),
    ])
});

/// Static map of supported output formats.
static OUTPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn OutputFormat>>> =
    Lazy::new(|| BTreeMap::from([("csv", Box::new(CsvOutputFormat) as Box<dyn OutputFormat>)]));

/// Trait that represents a specific data format.
///
/// This is a factory trait that creates parsers for a specific data format.
pub trait InputFormat: Send + Sync {
    /// Unique name of the data format.
    fn name(&self) -> Cow<'static, str>;

    /// Extract parser configuration from an HTTP request.
    ///
    /// Returns the extracted configuration cast to the `ErasedSerialize` trait object
    /// (to keep this trait object-safe).
    ///
    /// # Discussion
    ///
    /// We could rely on the `serde_urlencoded` crate to deserialize the config
    /// from the HTTP request, which is what most implementations will do internally; however
    /// allowing the implementation to override this method enables additional flexibility.
    /// For example, an implementation may use `Content-Type` and other request headers,
    /// set HTTP-specific defaults for config fields, etc.
    fn config_from_http_request(
        &self,
        request: &HttpRequest,
    ) -> AnyResult<Box<dyn ErasedSerialize>>;

    /// Create a new parser for the format.
    ///
    /// # Arguments
    ///
    /// * `input_stream` - Input stream of the circuit to push parsed data to.
    ///
    /// * `config` - Deserializer to extract format-specific configuration.
    fn new_parser(
        &self,
        input_stream: &dyn DeCollectionHandle,
        config: &mut dyn ErasedDeserializer,
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
    /// Push a fragment of the input stream to the parser.
    ///
    /// The parser breaks `data` up into records and pushes these records
    /// to the circuit using the
    /// [`DeCollectionHandle`](`crate::DeCollectionHandle`) API.
    /// `data` is not guaranteed to start or end on a record boundary.
    /// The parser is responsible for identifying record boundaries and
    /// buffering incomplete records to get prepended to the next
    /// input fragment.
    ///
    /// The parser must not buffer any data, except for any incomplete records
    /// that cannot be fully parsed until more data or an end-of-input
    /// notification is received.
    ///
    /// This method is invoked by transport adapters, such as file, URL, and
    /// HTTP adapters (for some configurations of the adapter), where the
    /// underlying transport does not enforce message boundaries.
    ///
    /// Returns the number of records in the parsed representation or an error
    /// if parsing fails.
    fn input_fragment(&mut self, data: &[u8]) -> AnyResult<usize>;

    /// Push a chunk of data to the parser.
    ///
    /// The parser breaks `data` up into records and pushes these records
    /// to the circuit using the
    /// [`DeCollectionHandle`](`crate::DeCollectionHandle`) API.
    /// The chunk is expected to contain complete records only.
    ///
    /// The parser must not buffer any data.
    ///
    /// Returns the number of records in the parsed representation or an error
    /// if parsing fails.
    fn input_chunk(&mut self, data: &[u8]) -> AnyResult<usize> {
        self.input_fragment(data)
    }

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
    /// * `config` - Deserializer object to extract format-specific
    ///   configuration.
    ///
    /// * `consumer` - Consumer to send encoded data batches to.
    fn new_encoder(
        &self,
        config: &mut dyn ErasedDeserializer,
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
    /// Returns a reference to the consumer that the encoder is connected to.
    fn consumer(&mut self) -> &mut dyn OutputConsumer;

    /// Encode a batch of updates, push encoded buffers to the consumer
    /// using [`OutputConsumer::push_buffer`].
    fn encode(&mut self, batches: &[Arc<dyn SerBatch>]) -> AnyResult<()>;
}

pub trait OutputConsumer: Send {
    fn batch_start(&mut self);
    fn push_buffer(&mut self, buffer: &[u8]);
    fn batch_end(&mut self);
}
