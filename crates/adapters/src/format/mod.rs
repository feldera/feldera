use crate::catalog::InputCollectionHandle;
use crate::{catalog::SerBatch, transport::Step, ControllerError, FieldParseError};
use actix_web::HttpRequest;
use anyhow::Result as AnyResult;
use erased_serde::Serialize as ErasedSerialize;
use once_cell::sync::Lazy;
use pipeline_types::program_schema::Relation;
use serde::Serialize;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    sync::Arc,
};

pub(crate) mod csv;
mod json;

pub use self::csv::{byte_record_deserializer, string_record_deserializer};
use self::{
    csv::{CsvInputFormat, CsvOutputFormat},
    json::{JsonInputFormat, JsonOutputFormat},
};

/// Error parsing input data.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(transparent)]
// Box the internals of `ParseError` to avoid
// "Error variant to large" clippy warnings".
pub struct ParseError(Box<ParseErrorInner>);
impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        self.0.fmt(f)
    }
}

impl StdError for ParseError {}

impl ParseError {
    pub fn new(
        description: String,
        event_number: Option<u64>,
        field: Option<String>,
        invalid_text: Option<&str>,
        invalid_bytes: Option<&[u8]>,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self(Box::new(ParseErrorInner::new(
            description,
            event_number,
            field,
            invalid_text,
            invalid_bytes,
            suggestion,
        )))
    }

    pub fn text_event_error<E>(
        msg: &str,
        error: E,
        event_number: u64,
        invalid_text: Option<&str>,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self
    where
        E: ToString,
    {
        Self(Box::new(ParseErrorInner::text_event_error(
            msg,
            error,
            event_number,
            invalid_text,
            suggestion,
        )))
    }

    pub fn text_envelope_error(
        description: String,
        invalid_text: &str,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self(Box::new(ParseErrorInner::text_envelope_error(
            description,
            invalid_text,
            suggestion,
        )))
    }

    pub fn bin_event_error(
        description: String,
        event_number: u64,
        invalid_bytes: &[u8],
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self(Box::new(ParseErrorInner::bin_event_error(
            description,
            event_number,
            invalid_bytes,
            suggestion,
        )))
    }

    pub fn bin_envelope_error(
        description: String,
        invalid_bytes: &[u8],
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self(Box::new(ParseErrorInner::bin_envelope_error(
            description,
            invalid_bytes,
            suggestion,
        )))
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ParseErrorInner {
    /// Error description.
    description: String,

    /// Event number relative to the start of the stream.
    ///
    /// An input stream is a series data change events (row insertions,
    /// deletions, and updates).  This field specifies the index (starting
    /// from 1) of the event that caused the error, relative to the start of
    /// the stream.  In some cases this index cannot be identified, e.g., if
    /// the error makes an entire block of events unparseable.
    event_number: Option<u64>,

    /// Field that failed to parse.
    ///
    /// Only set when the parsing error can be attributed to a
    /// specific field.
    field: Option<String>,

    /// Invalid fragment of input data.
    ///
    /// Used for binary data formats and for text-based formats when the input
    /// is not valid UTF-8 string.
    invalid_bytes: Option<Vec<u8>>,

    /// Invalid fragment of the input text.
    ///
    /// Only used for text-based formats and in cases when input is valid UTF-8.
    invalid_text: Option<String>,

    /// Any additional information that may help fix the problem, e.g., example
    /// of a valid input.
    suggestion: Option<Cow<'static, str>>,
}

impl Display for ParseErrorInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        let event = if let Some(event_number) = self.event_number {
            format!(" (event #{})", event_number)
        } else {
            String::new()
        };

        let invalid_fragment = if let Some(invalid_bytes) = &self.invalid_bytes {
            format!("\nInvalid bytes: {invalid_bytes:?}")
        } else if let Some(invalid_text) = &self.invalid_text {
            format!("\nInvalid fragment: '{invalid_text}'")
        } else {
            String::new()
        };

        let suggestion = if let Some(suggestion) = &self.suggestion {
            format!("\n{suggestion}")
        } else {
            String::new()
        };

        write!(
            f,
            "Parse error{event}: {}{invalid_fragment}{suggestion}",
            self.description
        )
    }
}

impl ParseErrorInner {
    pub fn new(
        description: String,
        event_number: Option<u64>,
        field: Option<String>,
        invalid_text: Option<&str>,
        invalid_bytes: Option<&[u8]>,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self {
            description,
            event_number,
            field,
            invalid_text: invalid_text.map(str::to_string),
            invalid_bytes: invalid_bytes.map(ToOwned::to_owned),
            suggestion,
        }
    }

    /// Error parsing an individual event in a text-based input format (e.g.,
    /// JSON, CSV).
    pub fn text_event_error<E>(
        msg: &str,
        error: E,
        event_number: u64,
        invalid_text: Option<&str>,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self
    where
        E: ToString,
    {
        let err_str = error.to_string();
        // Try to parse the error as `FieldParseError`.  If this is not a field-specific
        // error or the error was not returned by the `deserialize_table_record`
        // macro, this will fail and we'll store the error as is.
        let (descr, field) = if let Some(offset) = err_str.find("{\"field\":") {
            if let Some(Ok(err)) = serde_json::Deserializer::from_str(&err_str[offset..])
                .into_iter::<FieldParseError>()
                .next()
            {
                (err.description, Some(err.field))
            } else {
                (err_str, None)
            }
        } else {
            (err_str, None)
        };
        let column_name = if let Some(field) = &field {
            format!(": error parsing field '{field}'")
        } else {
            String::new()
        };

        Self::new(
            format!("{msg}{column_name}: {descr}",),
            Some(event_number),
            field,
            invalid_text,
            None,
            suggestion,
        )
    }

    /// Error parsing a container, e.g., a JSON array, with multiple events.
    ///
    /// Such errors cannot be attributed to an individual event numbers.
    pub fn text_envelope_error(
        description: String,
        invalid_text: &str,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self::new(
            description,
            None,
            None,
            Some(invalid_text),
            None,
            suggestion,
        )
    }

    /// Error parsing an individual event in a binary input format (e.g.,
    /// bincode).
    pub fn bin_event_error(
        description: String,
        event_number: u64,
        invalid_bytes: &[u8],
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self::new(
            description,
            Some(event_number),
            None,
            None,
            Some(invalid_bytes),
            suggestion,
        )
    }

    /// Error parsing a container with multiple events.
    ///
    /// Such errors cannot be attributed to an individual event numbers.
    pub fn bin_envelope_error(
        description: String,
        invalid_bytes: &[u8],
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self::new(
            description,
            None,
            None,
            None,
            Some(invalid_bytes),
            suggestion,
        )
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
static OUTPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn OutputFormat>>> = Lazy::new(|| {
    BTreeMap::from([
        ("csv", Box::new(CsvOutputFormat) as Box<dyn OutputFormat>),
        ("json", Box::new(JsonOutputFormat) as Box<dyn OutputFormat>),
    ])
});

/// Trait that represents a specific data format.
///
/// This is a factory trait that creates parsers for a specific data format.
pub trait InputFormat: Send + Sync {
    /// Unique name of the data format.
    fn name(&self) -> Cow<'static, str>;

    /// Extract parser configuration from an HTTP request.
    ///
    /// Returns the extracted configuration cast to the `ErasedSerialize` trait
    /// object (to keep this trait object-safe).
    ///
    /// # Discussion
    ///
    /// We could rely on the `serde_urlencoded` crate to deserialize the config
    /// from the HTTP request, which is what most implementations will do
    /// internally; however allowing the implementation to override this
    /// method enables additional flexibility. For example, an
    /// implementation may use `Content-Type` and other request headers, set
    /// HTTP-specific defaults for config fields, etc.
    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError>;

    /// Create a new parser for the format.
    ///
    /// # Arguments
    ///
    /// * `input_stream` - Input stream of the circuit to push parsed data to.
    ///
    /// * `config` - Format-specific configuration.
    fn new_parser(
        &self,
        endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        config: &YamlValue,
    ) -> Result<Box<dyn Parser>, ControllerError>;
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
    fn input_fragment(&mut self, data: &[u8]) -> (usize, Vec<ParseError>);

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
    fn input_chunk(&mut self, data: &[u8]) -> (usize, Vec<ParseError>) {
        self.input_fragment(data)
    }

    /// End-of-input-stream notification.
    ///
    /// No more data will be received from the stream.  The parser uses this
    /// notification to complete or discard any incompletely parsed records.
    ///
    /// Returns the number of additional records pushed to the circuit or an
    /// error if parsing fails.
    fn eoi(&mut self) -> (usize, Vec<ParseError>);

    /// Create a new parser with the same configuration as `self`.
    ///
    /// Used by multithreaded transport endpoints to create multiple parallel
    /// input pipelines.
    fn fork(&self) -> Box<dyn Parser>;
}

pub trait OutputFormat: Send + Sync {
    /// Unique name of the data format.
    fn name(&self) -> Cow<'static, str>;

    /// Extract encoder configuration from an HTTP request.
    ///
    /// Returns the extracted configuration cast to the `ErasedSerialize` trait
    /// object (to keep this trait object-safe).
    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError>;

    /// Create a new encoder for the format.
    ///
    /// # Arguments
    ///
    /// * `config` - Format-specific configuration.
    ///
    /// * `consumer` - Consumer to send encoded data batches to.
    fn new_encoder(
        &self,
        endpoint_name: &str,
        config: &YamlValue,
        schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError>;
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
    /// Maximum buffer size that this transport can transmit.
    /// The encoder should not generate buffers exceeding this size.
    fn max_buffer_size_bytes(&self) -> usize;

    fn batch_start(&mut self, step: Step);
    fn push_buffer(&mut self, buffer: &[u8]);
    fn push_key(&mut self, key: &[u8], val: &[u8]);
    fn batch_end(&mut self);
}
