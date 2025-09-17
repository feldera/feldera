use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::hash::Hasher;
use std::ops::{Add, AddAssign};

use actix_web::HttpRequest;
use anyhow::Result as AnyResult;
use dbsp::operator::input::StagedBuffers;
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::config::ConnectorConfig;
use feldera_types::program_schema::Relation;
use feldera_types::serde_with_context::FieldParseError;
use serde::de::StdError;
use serde::Serialize;
use serde_yaml::Value as YamlValue;

use crate::catalog::{InputCollectionHandle, SerBatchReader};
use crate::errors::controller::ControllerError;
use crate::transport::Step;

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

/// A collection of records associated with an input handle.
///
/// A [Parser] holds and adds records to an [InputBuffer].  The client, which is
/// typically an [InputReader](crate::transport::InputReader), collects one or
/// more [InputBuffer]s and pushes them to the circuit when the controller
/// requests it.
///
/// # Pushing buffers into a circuit
///
/// There are two ways to push `InputBuffer`s into a circuit:
///
/// - With [InputBuffer::flush].  This immediately pushes the input buffer into
///   the DBSP input handle.
///
/// - By passing buffers to [Parser::stage], which collects all of them into a
///   [StagedBuffers].  Then, later, call [StagedBuffers::flush] to push the
///   input buffers into the circuit.
///
/// Both approaches are equivalent in terms of correctness.  There can be a
/// difference in performance, because [InputBuffer::flush] has a significant
/// cost for a large number of records.  Using [StagedBuffers] has a similar
/// cost, but it incurs it in the call to [Parser::stage] rather than in
/// [StagedBuffers::flush].  This means that, if the input connector can buffer
/// data ahead of the circuit's demand for it, the cost can be hidden and the
/// circuit as a whole runs faster.
pub trait InputBuffer: Any + Send {
    /// Pushes all of the records into the circuit input handle, and discards
    /// those records.
    fn flush(&mut self);

    /// Returns the number of buffered records and bytes.
    fn len(&self) -> BufferSize;

    /// Hashes the records in the input buffer into `hasher`, in order.  This is
    /// used to ensure that input data remains the same for replay, so, for
    /// equal data, it should remain the same from one program run to the next.
    /// Data might be divided into `InputBuffer`s differently from one run to
    /// the next, so the data fed into `hasher` should be the same if, for
    /// example, records 0..10 then 10..20 are fed in one run and 0..5, 5..15,
    /// 15..20 in another.
    fn hash(&self, hasher: &mut dyn Hasher);

    fn is_empty(&self) -> bool {
        self.len().is_empty()
    }

    /// Removes the first `n` records from this input buffer and returns a new
    /// [InputBuffer] that holds them. If fewer than `n` records are available,
    /// returns all of them. May return `None` if this input buffer is empty (or
    /// if `n` is 0).
    ///
    /// Some implementations can't implement `n` with full granularity. These
    /// implementations might return more than `n` records.
    ///
    /// This is useful for extracting the records from one of several parser
    /// threads to send to a single common thread to be pushed later.
    ///
    /// # Byte accounting
    ///
    /// This function must not increase or decrease the total number of bytes.
    /// That is, if the returned buffer is named `head`, `self.len().bytes`
    /// before the call must equal `self.len().bytes + head.len().bytes`
    /// following the call.  Violating this invariant will cause the number of
    /// buffered bytes reported by a pipeline never to fall to zero (or to wrap
    /// around to `u64::MAX`).
    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>>;

    fn take_all(&mut self) -> Option<Box<dyn InputBuffer>> {
        self.take_some(usize::MAX)
    }
}

/// The size of an [InputBuffer].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct BufferSize {
    /// The exact number of records in the buffer.
    pub records: usize,

    /// The number of bytes attributable to the buffer.
    ///
    /// This need not be exact.  (When a buffer is split with
    /// [InputBuffer::take_some], it will usually not be exact.)
    pub bytes: usize,
}

impl BufferSize {
    /// The size of an empty buffer.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns true if this buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.records == 0 && self.bytes == 0
    }
}

impl Add for BufferSize {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        BufferSize {
            records: self.records + rhs.records,
            bytes: self.bytes + rhs.bytes,
        }
    }
}

impl AddAssign for BufferSize {
    fn add_assign(&mut self, rhs: Self) {
        self.records += rhs.records;
        self.bytes += rhs.bytes;
    }
}

impl InputBuffer for Option<Box<dyn InputBuffer>> {
    fn len(&self) -> BufferSize {
        self.as_ref()
            .map_or(BufferSize::empty(), |buffer| buffer.len())
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        if let Some(buffer) = self {
            buffer.hash(hasher)
        }
    }

    fn flush(&mut self) {
        if let Some(buffer) = self.as_mut() {
            buffer.flush()
        }
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.as_mut().and_then(|buffer| buffer.take_some(n))
    }
}

/// Parses raw bytes into database records.
pub trait Parser: Send + Sync {
    /// Parses `data` into records and returns the records and any parse errors
    /// that occurred.
    ///
    /// XXX it would be even better if this were `&self` and avoided keeping
    /// state entirely.
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>);

    /// Stages all of the `buffers`, which must have been obtained from this
    /// [Parser] or one forked from this one, into a [StagedBuffers] that may
    /// later be used to push the collected data into the circuit.  See
    /// [StagedBuffers] for more information.
    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers>;

    /// Returns an object that can be used to break a stream of incoming data
    /// into complete records to pass to [Parser::parse].
    fn splitter(&self) -> Box<dyn Splitter>;

    /// Create a new parser with the same configuration as `self`.
    ///
    /// Used by multithreaded transport endpoints to create multiple parallel
    /// input pipelines.
    fn fork(&self) -> Box<dyn Parser>;
}

/// Splits a data stream at boundaries between records.
///
/// [Parser::parse] can only parse complete records. For a byte stream source, a
/// format-specific [Splitter] allows a transport to find boundaries.
pub trait Splitter: Send {
    /// Looks for a record boundary in `data`. Returns:
    ///
    /// - `None`, if `data` does not necessarily complete a record.
    ///
    /// - `Some(n)`, if the first `n` bytes of data, plus any data previously
    ///   presented for which `None` was returned, form one or more complete
    ///   records. If `n < data.len()`, then the caller should re-present
    ///   `data[n..]` for further splitting.
    fn input(&mut self, data: &[u8]) -> Option<usize>;

    /// Clears any state in this splitter and prepares it to start splitting new
    /// data.
    fn clear(&mut self);
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
        config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError>;
}

pub trait Encoder: Send {
    /// Returns a reference to the consumer that the encoder is connected to.
    fn consumer(&mut self) -> &mut dyn OutputConsumer;

    /// Encode a batch of updates, push encoded buffers to the consumer
    /// using [`OutputConsumer::push_buffer`].
    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()>;
}

pub trait OutputConsumer: Send {
    /// Maximum buffer size that this transport can transmit.
    /// The encoder should not generate buffers exceeding this size.
    fn max_buffer_size_bytes(&self) -> usize;

    fn batch_start(&mut self, step: Step);

    /// See OutputEndpoint::push_buffer.
    fn push_buffer(&mut self, buffer: &[u8], num_records: usize);

    /// See OutputEndpoint::push_key.
    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
        num_records: usize,
    );
    fn batch_end(&mut self);
}

/// The largest weight of a record that can be output using
/// a format without explicit weights. Such formats require
/// duplicating the record `w` times, which is expensive
/// for large weights (and is most likely not what the user
/// intends).
pub const MAX_DUPLICATES: i64 = 1_000_000;

/// When including a long JSON record in an error message,
/// truncate it to `MAX_RECORD_LEN_IN_ERRMSG` bytes.
pub const MAX_RECORD_LEN_IN_ERRMSG: usize = 4096;

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

    /// Returns a new `ParseError` with the description modified by `f`.
    ///
    /// Can be used, e.g., to prepend context to the description.
    pub fn map_description<F>(self, f: F) -> Self
    where
        F: FnOnce(&str) -> String,
    {
        let mut inner = self.0;
        let description = f(&inner.description);
        inner.description = description;
        Self(inner)
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
