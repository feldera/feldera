use crate::{
    ControllerError, OutputConsumer,
    catalog::{CursorWithPolarity, DeCollectionStream, RecordFormat, SerCursor},
    format::{Encoder, InputFormat, OutputFormat, ParseError, Parser},
    util::truncate_ellipse,
};
use anyhow::{Result as AnyResult, bail};
use erased_serde::Serialize as ErasedSerialize;
use feldera_adapterlib::catalog::StagedBuffers;
use feldera_adapterlib::{ConnectorMetadata, catalog::SerCursorFlattened};
use feldera_types::{
    config::ConnectorConfig,
    format::csv::{CsvEncoderConfig, CsvFormatConfig},
};
use serde::Deserialize;
use serde_json::json;
use serde_urlencoded::Deserializer as UrlDeserializer;
use std::{borrow::Cow, mem::take, sync::Arc};

use crate::catalog::{InputCollectionHandle, SerBatchReader};
use feldera_types::program_schema::Relation;

use super::{InputBuffer, Splitter};

/// When including a long CSV record in an error message,
/// truncate it to `MAX_RECORD_LEN_IN_ERRMSG` bytes.
static MAX_RECORD_LEN_IN_ERRMSG: usize = 4096;

fn csv_trim(t: &feldera_types::format::csv::CsvTrim) -> csv::Trim {
    use feldera_types::format::csv::CsvTrim;
    match t {
        CsvTrim::None => csv::Trim::None,
        CsvTrim::Headers => csv::Trim::Headers,
        CsvTrim::Fields => csv::Trim::Fields,
        CsvTrim::All => csv::Trim::All,
    }
}

/// Build a [`csv::ReaderBuilder`] fully configured from `config`.
///
/// `has_headers` is always `false` — the adapter layer skips the header row
/// itself and must not pass it to the underlying CSV reader.
pub fn csv_reader_builder(config: &CsvFormatConfig) -> csv::ReaderBuilder {
    let mut b = csv::ReaderBuilder::new();
    b.has_headers(false)
        .delimiter(config.delimiter().0)
        .quote(config.quote as u8)
        .escape(config.escape.map(|c| c as u8))
        .double_quote(config.double_quote)
        .quoting(config.quoting)
        .comment(config.comment.map(|c| c as u8))
        .flexible(config.flexible)
        .trim(csv_trim(&config.trim));
    b
}

/// CSV format parser.
pub struct CsvInputFormat;

impl InputFormat for CsvInputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("csv")
    }

    /// Create a parser using configuration extracted from an HTTP request.
    // We could just rely on serde to deserialize the config from the
    // HTTP query, but a specialized method gives us more flexibility.
    fn config_from_http_query(
        &self,
        _endpoint_name: &str,
        _query: &str,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(CsvFormatConfig::default()))
    }

    fn new_parser(
        &self,
        endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        config: &serde_json::Value,
    ) -> Result<Box<dyn Parser>, ControllerError> {
        let config = if config.is_null() { &json!({}) } else { config };
        let config = CsvFormatConfig::deserialize(config).map_err(|e| {
            ControllerError::parser_config_parse_error(
                endpoint_name,
                &e,
                &serde_json::to_string(config).unwrap_or_default(),
            )
        })?;

        let input_stream = input_stream
            .handle
            .configure_deserializer(RecordFormat::Csv(config.clone()))?;
        Ok(Box::new(CsvParser::new(input_stream, &config)) as Box<dyn Parser>)
    }
}

struct CsvParser {
    /// Input handle to push parsed data to.
    input_stream: Box<dyn DeCollectionStream>,

    /// Whether the input starts with a header row.
    headers: bool,

    /// Quote character (as a byte), used for record splitting.
    quote: u8,

    /// Whether quoting is enabled.  When `false`, every newline ends a record.
    quoting: bool,

    /// Optional comment character.  Lines whose first byte matches this value
    /// are skipped and never passed to the deserializer.
    comment: Option<u8>,

    last_event_number: u64,
}

impl CsvParser {
    fn new(input_stream: Box<dyn DeCollectionStream>, config: &CsvFormatConfig) -> Self {
        Self {
            input_stream,
            last_event_number: 0,
            headers: config.headers,
            quote: config.quote as u8,
            quoting: config.quoting,
            comment: config.comment.map(|c| c as u8),
        }
    }

    /// Returns `true` if `record` is a comment line (its first byte matches
    /// the configured comment character).
    fn is_comment(&self, record: &[u8]) -> bool {
        self.comment.is_some_and(|c| record.first() == Some(&c))
    }

    fn parse_record(
        &mut self,
        record: &[u8],
        metadata: &Option<ConnectorMetadata>,
        errors: &mut Vec<ParseError>,
    ) {
        if (!self.headers || self.last_event_number > 0)
            && let Err(e) = self.input_stream.insert(record, metadata)
        {
            errors.push(ParseError::text_event_error(
                "failed to deserialize CSV record",
                e,
                self.last_event_number + 1,
                Some(
                    &std::str::from_utf8(record)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| format!("{:?}", record)),
                ),
                None,
            ));
        }
    }

    /// Tries to split `buffer` into a CSV record followed by any remaining text.
    ///
    /// A CSV record is usually one line that ends in a newline, but it can
    /// span multiple lines if newlines appear inside a quoted field.
    fn split_record<'a>(&mut self, buffer: &'a [u8]) -> Option<(&'a [u8], &'a [u8])> {
        let mut quoted = false;
        for (offset, &c) in buffer.iter().enumerate() {
            if self.quoting && c == self.quote {
                quoted = !quoted;
            } else if c == b'\n' && !quoted {
                return Some(buffer.split_at(offset + 1));
            }
        }
        None
    }
}

impl Parser for CsvParser {
    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self {
            input_stream: self.input_stream.fork(),
            headers: self.headers,
            quote: self.quote,
            quoting: self.quoting,
            comment: self.comment,
            last_event_number: 0,
        })
    }

    fn splitter(&self) -> Box<dyn super::Splitter> {
        Box::new(CsvSplitter::new(
            self.headers,
            self.quote,
            self.quoting,
            self.comment,
        ))
    }

    fn parse(
        &mut self,
        mut data: &[u8],
        metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut errors = Vec::new();
        while let Some((record, rest)) = self.split_record(data) {
            if !self.is_comment(record) {
                self.parse_record(record, &metadata, &mut errors);
                self.last_event_number += 1;
            }
            data = rest;
        }
        if !data.is_empty() && !self.is_comment(data) {
            self.parse_record(data, &metadata, &mut errors);
        }
        (self.input_stream.take_all(), errors)
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.input_stream.stage(buffers)
    }
}

struct CsvSplitter {
    quoted: bool,
    headers: bool,
    quote: u8,
    quoting: bool,
    /// Optional comment character: lines whose first byte matches this value
    /// are skipped entirely.
    comment: Option<u8>,
    /// True when the next byte to be processed begins a new line.
    at_line_start: bool,
    /// True while consuming a comment line (until the terminating newline).
    in_comment: bool,
}

impl CsvSplitter {
    fn new(headers: bool, quote: u8, quoting: bool, comment: Option<u8>) -> Self {
        Self {
            quoted: false,
            headers,
            quote,
            quoting,
            comment,
            at_line_start: true,
            in_comment: false,
        }
    }
}

impl Splitter for CsvSplitter {
    fn input(&mut self, data: &[u8]) -> Option<usize> {
        for (offset, &c) in data.iter().enumerate() {
            // While inside a comment line, skip everything up to the newline.
            if self.in_comment {
                if c == b'\n' {
                    self.in_comment = false;
                    self.at_line_start = true;
                }
                continue;
            }

            // At the start of a line: check whether this is a comment line.
            if self.at_line_start {
                self.at_line_start = false;
                if self.comment == Some(c) {
                    self.in_comment = true;
                    continue;
                }
            }

            if self.quoting && c == self.quote {
                self.quoted = !self.quoted;
            } else if c == b'\n' && !self.quoted {
                self.at_line_start = true;
                if self.headers {
                    self.headers = false;
                } else {
                    return Some(offset + 1);
                }
            }
        }
        None
    }

    fn clear(&mut self) {
        self.quoted = false;
        self.in_comment = false;
        self.at_line_start = true;
    }
}

/// CSV format encoder.
pub struct CsvOutputFormat;

impl OutputFormat for CsvOutputFormat {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("csv")
    }

    fn config_from_http_query(
        &self,
        endpoint_name: &str,
        query: &str,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError> {
        Ok(Box::new(
            CsvEncoderConfig::deserialize(UrlDeserializer::new(form_urlencoded::parse(
                query.as_bytes(),
            )))
            .map_err(|e| ControllerError::encoder_config_parse_error(endpoint_name, &e, query))?,
        ))
    }

    fn new_encoder(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        key_schema: &Option<Relation>,
        _value_schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
        is_index: bool,
    ) -> Result<Box<dyn Encoder>, ControllerError> {
        if is_index {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "CSV encoder cannot be attached to an index",
            ));
        }
        let format_config = &config.format.as_ref().unwrap().config;
        let format_config = if format_config.is_null() {
            &json!({})
        } else {
            format_config
        };
        let csv_config = CsvEncoderConfig::deserialize(format_config).map_err(|e| {
            ControllerError::encoder_config_parse_error(
                endpoint_name,
                &e,
                &serde_json::to_string(&config).unwrap_or_default(),
            )
        })?;

        if matches!(
            config.transport,
            feldera_types::config::TransportConfig::RedisOutput(_)
        ) {
            return Err(ControllerError::invalid_encoder_configuration(
                endpoint_name,
                "'csv' format not yet supported with Redis connector",
            ));
        }

        Ok(Box::new(CsvEncoder::new(
            consumer,
            csv_config,
            key_schema.is_some(),
        )))
    }
}

struct CsvEncoder {
    /// Input handle to push serialized data to.
    output_consumer: Box<dyn OutputConsumer>,

    config: CsvEncoderConfig,
    buffer: Vec<u8>,
    max_buffer_size: usize,
    /// Input stream is an indexed Z-set.
    input_is_indexed: bool,
}

impl CsvEncoder {
    fn new(
        output_consumer: Box<dyn OutputConsumer>,
        config: CsvEncoderConfig,
        input_is_index: bool,
    ) -> Self {
        let max_buffer_size = output_consumer.max_buffer_size_bytes();
        Self {
            output_consumer,
            config,
            buffer: Vec::new(),
            max_buffer_size,
            input_is_indexed: input_is_index,
        }
    }
}

impl Encoder for CsvEncoder {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self.output_consumer.as_mut()
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let mut buffer = take(&mut self.buffer);
        //let mut writer = self.builder.from_writer(buffer);
        let mut num_records = 0;

        let mut cursor = if self.input_is_indexed {
            CursorWithPolarity::new(Box::new(SerCursorFlattened::new(batch.cursor(
                RecordFormat::Csv(CsvFormatConfig {
                    delimiter: self.config.delimiter,
                    ..Default::default()
                }),
            )?)))
        } else {
            CursorWithPolarity::new(batch.cursor(RecordFormat::Csv(CsvFormatConfig {
                delimiter: self.config.delimiter,
                ..Default::default()
            }))?)
        };

        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key()?;
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
                    bail!(
                        "CSV record exceeds maximum buffer size supported by the output transport. Max supported buffer size is {} bytes, but the following record requires {} bytes: '{}'.",
                        self.max_buffer_size,
                        new_len - prev_len,
                        truncate_ellipse(record, MAX_RECORD_LEN_IN_ERRMSG, "...")
                    );
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
                cursor.step_key()?;
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

#[cfg(any(test, feature = "testing"))]
pub mod test {
    use super::CsvOutputFormat;
    #[cfg(test)]
    use super::CsvSplitter;
    #[cfg(test)]
    use crate::format::Splitter;
    use crate::{
        OutputFormat,
        test::{engine_test, parser, relations, render, rows},
    };
    use feldera_adapterlib::{
        format::{InputBuffer, ParseError, Parser},
        test::{ConnectorTestEngine, InputOptions, MockOutputConsumer, Updates},
    };
    use feldera_types::{
        config::{ConnectorConfig, FormatConfig},
        format::csv::{CsvFormatConfig, CsvTrim},
        program_schema::{ColumnType, Field, Relation},
    };
    use proptest::prelude::*;
    use serde_json::json;
    use std::{borrow::Cow, collections::BTreeMap};

    // ---- CsvSplitter option tests ----

    // `headers = true`: the first newline ends the header row and is consumed
    // without producing a record; subsequent newlines are record boundaries.
    #[test]
    fn splitter_headers() {
        let mut s = CsvSplitter::new(true, b'"', true, None);
        // Header row: consumed silently, no record returned.
        assert_eq!(s.input(b"col1,col2\n"), None);
        // First data record.
        assert_eq!(s.input(b"foo,bar\n"), Some(8));
    }

    // `headers = true` with a comment before the header line.
    #[test]
    fn splitter_headers_with_leading_comment() {
        let mut s = CsvSplitter::new(true, b'"', true, Some(b'#'));
        // Comment before the header: skipped.
        assert_eq!(s.input(b"# preamble\n"), None);
        // Header row: consumed silently.
        assert_eq!(s.input(b"col1,col2\n"), None);
        // First data record.
        assert_eq!(s.input(b"foo,bar\n"), Some(8));
    }

    // Default quoting: a newline inside a double-quoted span does not split.
    #[test]
    fn splitter_default_no_split_inside_quotes() {
        let mut s = CsvSplitter::new(false, b'"', true, None);
        // Opening quote is present; newline is inside the quoted span.
        assert_eq!(s.input(b"\"foo\n"), None);
        // Closing quote followed by newline terminates the record.
        assert_eq!(s.input(b"bar\"\n"), Some(5));
    }

    // Custom quote character: single-quote acts as the quoting delimiter.
    #[test]
    fn splitter_custom_quote() {
        let mut s = CsvSplitter::new(false, b'\'', true, None);
        // Newline inside single-quoted span — no split.
        assert_eq!(s.input(b"foo,'bar\n"), None);
        // Closing single-quote then newline — split.
        assert_eq!(s.input(b"baz'\n"), Some(5));
    }

    // `quoting = false`: every newline ends a record even when inside quotes.
    #[test]
    fn splitter_quoting_disabled() {
        let mut s = CsvSplitter::new(false, b'"', false, None);
        // Newline inside "quotes" splits immediately because quoting is off.
        assert_eq!(s.input(b"\"foo\n"), Some(5));
    }

    // `comment`: comment lines are skipped; the next real record is returned.
    #[test]
    fn splitter_comment() {
        let mut s = CsvSplitter::new(false, b'"', true, Some(b'#'));
        // A comment line is consumed without being returned as a record.
        assert_eq!(s.input(b"# comment\n"), None);
        // The following data record is returned normally.
        assert_eq!(s.input(b"foo,bar\n"), Some(8));
    }

    // Comment line interleaved with data records in a single chunk.
    #[test]
    fn splitter_comment_interleaved() {
        let mut s = CsvSplitter::new(false, b'"', true, Some(b'#'));
        // First record returned; comment + second record still pending.
        assert_eq!(s.input(b"a,b\n# skip\nc,d\n"), Some(4));
        // After the first record is consumed, the comment is skipped and the
        // second record is returned.
        assert_eq!(s.input(b"# skip\nc,d\n"), Some(11));
    }

    /// The table that the cases parse into.
    pub fn relation() -> Relation {
        Relation::new(
            "csv_input".into(),
            vec![
                Field::new("first".into(), ColumnType::varchar(false)),
                Field::new("second".into(), ColumnType::varchar(false)),
            ],
            false,
            BTreeMap::new(),
        )
    }

    /// Parses `data` with `config` into a fresh table of `relation`, and returns the errors that the parser
    /// reports and the updates that the table receives.
    fn parse(
        engine: &dyn ConnectorTestEngine,
        relation: &Relation,
        config: CsvFormatConfig,
        data: &[u8],
    ) -> (Vec<ParseError>, Updates) {
        let collection = engine
            .input_collection(relation, &InputOptions::default())
            .unwrap();
        let format = FormatConfig {
            name: Cow::from("csv"),
            config: serde_json::to_value(config).unwrap(),
        };
        let mut parser = parser(collection.as_ref(), &format);
        parser.on_error(Some(Box::new(|_, _| {})));
        let (mut buffer, errors) = parser.parse(data, None);
        buffer.flush().unwrap();
        (errors, collection.updates().unwrap())
    }

    /// Checks that parsing `data` with `config` reports no error and inserts `rows`, in order.
    #[track_caller]
    fn check(
        engine: &dyn ConnectorTestEngine,
        config: CsvFormatConfig,
        data: &[u8],
        rows: &[&str],
    ) {
        let (errors, updates) = parse(engine, &relation(), config, data);
        assert_eq!(errors, []);
        assert_eq!(updates, inserts(&updates, rows));
    }

    /// Returns `rows`, each inserted once, in the schema of `like`.
    fn inserts(like: &Updates, rows: &[&str]) -> Updates {
        let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, *row)).collect();
        Updates::from_json(like.rows().schema(), &rows).unwrap()
    }

    /// A quote character other than `"` wraps a field that holds the delimiter.
    #[engine_test]
    pub fn quote(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig {
                quote: '\'',
                ..Default::default()
            },
            b"foo,'bar,baz'\nqux,'quux,corge'\n",
            &[
                r#"{"first": "foo", "second": "bar,baz"}"#,
                r#"{"first": "qux", "second": "quux,corge"}"#,
            ],
        );
    }

    /// With double quoting off, the escape character escapes a quote.
    #[engine_test]
    pub fn escape(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig {
                escape: Some('\\'),
                double_quote: false,
                ..Default::default()
            },
            b"\"foo\\\"bar\",baz\n",
            &[r#"{"first": "foo\"bar", "second": "baz"}"#],
        );
    }

    /// By default, `""` inside a quoted field is one quote.
    #[engine_test]
    pub fn double_quote(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig::default(),
            b"\"foo\"\"bar\",baz\n",
            &[r#"{"first": "foo\"bar", "second": "baz"}"#],
        );
    }

    /// With double quoting off, the first quote of `""` ends the quoted field, and the rest of
    /// the field is kept as it is.
    #[engine_test]
    pub fn double_quote_disabled(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig {
                double_quote: false,
                ..Default::default()
            },
            b"\"foo\"\"bar\",baz\n",
            &[r#"{"first": "foo\"bar\"", "second": "baz"}"#],
        );
    }

    /// With quoting off, a quote is an ordinary character.
    #[engine_test]
    pub fn quoting_disabled(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig {
                quoting: false,
                ..Default::default()
            },
            b"\"hello\",world\n",
            &[r#"{"first": "\"hello\"", "second": "world"}"#],
        );
    }

    /// A line that starts with the comment character holds no record.
    #[engine_test]
    pub fn comment(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig {
                comment: Some('#'),
                ..Default::default()
            },
            b"# opening comment\nfoo,bar\n# mid comment\nbaz,qux\n",
            &[
                r#"{"first": "foo", "second": "bar"}"#,
                r#"{"first": "baz", "second": "qux"}"#,
            ],
        );
    }

    /// By default, whitespace around a field is part of it.
    #[engine_test]
    pub fn trim_none(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig::default(),
            b"  foo  ,  bar  \n",
            &[r#"{"first": "  foo  ", "second": "  bar  "}"#],
        );
    }

    /// With `trim = fields`, whitespace around a field is dropped.
    #[engine_test]
    pub fn trim_fields(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig {
                trim: CsvTrim::Fields,
                ..Default::default()
            },
            b"  foo  ,  bar  \n",
            &[r#"{"first": "foo", "second": "bar"}"#],
        );
    }

    /// By default, a record may have more fields than the table; the parser ignores the rest.
    #[engine_test]
    pub fn flexible(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig::default(),
            b"foo,bar\nqux,quux,extra\n",
            &[
                r#"{"first": "foo", "second": "bar"}"#,
                r#"{"first": "qux", "second": "quux"}"#,
            ],
        );
    }

    /// When the field count must not change, a record with another count is an error.
    #[engine_test]
    pub fn inflexible(engine: &dyn ConnectorTestEngine) {
        let (errors, updates) = parse(
            engine,
            &relation(),
            CsvFormatConfig {
                flexible: false,
                ..Default::default()
            },
            b"foo,bar\nqux,quux,extra\n",
        );
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(
            updates,
            inserts(&updates, &[r#"{"first": "foo", "second": "bar"}"#])
        );
    }

    /// A header row names the columns and holds no record.
    #[engine_test]
    pub fn headers(engine: &dyn ConnectorTestEngine) {
        check(
            engine,
            CsvFormatConfig {
                headers: true,
                ..Default::default()
            },
            b"first,second\nfoo,bar\n",
            &[r#"{"first": "foo", "second": "bar"}"#],
        );
    }

    /// The table of [`random_rows`].
    pub fn basic_relation() -> Relation {
        relations::basic("csv_basic")
    }

    /// Random rows, written as CSV records, read back unchanged and in order.
    #[engine_test]
    pub fn random_rows(engine: &dyn ConnectorTestEngine) {
        let relation = basic_relation();
        proptest!(ProptestConfig::with_cases(32), |(batches in rows::batches(&relation, "id", 1..=1, 20))| {
            let rows = &batches[0];
            let (errors, updates) = parse(
                engine,
                &relation,
                CsvFormatConfig::default(),
                &render::csv(&relation, rows.rows()),
            );
            prop_assert_eq!(errors, vec![]);
            prop_assert_eq!(&updates, rows);
        });
    }

    /// The encoder refuses an index.
    #[engine_test]
    pub fn encoder_refuses_an_index(_engine: &dyn ConnectorTestEngine) {
        let connector: ConnectorConfig = serde_json::from_value(json!({
            "transport": { "name": "file_output", "config": { "path": "/dev/null" } },
            "format": { "name": "csv" },
            "index": "csv_basic_key",
        }))
        .unwrap();
        let error = CsvOutputFormat
            .new_encoder(
                "test_output",
                &connector,
                &Some(relations::basic_key("csv_basic_key")),
                &basic_relation(),
                Box::new(MockOutputConsumer::new()),
                true,
            )
            .err()
            .unwrap();
        assert_eq!(
            error.to_string(),
            "invalid controller configuration: invalid format configuration for output endpoint \
             'test_output': CSV encoder cannot be attached to an index"
        );
    }
}
