use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Whitespace trimming policy applied to CSV fields or header names.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CsvTrim {
    /// Do not trim whitespace (default).
    #[default]
    None,
    /// Trim whitespace from header names only.
    Headers,
    /// Trim whitespace from field values only.
    Fields,
    /// Trim whitespace from both header names and field values.
    All,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct CsvParserConfig {
    /// Field delimiter (default `','`).
    ///
    /// Must be an ASCII character.
    pub delimiter: char,

    /// Whether the input begins with a header line (which is skipped).
    pub headers: bool,

    /// The quote character (default `'"'`).
    ///
    /// Must be an ASCII character.  Set `quoting` to `false` to disable
    /// quoting entirely.
    pub quote: char,

    /// The escape character for quoted fields (default: `None`).
    ///
    /// When `None` (the default), the CSV parser uses the double-quote
    /// convention: a literal quote inside a quoted field is written as two
    /// consecutive quote characters.  When set, the given character is used
    /// as the escape prefix instead (e.g. `Some('\\')` for backslash
    /// escaping).
    ///
    /// Must be an ASCII character.
    pub escape: Option<char>,

    /// Enable double-quote escaping (default `true`).
    ///
    /// When `true`, a quote character inside a quoted field may be escaped by
    /// doubling it.  Setting this to `false` disables double-quote escaping
    /// (an explicit `escape` character can still be used).
    pub double_quote: bool,

    /// Enable quoting (default `true`).
    ///
    /// When `false`, the `quote` and `escape` characters have no special
    /// meaning and every newline terminates a record regardless of context.
    pub quoting: bool,

    /// Comment character (default: `None`).
    ///
    /// When set, lines whose first byte matches this character are treated as
    /// comments and skipped entirely.  Must be an ASCII character.
    pub comment: Option<char>,

    /// Allow records with a variable number of fields (default `true`).
    ///
    /// When `true`, records that have fewer or more fields than expected are
    /// accepted rather than treated as errors.
    pub flexible: bool,

    /// Whitespace trimming policy (default [`CsvTrim::None`]).
    pub trim: CsvTrim,
}

impl CsvParserConfig {
    pub fn delimiter(&self) -> CsvDelimiter {
        self.delimiter.into()
    }
}

impl Default for CsvParserConfig {
    fn default() -> Self {
        Self {
            delimiter: CsvDelimiter::default().0.into(),
            headers: false,
            quote: '"',
            escape: None,
            double_quote: true,
            quoting: true,
            comment: None,
            flexible: true,
            trim: CsvTrim::None,
        }
    }
}

/// A delimiter between CSV records, typically `b','`.
#[derive(Copy, Clone)]
pub struct CsvDelimiter(pub u8);

impl CsvDelimiter {
    pub const DEFAULT: CsvDelimiter = CsvDelimiter(b',');
}

impl Default for CsvDelimiter {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl Debug for CsvDelimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CsvDelimiter")
            .field(&char::from(self.0))
            .finish()
    }
}

impl From<char> for CsvDelimiter {
    fn from(value: char) -> Self {
        Self(value.try_into().unwrap_or(b','))
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CsvEncoderConfig {
    /// Field delimiter (default `','`).
    ///
    /// This must be an ASCII character.
    pub delimiter: char,

    pub buffer_size_records: usize,
}

impl Default for CsvEncoderConfig {
    fn default() -> Self {
        Self {
            delimiter: CsvDelimiter::default().0.into(),
            buffer_size_records: 10_000,
        }
    }
}
