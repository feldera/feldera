use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct CsvParserConfig {
    /// Field delimiter (default `','`).
    ///
    /// This must be an ASCII character.
    pub delimiter: char,

    /// Whether the input begins with a header line (which is ignored).
    pub headers: bool,
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
#[serde(default)]
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
