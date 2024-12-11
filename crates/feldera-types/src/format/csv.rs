use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct CsvParserConfig {
    /// Field delimiter (default `','`).
    ///
    /// This must be an ASCII character.
    pub delimiter: char,
}

impl Default for CsvParserConfig {
    fn default() -> Self {
        Self {
            delimiter: CsvDelimiter::default().0.into(),
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

#[derive(Deserialize, Serialize, ToSchema)]
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
