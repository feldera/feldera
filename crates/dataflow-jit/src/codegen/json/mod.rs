mod deserialize;
mod serialize;
mod tests;

pub use deserialize::{call_deserialize_fn, DeserializeJsonFn, DeserializeResult, JsonDeserConfig};
pub use serialize::{JsonSerConfig, SerializeFn};

use serde::Deserialize;

// The index of a column within a row
// TODO: Newtyping for column indices within the layout interfaces
type ColumnIdx = usize;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonColumn {
    /// The name of the json key
    key: Box<str>,
    /// `None` means no parsing specification
    spec: Option<JsonColumnParseSpec>,
}

impl JsonColumn {
    pub fn new<K, S>(key: K, spec: S) -> Self
    where
        K: Into<Box<str>>,
        S: Into<Option<JsonColumnParseSpec>>,
    {
        Self {
            key: key.into(),
            spec: spec.into(),
        }
    }

    pub fn normal<K>(key: K) -> Self
    where
        K: Into<Box<str>>,
    {
        Self {
            key: key.into(),
            spec: None,
        }
    }

    pub fn datetime<K, F>(key: K, format: F) -> Self
    where
        K: Into<Box<str>>,
        F: Into<Box<str>>,
    {
        Self {
            key: key.into(),
            spec: Some(JsonColumnParseSpec::DateTimeFromStr {
                format: format.into(),
            }),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub const fn spec(&self) -> Option<&JsonColumnParseSpec> {
        self.spec.as_ref()
    }

    pub fn format(&self) -> Option<&str> {
        self.spec
            .as_ref()
            .and_then(|spec| spec.as_date_time_from_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum JsonColumnParseSpec {
    /// Parses a date or timestamp from a string of the specified format
    DateTimeFromStr { format: Box<str> },
    /// Parses a time or timestamp from microseconds
    TimeFromMicros,
    /// Parses a time or timestamp from milliseconds
    TimeFromMillis,
    /// Parses a date from an integer number of days
    DateFromDays,
}

impl JsonColumnParseSpec {
    #[must_use]
    pub const fn as_date_time_from_str(&self) -> Option<&str> {
        if let Self::DateTimeFromStr { format } = self {
            Some(format)
        } else {
            None
        }
    }
}
