#[cfg(feature = "with-avro")]
pub mod avro;
pub(crate) mod csv;
mod json;
pub mod parquet;
pub(crate) mod raw;

pub use parquet::relation_to_parquet_schema;

pub use self::csv::{byte_record_deserializer, string_record_deserializer};

pub use feldera_adapterlib::format::*;

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
