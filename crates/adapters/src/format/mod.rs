use crate::format::parquet::{ParquetInputFormat, ParquetOutputFormat};
#[cfg(feature = "with-avro")]
use avro::input::AvroInputFormat;
use once_cell::sync::Lazy;
use std::hash::Hasher;
use std::ops::Range;
use std::{
    cmp::max,
    collections::BTreeMap,
    fs::File,
    io::{Error as IoError, Read},
};

#[cfg(feature = "with-avro")]
pub(crate) mod avro;
pub(crate) mod csv;
mod json;
pub mod parquet;
pub(crate) mod raw;

#[cfg(feature = "with-avro")]
use crate::format::avro::output::AvroOutputFormat;
pub use parquet::relation_to_parquet_schema;

pub use self::csv::{byte_record_deserializer, string_record_deserializer};
use self::{
    csv::{CsvInputFormat, CsvOutputFormat},
    json::{JsonInputFormat, JsonOutputFormat},
    raw::RawInputFormat,
};

pub use feldera_adapterlib::format::*;

/// Static map of supported input formats.
// TODO: support for registering new formats at runtime in order to allow
// external crates to implement new formats.
static INPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn InputFormat>>> = Lazy::new(|| {
    BTreeMap::from([
        ("csv", Box::new(CsvInputFormat) as Box<dyn InputFormat>),
        ("json", Box::new(JsonInputFormat) as Box<dyn InputFormat>),
        (
            "parquet",
            Box::new(ParquetInputFormat) as Box<dyn InputFormat>,
        ),
        #[cfg(feature = "with-avro")]
        ("avro", Box::new(AvroInputFormat) as Box<dyn InputFormat>),
        ("raw", Box::new(RawInputFormat) as Box<dyn InputFormat>),
    ])
});

pub fn get_input_format(name: &str) -> Option<&'static dyn InputFormat> {
    INPUT_FORMATS.get(name).map(|f| &**f)
}

/// Static map of supported output formats.
static OUTPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn OutputFormat>>> = Lazy::new(|| {
    BTreeMap::from([
        ("csv", Box::new(CsvOutputFormat) as Box<dyn OutputFormat>),
        ("json", Box::new(JsonOutputFormat) as Box<dyn OutputFormat>),
        (
            "parquet",
            Box::new(ParquetOutputFormat) as Box<dyn OutputFormat>,
        ),
        #[cfg(feature = "with-avro")]
        ("avro", Box::new(AvroOutputFormat) as Box<dyn OutputFormat>),
    ])
});

pub fn get_output_format(name: &str) -> Option<&'static dyn OutputFormat> {
    OUTPUT_FORMATS.get(name).map(|f| &**f)
}

/// An empty [InputBuffer].
pub struct EmptyInputBuffer;

impl InputBuffer for EmptyInputBuffer {
    fn flush(&mut self) {}

    fn hash(&self, _hasher: &mut dyn Hasher) {}

    fn len(&self) -> usize {
        0
    }

    fn take_some(&mut self, _n: usize) -> Option<Box<dyn InputBuffer>> {
        None
    }
}

/// A [Splitter] that never breaks data into records.
///
/// This supports [Parser]s that need all of a streaming data source to be read
/// in full before parsing.
pub struct Sponge;

impl Splitter for Sponge {
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

/// Helper for breaking a stream of data into groups of records using a
/// [Splitter].
///
/// A [Splitter] finds breakpoints between records given data presented to
/// it. This is a higher-level data structure that takes input data and breaks
/// it into chunks.
pub struct StreamSplitter {
    buffer: Vec<u8>,
    start: u64,
    fragment: Range<usize>,
    fed: usize,
    splitter: Box<dyn Splitter>,
}

impl StreamSplitter {
    /// Returns a new stream splitter that finds breakpoints with `splitter`.
    pub fn new(splitter: Box<dyn Splitter>) -> Self {
        Self {
            buffer: Vec::new(),
            start: 0,
            fragment: 0..0,
            fed: 0,
            splitter,
        }
    }

    /// Returns the next full chunk of input, if any.  `eoi` specifies whether
    /// the input stream is complete. If `eoi` is true and this function returns
    /// `None`, then there are no more chunks.
    pub fn next(&mut self, eoi: bool) -> Option<&[u8]> {
        match self
            .splitter
            .input(&self.buffer[self.fed..self.fragment.end])
        {
            Some(n) => {
                let chunk = &self.buffer[self.fragment.start..self.fed + n];
                self.fed += n;
                self.fragment.start = self.fed;
                Some(chunk)
            }
            None => {
                self.fed = self.fragment.end;
                if eoi && !self.fragment.is_empty() {
                    let chunk = &self.buffer[self.fragment.clone()];
                    self.fragment.start = self.fragment.end;
                    Some(chunk)
                } else {
                    None
                }
            }
        }
    }

    /// Appends `data` to the data to be broken into chunks.
    pub fn append(&mut self, data: &[u8]) {
        let final_len = self.fragment.len() + data.len();
        if final_len > self.buffer.len() {
            self.buffer.reserve(final_len - self.buffer.len());
        }
        self.buffer.copy_within(self.fragment.clone(), 0);
        self.buffer.resize(self.fragment.len(), 0);
        self.buffer.extend(data);
        self.fed -= self.fragment.start;
        self.start += self.fragment.start as u64;
        self.fragment = 0..self.buffer.len();
    }

    // Reads no more than `limit` bytes of data from `file` into the splitter,
    // with an initial minimum buffer size of `buffer_size`. Returns the number
    // of bytes read or an I/O error.
    pub fn read(
        &mut self,
        file: &mut File,
        buffer_size: usize,
        limit: usize,
    ) -> Result<usize, IoError> {
        // Move data to beginning of buffer.
        if self.fragment.start != 0 {
            self.buffer.copy_within(self.fragment.clone(), 0);
            self.fed -= self.fragment.start;
            self.start += self.fragment.start as u64;
            self.fragment = 0..self.fragment.len();
        }

        // Make sure there's some space to read data.
        if self.fragment.len() == self.buffer.len() {
            self.buffer
                .resize(max(buffer_size, self.buffer.capacity() * 2), 0);
        }

        // Read data.
        let mut space = &mut self.buffer[self.fragment.len()..];
        if space.len() > limit {
            space = &mut space[..limit];
        }
        let result = file.read(space);
        if let Ok(n) = result {
            self.fragment.end += n;
        }
        result
    }

    /// Returns the logical stream position of the next byte to be returned by
    /// the splitter.
    pub fn position(&self) -> u64 {
        self.start + self.fragment.start as u64
    }

    /// Sets the logical stream position of the next byte to be returned by
    /// the splitter to `offset`, and discards other state.
    pub fn seek(&mut self, offset: u64) {
        self.start = offset;
        self.fragment = 0..0;
        self.fed = 0;
        self.splitter.clear();
    }

    /// Resets the splitter's state as if it were newly created.
    pub fn reset(&mut self) {
        self.seek(0);
    }
}
