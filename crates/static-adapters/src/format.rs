//! The record layer of the formats: records of the types that the SQL compiler generates, read
//! from CSV, Avro, and raw messages, and written as Avro datums.

#[cfg(feature = "with-avro")]
pub mod avro;
pub mod csv;
pub mod raw;
