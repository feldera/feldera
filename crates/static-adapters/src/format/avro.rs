//! The Avro record layer: Avro values read into, and Avro datums written from, the types that
//! the SQL compiler generates.

pub mod deserializer;
pub mod serializer;

// The tests also read `VARIANT` columns of the deprecated enum `Variant`.
#[cfg(test)]
#[allow(deprecated)]
mod test;

pub use deserializer::from_avro_value;
