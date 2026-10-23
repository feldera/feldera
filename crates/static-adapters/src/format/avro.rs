//! The Avro record layer: Avro values read into, and Avro datums written from, the types that
//! the SQL compiler generates.

pub mod deserializer;
pub mod serializer;

#[cfg(test)]
mod test;

pub use deserializer::from_avro_value;
