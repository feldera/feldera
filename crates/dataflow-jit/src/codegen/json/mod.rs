mod deserialize;
mod serialize;
mod tests;

pub use deserialize::{call_deserialize_fn, DeserializeJsonFn, DeserializeResult, JsonDeserConfig};
pub use serialize::{JsonSerConfig, SerializeFn};

// The index of a column within a row
// TODO: Newtyping for column indices within the layout interfaces
type ColumnIdx = usize;
