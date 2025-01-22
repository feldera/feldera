use arrow_convert::deserialize::ArrowDeserialize;
use arrow_convert::field::ArrowField;
use arrow_convert::serialize::ArrowSerialize;

/// Trait for trait objects that can be serialized and deserialized with `Arrow`.
pub trait ArrowTraits: ArrowField + ArrowSerialize + ArrowDeserialize {}
