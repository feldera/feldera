pub mod checkpoint;
pub mod completion_token;
pub mod config;
pub mod constants;
pub mod error;
pub mod format;
pub mod license;
pub mod program_schema;
pub mod query;
pub mod query_params;
pub mod runtime_status;
pub mod secret_ref;
pub mod secret_resolver;
pub mod serde_with_context;
pub mod suspend;
pub mod telemetry;
pub mod time_series;
pub mod transaction;
pub mod transport;

mod serde_via_value {
    use serde::{
        de::{DeserializeOwned, Error},
        ser::Error as _,
        Deserialize, Deserializer, Serialize, Serializer,
    };

    /// Use this as a serde deserialization function to work around
    /// [`serde_json` issues] with nested `f64`.  It works in two steps, first
    /// deserializing a `serde_json::Value` from `deserializer`, then
    /// deserializing `T` from that `serde_json::Value`.
    ///
    /// Use this and `serialize` as serde de/serialization functions to work
    /// around [`serde_yaml` issues] deserializing an enum variant with a
    /// payload that is enclosed directly or indirectly within a flattened
    /// struct.
    ///
    /// [`serde_json` issues]: https://github.com/serde-rs/json/issues/1157
    /// [`serde_yaml` issues]: https://github.com/dtolnay/serde-yaml/issues/395
    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: DeserializeOwned,
    {
        serde_json::from_value(serde_json::Value::deserialize(deserializer)?)
            .map_err(D::Error::custom)
    }

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
    {
        serde_json::to_value(value)
            .map_err(S::Error::custom)?
            .serialize(serializer)
    }
}
