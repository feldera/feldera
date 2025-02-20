use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Redis output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RedisOutputConfig {
    /// The URL format: `redis://[<username>][:<password>@]<hostname>[:port][/[<db>][?protocol=<protocol>]]`
    /// This is parsed by the [redis](https://docs.rs/redis/latest/redis/#connection-parameters) crate.
    pub connection_string: String,
    /// Separator used to join multiple components into a single key.
    /// ":" by default.
    #[serde(default = "default_separator")]
    pub key_separator: String,
}

fn default_separator() -> String {
    ":".to_string()
}
