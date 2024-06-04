use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Avro output format configuration.
#[derive(Serialize, Deserialize, Debug, Default, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AvroEncoderConfig {
    /// Avro schema used to encode output records.
    ///
    /// Specified as a string containing schema definition in JSON format.
    /// This schema must match precisely the SQL view definition, including
    /// nullability of columns.
    pub schema: String,
    /// List of schema registry URLs. When non-empty, the connector will
    /// post the schema to the registry and use the schema id returned
    /// by the registry.  Otherwise, schema id 0 is used.
    #[serde(default)]
    pub registry_urls: Vec<String>,
    /// Custom headers that will be added to every call to the schema registry.
    ///
    /// This option requires `registry_urls` to be set.
    #[serde(default)]
    pub registry_headers: HashMap<String, String>,
    /// Proxy that will be used to access the schema registry.
    ///
    /// Requires `registry_urls` to be set.
    pub registry_proxy: Option<String>,
    /// Timeout in seconds used to connect to the registry.
    ///
    /// Requires `registry_urls` to be set.
    pub registry_timeout_secs: Option<u64>,
    /// Username used to authenticate with the registry.
    ///
    /// Requires `registry_urls` to be set. This option is mutually exclusive with
    /// token-based authentication (see `registry_authorization_token`).
    pub registry_username: Option<String>,
    /// Password used to authenticate with the registry.
    ///
    /// Requires `registry_urls` to be set.
    pub registry_password: Option<String>,
    /// Token used to authenticate with the registry.
    ///
    /// Requires `registry_urls` to be set. This option is mutually exclusive with
    /// password-based authentication (see `registry_username` and `registry_password`).
    pub registry_authorization_token: Option<String>,
}
