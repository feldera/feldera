use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from AWS S3.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct S3InputConfig {
    /// AWS Access Key id. This property must be specified unless `no_sign_request` is set to `true`.
    pub aws_access_key_id: Option<String>,

    /// Secret Access Key. This property must be specified unless `no_sign_request` is set to `true`.
    pub aws_secret_access_key: Option<String>,

    /// Do not sign requests. This is equivalent to the `--no-sign-request` flag in the AWS CLI.
    #[serde(default)]
    pub no_sign_request: bool,

    /// Read a single object specified by a key.
    pub key: Option<String>,

    /// Read all objects whose keys match a prefix. Set to an empty string to read all objects in the bucket.
    pub prefix: Option<String>,

    /// AWS region.
    pub region: String,

    /// S3 bucket name to access.
    pub bucket_name: String,

    /// Determines how the connector ingests an individual S3 object. When `true`,
    /// the connector pushes the object to the pipeline chunk-by-chunk, so that the
    /// pipeline can parse and process initial chunks of the object before the entire
    /// object has been retrieved. This mode is suitable for streaming formats such as
    /// newline-delimited JSON. When `false`, the connector buffers the entire object
    /// in memory and pushes it to the pipeline as a single chunk.  Appropriate for
    /// formats like Parquet that cannot be streamed.
    ///
    /// The default value is `false`.
    #[serde(default)]
    pub streaming: bool,
}
