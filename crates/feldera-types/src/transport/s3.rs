use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

fn default_max_concurrent_fetches() -> u32 {
    8
}

fn default_max_retries() -> u32 {
    3
}

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

    /// The endpoint URL used to communicate with this service. Can be used to make this connector
    /// talk to non-AWS services with an S3 API.
    pub endpoint_url: Option<String>,

    /// Controls the number of S3 objects fetched in parallel.
    ///
    /// Increasing this value can improve throughput by enabling greater concurrency.
    /// However, higher concurrency may lead to timeouts or increased memory usage due to in-memory buffering.
    ///
    /// Recommended range: 1–10. Default: 8.
    #[serde(default = "default_max_concurrent_fetches")]
    pub max_concurrent_fetches: u32,

    /// Maximum number of retries for failed requests.
    /// This is used for both, the AWS SDK and by Feldera to handle transient connection errors.
    /// Recommended range: 2-5. Default: 3.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}
