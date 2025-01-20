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

    /// The endpoint URL used to communicate with this service. Can be used to make this connector
    /// talk to non-AWS services with an S3 API.
    pub endpoint_url: Option<String>,
}
