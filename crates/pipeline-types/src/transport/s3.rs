use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from AWS S3.
#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct S3InputConfig {
    /// Credentials to authenticate against AWS
    pub credentials: AwsCredentials,
    /// AWS region
    pub region: String,
    /// S3 bucket name to access
    pub bucket_name: String,
    /// Strategy that determines which objects to
    /// read from the bucket
    pub read_strategy: ReadStrategy,
}

/// Configuration to authenticate against AWS
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum AwsCredentials {
    /// Do not sign requests. This is equivalent to
    /// the `--no-sign-request` flag in the AWS CLI
    NoSignRequest,
    /// Authenticate using a long-lived AWS access key and secret
    AccessKey {
        aws_access_key_id: String,
        aws_secret_access_key: String,
    },
}

/// Strategy that determines which objects to read from a given bucket
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum ReadStrategy {
    /// Read a single object specified by a key
    SingleKey { key: String },
    /// Read all objects whose keys match a prefix
    Prefix { prefix: String },
}
