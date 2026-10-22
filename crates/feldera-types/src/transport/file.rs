use crate::size::{ConfigSize, LegacySizeUnit};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

crate::size_setting!(size_buffer_size, "buffer_size", LegacySizeUnit::Bytes);

/// Configuration for reading data from a file with `FileInputTransport`
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct FileInputConfig {
    /// File path.
    ///
    /// This may be a file name or a `file://` URL with an absolute path.
    pub path: String,

    /// Read buffer size, such as `"64Ki"`.
    ///
    /// Default: when this parameter is not specified, a platform-specific
    /// default is used.
    ///
    /// Before release 0.367.0 this setting was `buffer_size_bytes`, a number
    /// of bytes. That name is deprecated but still read.
    #[serde(
        default,
        alias = "buffer_size_bytes",
        deserialize_with = "size_buffer_size",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>)]
    pub buffer_size: Option<ConfigSize>,

    /// Enable file following.
    ///
    /// When `false`, the endpoint outputs an `InputConsumer::eoi`
    /// message and stops upon reaching the end of file.  When `true`, the
    /// endpoint will keep watching the file and outputting any new content
    /// appended to it.
    #[serde(default)]
    pub follow: bool,
}

/// Configuration for writing data to a file with `FileOutputTransport`.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct FileOutputConfig {
    /// File path.
    pub path: String,
}
