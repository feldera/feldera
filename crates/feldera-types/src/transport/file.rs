use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from a file with `FileInputTransport`
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct FileInputConfig {
    /// File path.
    ///
    /// This may be a file name or a `file://` URL with an absolute path.
    pub path: String,

    /// Read buffer size.
    ///
    /// Default: when this parameter is not specified, a platform-specific
    /// default is used.
    pub buffer_size_bytes: Option<usize>,

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
