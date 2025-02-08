use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub enum RawParserMode {
    #[default]
    #[serde(rename = "blob")]
    Blob,

    #[serde(rename = "lines")]
    Lines,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct RawParserConfig {
    pub mode: RawParserMode,
}
