use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
use utoipa::ToSchema;
use uuid::Uuid;

/// API key identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct ApiKeyId(
    #[cfg_attr(test, proptest(strategy = "crate::db::test::limited_uuid()"))] pub Uuid,
);
impl Display for ApiKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Permission types for invoking API endpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ApiPermission {
    Read,
    Write,
}

pub const API_PERMISSION_READ: &str = "read";
pub const API_PERMISSION_WRITE: &str = "write";

impl FromStr for ApiPermission {
    type Err = ();

    fn from_str(input: &str) -> Result<ApiPermission, Self::Err> {
        match input {
            API_PERMISSION_READ => Ok(ApiPermission::Read),
            API_PERMISSION_WRITE => Ok(ApiPermission::Write),
            _ => Err(()),
        }
    }
}

/// API key descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ApiKeyDescr {
    pub id: ApiKeyId,
    pub name: String,
    pub scopes: Vec<ApiPermission>,
}
