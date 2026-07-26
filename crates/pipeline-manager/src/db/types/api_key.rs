use crate::db::types::role::Role;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
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

// This doc comment becomes the schema description in the OpenAPI document, and
// is copied verbatim into the generated clients, so it is written for API
// consumers and must not use rustdoc intra-doc links: they cannot resolve in the
// generated crate and fail its `cargo doc -D warnings`.
/// API key descriptor.
///
/// A key carries a single role, `read` or `write`. `admin` and `owner` are
/// never issuable as API keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ApiKeyDescr {
    pub id: ApiKeyId,
    pub name: String,
    /// Always `read` or `write` (a key can never carry `admin`/`owner`).
    #[schema(value_type = MintableKeyRole)]
    pub role: Role,
}
