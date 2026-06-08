//! User identity and tenant membership types for RBAC.

use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;
use uuid::Uuid;

/// Identifier of a persisted user (the principal behind an OIDC `sub`).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize, ToSchema,
)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct UserId(
    #[cfg_attr(test, proptest(strategy = "crate::db::test::limited_uuid()"))] pub Uuid,
);

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A member of a tenant, as returned by the user-management API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantMember {
    pub user_id: UserId,
    /// OIDC issuer the user authenticates through.
    pub provider: String,
    /// OIDC subject.
    pub subject: String,
    /// Email, if the identity provider supplied one.
    #[serde(default)]
    pub email: Option<String>,
    /// The user's role within this tenant.
    pub role: Role,
}

/// A tenant, as returned by the platform (owner-only) tenant list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantInfo {
    pub id: TenantId,
    pub name: String,
    pub provider: String,
}
