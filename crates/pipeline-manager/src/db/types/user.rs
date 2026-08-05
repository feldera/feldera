//! User identity and tenant membership types for RBAC.

use crate::db::types::role::Role;
use crate::db::types::tenant::TenantId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
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

/// How a membership row came into existence, kept for audit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MembershipOrigin {
    /// The user's own token listed the tenant in its claim.
    Claim,
    /// The personal or issuer tenant derivation produced it.
    Derived,
    /// Granted through the RBAC endpoints.
    Api,
}

impl MembershipOrigin {
    pub fn as_str(&self) -> &'static str {
        match self {
            MembershipOrigin::Claim => "claim",
            MembershipOrigin::Derived => "derived",
            MembershipOrigin::Api => "api",
        }
    }
}

/// Parsing is total: an unknown string is an error, because the column's
/// CHECK constraint makes one data corruption, which must surface rather than
/// read as a row that predates provenance tracking.
impl FromStr for MembershipOrigin {
    type Err = InvalidMembershipOrigin;

    fn from_str(input: &str) -> Result<MembershipOrigin, Self::Err> {
        match input {
            "claim" => Ok(MembershipOrigin::Claim),
            "derived" => Ok(MembershipOrigin::Derived),
            "api" => Ok(MembershipOrigin::Api),
            other => Err(InvalidMembershipOrigin(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidMembershipOrigin(pub String);

impl fmt::Display for InvalidMembershipOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid membership origin '{}'", self.0)
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
    /// How the membership came into existence. `null` for rows created before
    /// provenance tracking.
    #[serde(default)]
    pub origin: Option<MembershipOrigin>,
}

/// One tenant a user may act in, as surfaced to that user (e.g. in the
/// session payload that drives the web console's tenant switcher).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UserMembership {
    pub tenant_id: TenantId,
    /// The tenant's name.
    pub name: String,
    /// The user's role within this tenant.
    pub role: Role,
}

/// A tenant, as returned by the owner-only tenant endpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantInfo {
    pub id: TenantId,
    pub name: String,
    /// The OIDC issuer this tenant was first provisioned under. Provenance
    /// only: a tenant is resolved by name, so this does not affect which tenant
    /// a login reaches.
    pub initial_provider: String,
}
