use crate::db::types::tenant::TenantId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;
use uuid::Uuid;

/// Allowed OIDC provider. In addition to the ones retrieved from the database, the root OIDC
/// provider is supplied through command-line arguments.
#[derive(Clone, ToSchema)]
pub struct Provider {
    /// OIDC issuer URL.
    pub issuer: String,

    /// Only subjects that satisfy this filter are allowed.
    /// - "*": all subjects are allowed
    /// - "example": exactly one subject is allowed: "example"
    /// - "example*: all subjects that have "example" as prefix are allowed
    pub subject_filter: String,

    /// OIDC client identifier for the Feldera instance.
    pub client_id: String,
}

/// User identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct UserId(
    #[cfg_attr(test, proptest(strategy = "crate::db::test::limited_uuid()"))] pub Uuid,
);
impl Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// User authenticated through its OIDC provider.
pub struct User {
    /// Unique identifier for the user.
    pub id: UserId,

    /// OIDC issuer.
    pub issuer: String,

    /// OIDC subject.
    pub subject: String,
}

/// Tenant.
pub struct Tenant {
    /// Unique identifier for the tenant.
    pub id: TenantId,

    /// Tenant name, which is system-generated if it is user-dedicated, and provided by the user
    /// otherwise.
    ///
    /// If the tenant is the user-dedicated one, it will have the format: `user-dedicated-<user UUID>`.
    /// It is not possible to create a tenant with this name format unless it's during user creation.
    pub name: String,
}
