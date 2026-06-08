//! Role-based access control roles.
//!
//! Roles form a single total order: a higher role may do everything a lower
//! role can. `owner` is platform-wide (acts across tenants); the others are
//! scoped to a single tenant. The ordering is the derived `Ord` on the
//! declaration order below, so it must stay `Read < Write < Admin < Owner`.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use utoipa::ToSchema;

pub const ROLE_READ: &str = "read";
pub const ROLE_WRITE: &str = "write";
pub const ROLE_ADMIN: &str = "admin";
pub const ROLE_OWNER: &str = "owner";

/// A role in the RBAC model. Declaration order defines the privilege order.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Read,
    Write,
    Admin,
    Owner,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Read => ROLE_READ,
            Role::Write => ROLE_WRITE,
            Role::Admin => ROLE_ADMIN,
            Role::Owner => ROLE_OWNER,
        }
    }

    /// True if this role is at least `required` in the privilege order.
    pub fn satisfies(&self, required: Role) -> bool {
        *self >= required
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parsing is total: an unknown string is an error, never a panic, because
/// these parse on the request (auth) path where a stored bad value must fail
/// the request rather than crash the process.
impl FromStr for Role {
    type Err = InvalidRole;

    fn from_str(input: &str) -> Result<Role, Self::Err> {
        match input {
            ROLE_READ => Ok(Role::Read),
            ROLE_WRITE => Ok(Role::Write),
            ROLE_ADMIN => Ok(Role::Admin),
            ROLE_OWNER => Ok(Role::Owner),
            other => Err(InvalidRole(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidRole(pub String);

impl fmt::Display for InvalidRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid role '{}'", self.0)
    }
}

/// The subset of roles that may be carried by a static API key. `admin` and
/// `owner` are deliberately not representable here, so an over-privileged key
/// cannot be constructed (a leaked static secret carrying admin/owner would be
/// a standing liability; those roles come only from signature-verified
/// principals: login JWTs and OIDC trust relationships).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum MintableKeyRole {
    Read,
    Write,
}

impl MintableKeyRole {
    /// Narrow a `Role` to a key-mintable role, rejecting `admin`/`owner`.
    pub fn from_role(role: Role) -> Option<Self> {
        match role {
            Role::Read => Some(MintableKeyRole::Read),
            Role::Write => Some(MintableKeyRole::Write),
            Role::Admin | Role::Owner => None,
        }
    }

    pub fn role(self) -> Role {
        match self {
            MintableKeyRole::Read => Role::Read,
            MintableKeyRole::Write => Role::Write,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn ordering_is_total_and_correct() {
        assert!(Role::Read < Role::Write);
        assert!(Role::Write < Role::Admin);
        assert!(Role::Admin < Role::Owner);
        assert!(Role::Owner.satisfies(Role::Read));
        assert!(!Role::Read.satisfies(Role::Write));
        assert!(Role::Write.satisfies(Role::Write));
    }

    #[test]
    fn parse_roundtrips_and_rejects_unknown() {
        for r in [Role::Read, Role::Write, Role::Admin, Role::Owner] {
            assert_eq!(Role::from_str(r.as_str()).unwrap(), r);
        }
        assert!(Role::from_str("superuser").is_err());
        assert!(Role::from_str("").is_err());
    }

    #[test]
    fn owner_and_admin_are_not_key_mintable() {
        assert_eq!(
            MintableKeyRole::from_role(Role::Read).map(|m| m.role()),
            Some(Role::Read)
        );
        assert_eq!(
            MintableKeyRole::from_role(Role::Write).map(|m| m.role()),
            Some(Role::Write)
        );
        assert!(MintableKeyRole::from_role(Role::Admin).is_none());
        assert!(MintableKeyRole::from_role(Role::Owner).is_none());
    }
}
