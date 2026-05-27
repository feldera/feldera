use crate::db::types::api_key::ApiPermission;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;
use uuid::Uuid;

/// Trust relationship identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct OidcTrustId(
    #[cfg_attr(test, proptest(strategy = "crate::db::test::limited_uuid()"))] pub Uuid,
);

impl Display for OidcTrustId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Trust relationship descriptor returned to clients.
///
/// Wildcards: a `*` in `subject` or `audience` matches any sequence of
/// characters; all other characters must match exactly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OidcTrustDescr {
    pub id: OidcTrustId,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub issuer: String,
    pub subject: String,
    #[serde(default)]
    pub audience: Option<String>,
    pub scopes: Vec<ApiPermission>,
}

/// Returns true if `pattern` matches `value`, where `*` in `pattern` matches
/// any sequence of characters and all other characters must match exactly.
pub fn claim_matches(pattern: &str, value: &str) -> bool {
    let p = pattern.as_bytes();
    let v = value.as_bytes();
    let mut pi = 0usize;
    let mut vi = 0usize;
    let mut star_pi: Option<usize> = None;
    let mut star_vi = 0usize;
    while vi < v.len() {
        if pi < p.len() && p[pi] == b'*' {
            star_pi = Some(pi);
            star_vi = vi;
            pi += 1;
        } else if pi < p.len() && p[pi] == v[vi] {
            pi += 1;
            vi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_vi += 1;
            vi = star_vi;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod test {
    use super::claim_matches;

    #[test]
    fn exact_match() {
        assert!(claim_matches("foo", "foo"));
        assert!(!claim_matches("foo", "bar"));
        assert!(!claim_matches("foo", "foobar"));
    }

    #[test]
    fn star_prefix() {
        assert!(claim_matches("prefix/*", "prefix/anything"));
        assert!(claim_matches("prefix/*", "prefix/"));
        assert!(!claim_matches("prefix/*", "other/x"));
    }

    #[test]
    fn star_middle_and_suffix() {
        assert!(claim_matches("a/*/c", "a/b/c"));
        assert!(claim_matches("a/*/c", "a/x/y/c"));
        assert!(!claim_matches("a/*/c", "a/b/d"));
        assert!(claim_matches("*-prod", "service-prod"));
    }

    #[test]
    fn full_wildcard() {
        assert!(claim_matches("*", ""));
        assert!(claim_matches("*", "anything-goes"));
    }
}
