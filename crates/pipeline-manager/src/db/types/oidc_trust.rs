use crate::db::types::role::Role;
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
    /// Role granted to a token that satisfies this trust. Capped at the
    /// creating principal's role; `owner` trusts are platform-wide and may be
    /// created only by an owner.
    pub role: Role,
}

/// Returns true if `pattern` matches `value`, where `*` in `pattern` matches
/// any sequence of characters and all other characters must match exactly.
///
/// The `*`-separated literals must occur in order, with the first anchored at
/// the start of `value` and the last at its end. Matching each middle literal at
/// its earliest position is optimal here because the trailing literal is
/// anchored separately, so no backtracking is needed. A plain matcher rather
/// than a regex: patterns come from user-registered trust relationships, and
/// this way there is nothing to escape and no pathological input to guard.
pub fn claim_matches(pattern: &str, value: &str) -> bool {
    let mut literals = pattern.split('*');
    // `split` always yields at least one item.
    let first = literals.next().unwrap_or_default();
    let Some(mut rest) = value.strip_prefix(first) else {
        return false;
    };
    let Some(last) = literals.next_back() else {
        // No `*` in the pattern, so the single literal must be the whole value.
        return rest.is_empty();
    };
    for literal in literals {
        match rest.find(literal) {
            Some(at) => rest = &rest[at + literal.len()..],
            None => return false,
        }
    }
    // The tail must fit in what is left, so a trailing literal cannot reuse
    // characters an earlier one already consumed.
    rest.len() >= last.len() && rest.ends_with(last)
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

    /// Literals around a `*` may not consume the same characters twice.
    #[test]
    fn literals_do_not_overlap() {
        assert!(claim_matches("a*a", "aa"));
        assert!(!claim_matches("a*a", "a"));
        assert!(claim_matches("*b*bb", "abbb"));
        assert!(!claim_matches("*b*bb", "abb"));
        assert!(claim_matches(
            "repo:org/*:ref:*",
            "repo:org/app:ref:refs/heads/main"
        ));
        assert!(!claim_matches(
            "repo:org/*:ref:*",
            "repo:other/app:ref:refs/heads/main"
        ));
    }
}
