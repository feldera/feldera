use crate::db::error::DBError;
use crate::db::types::utils::{
    PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN,
    PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN_DESCRIPTION, validate_name,
};

/// Longest permitted name for a trust relationship.
pub const MAXIMUM_OIDC_TRUST_NAME_LENGTH: usize = 100;

/// Checks the provided OIDC trust relationship name is valid.
pub fn validate_oidc_trust_name(name: &str) -> Result<(), DBError> {
    validate_name(
        name,
        MAXIMUM_OIDC_TRUST_NAME_LENGTH,
        PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN,
        PATTERN_NON_EMPTY_ALPHANUMERIC_UNDERSCORE_HYPHEN_DESCRIPTION,
    )
}
