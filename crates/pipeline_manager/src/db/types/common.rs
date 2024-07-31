use crate::db::error::DBError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use utoipa::ToSchema;

/// Version number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[repr(transparent)]
#[serde(transparent)]
pub struct Version(#[cfg_attr(test, proptest(strategy = "1..3i64"))] pub i64);
impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Checks whether the provided name is valid.
/// Currently the only constraint is that is cannot be empty.
pub fn validate_name(name: &str) -> Result<(), DBError> {
    if name.is_empty() {
        Err(DBError::EmptyName)
    } else {
        Ok(())
    }
}
