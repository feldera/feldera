use crate::db::error::DBError;
use regex::Regex;
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

/// Pattern that every name must adhere to.
pub const PATTERN_VALID_NAME: &str = r"^[a-zA-Z0-9_-]+$";

/// Maximum name length.
pub const MAXIMUM_NAME_LENGTH: usize = 100;

/// Checks whether the provided name is valid.
/// The constraints are as follows:
/// - It cannot be empty
/// - It must be at most 100 characters long
/// - It must contain only characters which are lowercase (a-z), uppercase (A-Z),
//    numbers (0-9), underscores (_) or hyphens (-)
pub fn validate_name(name: &str) -> Result<(), DBError> {
    if name.is_empty() {
        Err(DBError::EmptyName)
    } else if name.len() > MAXIMUM_NAME_LENGTH {
        Err(DBError::TooLongName {
            name: name.to_string(),
            length: name.len(),
            maximum: MAXIMUM_NAME_LENGTH,
        })
    } else {
        let re = Regex::new(PATTERN_VALID_NAME).expect("Pattern for name must be valid");
        if re.is_match(name) {
            Ok(())
        } else {
            Err(DBError::NameDoesNotMatchPattern {
                name: name.to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validate_name;
    use crate::db::error::DBError;

    #[test]
    fn test_valid_names() {
        let valid = vec![
            "a",
            "z",
            "A",
            "Z",
            "0",
            "9",
            "-",
            "_",
            "exampleExample",
            "example-1",
            "example-of-this",
            "Aa0_-",
            "example_2",
            "Example",
            "EXAMPLE_example-example1234",
        ];
        for name in valid {
            assert!(validate_name(name).is_ok());
        }
    }

    #[test]
    fn test_invalid_names() {
        assert!(matches!(validate_name(""), Err(DBError::EmptyName)));
        assert!(
            matches!(validate_name("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), Err(DBError::TooLongName {
            name, length, maximum
        }) if name == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" && length == 101 && maximum == 100)
        );
        let invalid_due_to_pattern = vec!["%", "$", "abc@", "example example", " ", "%20"];
        for invalid_name in invalid_due_to_pattern {
            assert!(
                matches!(validate_name(invalid_name), Err(DBError::NameDoesNotMatchPattern {
                name
            }) if name == invalid_name)
            );
        }
    }
}
