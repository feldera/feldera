use std::fmt;
use std::fmt::Formatter;

use serde::Deserialize;
use utoipa::ToSchema;

/// Enumeration which holds a string or a secret reference which is
/// not yet resolved.
///
/// Instantiation is done either directly, using deserialization or
/// by matching a basic string to a particular pattern. Note that
/// the former two allow simple strings to be instantiated that
/// the latter with the pattern would normally map into secret
/// reference.
///
/// This class is located in the pipeline-types crate because in future
/// versions it will likely become an object within the API.
/// Deserialization is implemented already for that reason.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, ToSchema)]
pub enum MaybeSecretRef {
    #[serde(rename = "string")]
    String(String),
    #[serde(rename = "secret_ref")]
    SecretRef(String),
}

impl fmt::Display for MaybeSecretRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl MaybeSecretRef {
    /// Determines whether a given string value refers to a secret or is simply
    /// a string based on whether it matches the pattern.
    ///
    /// A secret must follow the pattern:
    /// "${secret:secret-identifier}"
    ///
    /// For example for a secret identified by "database-1-user":
    /// "${secret:database-1-user}"
    ///
    /// Any string value which does not follow the above pattern is determined
    /// to be a simple string.
    ///
    /// Note that here is not checked whether the secret identifier can be
    /// resolved, nor is specified which requirements the resolver has for
    /// the identifier to be valid.
    pub fn new_using_pattern_match(value: String) -> MaybeSecretRef {
        if value.starts_with("${secret:") && value.ends_with('}') {
            // Because the pattern only has ASCII characters, they are encoded as single
            // bytes. The secret reference is extracted by slicing away the
            // first 9 bytes and the last byte.
            let from_idx_incl = 9;
            let till_idx_excl = value.len() - 1;
            MaybeSecretRef::SecretRef(value[from_idx_incl..till_idx_excl].to_string())
        } else {
            MaybeSecretRef::String(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MaybeSecretRef;

    #[test]
    fn test_format() {
        assert_eq!(
            format!("{}", MaybeSecretRef::String("example123".to_string())),
            "String(\"example123\")"
        );
        assert_eq!(
            format!("{:?}", MaybeSecretRef::String("example123".to_string())),
            "String(\"example123\")"
        );
        assert_eq!(
            format!("{}", MaybeSecretRef::SecretRef("example123".to_string())),
            "SecretRef(\"example123\")"
        );
        assert_eq!(
            format!("{:?}", MaybeSecretRef::SecretRef("example123".to_string())),
            "SecretRef(\"example123\")"
        );
    }

    #[test]
    fn test_deserialize_string() {
        let data = "{ \"string\": \"example123\" }";
        assert_eq!(
            json5::from_str::<MaybeSecretRef>(data).unwrap(),
            MaybeSecretRef::String("example123".to_string())
        );
    }

    #[test]
    fn test_deserialize_secret_ref() {
        let data = "{ \"secret_ref\": \"example456\" }";
        assert_eq!(
            json5::from_str::<MaybeSecretRef>(data).unwrap(),
            MaybeSecretRef::SecretRef("example456".to_string())
        );
    }

    #[test]
    fn test_string() {
        let test_values_and_expectations = vec![
            ("", MaybeSecretRef::String("".to_string())),
            ("a", MaybeSecretRef::String("a".to_string())),
            ("abc", MaybeSecretRef::String("abc".to_string())),
            (
                "/some/path/to/file.txt",
                MaybeSecretRef::String("/some/path/to/file.txt".to_string()),
            ),
            ("$abc", MaybeSecretRef::String("$abc".to_string())),
            ("${secret", MaybeSecretRef::String("${secret".to_string())),
            ("}", MaybeSecretRef::String("}".to_string())),
            ("${secre:}", MaybeSecretRef::String("${secre:}".to_string())),
            // Unicode "Slightly Smiling Face": U+1F642
            ("\u{1F642}", MaybeSecretRef::String("\u{1F642}".to_string())),
        ];
        for (value, expectation) in test_values_and_expectations {
            assert_eq!(
                MaybeSecretRef::new_using_pattern_match(value.to_string()),
                expectation
            );
        }
    }

    #[test]
    fn test_secret_ref() {
        let test_values_and_expectations = vec![
            ("${secret:}", MaybeSecretRef::SecretRef("".to_string())),
            ("${secret:a}", MaybeSecretRef::SecretRef("a".to_string())),
            (
                "${secret:abc}",
                MaybeSecretRef::SecretRef("abc".to_string()),
            ),
            (
                "${secret:/some/path/to/file.txt}",
                MaybeSecretRef::SecretRef("/some/path/to/file.txt".to_string()),
            ),
            (
                "${secret:$abc}",
                MaybeSecretRef::SecretRef("$abc".to_string()),
            ),
            (
                "${secret:${secret:}}",
                MaybeSecretRef::SecretRef("${secret:}".to_string()),
            ),
            (
                "${secret:${secret:abc}}",
                MaybeSecretRef::SecretRef("${secret:abc}".to_string()),
            ),
            // Unicode "Slightly Smiling Face": U+1F642
            (
                "${secret:\u{1F642}}",
                MaybeSecretRef::SecretRef("\u{1F642}".to_string()),
            ),
        ];
        for (value, expectation) in test_values_and_expectations {
            assert_eq!(
                MaybeSecretRef::new_using_pattern_match(value.to_string()),
                expectation
            );
        }
    }
}
