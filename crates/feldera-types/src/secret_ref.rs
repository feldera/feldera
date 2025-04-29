use regex::Regex;
use std::fmt;
use std::fmt::{Display, Formatter};
use thiserror::Error as ThisError;

/// RFC 1123 specification for a DNS label, which is also used by Kubernetes.
pub const PATTERN_RFC_1123_DNS_LABEL: &str = r"^[a-z0-9]+(-[a-z0-9]+)*$";

#[derive(Debug, Clone, PartialEq, Eq, ThisError)]
pub enum KubernetesSecretNameParseError {
    #[error("cannot be empty")]
    Empty,
    #[error("length ({name_len}) exceeds 63 characters")]
    TooLong { name_len: usize },
    #[error("must only contain lowercase alphanumeric characters or hyphens (-), and start and end with a lowercase alphanumeric character")]
    InvalidFormat,
}

/// Validates it is a valid Kubernetes `Secret` name (follows RFC 1123 DNS label).
pub fn validate_kubernetes_secret_name(name: &str) -> Result<(), KubernetesSecretNameParseError> {
    if name.is_empty() {
        Err(KubernetesSecretNameParseError::Empty)
    } else if name.len() > 63 {
        Err(KubernetesSecretNameParseError::TooLong {
            name_len: name.len(),
        })
    } else {
        let re = Regex::new(PATTERN_RFC_1123_DNS_LABEL).expect("valid regular expression");
        if re.is_match(name) {
            Ok(())
        } else {
            Err(KubernetesSecretNameParseError::InvalidFormat)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ThisError)]
pub enum KubernetesSecretDataKeyParseError {
    #[error("cannot be empty")]
    Empty,
    #[error("length ({data_key_len}) exceeds 255 characters")]
    TooLong { data_key_len: usize },
    #[error("must only contain lowercase alphanumeric characters and hyphens (-), and start and end with a lowercase alphanumeric character")]
    InvalidFormat,
}

/// Validates it is a valid Kubernetes Secret data key.
///
/// YAML itself imposes little restrictions on a quoted string key except
/// that it should not exceed 1024 Unicode characters.
///
/// However, it might be mounted as a file, as such characters such as `/`
/// are not desirable. For now, we mirror the convention of the secret name,
/// in which it restricts to lowercase alphanumeric characters and hyphens
/// (and start/end with lower alphanumeric). In the future, this requirement
/// might be loosened. The length is limited to 255 characters (which is
/// stricter than 1024 Unicode characters, but looser than 63 of a DNS label),
/// as the filename length limit for ext4 is 255.
pub fn validate_kubernetes_secret_data_key(
    data_key: &str,
) -> Result<(), KubernetesSecretDataKeyParseError> {
    if data_key.is_empty() {
        Err(KubernetesSecretDataKeyParseError::Empty)
    } else if data_key.len() > 255 {
        Err(KubernetesSecretDataKeyParseError::TooLong {
            data_key_len: data_key.len(),
        })
    } else {
        let re = Regex::new(PATTERN_RFC_1123_DNS_LABEL).expect("valid regular expression");
        if re.is_match(data_key) {
            Ok(())
        } else {
            Err(KubernetesSecretDataKeyParseError::InvalidFormat)
        }
    }
}

/// Enumeration of possible secret references.
/// Each variant corresponds to a secret provider.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum SecretRef {
    /// Reference to a data key in a specific Kubernetes `Secret`.
    Kubernetes {
        /// Name of the Kubernetes `Secret` object.
        name: String,
        /// Key inside the `data:` section of the `Secret` object.
        data_key: String,
    },
}

impl Display for SecretRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SecretRef::Kubernetes { name, data_key } => {
                write!(f, "${{secret:kubernetes:{name}/{data_key}}}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaybeSecretRef {
    String(String),
    SecretRef(SecretRef),
}

#[derive(Debug, Clone, PartialEq, Eq, ThisError)]
pub enum MaybeSecretRefParseError {
    #[error("secret reference '{secret_ref_str}' does not specify a valid provider (for example: 'kubernetes:')")]
    InvalidProvider { secret_ref_str: String },
    #[error("Kubernetes secret reference '{secret_ref_str}' is not valid: does not follow format `<name>/<data key>`")]
    InvalidKubernetesSecretFormat { secret_ref_str: String },
    #[error(
        "Kubernetes secret reference '{secret_ref_str}' has name '{name}' which is not valid: {e}"
    )]
    InvalidKubernetesSecretName {
        secret_ref_str: String,
        name: String,
        e: KubernetesSecretNameParseError,
    },
    #[error(
        "Kubernetes secret reference '{secret_ref_str}' has data key '{data_key}' which is not valid: {e}"
    )]
    InvalidKubernetesSecretDataKey {
        secret_ref_str: String,
        data_key: String,
        e: KubernetesSecretDataKeyParseError,
    },
}

impl MaybeSecretRef {
    /// Determines whether a string is just a plain string or a reference to a secret.
    ///
    /// - Secret reference: any string which starts with `${secret:` and ends with `}`
    ///   is regarded as an attempt to declare a secret reference
    /// - Plain string: any other string
    ///
    /// A secret reference must follow the following pattern:
    /// `${secret:<provider>:<identifier>}`
    ///
    /// An error is returned if a string is regarded as a secret reference (see above), but:
    /// - Specifies a `<provider>` which does not exist
    /// - Specifies a `<identifier>` which does not meet the provider-specific requirements
    ///
    /// Supported providers and their identifier expectations:
    /// - `${secret:kubernetes:<name>/<data key>}`
    ///
    /// Note that here is not checked whether the secret reference can actually be resolved.
    pub fn new(value: String) -> Result<MaybeSecretRef, MaybeSecretRefParseError> {
        if value.starts_with("${secret:") && value.ends_with('}') {
            // Because the pattern only has ASCII characters, they are encoded as single bytes.
            // The secret reference is extracted by slicing away the first 9 bytes and the last byte.
            let from_idx_incl = 9;
            let till_idx_excl = value.len() - 1;
            let content = value[from_idx_incl..till_idx_excl].to_string();
            if let Some(kubernetes_content) = content.strip_prefix("kubernetes:") {
                if let Some((name, data_key)) = kubernetes_content.split_once("/") {
                    if let Err(e) = validate_kubernetes_secret_name(name) {
                        Err(MaybeSecretRefParseError::InvalidKubernetesSecretName {
                            secret_ref_str: value,
                            name: name.to_string(),
                            e,
                        })
                    } else if let Err(e) = validate_kubernetes_secret_data_key(data_key) {
                        Err(MaybeSecretRefParseError::InvalidKubernetesSecretDataKey {
                            secret_ref_str: value,
                            data_key: data_key.to_string(),
                            e,
                        })
                    } else {
                        Ok(MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                            name: name.to_string(),
                            data_key: data_key.to_string(),
                        }))
                    }
                } else {
                    Err(MaybeSecretRefParseError::InvalidKubernetesSecretFormat {
                        secret_ref_str: value,
                    })
                }
            } else {
                Err(MaybeSecretRefParseError::InvalidProvider {
                    secret_ref_str: value,
                })
            }
        } else {
            Ok(MaybeSecretRef::String(value))
        }
    }
}

impl Display for MaybeSecretRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MaybeSecretRef::String(plain_str) => {
                write!(f, "{plain_str}")
            }
            MaybeSecretRef::SecretRef(secret_ref) => {
                write!(f, "{secret_ref}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        validate_kubernetes_secret_data_key, validate_kubernetes_secret_name,
        KubernetesSecretDataKeyParseError, KubernetesSecretNameParseError, MaybeSecretRef,
    };
    use super::{MaybeSecretRefParseError, SecretRef};

    #[test]
    #[rustfmt::skip] // Skip formatting to keep it short
    fn secret_ref_format() {
        assert_eq!(
            format!("{}", SecretRef::Kubernetes {
                name: "example".to_string(),
                data_key: "value".to_string(),
            }),
            "${secret:kubernetes:example/value}"
        );
    }

    #[test]
    #[rustfmt::skip] // Skip formatting to keep it short
    fn maybe_secret_ref_format() {
        assert_eq!(
            format!("{}", MaybeSecretRef::String("example".to_string())),
            "example"
        );
        assert_eq!(
            format!("{}", MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                name: "example".to_string(),
                data_key: "value".to_string(),
            })),
            "${secret:kubernetes:example/value}"
        );
    }

    /// Checks that the parsing of the `MaybeSecretRef` yields the correct result,
    /// and that the original string is preserved when again formatted.
    fn test_values_and_expectations(
        values_and_expectations: Vec<(&str, Result<MaybeSecretRef, MaybeSecretRefParseError>)>,
    ) {
        for (value, expectation) in values_and_expectations {
            let outcome = MaybeSecretRef::new(value.to_string());
            assert_eq!(outcome, expectation);
            if let Ok(maybe_secret_ref) = outcome {
                assert_eq!(maybe_secret_ref.to_string(), value);
            }
        }
    }

    #[test]
    #[rustfmt::skip] // Skip formatting to keep it short
    fn maybe_secret_ref_parse_string() {
        let values_and_expectations = vec![
            ("", Ok(MaybeSecretRef::String("".to_string()))),
            ("a", Ok(MaybeSecretRef::String("a".to_string()))),
            ("1", Ok(MaybeSecretRef::String("1".to_string()))),
            ("example", Ok(MaybeSecretRef::String("example".to_string()))),
            ("EXAMPLE", Ok(MaybeSecretRef::String("EXAMPLE".to_string()))),
            ("123", Ok(MaybeSecretRef::String("123".to_string()))),
            ("/path/to/file.txt", Ok(MaybeSecretRef::String("/path/to/file.txt".to_string()))),
            ("$abc", Ok(MaybeSecretRef::String("$abc".to_string()))),
            ("${secret", Ok(MaybeSecretRef::String("${secret".to_string()))),
            ("}", Ok(MaybeSecretRef::String("}".to_string()))),
            ("${secre:}", Ok(MaybeSecretRef::String("${secre:}".to_string()))),
            // Unicode "Slightly Smiling Face": U+1F642
            ("\u{1F642}", Ok(MaybeSecretRef::String("\u{1F642}".to_string()))),
        ];
        test_values_and_expectations(values_and_expectations);
    }

    #[test]
    #[rustfmt::skip] // Skip formatting to keep it short
    fn maybe_secret_ref_parse_secret_ref_kubernetes() {
        let secret_ref_name_len_63 = format!("${{secret:kubernetes:{}/b}}", "a".repeat(63));
        let secret_ref_name_len_64 = format!("${{secret:kubernetes:{}/b}}", "a".repeat(64));
        let secret_ref_data_key_len_255 = format!("${{secret:kubernetes:a/{}}}", "b".repeat(255));
        let secret_ref_data_key_len_256 = format!("${{secret:kubernetes:a/{}}}", "b".repeat(256));
        let values_and_expectations = vec![
            ("${secret:kubernetes:}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretFormat {
                secret_ref_str: "${secret:kubernetes:}".to_string()
            })),
            ("${secret:kubernetes:ab}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretFormat {
                secret_ref_str: "${secret:kubernetes:ab}".to_string()
            })),
            ("${secret:kubernetes:/}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretName {
                secret_ref_str: "${secret:kubernetes:/}".to_string(),
                name: "".to_string(),
                e: KubernetesSecretNameParseError::Empty
            })),
            ("${secret:kubernetes:/b}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretName {
                secret_ref_str: "${secret:kubernetes:/b}".to_string(),
                name: "".to_string(),
                e: KubernetesSecretNameParseError::Empty
            })),
            ("${secret:kubernetes:a/}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretDataKey {
                secret_ref_str: "${secret:kubernetes:a/}".to_string(),
                data_key: "".to_string(),
                e: KubernetesSecretDataKeyParseError::Empty
            })),
            // Name: character which is not lowercase alphanumeric
            ("${secret:kubernetes:A/b}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretName {
                secret_ref_str: "${secret:kubernetes:A/b}".to_string(),
                name: "A".to_string(),
                e: KubernetesSecretNameParseError::InvalidFormat
            })),
            // Name: cannot start with hyphen
            ("${secret:kubernetes:-a/b}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretName {
                secret_ref_str: "${secret:kubernetes:-a/b}".to_string(),
                name: "-a".to_string(),
                e: KubernetesSecretNameParseError::InvalidFormat
            })),
            // Name: cannot end in hyphen
            ("${secret:kubernetes:a-/b}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretName {
                secret_ref_str: "${secret:kubernetes:a-/b}".to_string(),
                name: "a-".to_string(),
                e: KubernetesSecretNameParseError::InvalidFormat
            })),
            // Data key: character which is not lowercase alphanumeric
            ("${secret:kubernetes:a/B}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretDataKey {
                secret_ref_str: "${secret:kubernetes:a/B}".to_string(),
                data_key: "B".to_string(),
                e: KubernetesSecretDataKeyParseError::InvalidFormat
            })),
            // Data key: cannot start with hyphen
            ("${secret:kubernetes:a/-b}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretDataKey {
                secret_ref_str: "${secret:kubernetes:a/-b}".to_string(),
                data_key: "-b".to_string(),
                e: KubernetesSecretDataKeyParseError::InvalidFormat
            })),
            // Data key: cannot end in hyphen
            ("${secret:kubernetes:a/b-}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretDataKey {
                secret_ref_str: "${secret:kubernetes:a/b-}".to_string(),
                data_key: "b-".to_string(),
                e: KubernetesSecretDataKeyParseError::InvalidFormat
            })),
            // Name: exceeds 63 characters
            (&secret_ref_name_len_64, Err(MaybeSecretRefParseError::InvalidKubernetesSecretName {
                secret_ref_str: secret_ref_name_len_64.to_string(),
                name: "a".repeat(64).to_string(),
                e: KubernetesSecretNameParseError::TooLong {
                    name_len: 64
                }
            })),
            // Data key: exceeds 255 characters
            (&secret_ref_data_key_len_256, Err(MaybeSecretRefParseError::InvalidKubernetesSecretDataKey {
                secret_ref_str: secret_ref_data_key_len_256.to_string(),
                data_key: "b".repeat(256).to_string(),
                e: KubernetesSecretDataKeyParseError::TooLong {
                    data_key_len: 256
                }
            })),
            // Name: Unicode character
            // Unicode "Slightly Smiling Face": U+1F642
            ("${secret:kubernetes:\u{1F642}/b}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretName {
                secret_ref_str: "${secret:kubernetes:\u{1F642}/b}".to_string(),
                name: "\u{1F642}".to_string(),
                e: KubernetesSecretNameParseError::InvalidFormat
            })),
            // Data key: Unicode character
            // Unicode "Slightly Smiling Face": U+1F642
            ("${secret:kubernetes:a/\u{1F642}}", Err(MaybeSecretRefParseError::InvalidKubernetesSecretDataKey {
                secret_ref_str: "${secret:kubernetes:a/\u{1F642}}".to_string(),
                data_key: "\u{1F642}".to_string(),
                e: KubernetesSecretDataKeyParseError::InvalidFormat
            })),
            ("${secret:kubernetes:a/b}", Ok(MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                name: "a".to_string(),
                data_key: "b".to_string()
            }))),
            ("${secret:kubernetes:0/1}", Ok(MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                name: "0".to_string(),
                data_key: "1".to_string()
            }))),
            ("${secret:kubernetes:a0/b1}", Ok(MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                name: "a0".to_string(),
                data_key: "b1".to_string()
            }))),
            ("${secret:kubernetes:0a/1b}", Ok(MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                name: "0a".to_string(),
                data_key: "1b".to_string()
            }))),
            ("${secret:kubernetes:a-b/c-d}", Ok(MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                name: "a-b".to_string(),
                data_key: "c-d".to_string()
            }))),
            (&secret_ref_name_len_63, Ok(MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                name: "a".repeat(63),
                data_key: "b".to_string()
            }))),
            (&secret_ref_data_key_len_255, Ok(MaybeSecretRef::SecretRef(SecretRef::Kubernetes {
                name: "a".to_string(),
                data_key: "b".repeat(255),
            }))),
        ];
        test_values_and_expectations(values_and_expectations);
    }

    #[test]
    #[rustfmt::skip] // Skip formatting to keep it short
    fn kubernetes_secret_name_validation() {
        let name_len_63 = "a".repeat(63);
        let name_len_64 = "a".repeat(64);
        for (value, expectation) in vec![
            ("a", Ok(())),
            ("0", Ok(())),
            ("a0", Ok(())),
            ("0a", Ok(())),
            ("a-0", Ok(())),
            (&name_len_63, Ok(())),
            ("", Err(KubernetesSecretNameParseError::Empty)),
            ("a-", Err(KubernetesSecretNameParseError::InvalidFormat)),
            ("-a", Err(KubernetesSecretNameParseError::InvalidFormat)),
            (&name_len_64, Err(KubernetesSecretNameParseError::TooLong {
                name_len: 64
            })),
        ] {
            assert_eq!(validate_kubernetes_secret_name(value), expectation);
        }
    }

    #[test]
    #[rustfmt::skip] // Skip formatting to keep it short
    fn kubernetes_secret_data_key_validation() {
        let data_key_len_255 = "a".repeat(255);
        let data_key_len_256 = "a".repeat(256);
        for (value, expectation) in vec![
            ("a", Ok(())),
            ("0", Ok(())),
            ("a0", Ok(())),
            ("0a", Ok(())),
            ("a-0", Ok(())),
            (&data_key_len_255, Ok(())),
            ("", Err(KubernetesSecretDataKeyParseError::Empty)),
            ("a-", Err(KubernetesSecretDataKeyParseError::InvalidFormat)),
            ("-a", Err(KubernetesSecretDataKeyParseError::InvalidFormat)),
            (&data_key_len_256, Err(KubernetesSecretDataKeyParseError::TooLong {
                data_key_len: 256
            })),
        ] {
            assert_eq!(validate_kubernetes_secret_data_key(value), expectation);
        }
    }
}
