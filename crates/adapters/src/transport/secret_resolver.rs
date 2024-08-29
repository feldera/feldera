#![allow(dead_code)]

use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::path::Path;

use anyhow::{anyhow, Result as AnyResult};
use feldera_types::secret_ref::MaybeSecretRef;
use log::debug;
use regex::Regex;

/// Enumeration which holds a simple string or a resolved secret's string.
///
/// This enumeration exists such that a resolved secret's string is not shown
/// upon formatting (irrespective if `Display` or `Debug`). This is preferable
/// over using a simple `String`, which would show its content upon formatting.
///
/// Creation of a secret happens using [MaybeSecret::new], which resolves the
/// secret reference it potentially receives to the actual secret string.
#[derive(Clone, PartialEq, Eq)]
pub enum MaybeSecret {
    String(String),
    Secret(String),
}

impl Display for MaybeSecret {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MaybeSecret::String(simple_string) => {
                write!(f, "String(\"{}\")", simple_string)
            }
            MaybeSecret::Secret(_) => {
                write!(f, "Secret(***)")
            }
        }
    }
}

impl Debug for MaybeSecret {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Use Display formatting
        write!(f, "{}", self)
    }
}

impl MaybeSecret {
    /// Path of the default secrets directory.
    pub const DEFAULT_SECRETS_DIRECTORY_PATH: &'static str = "/etc/secrets";

    /// Regex pattern for a valid secret reference name.
    ///
    /// A secret reference can only contain a-z and 0-9. There can be connecting
    /// hyphens (-), though not at the start, not at the end, and not two in
    /// succession. It cannot be empty.
    ///
    /// Valid examples:
    /// "example", "example-1", "example-of-this"
    ///
    /// Invalid examples:
    /// "", "example--1", "example_1", "-", "Example#1", "example-"
    pub const PATTERN_VALID_SECRET_REF: &'static str = r"^[a-z0-9]+(-[a-z0-9]+)*$";

    /// Checks whether the provided secret reference follows
    /// the valid secret reference pattern:
    /// [PATTERN_VALID_SECRET_REF](Self::PATTERN_VALID_SECRET_REF).
    fn is_valid_secret_ref_format(secret_ref: &str) -> bool {
        let re = Regex::new(Self::PATTERN_VALID_SECRET_REF).unwrap();
        re.is_match(secret_ref)
    }

    /// Resolves a potential secret reference to a secret or simply return the
    /// string if it is a string. The default secrets directory is used.
    /// More information regarding resolution is detailed in
    /// [MaybeSecret::new].
    pub fn new_using_default_directory(value: MaybeSecretRef) -> AnyResult<Self> {
        Self::new(Path::new(Self::DEFAULT_SECRETS_DIRECTORY_PATH), value)
    }

    /// Resolves a potential secret reference to a secret or simply return the
    /// string if it is a string.
    ///
    /// The current resolution method is through files within the secrets
    /// directory ("/path/to/secrets"). A secret reference "example" will have
    /// the secret string fetched from the file at
    /// "/path/to/secrets/.example".
    ///
    /// Note: prefixing each secret file with a dot is a convention from the
    /// Kubernetes documentation for mounted secret files:
    /// https://kubernetes.io/docs/concepts/configuration/secret/
    /// (such that they are are hidden in `ls -l` and require `ls -la` to show).
    pub fn new(secrets_directory_path: &Path, value: MaybeSecretRef) -> AnyResult<Self> {
        match value {
            MaybeSecretRef::String(simple_string) => Ok(Self::String(simple_string)),
            MaybeSecretRef::SecretRef(secret_ref) => {
                // Check format
                if !Self::is_valid_secret_ref_format(&secret_ref) {
                    return Err(anyhow!(
                        "Secret reference does not follow valid format: {}",
                        secret_ref
                    ));
                }

                // Create the file path by appending the filename to the secrets directory path
                let filename = ".".to_string() + &secret_ref;
                let file_path = secrets_directory_path.join(filename);

                // Check that metadata can be retrieved before actually trying to read
                // from it. This provides a more user-friendly distinguishing error message
                // for the file not being present at all versus the file not being readable
                // (e.g., because it is a directory, or insufficient permission).
                match fs::metadata(&file_path) {
                    Ok(_) => match fs::read_to_string(&file_path) {
                        Ok(content) => {
                            debug!("Secret reference resolution successful: {}", secret_ref);
                            Ok(Self::Secret(content))
                        },
                        Err(err) => Err(anyhow!(
                                "Could not resolve secret reference {} as file: {:?} (is it readable?) -- read failed: {}",
                                secret_ref, file_path, err
                        ))
                    },
                    Err(err) => Err(anyhow!(
                        "Could not resolve secret reference {} as file: {:?} (was it added to the secrets directory?) -- metadata check failed: {}",
                        secret_ref, file_path, err
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::Write;

    use super::MaybeSecret;
    use feldera_types::secret_ref::MaybeSecretRef;

    #[test]
    fn test_new_string() {
        match MaybeSecret::new_using_default_directory(MaybeSecretRef::String(
            "example".to_string(),
        )) {
            Ok(MaybeSecret::String(simple_string)) => {
                assert_eq!("example", simple_string);
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_new_secret_success() {
        // Create a temporary directory which acts as the secrets directory.
        // Create a file there with the secret reference name ".the-secret".
        // The file contains the secret string "example".
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let file_path = &dir_path.join(".the-secret");
        let mut file = File::create(file_path).unwrap();
        file.write_all(b"example").unwrap();
        println!("Temporary directory: {:?}", dir_path);
        println!("Temporary file: {:?}", file_path);

        // Check that a secret with the provided filename as reference is correctly
        // resolved
        match MaybeSecret::new(
            dir_path,
            MaybeSecretRef::SecretRef("the-secret".to_string()),
        ) {
            Ok(MaybeSecret::Secret(secret_string)) => {
                assert_eq!("example", secret_string);
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_format_check() {
        let valid = vec![
            "example",
            "example-1",
            "example-of-this",
            "e",
            "ab",
            "example-abcde",
        ];
        for secret_ref in valid {
            assert!(MaybeSecret::is_valid_secret_ref_format(
                &secret_ref.to_string()
            ));
        }
        let invalid = vec![
            "",
            "example--1",
            "example_1",
            "-",
            "Example#1",
            "example-",
            "a-",
            "-b",
            "-a-",
            "--",
            "EXAMPLE",
        ];
        for secret_ref in invalid {
            assert!(!MaybeSecret::is_valid_secret_ref_format(
                &secret_ref.to_string()
            ));
        }
    }

    #[test]
    fn test_new_secret_failure_invalid_format() {
        match MaybeSecret::new_using_default_directory(MaybeSecretRef::SecretRef(
            "-example".to_string(),
        )) {
            Ok(_) => {
                assert!(false);
            }
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "Secret reference does not follow valid format: -example"
                )
            }
        }
    }

    #[test]
    fn test_new_secret_failure_file_does_not_exist() {
        // Create a temporary directory which acts as the secrets directory.
        // No file is created inside explicitly.
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        println!("Temporary directory: {:?}", dir_path);

        // Provide the deleted file path
        match MaybeSecret::new(
            dir_path,
            MaybeSecretRef::SecretRef("the-secret".to_string()),
        ) {
            Ok(_) => {
                assert!(false);
            }
            Err(err) => {
                let err_string = err.to_string();
                assert!(err_string.starts_with(format!(
                    "Could not resolve secret reference {} as file: {:?} (was it added to the secrets directory?) -- metadata check failed: ",
                    "the-secret", dir_path.join(".the-secret")
                ).as_str()));
                // The exact string of the file system error is not matched, as
                // it might vary system-to-system
            }
        }
    }

    #[test]
    fn test_new_secret_failure_file_is_a_dir() {
        // Create a temporary directory which acts as the secrets directory.
        // A directory is created inside.
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let file_path = &dir_path.join(".the-secret");
        fs::create_dir(file_path).unwrap();
        println!("Temporary directory: {:?}", dir_path);
        println!("Temporary file: {:?}", file_path);

        // Provide the file path which is a directory
        match MaybeSecret::new(
            dir_path,
            MaybeSecretRef::SecretRef("the-secret".to_string()),
        ) {
            Ok(_) => {
                assert!(false);
            }
            Err(err) => {
                let err_string = err.to_string();
                assert!(err_string.starts_with(format!(
                    "Could not resolve secret reference {} as file: {:?} (is it readable?) -- read failed: ",
                    "the-secret", dir_path.join(".the-secret")
                ).as_str()));
                // The exact string of the file system error is not matched, as
                // it might vary system-to-system
            }
        }
    }

    #[test]
    fn test_format() {
        assert_eq!(
            "String(\"example123\")",
            format!("{}", MaybeSecret::String("example123".to_string()))
        );
        assert_eq!(
            "String(\"example123\")",
            format!("{:?}", MaybeSecret::String("example123".to_string()))
        );
        assert_eq!(
            "Secret(***)",
            format!("{}", MaybeSecret::Secret("example123".to_string()))
        );
        assert_eq!(
            "Secret(***)",
            format!("{:?}", MaybeSecret::Secret("example123".to_string()))
        );
    }

    #[test]
    fn test_internal_strings() {
        match MaybeSecret::String("example123".to_string()) {
            MaybeSecret::String(simple_string) => {
                assert_eq!(simple_string, "example123");
            }
            MaybeSecret::Secret(_) => {
                assert!(false);
            }
        }
        match MaybeSecret::Secret("example123".to_string()) {
            MaybeSecret::String(_) => {
                assert!(false);
            }
            MaybeSecret::Secret(secret_string) => {
                assert_eq!(secret_string, "example123");
            }
        }
    }
}
