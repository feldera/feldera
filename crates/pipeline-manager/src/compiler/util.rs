use crate::common_error::CommonError;
use log::{debug, warn};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Creates the new file and will error if it already exists.
pub async fn create_new_file(file_path: &Path) -> Result<File, CommonError> {
    File::create_new(file_path).await.map_err(|e| {
        CommonError::io_error(format!("creating new file '{}'", file_path.display()), e)
    })
}

/// Creates the new file and writes its content. It will error if it already exists.
pub async fn create_new_file_with_content(
    file_path: &Path,
    content: &str,
) -> Result<(), CommonError> {
    let mut file: File = create_new_file(file_path).await?;
    file.write_all(content.as_bytes())
        .await
        .map_err(|e| CommonError::io_error(format!("writing file '{}'", file_path.display()), e))?;
    file.flush().await.map_err(|e| {
        CommonError::io_error(format!("flushing file '{}'", file_path.display()), e)
    })?;
    Ok(())
}

/// Removes the file if it already exists, creates the file anew, and writes its content.
pub async fn recreate_file_with_content(
    file_path: &Path,
    content: &str,
) -> Result<(), CommonError> {
    if file_path.exists() {
        fs::remove_file(file_path).await.map_err(|e| {
            CommonError::io_error(format!("removing file '{}'", file_path.display()), e)
        })?;
    }
    let mut file = File::create_new(file_path).await.map_err(|e| {
        CommonError::io_error(format!("creating new file '{}'", file_path.display()), e)
    })?;
    file.write_all(content.as_bytes())
        .await
        .map_err(|e| CommonError::io_error(format!("writing file '{}'", file_path.display()), e))?;
    file.flush().await.map_err(|e| {
        CommonError::io_error(format!("flushing file '{}'", file_path.display()), e)
    })?;
    Ok(())
}

/// Reads the content of the file as a UTF-8 string.
pub async fn read_file_content(file_path: &Path) -> Result<String, CommonError> {
    fs::read_to_string(file_path)
        .await
        .map_err(|e| CommonError::io_error(format!("reading file '{}'", file_path.display()), e))
}

/// Reads the content of the file as bytes.
pub async fn read_file_content_bytes(file_path: &Path) -> Result<Vec<u8>, CommonError> {
    fs::read(file_path).await.map_err(|e| {
        CommonError::io_error(
            format!("reading as bytes file '{}'", file_path.display()),
            e,
        )
    })
}

/// Copies source file to target file, overriding it.
/// Only the source file must already exist.
pub async fn copy_file(
    source_file_path: &Path,
    target_file_path: &Path,
) -> Result<(), CommonError> {
    fs::copy(source_file_path, target_file_path)
        .await
        .map_err(|e| {
            CommonError::io_error(
                format!(
                    "copying file '{}' to '{}'",
                    source_file_path.display(),
                    target_file_path.display()
                ),
                e,
            )
        })
        .map(|_| ())
}

/// Removes the directory recursively if it already exists, and then creates the directory anew empty.
pub async fn recreate_dir(dir_path: &Path) -> Result<(), CommonError> {
    if dir_path.exists() {
        fs::remove_dir_all(dir_path).await.map_err(|e| {
            CommonError::io_error(format!("removing directory '{}'", dir_path.display()), e)
        })?;
    }
    fs::create_dir_all(dir_path).await.map_err(|e| {
        CommonError::io_error(format!("creating directory '{}'", dir_path.display()), e)
    })?;
    Ok(())
}

/// Lists the content of a directory.
/// Returns a list of the directory entry paths and their name if it can be retrieved.
pub(crate) async fn list_content(
    dir: &Path,
) -> Result<Vec<(PathBuf, Option<String>)>, CommonError> {
    let mut content = vec![];
    match tokio::fs::read_dir(dir).await {
        Ok(mut entries) => loop {
            match entries.next_entry().await {
                Ok(Some(entry)) => {
                    let name = entry
                        .path()
                        .file_name()
                        .map(|s| s.to_string_lossy().to_string());
                    content.push((entry.path(), name));
                }
                // We are done with the directory
                Ok(None) => break,
                Err(e) => {
                    return Err(CommonError::io_error(
                        format!("get next entry of directory '{}'", dir.display()),
                        e,
                    ));
                }
            }
        },
        Err(e) => {
            return Err(CommonError::io_error(
                format!("reading directory '{}'", dir.display()),
                e,
            ));
        }
    }
    Ok(content)
}

#[derive(Debug, PartialEq, Eq)]
pub enum CleanupDecision {
    Keep {
        motivation: String, // The part of the name which was the motivation to keep
    },
    Remove,
    Ignore,
}

/// Cleans up specific files in a directory as determined by the `decide` function.
/// If there is an expectation that there are no other (i.e., ignored) files or directories
/// in the directory, set `warn_ignore` to `true` such that they are warned in the log.
pub async fn cleanup_specific_files(
    cleanup_name: &str,
    dir: &Path,
    decide: Arc<dyn Fn(&str) -> CleanupDecision + Send + Sync>,
    warn_ignore: bool,
) -> Result<Vec<String>, CommonError> {
    let content = list_content(dir).await?;
    let mut keep_motivations = vec![];
    for (path, name) in content {
        if path.is_file() {
            if let Some(name) = name {
                match decide(&name) {
                    CleanupDecision::Keep { motivation } => {
                        // If it should be kept, nothing needs to happen to the file
                        keep_motivations.push(motivation);
                    }
                    CleanupDecision::Remove => {
                        debug!("{cleanup_name} cleanup: removing file '{}'", path.display());
                        fs::remove_file(&path).await.map_err(|e| {
                            CommonError::io_error(format!("removing file '{}'", path.display()), e)
                        })?;
                    }
                    CleanupDecision::Ignore => {
                        if warn_ignore {
                            warn!(
                                "{cleanup_name} cleanup: ignoring '{}' (unexpected file)",
                                path.display()
                            );
                        }
                    }
                }
            } else if warn_ignore {
                warn!(
                    "{cleanup_name} cleanup: ignoring '{}' (unexpected file without a name)",
                    path.display()
                );
            }
        } else if warn_ignore {
            warn!("{cleanup_name} cleanup: ignoring '{}' (unexpected directory entry which is not a file)", path.display());
        }
    }
    Ok(keep_motivations)
}

/// Cleans up specific directories in a directory as determined by the `decide` function.
/// If there is an expectation that there are no other (i.e., ignored) files or directories
/// in the directory, set `warn_ignore` to `true` such that they are warned in the log.
pub async fn cleanup_specific_directories(
    cleanup_name: &str,
    dir: &Path,
    decide: Arc<dyn Fn(&str) -> CleanupDecision + Send + Sync>,
    warn_ignore: bool,
) -> Result<Vec<String>, CommonError> {
    let content = list_content(dir).await?;
    let mut keep_motivations = vec![];
    for (path, name) in content {
        if path.is_dir() {
            if let Some(name) = name {
                match decide(&name) {
                    CleanupDecision::Keep { motivation } => {
                        // If it should be kept, nothing needs to happen to the directory
                        keep_motivations.push(motivation);
                    }
                    CleanupDecision::Remove => {
                        debug!(
                            "{cleanup_name} cleanup: removing directory '{}'",
                            path.display()
                        );
                        fs::remove_dir_all(&path).await.map_err(|e| {
                            CommonError::io_error(
                                format!("removing directory '{}'", path.display()),
                                e,
                            )
                        })?;
                    }
                    CleanupDecision::Ignore => {
                        if warn_ignore {
                            warn!(
                                "{cleanup_name} cleanup: ignoring '{}' (unexpected directory)",
                                path.display()
                            );
                        }
                    }
                }
            } else if warn_ignore {
                warn!(
                    "{cleanup_name} cleanup: ignoring '{}' (unexpected directory without a name)",
                    path.display()
                );
            }
        } else if warn_ignore {
            warn!("{cleanup_name} cleanup: ignoring '{}' (unexpected directory entry which is not a directory)", path.display());
        }
    }
    Ok(keep_motivations)
}

/// Truncates a hex-encoded SHA256 checksum which is 64 characters long into 12 characters.
/// The result should only be used directly for display purposes (e.g., to improve log readability),
/// and should not be propagated. Panics if the truncation does not lie on a char boundary, which
/// should not happen when provided with only `[0-9]` and `[a-f]` characters from a hex encoding.
pub fn truncate_sha256_checksum(checksum: &str) -> String {
    let mut truncated_checksum = checksum.to_string();
    truncated_checksum.truncate(12);
    truncated_checksum
}

/// Validates the provided string is a hex-encoded SHA256 checksum.
pub fn validate_is_sha256_checksum(s: &str) -> Result<(), String> {
    if s.len() != 64 {
        return Err(format!(
            "{} characters long instead of expected 64",
            s.len()
        ));
    };
    for c in s.chars() {
        if !c.is_ascii_digit()
            && c != 'a'
            && c != 'b'
            && c != 'c'
            && c != 'd'
            && c != 'e'
            && c != 'f'
        {
            return Err(format!("character '{c}' is not hexadecimal (0-9, a-f)"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::compiler::util::{
        cleanup_specific_directories, cleanup_specific_files, copy_file, create_new_file,
        create_new_file_with_content, read_file_content, read_file_content_bytes, recreate_dir,
        recreate_file_with_content, truncate_sha256_checksum, validate_is_sha256_checksum,
        CleanupDecision,
    };
    use openssl::sha::sha256;
    use std::sync::Arc;

    #[tokio::test]
    async fn basic_file_dir_operations() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path().to_path_buf();

        // New file
        let file1 = dir_path.join("file1.txt");
        create_new_file(&file1).await.unwrap();
        create_new_file(&file1).await.unwrap_err();

        // New file with string content
        let file2 = dir_path.join("file2.txt");
        create_new_file_with_content(&file2, "abc").await.unwrap();
        create_new_file_with_content(&file2, "def")
            .await
            .unwrap_err();

        // Non existent file
        let non_existent_file = dir_path.join("non_existent_file.txt");

        // Read file contents as string
        assert_eq!(read_file_content(&file1).await.unwrap(), "");
        assert_eq!(read_file_content(&file2).await.unwrap(), "abc");
        read_file_content(&non_existent_file).await.unwrap_err();

        // Read file contents as bytes
        assert_eq!(
            read_file_content_bytes(&file1).await.unwrap(),
            Vec::<u8>::from([])
        );
        assert_eq!(
            read_file_content_bytes(&file2).await.unwrap(),
            Vec::<u8>::from([97, 98, 99])
        );
        read_file_content_bytes(&non_existent_file)
            .await
            .unwrap_err();

        // Recreate file with string content
        let file3 = dir_path.join("file3.txt");
        recreate_file_with_content(&file2, "ghi").await.unwrap();
        recreate_file_with_content(&file3, "jkl").await.unwrap();
        recreate_file_with_content(&file3, "mno").await.unwrap();
        assert_eq!(read_file_content(&file2).await.unwrap(), "ghi");
        assert_eq!(read_file_content(&file3).await.unwrap(), "mno");

        // Copy file
        let non_existent_file2 = dir_path.join("non_existent_file2.txt");
        let file4 = dir_path.join("file4.txt");
        // Copy file: source file must exist
        copy_file(&non_existent_file, &file1).await.unwrap_err();
        copy_file(&non_existent_file, &non_existent_file)
            .await
            .unwrap_err();
        copy_file(&non_existent_file, &non_existent_file2)
            .await
            .unwrap_err();
        // Copy file: successful copy to existing file
        copy_file(&file2, &file3).await.unwrap();
        assert_eq!(read_file_content(&file3).await.unwrap(), "ghi");
        // Copy file: successful copy to not-yet-existing file
        copy_file(&file2, &file4).await.unwrap();
        assert_eq!(read_file_content(&file4).await.unwrap(), "ghi");
        // Copy file: copy to itself leads to truncation
        copy_file(&file2, &file2).await.unwrap();
        assert_eq!(read_file_content(&file2).await.unwrap(), "");

        // Recreate directory
        let dir1 = dir_path.join("dir1");
        recreate_dir(&dir1).await.unwrap();
        assert!(dir1.is_dir());
        let dir1_file5 = dir1.join("file5.txt");
        create_new_file(&dir1_file5).await.unwrap();
        assert!(dir1_file5.is_file());
        recreate_dir(&dir1).await.unwrap();
        assert!(!dir1_file5.is_file()); // After recreating the directory is empty again
    }

    #[tokio::test]
    async fn large_file_writing_and_reading() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path().to_path_buf();
        let file1 = dir_path.join("file1");
        let file2 = dir_path.join("file2");

        // Content of large file (100 MiB)
        let bytes_100_mebibyte: Vec<u8> = vec![0; 100 * 1024 * 1024];

        // Write
        create_new_file_with_content(
            &file1,
            &String::from_utf8(bytes_100_mebibyte.clone()).unwrap(),
        )
        .await
        .unwrap();
        recreate_file_with_content(
            &file2,
            &String::from_utf8(bytes_100_mebibyte.clone()).unwrap(),
        )
        .await
        .unwrap();

        // Read
        assert_eq!(
            bytes_100_mebibyte,
            read_file_content_bytes(&file1).await.unwrap()
        );
        assert_eq!(
            bytes_100_mebibyte,
            read_file_content_bytes(&file2).await.unwrap()
        );
        assert_eq!(
            bytes_100_mebibyte,
            read_file_content(&file1).await.unwrap().as_bytes()
        );
        assert_eq!(
            bytes_100_mebibyte,
            read_file_content(&file2).await.unwrap().as_bytes()
        );
    }

    #[tokio::test]
    async fn file_cleanup() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path().to_path_buf();

        // Files intended to be cleaned up
        for i in 0..100 {
            create_new_file(&dir_path.join(format!("file-{i}.txt")))
                .await
                .unwrap();
        }

        // Ignored ones
        let ignored_dir = dir_path.join("ignore-this-dir");
        let ignored_file = dir_path.join("ignore-this-file.txt");
        recreate_dir(&ignored_dir).await.unwrap();
        create_new_file(&ignored_file).await.unwrap();

        // Perform cleanup
        cleanup_specific_files(
            "",
            &dir_path,
            Arc::new(|name: &str| {
                if name.starts_with("file-") {
                    CleanupDecision::Remove
                } else {
                    CleanupDecision::Keep {
                        motivation: "file-".to_string(),
                    }
                }
            }),
            true,
        )
        .await
        .unwrap();

        // Check that the files are cleaned up
        for i in 0..100 {
            assert!(!dir_path.join(format!("file-{i}.txt")).exists());
        }

        // Check that the ignored ones are still there
        assert!(ignored_dir.is_dir());
        assert!(ignored_file.is_file());
    }

    #[tokio::test]
    async fn dir_cleanup() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path().to_path_buf();

        // Directories intended to be cleaned up
        for j in 0..50 {
            recreate_dir(&dir_path.join(format!("dir-{j}")))
                .await
                .unwrap();
        }

        // Ignored ones
        let ignored_dir = dir_path.join("ignore-this-dir");
        let ignored_file = dir_path.join("ignore-this-file.txt");
        recreate_dir(&ignored_dir).await.unwrap();
        create_new_file(&ignored_file).await.unwrap();

        // Perform cleanup
        cleanup_specific_directories(
            "",
            &dir_path,
            Arc::new(|name: &str| {
                if name.starts_with("dir-") {
                    CleanupDecision::Remove
                } else {
                    CleanupDecision::Keep {
                        motivation: "dir-".to_string(),
                    }
                }
            }),
            true,
        )
        .await
        .unwrap();

        // Check that the directories are cleaned up
        for j in 0..50 {
            assert!(!dir_path.join(format!("dir-{j}")).exists());
        }

        // Check that the ignored ones are still there
        assert!(ignored_dir.is_dir());
        assert!(ignored_file.is_file());
    }

    #[test]
    fn sha256_checksum_truncation() {
        for (input, expected) in [
            ("", ""),
            ("0", "0"),
            ("00", "00"),
            ("00000000000", "00000000000"),
            ("000000000000", "000000000000"),
            ("0000000000000", "000000000000"),
            (
                "0000000000000000000000000000000000000000000000000000000000000000",
                "000000000000",
            ),
            (
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "0123456789ab",
            ),
            (
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "ffffffffffff",
            ),
        ] {
            assert_eq!(truncate_sha256_checksum(input), expected.to_string())
        }
    }

    #[test]
    fn sha256_checksum_validation() {
        // Valid
        for s in [
            "0000000000000000000000000000000000000000000000000000000000000000",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            &hex::encode(sha256(&[])),
            &hex::encode(sha256(&[0])),
            &hex::encode(sha256(&[1])),
        ] {
            assert!(
                validate_is_sha256_checksum(s).is_ok(),
                "'{s}' should be a valid sha256 checksum"
            );
        }

        // Invalid due to length (and/or hexadecimal, but length is checked first)
        for s in [
            "",                                                                  // Too short
            "0",                                                                 // Too short
            "00",                                                                // Too short
            "../other", // Too short and not hexadecimal
            "000000000000000000000000000000000000000000000000000000000000000", // Too short
            "00000000000000000000000000000000000000000000000000000000000000000", // Too long
        ] {
            let result = validate_is_sha256_checksum(s);
            assert!(
                result.is_err(),
                "'{s}' should be an invalid sha256 checksum"
            );
            assert_eq!(
                result.unwrap_err(),
                format!("{} characters long instead of expected 64", s.len()),
                "'{s}' does not have the correct error"
            );
        }

        // Invalid due to not hexadecimal
        for (s, c) in [
            (
                "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
                'g',
            ),
            (
                "00000000000000000000000000000000000000000000000000000000000000\u{00e9}",
                '\u{00e9}',
            ),
            (
                "////////////////////////////////////////////////////////////////",
                '/',
            ),
            (
                "../../../aa/aa/abcdef/../abcdef/../abcdef/abcdef/../abcdef/abc..",
                '.',
            ),
        ] {
            let result = validate_is_sha256_checksum(s);
            assert!(
                result.is_err(),
                "'{s}' should be an invalid sha256 checksum"
            );
            assert_eq!(
                result.unwrap_err(),
                format!("character '{c}' is not hexadecimal (0-9, a-f)"),
                "'{s}' does not have the correct error"
            );
        }
    }
}
