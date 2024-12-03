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
    Ok(())
}

/// Removes the file if it already exists, and then creates the file anew.
pub async fn recreate_file(file_path: &Path) -> Result<File, CommonError> {
    if file_path.exists() {
        fs::remove_file(file_path).await.map_err(|e| {
            CommonError::io_error(format!("removing file '{}'", file_path.display()), e)
        })?;
    }
    File::create_new(file_path).await.map_err(|e| {
        CommonError::io_error(format!("creating new file '{}'", file_path.display()), e)
    })
}

/// Removes the file if it already exists, creates the file anew, and writes its content.
pub async fn recreate_file_with_content(
    file_path: &Path,
    content: &str,
) -> Result<(), CommonError> {
    let mut file: File = recreate_file(file_path).await?;
    file.write_all(content.as_bytes())
        .await
        .map_err(|e| CommonError::io_error(format!("writing file '{}'", file_path.display()), e))?;
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
/// Both source and target file must already exist.
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
async fn list_content(dir: &Path) -> Result<Vec<(PathBuf, Option<String>)>, CommonError> {
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

pub enum CleanupDecision {
    Keep,
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
) -> Result<(), CommonError> {
    let content = list_content(dir).await?;
    for (path, name) in content {
        if path.is_file() {
            if let Some(name) = name {
                match decide(&name) {
                    CleanupDecision::Keep => {
                        // If it should be kept, nothing needs to happen
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
    Ok(())
}

/// Cleans up specific directories in a directory as determined by the `decide` function.
/// If there is an expectation that there are no other (i.e., ignored) files or directories
/// in the directory, set `warn_ignore` to `true` such that they are warned in the log.
pub async fn cleanup_specific_directories(
    cleanup_name: &str,
    dir: &Path,
    decide: Arc<dyn Fn(&str) -> CleanupDecision + Send + Sync>,
    warn_ignore: bool,
) -> Result<(), CommonError> {
    let content = list_content(dir).await?;
    for (path, name) in content {
        if path.is_dir() {
            if let Some(name) = name {
                match decide(&name) {
                    CleanupDecision::Keep => {
                        // If it should be kept, nothing needs to happen
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
    Ok(())
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
