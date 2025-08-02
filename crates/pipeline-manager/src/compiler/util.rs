use crate::db::types::pipeline::PipelineId;
use base64::prelude::{Engine, BASE64_STANDARD};
use flate2::Compression;
use log::{debug, error, warn};
use nix::libc::pid_t;
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use openssl::sha::sha256;
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error as ThisError;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Automatically terminates a process and all subprocesses it spawns using
/// the group they are all in. The process must have set a process group ID
/// via `Command::process_group()`.
///
/// By having set the process group ID, it is now different from the parent process.
/// As a consequence, using Ctrl-C in the terminal will no longer terminate the
/// compiler process by itself. To preserve this behavior, this special struct is
/// used which will kill the process group when it goes out of scope via the `Drop`
/// trait. This happens generally when the parent process terminates (effectively,
/// whenever the `drop()` function is still called gracefully).
pub struct ProcessGroupTerminator {
    /// Name of what is being terminated (used for logging).
    subject: String,
    process_group: u32,
    is_cancelled: bool,
}

impl ProcessGroupTerminator {
    pub fn new(subject: &str, process_group: u32) -> Self {
        Self {
            subject: subject.to_string(),
            process_group,
            is_cancelled: false,
        }
    }

    /// Cancels the attempt to terminate the process group when it is dropped.
    pub fn cancel(&mut self) {
        self.is_cancelled = true;
    }
}

impl Drop for ProcessGroupTerminator {
    /// Terminates all processes in the process group by sending a SIGKILL using `killpg`.
    fn drop(&mut self) {
        if !self.is_cancelled {
            // Convert the process group (u32) to the pgrp (i32)
            let pgrp = match pid_t::try_from(self.process_group) {
                Ok(pgrp) => pgrp,
                Err(e) => {
                    error!(
                        "Failed to cancel {}: unable to convert process group ({}): {e}",
                        self.subject, self.process_group
                    );
                    return;
                }
            };
            // Send the SIGKILL to the PGRP
            if let Err(e) = killpg(Pid::from_raw(pgrp), Signal::SIGKILL) {
                error!("Failed to cancel {}: attempt to kill the process and its subprocesses (PGRP: {}) failed: {e}", self.subject, self.process_group);
            }
            debug!(
                "Successfully cancelled {} by killing its process group",
                self.subject
            );
        }
    }
}

/// Errors that can occur within the compiler utility functions.
/// Not every function can cause every error, but this does not
/// have to further specified as the errors are bubbled up as
/// textual system errors during the SQL and Rust compilation.
#[derive(ThisError, Debug)]
pub enum UtilError {
    /// I/O operation error (e.g., file creation failed).
    #[error("{0}: {1}")]
    IoError(String, std::io::Error),
    /// Encountered directory entry type which should not be there.
    #[error("{0}")]
    InvalidDirEntry(String),
    /// Encountered a file or directory that should not be there.
    #[error("{0}")]
    InvalidContent(String),
    /// Base64 encoding or decoding error.
    #[error("{0}")]
    Base64(String),
}

/// Creates the new file and will error if it already exists.
pub async fn create_new_file(file_path: &Path) -> Result<File, UtilError> {
    File::create_new(file_path)
        .await
        .map_err(|e| UtilError::IoError(format!("creating new file '{}'", file_path.display()), e))
}

/// Creates the new file and writes its content. It will error if it already exists.
pub async fn create_new_file_with_content(
    file_path: &Path,
    content: &str,
) -> Result<(), UtilError> {
    let mut file: File = create_new_file(file_path).await?;
    file.write_all(content.as_bytes())
        .await
        .map_err(|e| UtilError::IoError(format!("writing file '{}'", file_path.display()), e))?;
    file.flush()
        .await
        .map_err(|e| UtilError::IoError(format!("flushing file '{}'", file_path.display()), e))?;
    Ok(())
}

/// Removes the file if it already exists, creates the file anew, and writes its content.
pub async fn recreate_file_with_content(file_path: &Path, content: &str) -> Result<(), UtilError> {
    if file_path.exists() {
        fs::remove_file(file_path).await.map_err(|e| {
            UtilError::IoError(format!("removing file '{}'", file_path.display()), e)
        })?;
    }
    let mut file = File::create_new(file_path).await.map_err(|e| {
        UtilError::IoError(format!("creating new file '{}'", file_path.display()), e)
    })?;
    file.write_all(content.as_bytes())
        .await
        .map_err(|e| UtilError::IoError(format!("writing file '{}'", file_path.display()), e))?;
    file.flush()
        .await
        .map_err(|e| UtilError::IoError(format!("flushing file '{}'", file_path.display()), e))?;
    Ok(())
}

/// Reads the content of the file as a UTF-8 string.
pub async fn read_file_content(file_path: &Path) -> Result<String, UtilError> {
    fs::read_to_string(file_path)
        .await
        .map_err(|e| UtilError::IoError(format!("reading file '{}'", file_path.display()), e))
}

/// Reads the content of the file as bytes.
pub async fn read_file_content_bytes(file_path: &Path) -> Result<Vec<u8>, UtilError> {
    fs::read(file_path).await.map_err(|e| {
        UtilError::IoError(
            format!("reading as bytes file '{}'", file_path.display()),
            e,
        )
    })
}

/// Copies source file to target file, overwriting it.
/// Only the source file must already exist.
pub async fn copy_file(source_file_path: &Path, target_file_path: &Path) -> Result<(), UtilError> {
    fs::copy(source_file_path, target_file_path)
        .await
        .map_err(|e| {
            UtilError::IoError(
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

/// Copies source file to target file, overwriting it.
/// Only the source file must already exist.
///
/// The copy only occurs if either:
/// (1) The target file does not exist
/// (2) The target file sha256 checksum differs from that of the source file
pub async fn copy_file_if_checksum_differs(
    source_file_path: &Path,
    target_file_path: &Path,
) -> Result<(), UtilError> {
    if !target_file_path.exists() {
        copy_file(source_file_path, target_file_path).await
    } else {
        let source_checksum =
            hex::encode(sha256(&read_file_content_bytes(source_file_path).await?));
        let target_checksum =
            hex::encode(sha256(&read_file_content_bytes(target_file_path).await?));
        if source_checksum != target_checksum {
            copy_file(source_file_path, target_file_path).await
        } else {
            Ok(())
        }
    }
}

/// Removes the directory recursively if it already exists, and then creates the directory anew empty.
pub async fn recreate_dir(dir_path: &Path) -> Result<(), UtilError> {
    if dir_path.exists() {
        fs::remove_dir_all(dir_path).await.map_err(|e| {
            UtilError::IoError(format!("removing directory '{}'", dir_path.display()), e)
        })?;
    }
    fs::create_dir_all(dir_path).await.map_err(|e| {
        UtilError::IoError(format!("creating directory '{}'", dir_path.display()), e)
    })?;
    Ok(())
}

/// Create the directory if it does not exist yet.
pub async fn create_dir_if_not_exists(dir_path: &Path) -> Result<(), UtilError> {
    fs::create_dir_all(dir_path).await.map_err(|e| {
        UtilError::IoError(format!("creating directory '{}'", dir_path.display()), e)
    })?;
    Ok(())
}

/// Encodes the directory as a string by:
/// - Compressing it into a gzipped tar archive
/// - Base64-encoding the archive
pub fn encode_dir_as_string(dir_path: &Path) -> Result<String, UtilError> {
    // Compress as gzipped tar archive
    let encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = tar::Builder::new(encoder);
    builder.append_dir_all("", dir_path).map_err(|e| {
        UtilError::IoError(
            format!(
                "appending directory '{}' in gzipped tar archive",
                dir_path.display()
            ),
            e,
        )
    })?;
    builder
        .finish()
        .map_err(|e| UtilError::IoError("finish building tar archive".to_string(), e))?;
    let encoder = builder
        .into_inner()
        .map_err(|e| UtilError::IoError("retrieve gz encoder for tar archive".to_string(), e))?;
    let data_bytes = encoder
        .finish()
        .map_err(|e| UtilError::IoError("finish gz encoding".to_string(), e))?;

    // Base-64 encode the raw bytes into a UTF-8 string
    let data_str = BASE64_STANDARD.encode(&data_bytes);
    Ok(data_str)
}

/// Decodes the base64-encoded gzipped tar archive into a directory.
pub fn decode_string_as_dir(data_str: &str, target_dir_path: &Path) -> Result<(), UtilError> {
    // Base-64 decode the UTF-8 string into raw bytes
    let data_bytes = BASE64_STANDARD
        .decode(data_str)
        .map_err(|e| UtilError::Base64(format!("invalid base64 encoding: {e}")))?;

    // Decompress gzipped tar archive
    let decoder = flate2::read::GzDecoder::new(data_bytes.as_slice());
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(target_dir_path).map_err(|e| {
        UtilError::IoError(
            format!(
                "unpacking gzipped tar archive to directory '{}'",
                target_dir_path.display()
            ),
            e,
        )
    })?;
    Ok(())
}

pub struct DirectoryContent {
    dir: PathBuf,
    pub content: Vec<(PathBuf, String, bool)>,
}

impl DirectoryContent {
    /// Reads the content of a directory, and returns it.
    ///
    /// Returns an error if:
    /// - An I/O operation failed
    /// - A directory entry has a file name which is not valid UTF-8
    /// - A directory entry is a symlink
    pub async fn new(dir: &Path) -> Result<Self, UtilError> {
        let mut content = vec![];
        match tokio::fs::read_dir(dir).await {
            Ok(mut entries) => loop {
                match entries.next_entry().await {
                    Ok(Some(entry)) => {
                        // Entry information
                        let path = entry.path();
                        let Some(name) = entry.file_name().to_str().map(|v| v.to_string()) else {
                            return Err(UtilError::InvalidDirEntry(format!(
                                "Directory entry is not valid UTF-8 file name: '{}'",
                                path.display()
                            )));
                        };

                        // Entry type
                        let file_type = entry.file_type().await.map_err(|e| {
                            UtilError::IoError(
                                format!("obtain file type of: '{}'", path.display(),),
                                e,
                            )
                        })?;
                        if file_type.is_symlink() {
                            return Err(UtilError::InvalidDirEntry(format!(
                                "Directory entry is a symlink, which is not allowed: '{}'",
                                path.display()
                            )));
                        } else if file_type.is_file() {
                            content.push((path, name.to_string(), true))
                        } else if file_type.is_dir() {
                            content.push((path, name.to_string(), false))
                        } else {
                            return Err(UtilError::InvalidDirEntry(format!(
                                "Directory entry is not a file nor directory, which is not allowed: '{}'",
                                path.display()
                            )));
                        }
                    }
                    Ok(None) => {
                        // We have gone over all the entries
                        break;
                    }
                    Err(e) => {
                        return Err(UtilError::IoError(
                            format!("get next entry of directory '{}'", dir.display()),
                            e,
                        ));
                    }
                }
            },
            Err(e) => {
                return Err(UtilError::IoError(
                    format!("reading directory '{}'", dir.display()),
                    e,
                ));
            }
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            content,
        })
    }

    /// Returns sorted list of all file and directory names, matched with a boolean
    /// which is `true` if it is a file, and `false` if it is a directory.
    pub fn list_names_and_is_file(&self) -> Vec<(String, bool)> {
        let mut content: Vec<(String, bool)> = self
            .content
            .iter()
            .map(|(_, name, is_file)| (name.clone(), *is_file))
            .collect();
        content.sort();
        content
    }

    /// Validates the named content of a directory with an expectation.
    /// The expectation is not met if either a file/directory that should
    /// be there is not, or if a file/directory that shouldn't be there is.
    /// An error is returned if the expectation is not met.
    pub(crate) fn validate(&self, expected: &Vec<(&str, bool)>) -> Result<(), UtilError> {
        let content = self.list_names_and_is_file();

        // A file or directory is expected to be there but is not
        for (name, is_file) in expected {
            if !content.contains(&(name.to_string(), *is_file)) {
                return Err(UtilError::InvalidContent(format!(
                    "{} '{}' is missing in: '{}'",
                    if *is_file { "File" } else { "Directory" },
                    name,
                    self.dir.display()
                )));
            }
        }

        // A file or directory is there, but should not be
        for (name, is_file) in content {
            if !expected.contains(&(&name, is_file)) {
                return Err(UtilError::InvalidContent(format!(
                    "{} '{}' is not allowed to be in: '{}'",
                    if is_file { "File" } else { "Directory" },
                    name,
                    self.dir.display()
                )));
            }
        }

        Ok(())
    }

    /// Only keep the specific content (name, is_file) of the directory.
    /// This method consumes `self` as it is potentially no longer valid afterward.
    pub(crate) async fn keep(
        self,
        keep: &Vec<(&str, bool)>,
        remove_no_warn: &Vec<(&str, bool)>,
    ) -> Result<(), UtilError> {
        for (path, name, is_file) in self.content {
            if !keep.contains(&(&name, is_file)) {
                if !remove_no_warn.contains(&(&name, is_file)) {
                    warn!(
                        "Removing {} '{}' because it should not be in: '{}'",
                        if is_file { "file" } else { "directory" },
                        name,
                        self.dir.display()
                    );
                }
                if is_file {
                    fs::remove_file(&path).await.map_err(|e| {
                        UtilError::IoError(format!("removing file '{}'", path.display()), e)
                    })?;
                } else {
                    fs::remove_dir_all(&path).await.map_err(|e| {
                        UtilError::IoError(format!("removing directory '{}'", path.display()), e)
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CleanupDecision {
    Keep {
        motivation: String, // The part of the name which was the motivation to keep
    },
    Remove,
    Ignore,
}

/// Decides whether to keep a file around or not.
pub type DecisionFn = Arc<dyn Fn(&str, Option<Metadata>) -> CleanupDecision + Send + Sync>;

/// Cleans up specific files in a directory as determined by the `decide` function.
/// If there is an expectation that there are no other (i.e., ignored) files or directories
/// in the directory, set `warn_ignore` to `true` such that they are warned in the log.
pub async fn cleanup_specific_files(
    cleanup_name: &str,
    dir: &Path,
    decide: DecisionFn,
    warn_ignore: bool,
    add_metadata: bool,
) -> Result<Vec<String>, UtilError> {
    let content = DirectoryContent::new(dir).await?.content;
    let mut keep_motivations = vec![];
    for (path, name, is_file) in content {
        if is_file {
            let metrics = if add_metadata {
                fs::metadata(&path).await.ok()
            } else {
                None
            };
            match decide(&name, metrics) {
                CleanupDecision::Keep { motivation } => {
                    // If it should be kept, nothing needs to happen to the file
                    keep_motivations.push(motivation);
                }
                CleanupDecision::Remove => {
                    debug!("{cleanup_name} cleanup: removing file '{}'", path.display());
                    fs::remove_file(&path).await.map_err(|e| {
                        UtilError::IoError(format!("removing file '{}'", path.display()), e)
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
) -> Result<Vec<String>, UtilError> {
    let content = DirectoryContent::new(dir).await?.content;
    let mut keep_motivations = vec![];
    for (path, name, is_file) in content {
        if !is_file {
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
                        UtilError::IoError(format!("removing directory '{}'", path.display()), e)
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

/// Name base for the `main` and `globals` crates.
pub fn crate_name_pipeline_base(pipeline_id: PipelineId) -> String {
    format!("pipeline_{}", pipeline_id.to_string().replace("-", "_"))
}

/// Name of the `main` crate belonging to the pipeline.
pub fn crate_name_pipeline_main(pipeline_id: PipelineId) -> String {
    format!(
        "feldera_pipe_{}_main",
        crate_name_pipeline_base(pipeline_id)
    )
}

/// Name of the `globals` crate belonging to the pipeline.
pub fn crate_name_pipeline_globals(pipeline_id: PipelineId) -> String {
    format!(
        "feldera_pipe_{}_globals",
        crate_name_pipeline_base(pipeline_id)
    )
}

#[cfg(test)]
mod test {
    use crate::compiler::util::{
        cleanup_specific_directories, cleanup_specific_files, copy_file,
        copy_file_if_checksum_differs, crate_name_pipeline_base, crate_name_pipeline_globals,
        crate_name_pipeline_main, create_dir_if_not_exists, create_new_file,
        create_new_file_with_content, decode_string_as_dir, encode_dir_as_string,
        read_file_content, read_file_content_bytes, recreate_dir, recreate_file_with_content,
        truncate_sha256_checksum, validate_is_sha256_checksum, CleanupDecision, DirectoryContent,
    };
    use crate::db::types::pipeline::PipelineId;
    use openssl::sha::sha256;
    use std::fs::Metadata;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::fs;
    use tokio::time::sleep;
    use uuid::Uuid;

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

        // Create directory if it doesn't exist
        let dir2 = dir_path.join("dir2");
        create_dir_if_not_exists(&dir2).await.unwrap();
        assert!(dir2.is_dir());
        create_dir_if_not_exists(&dir2).await.unwrap();
        assert!(dir2.is_dir());
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
    async fn checksum_based_file_copying() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path().to_path_buf();

        // Create some files
        create_new_file_with_content(&dir_path.join("file1.txt"), "a")
            .await
            .unwrap();

        // File does not exist
        copy_file_if_checksum_differs(&dir_path.join("file1.txt"), &dir_path.join("file2.txt"))
            .await
            .unwrap();
        assert_eq!(
            read_file_content(&dir_path.join("file2.txt"))
                .await
                .unwrap(),
            "a"
        );

        // File exist, but is different, so is overwritten (modified timestamp changes)
        create_new_file_with_content(&dir_path.join("file3.txt"), "b")
            .await
            .unwrap();
        let metadata3_before = fs::metadata(&dir_path.join("file3.txt")).await.unwrap();
        sleep(Duration::from_millis(5)).await; // Shortly sleep to be sure clock progress
        copy_file_if_checksum_differs(&dir_path.join("file1.txt"), &dir_path.join("file3.txt"))
            .await
            .unwrap();
        assert_eq!(
            read_file_content(&dir_path.join("file3.txt"))
                .await
                .unwrap(),
            "a"
        );
        let metadata3_after = fs::metadata(&dir_path.join("file3.txt")).await.unwrap();
        assert_ne!(
            metadata3_before.modified().unwrap(),
            metadata3_after.modified().unwrap(),
        );

        // File exists, but is the same so is not overwritten (modified timestamp does not change)
        create_new_file_with_content(&dir_path.join("file4.txt"), "a")
            .await
            .unwrap();
        let metadata4_before = fs::metadata(&dir_path.join("file4.txt")).await.unwrap();
        sleep(Duration::from_millis(5)).await; // Shortly sleep to be sure clock progress
        copy_file_if_checksum_differs(&dir_path.join("file1.txt"), &dir_path.join("file4.txt"))
            .await
            .unwrap();
        assert_eq!(
            read_file_content(&dir_path.join("file4.txt"))
                .await
                .unwrap(),
            "a"
        );
        let metadata4_after = fs::metadata(&dir_path.join("file4.txt")).await.unwrap();
        assert_eq!(
            metadata4_before.modified().unwrap(),
            metadata4_after.modified().unwrap(),
        );
    }

    #[tokio::test]
    async fn directory_encoding_decoding() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path().to_path_buf();

        // Create directory with some content
        let dir1 = dir_path.join("dir1");
        recreate_dir(&dir1).await.unwrap();
        create_new_file(&dir1.join("empty_file.txt")).await.unwrap();
        create_new_file_with_content(&dir1.join("file.txt"), "abc")
            .await
            .unwrap();
        create_dir_if_not_exists(&dir1.join("empty_subdir"))
            .await
            .unwrap();
        create_dir_if_not_exists(&dir1.join("subdir"))
            .await
            .unwrap();
        create_new_file_with_content(&dir1.join("subdir").join("file.txt"), "def")
            .await
            .unwrap();

        // Encode
        let encoded = encode_dir_as_string(&dir1).unwrap();

        // Decode
        let dir2 = dir_path.join("dir2");
        recreate_dir(&dir2).await.unwrap();
        decode_string_as_dir(&encoded, &dir2).unwrap();

        // Check dir2
        assert_eq!(
            DirectoryContent::new(&dir2)
                .await
                .unwrap()
                .list_names_and_is_file(),
            vec![
                ("empty_file.txt".to_string(), true),
                ("empty_subdir".to_string(), false),
                ("file.txt".to_string(), true),
                ("subdir".to_string(), false),
            ]
        );
        assert_eq!(
            read_file_content(&dir2.join("empty_file.txt"))
                .await
                .unwrap(),
            ""
        );
        assert_eq!(
            read_file_content(&dir2.join("file.txt")).await.unwrap(),
            "abc"
        );
        assert_eq!(
            DirectoryContent::new(&dir2.join("empty_subdir"))
                .await
                .unwrap()
                .list_names_and_is_file(),
            vec![]
        );
        assert_eq!(
            DirectoryContent::new(&dir2.join("subdir"))
                .await
                .unwrap()
                .list_names_and_is_file(),
            vec![("file.txt".to_string(), true),]
        );
        assert_eq!(
            read_file_content(&dir2.join("subdir").join("file.txt"))
                .await
                .unwrap(),
            "def"
        );
    }

    #[tokio::test]
    async fn directory_content_validation() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path().to_path_buf();

        // Create directory with some content
        recreate_dir(&dir_path).await.unwrap();
        create_new_file(&dir_path.join("file.txt")).await.unwrap();
        create_dir_if_not_exists(&dir_path.join("subdir"))
            .await
            .unwrap();

        // Correct content
        DirectoryContent::new(&dir_path)
            .await
            .unwrap()
            .validate(&vec![("file.txt", true), ("subdir", false)])
            .unwrap();

        // Incorrect content
        for expected in [
            vec![],
            vec![("file.txt", true)], // Directory 'subdir' is not allowed
            vec![("subdir", false)],  // File 'file.txt' is not allowed
            vec![("file.txt", true), ("subdir", true)], // Incorrectly marked as file/dir
            vec![("file.txt", false), ("subdir", false)], // Incorrectly marked as file/dir
            vec![("file.txt", false), ("subdir", true)], // Incorrectly marked as file/dir
            vec![("file.txt", true), ("subdir", false), ("extra.txt", true)], // Missing file 'extra.txt'
            vec![("file.txt", true), ("subdir", false), ("extra", false)], // Missing directory 'extra'
        ] {
            DirectoryContent::new(&dir_path)
                .await
                .unwrap()
                .validate(&expected)
                .unwrap_err();
        }
    }

    #[tokio::test]
    async fn directory_content_keep() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir_path = tempdir.path().to_path_buf();

        // Create directory with some content
        recreate_dir(&dir_path).await.unwrap();
        create_new_file(&dir_path.join("file1.txt")).await.unwrap();
        create_new_file(&dir_path.join("file2.txt")).await.unwrap();
        create_dir_if_not_exists(&dir_path.join("subdir1"))
            .await
            .unwrap();
        create_dir_if_not_exists(&dir_path.join("subdir2"))
            .await
            .unwrap();
        create_new_file(&dir_path.join("subdir2").join("file3.txt"))
            .await
            .unwrap();

        // Keep only desired content
        DirectoryContent::new(&dir_path)
            .await
            .unwrap()
            .keep(&vec![("file1.txt", true), ("subdir1", false)], &vec![])
            .await
            .unwrap();
        assert!(dir_path.join("file1.txt").is_file());
        assert!(!dir_path.join("file2.txt").exists());
        assert!(dir_path.join("subdir1").is_dir());
        assert!(!dir_path.join("subdir2").exists());
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
            Arc::new(|name: &str, metadata: Option<Metadata>| {
                assert!(metadata.is_some());
                if name.starts_with("file-") {
                    CleanupDecision::Remove
                } else {
                    CleanupDecision::Keep {
                        motivation: "file-".to_string(),
                    }
                }
            }),
            true,
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

    #[test]
    fn pipeline_crate_names() {
        let pipeline_id = PipelineId(Uuid::nil());
        assert_eq!(
            crate_name_pipeline_base(pipeline_id),
            "pipeline_00000000_0000_0000_0000_000000000000"
        );
        assert_eq!(
            crate_name_pipeline_main(pipeline_id),
            "feldera_pipe_pipeline_00000000_0000_0000_0000_000000000000_main"
        );
        assert_eq!(
            crate_name_pipeline_globals(pipeline_id),
            "feldera_pipe_pipeline_00000000_0000_0000_0000_000000000000_globals"
        );
    }
}
