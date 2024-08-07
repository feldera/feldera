use crate::db::types::pipeline::PipelineDescr;
use crate::error::ManagerError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use thiserror::Error as ThisError;
use utoipa::ToSchema;

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum DemoError {
    /// Error when unable to read the demos directory.
    #[error("could not read demos directory {path} due to: {error}")]
    UnableToReadDirectory { path: String, error: String },
    /// Error when unable to read a directory entry in the demo directory.
    #[error("could not read directory entry due to: {error}")]
    UnableToReadDirEntry { error: String },
    /// Error when the demo file could not be read to a string.
    #[error("could not read file {path} to string due to: {error}")]
    UnableToReadFile { path: String, error: String },
    /// Error when the demo JSON could not be deserialized.
    #[error("could not JSON deserialize string read from {path} as demo due to: {error}")]
    DeserializationFailed { path: String, error: String },
}

#[derive(Deserialize, Serialize, ToSchema, Eq, PartialEq, Debug, Clone)]
pub struct Demo {
    /// Demo title.
    pub title: String,

    /// Demo pipeline.
    pub pipeline: PipelineDescr,
}

/// Reads the JSON demos from the demos directory.
///
/// Every file in the directory must be a JSON file, and there cannot be
/// any other files or directories in there.
pub fn read_demos_from_directory(demos_dir: &Path) -> Result<Vec<Demo>, ManagerError> {
    let mut result: Vec<Demo> = vec![];

    // Directory entries
    let entries = fs::read_dir(demos_dir)
        .map_err(|error| ManagerError::DemoError {
            demo_error: DemoError::UnableToReadDirectory {
                path: demos_dir.to_string_lossy().to_string(),
                error: error.to_string(),
            },
        })?
        .collect::<Vec<_>>();

    // Sorted paths
    let mut paths = vec![];
    for entry in entries {
        let entry = entry.map_err(|error| ManagerError::DemoError {
            demo_error: DemoError::UnableToReadDirEntry {
                error: error.to_string(),
            },
        })?;
        paths.push(entry.path());
    }
    paths.sort();

    // Convert each file read from a path to a demo
    for path in paths {
        let content =
            fs::read_to_string(path.as_path()).map_err(|error| ManagerError::DemoError {
                demo_error: DemoError::UnableToReadFile {
                    path: path.to_string_lossy().to_string(),
                    error: error.to_string(),
                },
            })?;
        let val = serde_json::from_str(&content).map_err(|error| ManagerError::DemoError {
            demo_error: DemoError::DeserializationFailed {
                path: path.to_string_lossy().to_string(),
                error: error.to_string(),
            },
        })?;
        result.push(val);
    }
    Ok(result)
}

#[cfg(test)]
mod test {
    use crate::db::types::pipeline::PipelineDescr;
    use crate::demo::{read_demos_from_directory, Demo, DemoError};
    use crate::error::ManagerError;
    use std::fs;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn demos_dir_empty() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        assert!(read_demos_from_directory(dir_path).unwrap().is_empty());
    }

    #[test]
    fn demos_dir_does_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().join("does-not-exist");
        assert!(
            match read_demos_from_directory(dir_path.as_path()).unwrap_err() {
                ManagerError::DemoError { demo_error } => {
                    match demo_error {
                        DemoError::UnableToReadDirectory { .. } => true,
                        _ => false,
                    }
                }
                _ => false,
            }
        );
    }

    #[test]
    fn demos_dir_directory_present() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        fs::create_dir(dir_path.join("does-exist").as_path()).unwrap();
        assert!(match read_demos_from_directory(dir_path).unwrap_err() {
            ManagerError::DemoError { demo_error } => {
                match demo_error {
                    DemoError::UnableToReadFile { .. } => true,
                    _ => false,
                }
            }
            _ => false,
        });
    }

    #[test]
    fn demos_dir_deserialization_failed() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let mut file = File::create(dir_path.join("file.txt").as_path()).unwrap();
        file.write("this_is_not_valid".as_bytes()).unwrap();
        assert!(match read_demos_from_directory(dir_path).unwrap_err() {
            ManagerError::DemoError { demo_error } => {
                match demo_error {
                    DemoError::DeserializationFailed { .. } => true,
                    _ => false,
                }
            }
            _ => false,
        });
    }

    #[test]
    fn demos_dir_one() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let demo = Demo {
            title: "Example 1".to_string(),
            pipeline: PipelineDescr {
                name: "example1".to_string(),
                description: "Description of example1".to_string(),
                runtime_config: Default::default(),
                program_code: "CREATE TABLE example1(col1 INT);".to_string(),
                program_config: Default::default(),
            },
        };
        let mut file = File::create(dir_path.join("file.txt").as_path()).unwrap();
        file.write(serde_json::to_string(&demo).unwrap().as_bytes())
            .unwrap();
        let read_demos = read_demos_from_directory(dir_path).unwrap();
        assert_eq!(read_demos.len(), 1);
        assert_eq!(demo, read_demos[0]);
    }

    #[test]
    fn demos_dir_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let demos = vec![
            Demo {
                title: "Example 1".to_string(),
                pipeline: PipelineDescr {
                    name: "example1".to_string(),
                    description: "Description of example1".to_string(),
                    runtime_config: Default::default(),
                    program_code: "CREATE TABLE example1(col1 INT);".to_string(),
                    program_config: Default::default(),
                },
            },
            Demo {
                title: "Example 3".to_string(),
                pipeline: PipelineDescr {
                    name: "example3".to_string(),
                    description: "Description of example3".to_string(),
                    runtime_config: Default::default(),
                    program_code: "CREATE TABLE example3(col3 INT);".to_string(),
                    program_config: Default::default(),
                },
            },
            Demo {
                title: "Example 2".to_string(),
                pipeline: PipelineDescr {
                    name: "example2".to_string(),
                    description: "Description of example2".to_string(),
                    runtime_config: Default::default(),
                    program_code: "CREATE TABLE example2(col2 INT);".to_string(),
                    program_config: Default::default(),
                },
            },
        ];
        for demo in &demos {
            let mut file = File::create(
                dir_path
                    .join(format!("{}.json", demo.pipeline.name))
                    .as_path(),
            )
            .unwrap();
            file.write(serde_json::to_string(&demo).unwrap().as_bytes())
                .unwrap();
        }
        let read_demos = read_demos_from_directory(dir_path).unwrap();
        assert_eq!(read_demos.len(), 3);
        assert!(
            read_demos[0] == demos[0] && read_demos[1] == demos[2] && read_demos[2] == demos[1]
        );
    }
}
