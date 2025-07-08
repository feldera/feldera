use log::{debug, error, warn};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use thiserror::Error as ThisError;
use utoipa::ToSchema;

#[derive(ThisError, Serialize, Debug, Eq, PartialEq)]
pub enum DemoError {
    /// Error when unable to read the demos directory.
    #[error("could not read demos directory {path} due to: {error}")]
    UnableToReadDirectory { path: String, error: String },
    /// Error when unable to read a directory entry in the demos directory.
    #[error("could not read demos directory entry due to: {error}")]
    UnableToReadDirEntry { error: String },
    /// Error when the demo file could not be read to a string.
    #[error("could not read demo file {path} to string due to: {error}")]
    UnableToReadFile { path: String, error: String },
    /// Error when the SQL preamble could not be matched.
    #[error("demo file {path} has an invalid SQL preamble: {reason}")]
    InvalidSqlWithPreamble { path: String, reason: String },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Demo {
    /// Name of the demo (parsed from SQL preamble).
    name: String,
    /// Title of the demo (parsed from SQL preamble).
    title: String,
    /// Description of the demo (parsed from SQL preamble).
    description: String,
    /// Program SQL code.
    program_code: String,
    /// User defined function (UDF) Rust code.
    udf_rust: String,
    /// User defined function (UDF) TOML dependencies.
    udf_toml: String,
}

/// Parses the SQL preamble to retrieve the demo metadata.
pub fn parse_demo(
    path: String,
    program_code: String,
    udf_rust: String,
    udf_toml: String,
) -> Result<Demo, DemoError> {
    // Match preamble
    let re_preamble = Regex::new(
        r"^-- (.+) \(([a-zA-Z0-9_-]+)\)[ \t]*\r?\n--[ \t]*\r?\n((-- .+\r?\n)+)--[ \t]*\r?\n",
    )
    .expect("Invalid preamble regex");
    let Some(captures) = re_preamble.captures(&program_code) else {
        return Err(DemoError::InvalidSqlWithPreamble {
            path,
            reason: "does not match preamble regex pattern".to_string(),
        });
    };
    let title = captures
        .get(1)
        .expect("Regex title is missing")
        .as_str()
        .trim()
        .to_string();
    let name = captures
        .get(2)
        .expect("Regex name is missing")
        .as_str()
        .to_string();
    let description_lines = captures
        .get(3)
        .expect("Regex description is missing")
        .as_str();

    // Post-process description lines
    let re_description_line = Regex::new(r"-- .+\r?\n").expect("Invalid description line regex");
    let matches: Vec<_> = re_description_line
        .find_iter(description_lines)
        .map(|m| {
            m.as_str()
                .strip_prefix("-- ")
                .expect("Regex description line prefix does not exist")
                .trim()
                .to_string()
        })
        .collect();
    let description = matches.join(" ").trim().to_string();

    // Character limits
    if title.is_empty() {
        return Err(DemoError::InvalidSqlWithPreamble {
            path,
            reason: "title is empty".to_string(),
        });
    }
    if title.len() > 100 {
        return Err(DemoError::InvalidSqlWithPreamble {
            path,
            reason: format!("title '{title}' exceeds 100 characters"),
        });
    }
    // Name is already checked to be non-empty due to regex
    if name.len() > 100 {
        return Err(DemoError::InvalidSqlWithPreamble {
            path,
            reason: format!("name '{name}' exceeds 100 characters"),
        });
    }
    if description.is_empty() {
        return Err(DemoError::InvalidSqlWithPreamble {
            path,
            reason: "description is empty".to_string(),
        });
    }
    if description.len() > 1000 {
        return Err(DemoError::InvalidSqlWithPreamble {
            path,
            reason: format!("description '{description}' exceeds 1000 characters"),
        });
    }

    Ok(Demo {
        name,
        title,
        description,
        program_code,
        udf_rust,
        udf_toml,
    })
}

/// Reads the content of a file fully as string.
/// Prints error message and returns `None` if the content could not be read.
fn read_from_file_or_print_error(path: &Path) -> Option<String> {
    match fs::read_to_string(path) {
        Ok(content) => Some(content),
        Err(e) => {
            error!(
                "{}",
                DemoError::UnableToReadFile {
                    path: path.to_string_lossy().to_string(),
                    error: e.to_string(),
                }
            );
            None
        }
    }
}

/// Reads the demos from the demos directories.
///
/// For each directory, the files are read sorted on the filename.
/// For multiple directories, the lists of demos are appended one after the other into a single one.
/// Files which do not end in `.sql` and directories are ignored. Symlinks are followed.
/// If a `<filename>.sql` exists, checks for `<filename>.udf.rs` and `<filename>.udf.toml`.
/// If present, these will be included in the demo as well.
pub fn read_demos_from_directories(demos_dir: &Vec<String>) -> Vec<Demo> {
    let mut result: Vec<Demo> = vec![];

    // Go over the directories one-by-one in the order they were passed
    for dir in demos_dir {
        // Directory entries
        let entries = match fs::read_dir(Path::new(dir)) {
            Ok(entries) => entries.collect::<Vec<_>>(),
            Err(e) => {
                warn!(
                    "{}",
                    DemoError::UnableToReadDirectory {
                        path: dir.clone(),
                        error: e.to_string(),
                    }
                );
                continue;
            }
        };

        // Sorted paths
        let mut paths = vec![];
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    error!(
                        "{}",
                        DemoError::UnableToReadDirEntry {
                            error: e.to_string(),
                        }
                    );
                    continue;
                }
            };
            paths.push(entry.path());
        }
        paths.sort();

        // Convert each file read from a path to a demo
        for path in paths {
            if path.is_file() && path.extension().is_some_and(|ext| ext == "sql") {
                // SQL
                let Some(program_code) = read_from_file_or_print_error(&path) else {
                    continue;
                };

                // User defined function (UDF) Rust
                let path_udf_rs = path.with_extension("udf.rs");
                let udf_rs = if path_udf_rs.is_file() {
                    let Some(udf_rs) = read_from_file_or_print_error(&path_udf_rs) else {
                        // If a UDF Rust file was detected, it must have been successfully read.
                        continue;
                    };
                    udf_rs
                } else {
                    "".to_string()
                };

                // User defined function (UDF) TOML dependencies
                let path_udf_toml = path.with_extension("udf.toml");
                let udf_toml = if path_udf_toml.is_file() {
                    let Some(udf_toml) = read_from_file_or_print_error(&path_udf_toml) else {
                        // If a UDF TOML file was detected, it must have been successfully read.
                        continue;
                    };
                    udf_toml
                } else {
                    "".to_string()
                };

                // Create demo
                let demo = match parse_demo(
                    path.to_string_lossy().to_string(),
                    program_code,
                    udf_rs,
                    udf_toml,
                ) {
                    Ok(demo) => demo,
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                };
                result.push(demo);
            } else {
                debug!("Not a file or does not end with '.sql', thus ignored as demo: {path:?}");
            }
        }
    }

    result
}

#[cfg(test)]
mod test {
    use super::{parse_demo, read_demos_from_directories, Demo, DemoError};
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    // SQL examples
    const EXAMPLE_SQL_1: &str = "-- Example 1 (example-1)\n--\n-- Line A\n--\n";
    const EXAMPLE_SQL_2: &str = "-- Example 2 (example-2)\n--\n-- Line A\n--\n-- Line C\n\nLine D";
    const EXAMPLE_SQL_3: &str =
        "-- Example 3 (example-3)\n--\n-- Line A\t\n-- Line B\n--\n-- Line C\n\nLine D";
    const EXAMPLE_SQL_4: &str =
        "-- Example 4 (example-4)\n--\n-- description4\tdescription4 \t\n--\n\n\nThe SQL";

    /// Expected demo structures
    fn expected_demos() -> Vec<Demo> {
        vec![
            Demo {
                name: "example-1".to_string(),
                title: "Example 1".to_string(),
                description: "Line A".to_string(),
                program_code: EXAMPLE_SQL_1.to_string(),
                udf_rust: "".to_string(),
                udf_toml: "".to_string(),
            },
            Demo {
                name: "example-2".to_string(),
                title: "Example 2".to_string(),
                description: "Line A".to_string(),
                program_code: EXAMPLE_SQL_2.to_string(),
                udf_rust: "".to_string(),
                udf_toml: "".to_string(),
            },
            Demo {
                name: "example-3".to_string(),
                title: "Example 3".to_string(),
                description: "Line A Line B".to_string(),
                program_code: EXAMPLE_SQL_3.to_string(),
                udf_rust: "".to_string(),
                udf_toml: "".to_string(),
            },
            Demo {
                name: "example-4".to_string(),
                title: "Example 4".to_string(),
                description: "description4\tdescription4".to_string(),
                program_code: EXAMPLE_SQL_4.to_string(),
                udf_rust: "".to_string(),
                udf_toml: "".to_string(),
            },
        ]
    }

    #[test]
    fn test_invalid_preamble_regex() {
        for invalid_sql in [
            "",
            "-- ",
            "-- title\n--\n-- description\n--\n,", // Missing name
            "-- (name)\n--\n-- description\n--\n,", // Missing title
            "-- title (name)\n--\n-- \n--\n,",     // Missing description
        ] {
            let file_path = "does-not-exist".to_string();
            let err = parse_demo(
                file_path.clone(),
                invalid_sql.to_string(),
                "".to_string(),
                "".to_string(),
            )
            .unwrap_err();
            assert!(matches!(
                err,
                DemoError::InvalidSqlWithPreamble {
                    path,
                    reason,
                } if path == file_path && reason == *"does not match preamble regex pattern"
            ));
        }
    }

    #[test]
    fn test_invalid_lengths() {
        for (invalid_sql, expected_reason) in [
            (
                "--   (name)\n--\n-- description\n--\nSQL".to_string(),
                "title is empty".to_string(),
            ),
            (
                format!("-- {} (name)\n--\n-- description\n--\nSQL", "a".repeat(101)),
                format!("title '{}' exceeds 100 characters", "a".repeat(101)),
            ),
            // Name is already checked to be non-empty due to regex
            (
                format!(
                    "-- title ({})\n--\n-- description\n--\nSQL",
                    "a".repeat(101)
                ),
                format!("name '{}' exceeds 100 characters", "a".repeat(101)),
            ),
            (
                "-- title (name)\n--\n--  \n--\nSQL".to_string(),
                "description is empty".to_string(),
            ),
            (
                format!("-- title (name)\n--\n-- {}\n--\nSQL", "a".repeat(1001)),
                format!("description '{}' exceeds 1000 characters", "a".repeat(1001)),
            ),
        ] {
            let file_path = "does-not-exist".to_string();
            let err = parse_demo(
                file_path.clone(),
                invalid_sql,
                "".to_string(),
                "".to_string(),
            )
            .unwrap_err();
            assert!(matches!(
                err,
                DemoError::InvalidSqlWithPreamble {
                    path,
                    reason,
                } if path == file_path && reason == expected_reason
            ));
        }
    }

    #[test]
    fn no_dir() {
        assert!(read_demos_from_directories(&vec![]).is_empty());
    }

    #[test]
    fn dir_empty() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();
        assert!(read_demos_from_directories(&vec![dir_path.to_string()]).is_empty());
    }

    #[test]
    fn dir_does_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir
            .path()
            .join("does-not-exist")
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(read_demos_from_directories(&vec![dir_path]), vec![]);
    }

    #[test]
    fn ignore_non_sql_and_invalid_files() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Is a directory
        fs::create_dir(dir_path.join("does-exist").as_path()).unwrap();

        // Valid file
        let mut file = File::create(dir_path.join("file.sql").as_path()).unwrap();
        let sql = "-- title (name)\n--\n-- description\n--\nSQL";
        file.write_all(sql.as_bytes()).unwrap();

        // Does not end in .sql
        let mut file = File::create(dir_path.join("file.txt").as_path()).unwrap();
        file.write_all("txt".as_bytes()).unwrap();

        // Invalid file
        let mut file = File::create(dir_path.join("file2.sql").as_path()).unwrap();
        file.write_all("abc".as_bytes()).unwrap();

        // Only the valid one should have been read as demo
        assert_eq!(
            read_demos_from_directories(&vec![dir_path.to_str().unwrap().to_string()]),
            vec![Demo {
                name: "name".to_string(),
                title: "title".to_string(),
                description: "description".to_string(),
                program_code: sql.to_string(),
                udf_rust: "".to_string(),
                udf_toml: "".to_string(),
            }]
        );
    }

    #[test]
    fn one_demo() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let mut file = File::create(dir_path.join("file.sql").as_path()).unwrap();
        file.write_all(EXAMPLE_SQL_1.as_bytes()).unwrap();
        assert_eq!(
            read_demos_from_directories(&vec![dir_path.to_str().unwrap().to_string()]),
            vec![expected_demos()[0].clone()]
        );
    }

    /// Create several demo files whose names (excluding '.sql') equal their content.
    fn make_demo_files_in_dir(dir_path: &Path, names: &Vec<&str>) {
        for name in names {
            let mut file = File::create(dir_path.join(format!("{}.sql", name)).as_path()).unwrap();
            file.write_all(name.as_bytes()).unwrap();
        }
    }

    #[test]
    fn multiple_demos_in_one_dir() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let demos = vec![EXAMPLE_SQL_1, EXAMPLE_SQL_3, EXAMPLE_SQL_2];
        make_demo_files_in_dir(dir_path, &demos);
        let read_demos = read_demos_from_directories(&vec![dir_path.to_str().unwrap().to_string()]);
        assert_eq!(
            read_demos,
            vec![
                expected_demos()[0].clone(),
                expected_demos()[1].clone(),
                expected_demos()[2].clone(),
            ]
        );
    }

    #[test]
    fn multiple_demos_in_multiple_dirs() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir_path1 = dir1.path();
        let demos = vec![EXAMPLE_SQL_2, EXAMPLE_SQL_4];
        make_demo_files_in_dir(dir_path1, &demos);

        let dir2 = tempfile::tempdir().unwrap();
        let dir_path2 = dir2.path();
        let demos = vec![EXAMPLE_SQL_3, EXAMPLE_SQL_1];
        make_demo_files_in_dir(dir_path2, &demos);

        let read_demos = read_demos_from_directories(&vec![
            dir_path1.to_str().unwrap().to_string(),
            dir_path2.to_str().unwrap().to_string(),
        ]);
        assert_eq!(
            read_demos,
            vec![
                expected_demos()[1].clone(),
                expected_demos()[3].clone(),
                expected_demos()[0].clone(),
                expected_demos()[2].clone(),
            ]
        );
    }

    #[test]
    fn one_demo_with_udf_rust_toml() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let mut file = File::create(dir_path.join("file.sql").as_path()).unwrap();
        file.write_all(EXAMPLE_SQL_1.as_bytes()).unwrap();
        let mut file = File::create(dir_path.join("file.udf.rs").as_path()).unwrap();
        file.write_all("123".as_bytes()).unwrap();
        let mut file = File::create(dir_path.join("file.udf.toml").as_path()).unwrap();
        file.write_all("456".as_bytes()).unwrap();
        let mut expectation = expected_demos()[0].clone();
        expectation.udf_rust = "123".to_string();
        expectation.udf_toml = "456".to_string();
        assert_eq!(
            read_demos_from_directories(&vec![dir_path.to_str().unwrap().to_string()]),
            vec![expectation]
        );
    }

    #[test]
    fn one_demo_with_udf_only_rust() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let mut file = File::create(dir_path.join("file.sql").as_path()).unwrap();
        file.write_all(EXAMPLE_SQL_1.as_bytes()).unwrap();
        let mut file = File::create(dir_path.join("file.udf.rs").as_path()).unwrap();
        file.write_all("rust_code".as_bytes()).unwrap();
        let mut expectation = expected_demos()[0].clone();
        expectation.udf_rust = "rust_code".to_string();
        expectation.udf_toml = "".to_string();
        assert_eq!(
            read_demos_from_directories(&vec![dir_path.to_str().unwrap().to_string()]),
            vec![expectation]
        );
    }

    #[test]
    fn one_demo_with_udf_only_toml() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let mut file = File::create(dir_path.join("file.sql").as_path()).unwrap();
        file.write_all(EXAMPLE_SQL_1.as_bytes()).unwrap();
        let mut file = File::create(dir_path.join("file.udf.toml").as_path()).unwrap();
        file.write_all("example = \"1.0.0\"".as_bytes()).unwrap();
        let mut expectation = expected_demos()[0].clone();
        expectation.udf_rust = "".to_string();
        expectation.udf_toml = "example = \"1.0.0\"".to_string();
        assert_eq!(
            read_demos_from_directories(&vec![dir_path.to_str().unwrap().to_string()]),
            vec![expectation]
        );
    }
}
