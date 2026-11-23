//! File handling for the CLI; graph comparison remains independent of the filesystem.

use feldera_sql_difftest::{compare_dumps, parse_json};
use serde_json::Value;
use std::{collections::BTreeSet, ffi::OsString, fmt, fs, path::Path};

#[derive(Debug)]
pub enum Error {
    Input(String),
    Difference(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(message) | Self::Difference(message) => f.write_str(message),
        }
    }
}

pub fn read(path: &Path) -> Result<Value, Error> {
    read_with(path, parse_json)
}

/// The graph comparator checks structure and identity independently of the production IR reader.
pub fn read_circuit(path: &Path) -> Result<Value, Error> {
    read_with(path, feldera_sql_ir::parse_circuit_json)
}

pub(super) fn read_with(
    path: &Path,
    decode: fn(&[u8]) -> Result<Value, serde_json::Error>,
) -> Result<Value, Error> {
    let bytes = fs::read(path)
        .map_err(|error| Error::Input(format!("cannot read {}: {error}", path.display())))?;
    decode(&bytes)
        .map_err(|error| Error::Input(format!("invalid JSON in {}: {error}", path.display())))
}

struct Capture {
    dump_count: u64,
    final_stage: String,
}

impl Capture {
    fn read(directory: &Path) -> Result<Self, Error> {
        let manifest = read(&directory.join("complete.json"))?;
        let invalid = || {
            Error::Difference(format!(
                "invalid completion manifest in {}",
                directory.display()
            ))
        };
        if manifest.as_object().map(|object| object.len()) != Some(3)
            || manifest["schema_version"].as_u64() != Some(1)
        {
            return Err(invalid());
        }
        let dump_count = manifest["dump_count"].as_u64().ok_or_else(invalid)?;
        let final_stage = manifest["final_stage"].as_str().ok_or_else(invalid)?;
        if !matches!((final_stage, dump_count), ("S2", 1) | ("S3", 1..)) {
            return Err(invalid());
        }
        let capture = Self {
            dump_count,
            final_stage: final_stage.into(),
        };
        let mut names = BTreeSet::new();
        let entries = fs::read_dir(directory).map_err(|error| {
            Error::Input(format!("cannot list {}: {error}", directory.display()))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| Error::Input(error.to_string()))?;
            if !entry
                .file_type()
                .map_err(|error| Error::Input(error.to_string()))?
                .is_file()
            {
                return Err(Error::Input(format!(
                    "expected a regular file: {}",
                    entry.path().display()
                )));
            }
            names.insert(entry.file_name());
        }
        // Check the count before iterating over manifest-controlled indices.
        if names.len() as u64 != dump_count.saturating_add(1) {
            return Err(Error::Difference(format!(
                "missing or extra capture files in {}",
                directory.display()
            )));
        }
        names.remove(&OsString::from("complete.json"));
        for sequence in 0..dump_count {
            if !names.remove(&OsString::from(capture.file_name(sequence))) {
                return Err(Error::Difference(format!(
                    "missing observation {} in {}",
                    sequence,
                    directory.display()
                )));
            }
        }
        Ok(capture)
    }

    fn stage(&self, sequence: u64) -> &str {
        if self.dump_count == 1 {
            &self.final_stage
        } else if sequence == 0 {
            "S2"
        } else if sequence == self.dump_count - 1 {
            &self.final_stage
        } else {
            "S4"
        }
    }

    fn file_name(&self, sequence: u64) -> String {
        format!("{sequence:06}-{}.json", self.stage(sequence))
    }

    fn observation(&self, directory: &Path, sequence: u64) -> Result<Value, Error> {
        let file = directory.join(self.file_name(sequence));
        let dump = read_circuit(&file)?;
        if dump["observation"]["sequence"].as_u64() != Some(sequence)
            || dump["observation"]["stage"].as_str() != Some(self.stage(sequence))
        {
            return Err(Error::Difference(format!(
                "observation identity disagrees with filename: {}",
                file.display()
            )));
        }
        Ok(dump)
    }
}

pub fn compare_directories(expected: &Path, actual: &Path) -> Result<u64, Error> {
    let left = Capture::read(expected)?;
    let right = Capture::read(actual)?;
    if left.dump_count != right.dump_count || left.final_stage != right.final_stage {
        return Err(Error::Difference(
            "capture lengths or final stages differ".into(),
        ));
    }
    for sequence in 0..left.dump_count {
        let expected_dump = left.observation(expected, sequence)?;
        let actual_dump = right.observation(actual, sequence)?;
        compare_dumps(&expected_dump, &actual_dump)
            .map_err(|error| Error::Difference(format!("{}: {error}", left.file_name(sequence))))?;
    }
    Ok(left.dump_count)
}
