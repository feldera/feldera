use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    sync::Arc,
};

use dbsp::storage::{
    backend::{StorageBackend, StorageError, StoragePath},
    buffer_cache::FBuf,
};
use feldera_adapterlib::{errors::journal::StepError, transport::Step};
use feldera_types::config::InputEndpointConfig;
use rmpv::Value as RmpValue;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub struct Journal {
    /// Underlying storage.
    backend: Arc<dyn StorageBackend>,

    /// Directory name.
    path: StoragePath,
}

impl Journal {
    /// Opens an existing journal under `path`.
    pub fn open(backend: Arc<dyn StorageBackend>, path: &StoragePath) -> Self {
        Self {
            backend,
            path: path.clone(),
        }
    }

    /// Creates a new journal under `path`, deleting any journal previously there.
    pub fn create(backend: Arc<dyn StorageBackend>, path: &StoragePath) -> Result<Self, StepError> {
        let this = Self::open(backend, path);
        this.truncate()?;
        Ok(this)
    }

    pub fn read(&self, step: Step) -> Result<Option<StepMetadata>, StepError> {
        let path = self.path.child(format!("{step}.bin"));
        let data = match self.backend.read(&path) {
            Ok(data) => data,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(StepError::storage_error(&path, error)),
        };
        let record = rmp_serde::decode::from_slice::<StepMetadata>(&data).map_err(|error| {
            StepError::DecodeError {
                path: path.as_ref().into(),
                error,
            }
        })?;
        if record.step != step {
            return Err(StepError::WrongStep {
                path: path.as_ref().into(),
                expected: step,
                found: record.step,
            });
        }
        Ok(Some(record))
    }

    pub fn write(&self, record: &StepMetadata) -> Result<(), StepError> {
        let path = self.path.child(format!("{}.bin", record.step));
        let mut data = FBuf::new();
        rmp_serde::encode::write(&mut data, record).map_err(|error| StepError::EncodeError {
            path: self.path.as_ref().into(),
            error,
        })?;
        self.backend
            .write(&path, data)
            .map_err(|error| StepError::storage_error(&path, error))?;
        Ok(())
    }

    pub fn truncate(&self) -> Result<(), StepError> {
        self.backend
            .delete_recursive(&self.path)
            .map_err(|error| self.storage_error(error))
    }

    fn storage_error(&self, error: StorageError) -> StepError {
        StepError::storage_error(&self.path, error)
    }
}

/// A record in the journal, useful for replaying a step.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StepMetadata {
    /// Step number.
    pub step: Step,

    /// Names of input endpoints removed in the step.
    pub remove_inputs: HashSet<String>,

    /// Input endpoints added in the step, with their configurations.
    ///
    /// If a given name is in both `remove_inputs` and `add_inputs`, then the
    /// step replaced an endpoint with the given name by a new, otherwise
    /// unrelated endpoint.
    pub add_inputs: HashMap<String, InputEndpointConfig>,

    /// Logs for the endpoints included in the step.
    ///
    /// A given endpoint is included if it existed before the step and is not in
    /// `remove_inputs`, or if it is included in `add_inputs`.
    pub input_logs: HashMap<String, InputLog>,
}

/// A journal record for a single endpoint for a single step.
///
/// The endpoint's name is the key in [StepMetadata::input_logs].
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct InputLog {
    /// Data for replay.
    ///
    /// This is filled in by input adapters that log actual data records
    /// (e.g. the HTTP and ad hoc query input adapters). For the other adapters,
    /// which only log metadata (such as record offsets), this field is
    /// [RmpValue::Nil].
    pub data: RmpValue,

    /// Metadata for seek and replay.
    ///
    /// This is filled in by input adapters that log metadata (such as record
    /// offsets).
    #[serde(with = "as_json_string")]
    pub metadata: JsonValue,

    /// Checksums of the input data.
    pub checksums: InputChecksums,
}

mod as_json_string {
    use serde::de::{Deserialize, Deserializer};
    use serde::ser::{Serialize, Serializer};
    use serde_json::Value as JsonValue;

    pub(super) fn serialize<S>(value: &JsonValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
        s.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<JsonValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        serde_json::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Input data statistics.
///
/// This allows checking that an input step replayed the same data as the
/// original run.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct InputChecksums {
    /// Number of records.
    pub num_records: u64,

    /// Hash of the records.
    pub hash: u64,
}

/// Checksums for the input endpoints in a step.
///
/// This is a subset of [StepMetadata] that is useful for verifying that an
/// input step replayed the same data as the original run.
#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct StepInputChecksums(
    /// Maps from an input endpoint name to its checksums.
    pub HashMap<String, InputChecksums>,
);

impl From<&HashMap<String, InputLog>> for StepInputChecksums {
    fn from(input_logs: &HashMap<String, InputLog>) -> Self {
        Self(
            input_logs
                .iter()
                .map(|(name, log)| (name.clone(), log.checksums.clone()))
                .collect(),
        )
    }
}

impl From<&StepMetadata> for StepInputChecksums {
    fn from(value: &StepMetadata) -> Self {
        Self::from(&value.input_logs)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use dbsp::{
        circuit::StorageCacheConfig,
        storage::backend::{posixio_impl::PosixBackend, StoragePath},
    };

    use tempfile::TempDir;

    use crate::{controller::journal::Journal, test::init_test_logger};

    use super::StepMetadata;

    /// Create and write a steps file and then read it back.
    #[test]
    fn test_create() {
        init_test_logger();

        let tempdir = TempDir::new().unwrap();
        let backend = Arc::new(PosixBackend::new(tempdir, StorageCacheConfig::default()));
        let path = StoragePath::from("journal");

        let records = (0..10)
            .map(|step| StepMetadata {
                step,
                remove_inputs: HashSet::new(),
                add_inputs: HashMap::new(),
                input_logs: HashMap::new(),
            })
            .collect::<Vec<_>>();

        let journal = Journal::create(backend, &path).unwrap();
        for record in records.iter() {
            journal.write(record).unwrap();
        }

        for expected in records.iter() {
            let actual = journal.read(expected.step).unwrap().unwrap();
            assert_eq!(expected, &actual);
        }

        let last_record = records.last().unwrap();
        assert_eq!(journal.read(last_record.step + 1).unwrap(), None);
    }
}
