//! The storage backend exported by statically compiled circuits. This backend is used
//! by the controller to maintain checkpoint metadata.

use std::any::Any;
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::ops::{Bound, Range, RangeBounds};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};

use anyhow::Result as AnyResult;
use dbsp::circuit::CircuitStorageConfig;
use dbsp::circuit::checkpointer::Checkpointer;
use dbsp_adapters::engine::{
    Checkpoint, CheckpointSync, JournalMode, PipelineStorage, StepJournal, StepMetadata,
    StoredStatus,
};
use feldera_adapterlib::errors::journal::StepError;
use feldera_adapterlib::transport::Step;
use feldera_storage::checkpoint_synchronizer::CheckpointSynchronizer;
use feldera_storage::error::StorageError;
use feldera_storage::fbuf::FBuf;
use feldera_storage::{StorageBackend, StoragePath};
use feldera_types::checkpoint::{
    CheckpointMetadata, CheckpointSyncMetrics, HostInfo, RemoteCheckpoint,
};
use feldera_types::config::{PipelineConfig, PipelineIdentity, SyncConfig};
use feldera_types::constants::{
    ACTIVATION_MARKER_FILE, CHECKPOINT_FILE_NAME, STATE_FILE, STATUS_FILE, STEPS_FILE,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::ControllerError;

/// The storage of a pipeline that the static engine runs.
pub struct StaticStorage {
    /// dbsp's storage.
    circuit: CircuitStorageConfig,

    /// Copies checkpoints between the storage and object storage.
    sync: StaticCheckpointSync,
}

impl StaticStorage {
    /// Opens the storage that `config` names, or returns `None` if it names none.
    pub fn open(config: &PipelineConfig) -> Result<Option<Arc<Self>>, ControllerError> {
        config
            .storage()
            .map(|(storage_config, storage_options)| {
                CircuitStorageConfig::for_config(storage_config.clone(), storage_options.clone())
                    .map(|circuit| Arc::new(Self::new(circuit)))
                    .map_err(|error| {
                        ControllerError::storage_error("failed to initialize storage", error)
                    })
            })
            .transpose()
    }

    /// Returns the storage over dbsp's storage `circuit`.
    fn new(circuit: CircuitStorageConfig) -> Self {
        let sync = StaticCheckpointSync {
            backend: circuit.backend.clone(),
        };
        Self { circuit, sync }
    }

    /// dbsp's storage backend.
    pub(crate) fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.circuit.backend
    }

    /// dbsp's storage configuration for a circuit that restores `init_checkpoint`, if any,
    /// unless `defer_restore` leaves the restore to a concurrent bootstrap.
    pub(crate) fn circuit_config(
        &self,
        init_checkpoint: Option<Uuid>,
        defer_restore: bool,
    ) -> CircuitStorageConfig {
        self.circuit
            .clone()
            .with_init_checkpoint(init_checkpoint)
            .with_defer_restore(defer_restore)
    }
}

impl PipelineStorage for StaticStorage {
    fn read_status(&self) -> Option<StoredStatus> {
        match self
            .backend()
            .read_json::<StoredStatus>(&StoragePath::from(STATUS_FILE))
        {
            Ok(status) => Some(status),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => {
                warn!("{STATUS_FILE}: failed to read from storage ({e})");
                None
            }
        }
    }

    fn write_status(&self, status: &StoredStatus) {
        if let Err(e) = self
            .backend()
            .write_json(&StoragePath::from(STATUS_FILE), status)
            .and_then(|reader| reader.commit())
        {
            warn!("{STATUS_FILE}: failed to write to storage ({e})");
        }
    }

    fn checkpoints(&self) -> Result<VecDeque<CheckpointMetadata>, ControllerError> {
        Checkpointer::read_checkpoints(&**self.backend()).map_err(ControllerError::dbsp_error)
    }

    fn gc_startup(&self) -> Result<(), ControllerError> {
        Checkpointer::new(self.backend().clone())
            .map_err(ControllerError::dbsp_error)?
            .gc_startup()
            .map(|_| ())
            .map_err(ControllerError::dbsp_error)
    }

    /// The root `state.json` can name an older checkpoint: a checkpoint is published to the
    /// catalog before the root file is rewritten, so a crash in between leaves the root one
    /// checkpoint behind, and resuming from it would fork the checkpoint history. The root
    /// file still decides when the catalog is empty, because a fault-tolerant pipeline that
    /// has not checkpointed yet records its starting point there.
    fn latest_checkpoint(&self) -> Result<Option<Checkpoint>, ControllerError> {
        let backend = &**self.backend();
        let root = match read_checkpoint(backend, &StoragePath::from(STATE_FILE)) {
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            other => Some(other?),
        };
        let catalog = self.checkpoints()?;
        let Some(newest) = catalog.back() else {
            return Ok(root);
        };
        let root_uuid = root
            .as_ref()
            .and_then(|root| root.circuit.as_ref())
            .map(|circuit| circuit.uuid);
        if root_uuid == Some(newest.uuid) {
            return Ok(root);
        }

        let newest_path = StoragePath::from(newest.uuid.to_string()).join(STATE_FILE);
        let checkpoint = match read_checkpoint(backend, &newest_path) {
            Ok(checkpoint) => checkpoint,
            // Checkpoints written before July 2025 have no state file of their own.
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(root),
            Err(error) => return Err(error),
        };
        warn!(
            "{STATE_FILE} names checkpoint {} but the newest checkpoint in {CHECKPOINT_FILE_NAME} is {}; resuming from {} and updating {STATE_FILE}",
            root_uuid.map_or_else(|| String::from("none"), |uuid| uuid.to_string()),
            newest.uuid,
            newest.uuid,
        );
        write_checkpoint(&checkpoint, backend, &StoragePath::from(STATE_FILE))?;
        Ok(Some(checkpoint))
    }

    fn checkpoint(&self, uuid: Uuid) -> Result<Checkpoint, ControllerError> {
        read_checkpoint(
            &**self.backend(),
            &StoragePath::from(uuid.to_string()).join(STATE_FILE),
        )
    }

    fn open_journal(&self, mode: JournalMode<'_>) -> Result<Box<dyn StepJournal>, ControllerError> {
        let backend = self.backend().clone();
        let path = StoragePath::from(STEPS_FILE);
        match mode {
            JournalMode::Open(step) => {
                info!("{STEPS_FILE}: opening to start replay from step {step}");
                Ok(Box::new(Journal::open(backend, &path)))
            }
            JournalMode::Truncate => {
                info!("{STEPS_FILE}: truncating journal");
                Ok(Box::new(Journal::create(backend, &path)?))
            }
            JournalMode::Create(initial) => {
                for file in [STATE_FILE, STEPS_FILE] {
                    backend
                        .delete_if_exists(&StoragePath::from(file))
                        .map_err(|error| {
                            ControllerError::storage_error(
                                "initializing fault tolerant pipeline",
                                error,
                            )
                        })?;
                }

                info!("{STEPS_FILE}: creating");
                let journal = Journal::create(backend.clone(), &path)?;

                info!("{STATE_FILE}: creating");
                write_checkpoint(initial, &*backend, &StoragePath::from(STATE_FILE))?;
                Ok(Box::new(journal))
            }
        }
    }

    fn usage_bytes(&self) -> Option<i64> {
        Some(self.backend().usage().load(Ordering::Relaxed))
    }

    fn local_path(&self) -> Option<&Path> {
        self.backend().file_system_path()
    }

    fn sync(&self) -> Option<&dyn CheckpointSync> {
        Some(&self.sync)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Reads the controller's state, in JSON, from `path`.
fn read_checkpoint(
    storage: &dyn StorageBackend,
    path: &StoragePath,
) -> Result<Checkpoint, ControllerError> {
    let data = storage.read(path).map_err(|error| {
        ControllerError::storage_error(format!("{path}: failed to read checkpoint"), error)
    })?;
    serde_json_path_to_error::from_slice::<Checkpoint>(&data).map_err(|e| {
        ControllerError::CheckpointParseError {
            error: e.to_string(),
        }
    })
}

/// Writes the controller's state, in JSON, to `path`, atomically replacing any file that was
/// previously there.
fn write_checkpoint(
    checkpoint: &Checkpoint,
    storage: &dyn StorageBackend,
    path: &StoragePath,
) -> Result<(), ControllerError> {
    storage
        .write_json(path, checkpoint)
        .and_then(|file| file.commit())
        .map_err(|error| {
            ControllerError::storage_error(format!("{path}: failed to write pipeline state"), error)
        })
}

/// Stores `state` with checkpoint `uuid`, publishes the checkpoint with `publish`, and records
/// `state` as the one to resume from.
///
/// The order matters. The checkpoint's own `state.json` must exist before the checkpoint is
/// published, because a published checkpoint must be complete: checkpoint sync reads that
/// file to describe the checkpoint it uploads, and a multihost host restoring a checkpoint the
/// coordinator names reads it to find the circuit to open and the input offsets to resume
/// from. The root `state.json` changes only after the publication, because `gc_startup`
/// keeps only files that the catalog references, so a root pointer to an uncatalogued
/// checkpoint would get its files deleted.
pub(crate) fn publish_with_state(
    storage: &dyn StorageBackend,
    state: &Checkpoint,
    uuid: Uuid,
    publish: impl FnOnce() -> Result<CheckpointMetadata, ControllerError>,
) -> Result<CheckpointMetadata, ControllerError> {
    let checkpoint_dir = StoragePath::from(uuid.to_string());
    write_checkpoint(state, storage, &checkpoint_dir.clone().join(STATE_FILE))?;
    // Writing the file renames it into the checkpoint's directory, and a rename is durable
    // only once that directory entry is. Nothing else fsyncs the directory this late: the
    // commit's barrier ran before this write, and publication fsyncs the storage root.
    // Without this barrier a crash could keep the catalog entry while losing the file it
    // vouches for, which is the state this ordering exists to prevent.
    storage.fsync_dir(&checkpoint_dir).map_err(|error| {
        ControllerError::storage_error(format!("{checkpoint_dir}: failed to fsync"), error)
    })?;
    let metadata = publish()?;
    write_checkpoint(state, storage, &StoragePath::from(STATE_FILE))?;
    Ok(metadata)
}

/// The journal of a fault-tolerant pipeline, one file per step in a directory.
struct Journal {
    /// Underlying storage.
    backend: Arc<dyn StorageBackend>,

    /// Directory name.
    path: StoragePath,
}

impl Journal {
    /// Opens an existing journal under `path`.
    fn open(backend: Arc<dyn StorageBackend>, path: &StoragePath) -> Self {
        Self {
            backend,
            path: path.clone(),
        }
    }

    /// Creates a new journal under `path`, deleting any journal previously there.
    fn create(backend: Arc<dyn StorageBackend>, path: &StoragePath) -> Result<Self, StepError> {
        let this = Self::open(backend, path);
        this.delete(..)?;
        Ok(this)
    }

    /// Deletes steps in the journal in the range specified by `steps`.
    fn delete<R>(&self, steps: R) -> Result<(), StepError>
    where
        R: RangeBounds<Step>,
    {
        if steps.start_bound() == Bound::Unbounded && steps.end_bound() == Bound::Unbounded {
            // We're deleting all steps, so delete everything.  It might be more
            // efficient on some backends.
            self.backend
                .delete_recursive(&self.path)
                .map_err(|error| self.storage_error(error))
        } else {
            let mut result = Ok(());
            self.backend
                .list(&self.path, &mut |entry| {
                    let Some(file_name) = entry.name.filename() else {
                        return;
                    };
                    let Some(number) = file_name.strip_suffix(".bin") else {
                        return;
                    };
                    let Ok(step) = number.parse::<Step>() else {
                        return;
                    };
                    if !steps.contains(&step) {
                        return;
                    }
                    if let Err(error) = self.backend.delete(&entry.name) {
                        result = Err(StepError::storage_error(&entry.name, error));
                    }
                })
                .map_err(|error| self.storage_error(error))?;
            result
        }
    }

    fn storage_error(&self, error: StorageError) -> StepError {
        StepError::storage_error(&self.path, error)
    }
}

impl StepJournal for Journal {
    fn read(&self, step: Step) -> Result<Option<StepMetadata>, StepError> {
        let path = self.path.clone().join(format!("{step}.bin"));
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

    fn write(&self, record: &StepMetadata) -> Result<(), StepError> {
        let path = self.path.clone().join(format!("{}.bin", record.step));
        let mut data = FBuf::new();
        rmp_serde::encode::write(&mut data, record).map_err(|error| StepError::EncodeError {
            path: self.path.as_ref().into(),
            error,
        })?;
        // Commit before returning: a step is replayable only if its journal
        // record survives a crash, and `write` alone leaves the record in the
        // page cache.
        self.backend
            .write(&path, data)
            .and_then(|record| record.commit())
            .map_err(|error| StepError::storage_error(&path, error))?;
        Ok(())
    }

    fn delete(&self, steps: Range<Step>) -> Result<(), StepError> {
        Journal::delete(self, steps)
    }

    /// Replay iterates these in order (decoupled from the physical step counter), so a
    /// transaction's commit taking a different number of steps on replay than during
    /// recording cannot shift logged input onto the wrong entry. Tolerates gaps in the step
    /// numbering (e.g. steps that were not journaled because they only continued a
    /// transaction commit).
    fn list_steps(&self, start: Step) -> Result<Vec<Step>, StepError> {
        let mut steps = Vec::new();
        self.backend
            .list(&self.path, &mut |entry| {
                let Some(file_name) = entry.name.filename() else {
                    return;
                };
                let Some(number) = file_name.strip_suffix(".bin") else {
                    return;
                };
                let Ok(step) = number.parse::<Step>() else {
                    return;
                };
                if step >= start {
                    steps.push(step);
                }
            })
            .map_err(|error| self.storage_error(error))?;
        steps.sort_unstable();
        Ok(steps)
    }
}

/// Lazily resolves the checkpoint synchronizer.
///
/// This panic is safe as all enterprise builds must include the checkpoint-sync
/// crate.
static SYNCHRONIZER: LazyLock<&'static dyn CheckpointSynchronizer> = LazyLock::new(|| {
    let Some(synchronizer) = inventory::iter::<&dyn CheckpointSynchronizer>
        .into_iter()
        .next()
    else {
        unreachable!("no checkpoint synchronizer found; are enterprise features enabled?");
    };

    *synchronizer
});

/// Copies checkpoints between dbsp's storage and object storage, through the checkpoint
/// synchronizer that an enterprise build registers.
struct StaticCheckpointSync {
    /// The storage that checkpoints sync from and to.
    backend: Arc<dyn StorageBackend>,
}

impl CheckpointSync for StaticCheckpointSync {
    fn push(
        &self,
        checkpoint: Uuid,
        sync: SyncConfig,
        host_info: Option<HostInfo>,
        pipeline: PipelineIdentity,
    ) -> AnyResult<Option<CheckpointSyncMetrics>> {
        SYNCHRONIZER.push(checkpoint, self.backend.clone(), sync, host_info, pipeline)
    }

    fn pull(
        &self,
        sync: SyncConfig,
        host_info: Option<HostInfo>,
        standby: bool,
        pipeline: PipelineIdentity,
    ) -> AnyResult<(CheckpointMetadata, Option<CheckpointSyncMetrics>)> {
        SYNCHRONIZER.pull(self.backend.clone(), sync, host_info, standby, pipeline)
    }

    fn take_ownership(&self, sync: SyncConfig, pipeline: PipelineIdentity) -> AnyResult<()> {
        SYNCHRONIZER.take_ownership(self.backend.clone(), sync, pipeline)
    }

    fn list_remote(&self, sync: SyncConfig) -> AnyResult<Vec<RemoteCheckpoint>> {
        SYNCHRONIZER.list_remote(sync)
    }

    fn read_activation(&self) -> Option<Option<CheckpointMetadata>> {
        let activation_file = StoragePath::from(ACTIVATION_MARKER_FILE);
        if !self.backend.exists(&activation_file).unwrap_or(false) {
            return None;
        }
        Some(
            self.backend
                .read_json::<Option<CheckpointMetadata>>(&activation_file)
                .ok()
                .flatten(),
        )
    }

    fn write_activation(&self, checkpoint: &Option<CheckpointMetadata>) -> AnyResult<()> {
        let activation_file = StoragePath::from(ACTIVATION_MARKER_FILE);
        tracing::debug!("creating activation marker file: {}", activation_file);
        self.backend
            .write_json(&activation_file, checkpoint)
            .and_then(|reader| reader.commit())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use dbsp::circuit::StorageCacheConfig;
    use dbsp::circuit::metrics::FILES_SYNCED;
    use dbsp::storage::backend::posixio_impl::PosixBackend;
    use dbsp_adapters::engine::{
        Checkpoint, CheckpointOffsets, JournalMode, PipelineStorage, StepJournal, StepMetadata,
    };
    use feldera_storage::StoragePath;
    use feldera_types::checkpoint::CheckpointMetadata;
    use feldera_types::config::{FileBackendConfig, PipelineConfig};
    use feldera_types::constants::{CHECKPOINT_FILE_NAME, STATE_FILE, STATUS_FILE, STEPS_FILE};
    use serde_json::json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{Journal, StaticStorage, publish_with_state};
    use crate::test::init_test_logger;

    /// The storage of a pipeline whose storage is the directory `dir`.
    fn storage_in(dir: &Path) -> Arc<StaticStorage> {
        let config: PipelineConfig = serde_json::from_value(json!({
            "name": "test",
            "workers": 1,
            "storage_config": { "path": dir },
            "storage": true,
            "inputs": {},
        }))
        .unwrap();
        StaticStorage::open(&config).unwrap().unwrap()
    }

    /// The controller's state at step `step`, at circuit checkpoint `circuit`, if any.
    fn state(step: u64, circuit: Option<Uuid>) -> Checkpoint {
        Checkpoint {
            circuit: circuit.map(|uuid| CheckpointMetadata {
                uuid,
                ..CheckpointMetadata::default()
            }),
            step,
            config: serde_json::from_value(json!({"name": "test", "inputs": {}})).unwrap(),
            processed_records: step * 10,
            transaction_number: 0,
            initial_start_time: chrono::DateTime::UNIX_EPOCH,
            input_metadata: CheckpointOffsets::default(),
            input_statistics: HashMap::new(),
            output_statistics: HashMap::new(),
            input_distribution: BTreeMap::new(),
        }
    }

    /// Writes the checkpoint catalog with `uuids`, oldest first, as dbsp writes it.
    fn write_catalog(dir: &Path, uuids: &[Uuid]) {
        let catalog: VecDeque<CheckpointMetadata> = uuids
            .iter()
            .map(|&uuid| CheckpointMetadata {
                uuid,
                ..CheckpointMetadata::default()
            })
            .collect();
        std::fs::write(
            dir.join(CHECKPOINT_FILE_NAME),
            serde_json::to_vec(&catalog).unwrap(),
        )
        .unwrap();
    }

    /// Writes `state` as the state of checkpoint `uuid`, or as the root state for `None`.
    fn write_state(dir: &Path, uuid: Option<Uuid>, state: &Checkpoint) {
        let path = match uuid {
            Some(uuid) => {
                std::fs::create_dir_all(dir.join(uuid.to_string())).unwrap();
                dir.join(uuid.to_string()).join(STATE_FILE)
            }
            None => dir.join(STATE_FILE),
        };
        std::fs::write(path, serde_json::to_vec(state).unwrap()).unwrap();
    }

    /// The step of the root state, read back from the file.
    fn root_step(dir: &Path) -> u64 {
        let state: Checkpoint =
            serde_json::from_slice(&std::fs::read(dir.join(STATE_FILE)).unwrap()).unwrap();
        state.step
    }

    #[test]
    fn a_stored_status_survives_a_reopen_and_starts_absent() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(dir.path());
        assert!(storage.read_status().is_none());

        let status: dbsp_adapters::engine::StoredStatus = serde_json::from_value(json!({
            "desired_status": "Paused",
            "bootstrap_policy": "allow",
            "deployment_id": Uuid::nil(),
        }))
        .unwrap();
        storage.write_status(&status);
        assert_eq!(storage_in(dir.path()).read_status(), Some(status));
    }

    /// The storage counts the bytes that it writes, which the pipeline reports as its use.
    #[test]
    fn the_storage_counts_the_bytes_that_it_writes() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(dir.path());
        let before = storage.usage_bytes().unwrap();

        let status: dbsp_adapters::engine::StoredStatus = serde_json::from_value(json!({
            "desired_status": "Paused",
            "bootstrap_policy": "allow",
            "deployment_id": Uuid::nil(),
        }))
        .unwrap();
        storage.write_status(&status);
        let written = std::fs::metadata(dir.path().join(STATUS_FILE))
            .unwrap()
            .len();
        assert_eq!(
            storage.usage_bytes(),
            Some(before + i64::try_from(written).unwrap())
        );
    }

    #[test]
    fn the_latest_checkpoint_follows_the_catalog_over_the_root_state() {
        let dir = TempDir::new().unwrap();
        let (older, newest) = (Uuid::now_v7(), Uuid::now_v7());

        // Nothing stored: start fresh.
        assert!(
            storage_in(dir.path())
                .latest_checkpoint()
                .unwrap()
                .is_none()
        );

        // Only the root state, as a fault-tolerant pipeline leaves it before its first
        // checkpoint: resume from it.
        write_state(dir.path(), None, &state(1, None));
        let latest = storage_in(dir.path()).latest_checkpoint().unwrap().unwrap();
        assert_eq!((latest.step, latest.circuit), (1, None));

        // The root names the newest checkpoint: resume from the root.
        write_state(dir.path(), Some(older), &state(2, Some(older)));
        write_state(dir.path(), None, &state(2, Some(older)));
        write_catalog(dir.path(), &[older]);
        let latest = storage_in(dir.path()).latest_checkpoint().unwrap().unwrap();
        assert_eq!(latest.circuit.unwrap().uuid, older);

        // A crash after publishing `newest` but before rewriting the root: resume from
        // `newest`, and rewrite the root to name it.
        write_state(dir.path(), Some(newest), &state(3, Some(newest)));
        write_catalog(dir.path(), &[older, newest]);
        let latest = storage_in(dir.path()).latest_checkpoint().unwrap().unwrap();
        assert_eq!((latest.step, latest.circuit.unwrap().uuid), (3, newest));
        assert_eq!(root_step(dir.path()), 3);
    }

    #[test]
    fn a_newest_checkpoint_without_its_own_state_resumes_from_the_root() {
        let dir = TempDir::new().unwrap();
        let (older, newest) = (Uuid::now_v7(), Uuid::now_v7());
        write_state(dir.path(), None, &state(2, Some(older)));
        write_catalog(dir.path(), &[older, newest]);
        let latest = storage_in(dir.path()).latest_checkpoint().unwrap().unwrap();
        assert_eq!(latest.circuit.unwrap().uuid, older);
    }

    #[test]
    fn a_named_checkpoint_reads_its_own_state() {
        let dir = TempDir::new().unwrap();
        let uuid = Uuid::now_v7();
        let storage = storage_in(dir.path());
        assert_eq!(
            storage.checkpoint(uuid).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        write_state(dir.path(), Some(uuid), &state(5, Some(uuid)));
        assert_eq!(storage.checkpoint(uuid).unwrap().step, 5);
    }

    #[test]
    fn publishing_stores_the_state_first_and_names_it_last() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(dir.path());
        let uuid = Uuid::now_v7();
        write_state(dir.path(), None, &state(1, None));

        let published =
            publish_with_state(&**storage.backend(), &state(7, Some(uuid)), uuid, || {
                // The checkpoint's own state exists before publication, and the root still
                // names the previous state.
                let own: Checkpoint = serde_json::from_slice(
                    &std::fs::read(dir.path().join(uuid.to_string()).join(STATE_FILE)).unwrap(),
                )
                .unwrap();
                assert_eq!(own.step, 7);
                assert_eq!(root_step(dir.path()), 1);
                Ok(CheckpointMetadata {
                    uuid,
                    ..CheckpointMetadata::default()
                })
            })
            .unwrap();
        assert_eq!(published.uuid, uuid);
        assert_eq!(root_step(dir.path()), 7);

        // A failed publication leaves the root alone.
        let failed = Uuid::now_v7();
        publish_with_state(
            &**storage.backend(),
            &state(9, Some(failed)),
            failed,
            || {
                Err(dbsp_adapters::ControllerError::not_supported(
                    "publication failed",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(root_step(dir.path()), 7);
    }

    /// A journal record of `step`, with no input.
    fn record(step: u64) -> StepMetadata {
        StepMetadata {
            step,
            transaction_id: step as i64,
            remove_inputs: HashSet::new(),
            add_inputs: HashMap::new(),
            changed_inputs: HashMap::new(),
            input_logs: HashMap::new(),
            changed_outputs: HashMap::new(),
        }
    }

    /// A journal in a new directory under `tempdir`, over a POSIX backend.
    fn journal_in(tempdir: &TempDir) -> Journal {
        let backend = Arc::new(PosixBackend::new(
            tempdir.path(),
            StorageCacheConfig::default(),
            &FileBackendConfig::default(),
        ));
        Journal::create(backend, &StoragePath::from("journal")).unwrap()
    }

    #[test]
    fn journal_records_read_back_as_written() {
        init_test_logger();
        let tempdir = TempDir::new().unwrap();
        let journal = journal_in(&tempdir);
        let records = (0..10).map(record).collect::<Vec<_>>();
        for record in &records {
            journal.write(record).unwrap();
        }
        for expected in &records {
            assert_eq!(&journal.read(expected.step).unwrap().unwrap(), expected);
        }
        assert_eq!(journal.read(records.len() as u64).unwrap(), None);
    }

    /// A step is replayable only if its record survives a crash, and a backend's `write`
    /// leaves the record in the page cache. `FILES_SYNCED` counts the whole process, and
    /// concurrent tests can only inflate it, so this asserts a lower bound.
    #[test]
    fn journal_write_commits_every_record() {
        init_test_logger();
        let tempdir = TempDir::new().unwrap();
        let journal = journal_in(&tempdir);
        const RECORDS: u64 = 10;
        let before = FILES_SYNCED.load(Ordering::Relaxed);
        for step in 0..RECORDS {
            journal.write(&record(step)).unwrap();
        }
        let synced = FILES_SYNCED.load(Ordering::Relaxed) - before;
        assert!(
            synced >= RECORDS,
            "{RECORDS} journal records must produce at least {RECORDS} fsyncs, saw {synced}"
        );
    }

    #[test]
    fn the_journal_records_steps_and_its_modes_keep_or_empty_it() {
        let dir = TempDir::new().unwrap();
        let storage = storage_in(dir.path());
        write_state(dir.path(), None, &state(4, None));

        // Create empties the journal and records the initial state.
        let journal = storage
            .open_journal(JournalMode::Create(&state(0, None)))
            .unwrap();
        assert_eq!(root_step(dir.path()), 0);
        for step in [0, 1, 3, 4] {
            journal.write(&record(step)).unwrap();
        }
        assert_eq!(journal.list_steps(1).unwrap(), vec![1, 3, 4]);
        assert_eq!(journal.read(3).unwrap().unwrap().step, 3);
        assert!(journal.read(2).unwrap().is_none());

        // Open keeps the records; delete drops the ones in its range.
        let journal = storage.open_journal(JournalMode::Open(1)).unwrap();
        journal.delete(0..3).unwrap();
        assert_eq!(journal.list_steps(0).unwrap(), vec![3, 4]);
        assert!(dir.path().join(STEPS_FILE).exists());

        // Truncate deletes the journal and keeps the state.
        let journal = storage.open_journal(JournalMode::Truncate).unwrap();
        assert!(journal.read(3).unwrap().is_none());
        assert!(!dir.path().join(STEPS_FILE).exists());
        assert_eq!(root_step(dir.path()), 0);
    }
}
