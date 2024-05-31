//! Logic to manage persistent checkpoints for a circuit.

use crate::dynamic::{self, data::DataTyped, DataTrait, WeightTrait};
use crate::trace::ord::{
    vec::{VecIndexedWSet, VecWSet},
    FallbackIndexedWSet, FallbackWSet,
};
use crate::typed_batch::TypedBatch;
use crate::{DBData, DBWeight, Error};

use std::collections::{HashSet, VecDeque};
use std::fs::{self, create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::operator::NeighborhoodDescrBox;
use crate::trace::Serializer;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

/// Holds meta-data about a checkpoint that was taken for persistent storage
/// and recovery of a circuit's state.
#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
pub struct CheckpointMetadata {
    /// A unique identifier for the given checkpoint.
    ///
    /// This is used to identify the checkpoint in the file-system hierarchy.
    pub uuid: Uuid,
    /// An optional name for the checkpoint.
    pub identifier: Option<String>,
    /// Fingerprint of the circuit at the time of the checkpoint.
    pub fingerprint: u64,
    /// Which `step` the circuit was at when the checkpoint was created.
    pub step_id: u64,
}

impl CheckpointMetadata {
    pub fn new(uuid: Uuid, identifier: Option<String>, fingerprint: u64, step_id: u64) -> Self {
        CheckpointMetadata {
            uuid,
            identifier,
            fingerprint,
            step_id,
        }
    }
}

/// A "checkpointer" is responsible for the creation, and removal of
/// checkpoints for a circuit.
///
/// It handles list of available checkpoints, and the files associated
/// with each checkpoint.
#[derive(Debug)]
pub(crate) struct Checkpointer {
    storage_path: PathBuf,
    checkpoint_list: VecDeque<CheckpointMetadata>,
}

impl Checkpointer {
    /// We keep at least this many checkpoints around.
    pub(super) const MIN_CHECKPOINT_THRESHOLD: usize = 2;
    /// Name of the checkpoint list file.
    ///
    /// File will be stored inside the runtime storage directory with this
    /// name.
    const CHECKPOINT_FILE_NAME: &'static str = "checkpoints.feldera";
    /// A slice of all file-extension the system can create.
    const DBSP_FILE_EXTENSION: &'static [&'static str] = &["mut", "feldera"];

    pub fn new(storage_path: PathBuf) -> Self {
        let checkpoint_list =
            Self::try_read_checkpoints_from_file(&storage_path).unwrap_or_else(|_| {
                panic!(
                    "The checkpoint file in '{}' should be valid",
                    storage_path.display()
                )
            });

        Checkpointer {
            storage_path,
            checkpoint_list,
        }
    }

    /// Remove unexpected/leftover files from a previous run in the storage
    /// directory.
    pub(super) fn gc_startup(&self) -> Result<(), Error> {
        if self.checkpoint_list.is_empty() {
            // This is a safety measure we take to ensure that we don't
            // accidentally remove files just in case we fail to read the checkpoint
            // file. We don't remove anything.
            return Ok(());
        }

        // Collect all directories and files still referenced by a checkpoint
        let mut in_use_paths: HashSet<PathBuf> = HashSet::new();
        in_use_paths.insert(self.storage_path.join(Checkpointer::CHECKPOINT_FILE_NAME));
        for cpm in self.checkpoint_list.iter() {
            in_use_paths.insert(self.storage_path.join(cpm.uuid.to_string()));
            let batches = self
                .gather_batches_for_checkpoint(cpm)
                .expect("Batches for a checkpoint should be discoverable");
            for batch in batches {
                in_use_paths.insert(self.storage_path.join(batch));
            }
        }

        // Collect everything found in the storage directory
        let mut all_paths: HashSet<PathBuf> = HashSet::new();
        let files = fs::read_dir(&self.storage_path)?;
        for file in files {
            let file = file?;
            let path = file.path();
            all_paths.insert(path);
        }

        // Remove everything that is not referenced by a checkpoint
        let to_remove = all_paths.difference(&in_use_paths);
        for path in to_remove {
            if Checkpointer::DBSP_FILE_EXTENSION.contains(
                &path
                    .extension()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or_default(),
            ) || path.is_dir()
            {
                if path.is_dir() {
                    log::debug!("Removing unused directory '{}'", path.display());
                    fs::remove_dir_all(path)?;
                } else {
                    match fs::remove_file(path) {
                        Ok(_) => {
                            log::debug!("Removed unused file '{}'", path.display());
                        }
                        Err(e) => {
                            log::warn!("Unable to remove old-checkpoint file {}: {} (the pipeline will try to delete the file again on a restart)", path.display(), e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) fn create_checkpoint_dir(&self, uuid: Uuid) -> Result<(), Error> {
        let checkpoint_dir = self.storage_path.join(uuid.to_string());
        create_dir_all(checkpoint_dir)?;
        Ok(())
    }

    pub(super) fn commit(
        &mut self,
        uuid: Uuid,
        identifier: Option<String>,
        fingerprint: u64,
        step_id: u64,
    ) -> Result<CheckpointMetadata, Error> {
        let md = CheckpointMetadata {
            uuid,
            identifier,
            fingerprint,
            step_id,
        };
        self.checkpoint_list.push_back(md.clone());
        self.update_checkpoint_file()?;
        Ok(md)
    }

    /// List all currently available checkpoints.
    pub(super) fn list_checkpoints(&mut self) -> Result<Vec<CheckpointMetadata>, Error> {
        Ok(self.checkpoint_list.clone().into())
    }

    fn try_read_checkpoints_from_file<P: AsRef<Path>>(
        storage_path: P,
    ) -> Result<VecDeque<CheckpointMetadata>, Error> {
        let checkpoint_file_path = storage_path
            .as_ref()
            .to_path_buf()
            .join(Self::CHECKPOINT_FILE_NAME);
        if !checkpoint_file_path.exists() {
            return Ok(VecDeque::new());
        }
        let content = fs::read(checkpoint_file_path)?;
        let archived = unsafe { rkyv::archived_root::<VecDeque<CheckpointMetadata>>(&content) };
        let checkpoint_list: VecDeque<CheckpointMetadata> =
            archived.deserialize(&mut rkyv::Infallible).unwrap();
        Ok(checkpoint_list)
    }

    pub(super) fn gather_batches_for_checkpoint(
        &self,
        cpm: &CheckpointMetadata,
    ) -> Result<HashSet<String>, Error> {
        assert_ne!(cpm.uuid, Uuid::nil());
        let checkpoint_dir = self.storage_path.join(cpm.uuid.to_string());
        assert!(
            checkpoint_dir.exists(),
            "Checkpoint directory does not exist"
        );

        let mut batch_files_in_commit: HashSet<String> = HashSet::new();
        let files = fs::read_dir(&checkpoint_dir)?;
        for file in files {
            let file = file?;
            let path = file.path();
            let file_name = path.file_name().unwrap().to_string_lossy();

            if file_name.starts_with("pspine-batches") {
                let content = fs::read(file.path())?;
                let archived = rkyv::check_archived_root::<Vec<String>>(&content).unwrap();
                for batch in archived.iter() {
                    let batch_path = self.storage_path.join(batch.to_string());
                    if batch_path.exists() {
                        batch_files_in_commit.insert(batch.to_string());
                    }
                }
            }
        }

        Ok(batch_files_in_commit)
    }

    fn update_checkpoint_file(&self) -> Result<(), Error> {
        let checkpoint_file =
            self.storage_path
                .join(format!("{}{}", Self::CHECKPOINT_FILE_NAME, ".mut"));

        // write checkpoint list to a file:
        let as_bytes = crate::storage::file::to_bytes(&self.checkpoint_list)
            .expect("failed to serialize checkpoint-list data");
        let mut f = File::create(&checkpoint_file)?;
        f.write_all(as_bytes.as_slice())?;
        f.sync_all()?;
        fs::rename(&checkpoint_file, checkpoint_file.with_extension(""))?;

        Ok(())
    }

    /// Removes all the files in `files` from the storage base directory.
    fn remove_batch_files<P: AsRef<Path>>(&self, files: &Vec<P>) -> Result<(), Error> {
        for file in files {
            assert!(file.as_ref().is_file());
            assert!(
                file.as_ref().starts_with(&self.storage_path),
                "File {} is not in the storage directory",
                file.as_ref().display()
            );
            match fs::remove_file(file) {
                Ok(_) => {
                    log::debug!("Removed file {}", file.as_ref().display());
                }
                Err(e) => {
                    log::warn!("Unable to remove old-checkpoint file {}: {} (the pipeline will try to delete the file again on a restart)", file.as_ref().display(), e);
                }
            }
        }
        Ok(())
    }

    /// Removes all meta-data files associated with the checkpoint given by
    /// `cpm` by removing the folder associated with the checkpoint.
    fn remove_checkpoint_dir(&self, cpm: &CheckpointMetadata) -> Result<(), Error> {
        assert_ne!(cpm.uuid, Uuid::nil());
        let checkpoint_dir = self.storage_path.join(cpm.uuid.to_string());
        if !checkpoint_dir.exists() {
            log::warn!(
                "Tried to remove a checkpoint directory '{}' that doesn't exist.",
                checkpoint_dir.display()
            );
            return Ok(());
        }
        fs::remove_dir_all(checkpoint_dir)?;
        Ok(())
    }

    /// Remove the oldest checkpoint from the list.
    ///
    /// # Returns
    /// - Metadata of the removed checkpoint, if there are more than
    ///   `MIN_CHECKPOINT_THRESHOLD`
    /// - None otherwise.
    pub fn gc_checkpoint(&mut self) -> Result<Option<CheckpointMetadata>, Error> {
        // Ensures that we can unwrap the call to pop_front, and front later:
        static_assertions::const_assert!(Checkpointer::MIN_CHECKPOINT_THRESHOLD >= 2);
        if self.checkpoint_list.len() > Self::MIN_CHECKPOINT_THRESHOLD {
            let cp_to_remove = self.checkpoint_list.pop_front().unwrap();
            let next_in_line = self.checkpoint_list.front().unwrap();

            // Update the checkpoint list file, we do this first intentionally, in case
            // later operations fail we don't want the checkpoint list to
            // contain a checkpoint that only has part of the files.
            //
            // If any of the later operations fail, restarting the circuit will try
            // to remove the checkpoint files again (see also [`Self::gc_startup`]).
            self.update_checkpoint_file()?;

            let potentially_remove = self.gather_batches_for_checkpoint(&cp_to_remove)?;
            let need_to_keep = self.gather_batches_for_checkpoint(next_in_line)?;
            let to_remove = potentially_remove
                .difference(&need_to_keep)
                .map(PathBuf::from)
                .collect::<Vec<_>>();

            self.remove_batch_files(&to_remove)?;
            self.remove_checkpoint_dir(&cp_to_remove)?;

            Ok(Some(cp_to_remove))
        } else {
            Ok(None)
        }
    }
}

/// Trait for types that can be check-pointed and restored.
///
/// This is to be used for any additional state within circuit operators
/// that's not stored within a batch (which are already stored in files).
pub trait Checkpoint {
    fn checkpoint(&self) -> Result<Vec<u8>, Error>;
    fn restore(&mut self, data: &[u8]) -> Result<(), Error>;
}

impl Checkpoint for isize {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl Checkpoint for usize {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl Checkpoint for i32 {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<N> Checkpoint for Box<N>
where
    N: Checkpoint + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        self.as_ref().checkpoint()
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), Error> {
        self.as_mut().restore(data)
    }
}

impl<T> Checkpoint for Option<T>
where
    T: Checkpoint,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<K, V> Checkpoint for NeighborhoodDescrBox<K, V>
where
    K: DBData,
    V: DBData,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl Checkpoint for dyn dynamic::data::Data + 'static {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        let mut s = Serializer::default();
        let _r = self.serialize(&mut s).unwrap();
        let fbuf = s.into_serializer().into_inner();
        Ok(fbuf.into_vec())
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), Error> {
        unsafe { self.deserialize_from_bytes(data, 0) };
        Ok(())
    }
}

impl Checkpoint for dyn DataTyped<Type = u64> + 'static {
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<K, V, R, B> Checkpoint for TypedBatch<K, V, R, B>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<K, R> Checkpoint for VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<K, V, R> Checkpoint for VecIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<K, V, R> Checkpoint for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}

impl<K, R> Checkpoint for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn restore(&mut self, _data: &[u8]) -> Result<(), Error> {
        todo!()
    }
}
