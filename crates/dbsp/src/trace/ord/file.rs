//! Trace and batch implementations based on files.

pub mod indexed_wset_batch;
pub mod key_batch;
pub mod val_batch;
pub mod wset_batch;

use crate::storage::file::reader::Error as ReaderError;
use feldera_storage::error::StorageError;
pub use indexed_wset_batch::{FileIndexedWSet, FileIndexedWSetFactories};
pub use key_batch::{FileKeyBatch, FileKeyBatchFactories};
pub use val_batch::{FileValBatch, FileValBatchFactories};
pub use wset_batch::{FileWSet, FileWSetFactories};

trait UnwrapStorage<T> {
    fn unwrap_storage(self) -> T;
}

impl<T> UnwrapStorage<T> for Result<T, StorageError> {
    fn unwrap_storage(self) -> T {
        fn inner(error: StorageError) -> ! {
            unwrap_storage_failed(error.to_string());
        }

        match self {
            Ok(t) => t,
            Err(error) => inner(error),
        }
    }
}

impl<T> UnwrapStorage<T> for Result<T, ReaderError> {
    fn unwrap_storage(self) -> T {
        fn inner(error: ReaderError) -> ! {
            unwrap_storage_failed(error.to_string());
        }

        match self {
            Ok(t) => t,
            Err(error) => inner(error),
        }
    }
}

fn unwrap_storage_failed(error: String) -> ! {
    panic!(
        r#"The pipeline crashed because storage I/O failed: {error}.

The crash was likely caused by storage exhaustion or storage failure, which will
likely happen again, so it is not recommended to automatically restart the
pipeline.  To avoid repeating the crash, take some remedial action first, such
as one of the following:

 * Reconfigure the container to use a larger, initially empty storage volume.
   If the pipeline had a checkpoint synchronized to object storage (such as S3),
   consider setting `storage.backend.config.sync.start_from_checkpoint` in the
   runtime configuration to restart from the checkpoint.

 * Tune your SQL program to use fewer resources.  (Editing the SQL
   program requires clearing storage, which deletes checkpoints.)

Afterward, restart the pipeline with ▶️ Start or the "/start" API call.

If these suggestions do not help, or you do not believe the crash was caused
by storage exhaustion or failure, please report the issue to the Feldera team.
"#
    );
}
