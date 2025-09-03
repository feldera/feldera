use std::sync::{Arc, LazyLock};

use feldera_types::{checkpoint::CheckpointMetadata, config::SyncConfig};

use crate::StorageBackend;

pub trait CheckpointSynchronizer: Sync {
    fn push(
        &self,
        checkpoint: uuid::Uuid,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
    ) -> anyhow::Result<()>;

    fn pull(
        &self,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
    ) -> anyhow::Result<CheckpointMetadata>;
}

inventory::collect!(&'static dyn CheckpointSynchronizer);

/// Lazily resolves the checkpoint synchronizer.
///
/// This panic is safe as all enterprise builds must include the checkpoint-sync
/// crate.
pub static SYNCHRONIZER: LazyLock<&'static dyn CheckpointSynchronizer> = LazyLock::new(|| {
    *inventory::iter::<&dyn CheckpointSynchronizer>
        .into_iter()
        .next()
        .expect("no checkpoint synchronizer found; are enterprise features enabled?")
});
