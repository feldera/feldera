use std::sync::Arc;

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
