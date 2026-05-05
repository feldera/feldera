use std::sync::{Arc, LazyLock};

use feldera_types::{
    checkpoint::{CheckpointMetadata, CheckpointSyncMetrics, HostInfo, RemoteCheckpoint},
    config::SyncConfig,
};

use crate::StorageBackend;

pub trait CheckpointSynchronizer: Sync {
    /// Push a checkpoint to remote object storage.
    ///
    /// `host_info` identifies the calling host within a multihost pipeline.
    /// When `Some`, the checkpoint zip and catalog are written under
    /// `host{N}/` in the remote bucket; when `None` (solo pipeline), the
    /// existing flat layout is used for backward compatibility.
    fn push(
        &self,
        checkpoint: uuid::Uuid,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
        host_info: Option<HostInfo>,
    ) -> anyhow::Result<Option<CheckpointSyncMetrics>>;

    /// Pull a checkpoint from remote object storage.
    ///
    /// `host_info` scopes the pull to the correct `host{N}/` subdirectory.
    /// Pass `None` for solo pipelines to use the existing flat layout.
    ///
    /// `standby` indicates that the pipeline is in standby mode: the
    /// local-storage cache is bypassed (always pull from remote) and a missing
    /// remote checkpoint is treated as an error rather than a fresh start.
    fn pull(
        &self,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
        host_info: Option<HostInfo>,
        standby: bool,
    ) -> anyhow::Result<(CheckpointMetadata, Option<CheckpointSyncMetrics>)>;

    /// List checkpoints available in remote object storage.
    fn list_remote(&self, remote_config: SyncConfig) -> anyhow::Result<Vec<RemoteCheckpoint>>;
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
