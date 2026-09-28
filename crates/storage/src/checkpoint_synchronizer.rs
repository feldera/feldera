use std::sync::{Arc, LazyLock};

use feldera_types::{
    checkpoint::{CheckpointMetadata, CheckpointSyncMetrics, HostInfo, RemoteCheckpoint},
    config::{PipelineIdentity, SyncConfig},
};

use crate::StorageBackend;

pub trait CheckpointSynchronizer: Sync {
    /// Push a checkpoint to remote object storage.
    ///
    /// `host_info` identifies the calling host within a multihost pipeline.
    /// When `Some`, the checkpoint zip and catalog are written under
    /// `host{N}/` in the remote bucket; when `None` (solo pipeline), the
    /// existing flat layout is used for backward compatibility.
    ///
    /// `pipeline` identifies the pipeline performing the push and is used to
    /// enforce bucket ownership: the push fails before writing any data if the
    /// bucket is already owned by a different pipeline.
    fn push(
        &self,
        checkpoint: uuid::Uuid,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
        host_info: Option<HostInfo>,
        pipeline: PipelineIdentity,
    ) -> anyhow::Result<Option<CheckpointSyncMetrics>>;

    /// Pull a checkpoint from remote object storage.
    ///
    /// `host_info` scopes the pull to the correct `host{N}/` subdirectory.
    /// Pass `None` for solo pipelines to use the existing flat layout.
    ///
    /// `standby` indicates that the pipeline is in standby mode: the
    /// local-storage cache is bypassed (always pull from remote) and a missing
    /// remote checkpoint is treated as an error rather than a fresh start.
    ///
    /// `pipeline` identifies the pipeline performing the pull. It is used only
    /// to warn (never fail) when pulling from a bucket owned by a different
    /// pipeline.
    fn pull(
        &self,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
        host_info: Option<HostInfo>,
        standby: bool,
        pipeline: PipelineIdentity,
    ) -> anyhow::Result<(CheckpointMetadata, Option<CheckpointSyncMetrics>)>;

    /// Takes ownership of the remote bucket for `pipeline` as it starts.
    ///
    /// Called once per run, before the pipeline starts processing, when
    /// `remote_config.take_bucket_ownership` is set.  Claims an unowned
    /// bucket, refreshes the ownership of a bucket `pipeline` already owns,
    /// and takes over a bucket owned by a different pipeline, logging a
    /// warning that names the previous and new owners.  [`Self::push`] never
    /// takes ownership, so if another pipeline takes the bucket over later in
    /// this run, the next push fails.
    ///
    /// Every host of a multihost pipeline calls this as it starts.  The hosts
    /// share `pipeline`, so their concurrent writes agree, and a write by
    /// another host of the same pipeline counts as success.
    ///
    /// The default implementation fails, reporting `take_bucket_ownership` as
    /// unsupported.  It keeps this method optional, so that a synchronizer
    /// that predates it still compiles: the enterprise synchronizer is built
    /// from a separate repository, which may lag behind this trait, and a
    /// runtime version override builds a newer runtime against an older
    /// platform's synchronizer.
    ///
    /// # Arguments
    /// - `storage`: local storage, used to stage the ownership file.
    /// - `remote_config`: sync settings; `bucket` is the location to take.
    /// - `pipeline`: identity of the pipeline taking ownership.
    ///
    /// # Returns
    /// `Ok(())` once the bucket's ownership file names `pipeline`; an error if
    /// it could not be written, a different pipeline overwrote it at the same
    /// moment, or the synchronizer does not support taking ownership.
    fn take_ownership(
        &self,
        _storage: Arc<dyn StorageBackend>,
        _remote_config: SyncConfig,
        _pipeline: PipelineIdentity,
    ) -> anyhow::Result<()> {
        anyhow::bail!(
            "this build's checkpoint synchronizer does not support `take_bucket_ownership`"
        )
    }

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
