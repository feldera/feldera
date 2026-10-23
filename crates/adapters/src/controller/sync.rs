#[cfg(feature = "feldera-enterprise")]
use anyhow::Context;
use anyhow::anyhow;
use std::sync::atomic::AtomicU64;
#[cfg(feature = "feldera-enterprise")]
use std::sync::atomic::Ordering;

use crate::engine::{CheckpointSync, PipelineStorage};
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_storage::histogram::ExponentialHistogram;
#[cfg(feature = "feldera-enterprise")]
use feldera_types::{
    checkpoint::CheckpointMetadata,
    config::{FileBackendConfig, StorageBackendConfig, StorageOptions},
};
use feldera_types::{
    checkpoint::HostInfo,
    config::{PipelineIdentity, SyncConfig},
};

// Pull metrics
/// Bytes transferred when pulling a checkpoint.
pub static CHECKPOINT_SYNC_PULL_TRANSFERRED_BYTES: ExponentialHistogram =
    ExponentialHistogram::new();

/// Transfer speed when pulling a checkpoint, in bytes per second.
pub static CHECKPOINT_SYNC_PULL_TRANSFER_SPEED: ExponentialHistogram = ExponentialHistogram::new();

/// Number of checkpoints pulled successfully.
pub static CHECKPOINT_SYNC_PULL_SUCCESS: AtomicU64 = AtomicU64::new(0);

/// Number of failures when pulling a checkpoint.
pub static CHECKPOINT_SYNC_PULL_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Time taken to pull a checkpoint from object store in seconds.
pub static CHECKPOINT_SYNC_PULL_DURATION_SECONDS: ExponentialHistogram =
    ExponentialHistogram::new();

// Push metrics
/// Bytes transferred when pushing a checkpoint.
pub static CHECKPOINT_SYNC_PUSH_TRANSFERRED_BYTES: ExponentialHistogram =
    ExponentialHistogram::new();

/// Transfer speed when pushing a checkpoint, in bytes per second.
pub static CHECKPOINT_SYNC_PUSH_TRANSFER_SPEED: ExponentialHistogram = ExponentialHistogram::new();

/// Number of checkpoints pushed successfully.
pub static CHECKPOINT_SYNC_PUSH_SUCCESS: AtomicU64 = AtomicU64::new(0);

/// Number of failures when pushing a checkpoint.
pub static CHECKPOINT_SYNC_PUSH_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Time taken to push a checkpoint to object store in seconds.
pub static CHECKPOINT_SYNC_PUSH_DURATION_SECONDS: ExponentialHistogram =
    ExponentialHistogram::new();

/// The checkpoint sync of `storage`, or an error if its engine has none.
pub(crate) fn checkpoint_sync(
    storage: &dyn PipelineStorage,
) -> anyhow::Result<&dyn CheckpointSync> {
    storage.sync().ok_or_else(|| {
        anyhow!("this pipeline's engine cannot sync checkpoints with object storage")
    })
}

/// Pulls the checkpoint specified by the sync config and garbage collects all
/// older checkpoints.
#[cfg(feature = "feldera-enterprise")]
fn pull_and_gc(
    storage: &dyn PipelineStorage,
    sync: &SyncConfig,
    prev: &mut uuid::Uuid,
    host_info: Option<HostInfo>,
    standby: bool,
    pipeline: &PipelineIdentity,
) -> Result<CheckpointMetadata, ControllerError> {
    match checkpoint_sync(storage)
        .and_then(|checkpoint_sync| {
            checkpoint_sync.pull(sync.to_owned(), host_info, standby, pipeline.clone())
        })
        .map_err(|e| ControllerError::checkpoint_fetch_error(format!("{e:?}")))
    {
        Err(err) => {
            tracing::error!("{:?}", err.to_string());
            CHECKPOINT_SYNC_PULL_FAILURES.fetch_add(1, Ordering::Relaxed);
            Err(err)
        }
        Ok((cpm, metrics)) => {
            if cpm.uuid != *prev {
                CHECKPOINT_SYNC_PULL_SUCCESS.fetch_add(1, Ordering::Relaxed);
                *prev = cpm.uuid;
                if let Some(metrics) = metrics {
                    CHECKPOINT_SYNC_PULL_TRANSFER_SPEED.record(metrics.speed);
                    CHECKPOINT_SYNC_PULL_DURATION_SECONDS.record(metrics.duration.as_secs());
                    CHECKPOINT_SYNC_PULL_TRANSFERRED_BYTES.record(metrics.bytes);
                }
            }

            storage.gc_startup()?;

            Ok(cpm)
        }
    }
}

#[cfg(feature = "feldera-enterprise")]
pub fn is_pull_necessary(options: &StorageOptions) -> Option<&SyncConfig> {
    let StorageBackendConfig::File(ref file_cfg) = options.backend else {
        return None;
    };

    let FileBackendConfig {
        sync: Some(ref sync),
        ..
    } = **file_cfg
    else {
        return None;
    };

    sync.start_from_checkpoint.as_ref()?;

    Some(sync)
}

/// Pulls the latest checkpoint from object storage into `storage`, once.
///
/// Used as a pipeline starts, and by the multihost coordinator's pull endpoint.
#[cfg(feature = "feldera-enterprise")]
pub fn pull_once(
    storage: &dyn PipelineStorage,
    sync: &SyncConfig,
    host_info: Option<HostInfo>,
    standby: bool,
    pipeline: &PipelineIdentity,
) -> Result<(), ControllerError> {
    pull_and_gc(
        storage,
        sync,
        &mut uuid::Uuid::nil(),
        host_info,
        standby,
        pipeline,
    )?;
    Ok(())
}

/// Takes ownership of the sync bucket for `pipeline` as it starts.
///
/// Called once per run, before the pipeline starts processing, when
/// `take_bucket_ownership` is set; see
/// [`CheckpointSync::take_ownership`].
///
/// # Arguments
/// - `storage`: local storage, used to stage the ownership file.
/// - `sync`: the pipeline's sync configuration.
/// - `pipeline`: identity of this pipeline.
///
/// # Returns
/// `Ok(())` once this pipeline owns the bucket; otherwise the error that fails
/// pipeline startup.
#[cfg(feature = "feldera-enterprise")]
pub fn take_bucket_ownership(
    storage: &dyn PipelineStorage,
    sync: &SyncConfig,
    pipeline: &PipelineIdentity,
) -> Result<(), ControllerError> {
    checkpoint_sync(storage)
        .and_then(|checkpoint_sync| {
            checkpoint_sync.take_ownership(sync.to_owned(), pipeline.clone())
        })
        .map_err(|e| {
            ControllerError::checkpoint_push_error(format!(
                "cannot take ownership of bucket '{}' at startup: {e:?}",
                sync.bucket
            ))
        })
}

#[cfg(not(feature = "feldera-enterprise"))]
pub fn pull_once(
    _storage: &dyn PipelineStorage,
    _sync: &SyncConfig,
    _host_info: Option<HostInfo>,
    _standby: bool,
    _pipeline: &PipelineIdentity,
) -> Result<(), ControllerError> {
    Err(ControllerError::EnterpriseFeature("checkpoint pull"))
}

#[cfg(feature = "feldera-enterprise")]
pub fn list_remote_checkpoints(
    storage: &dyn PipelineStorage,
    sync: &SyncConfig,
) -> Result<Vec<feldera_types::checkpoint::RemoteCheckpoint>, ControllerError> {
    checkpoint_sync(storage)
        .and_then(|checkpoint_sync| checkpoint_sync.list_remote(sync.to_owned()))
        .map_err(|e| ControllerError::checkpoint_fetch_error(format!("{e:?}")))
}

#[cfg(not(feature = "feldera-enterprise"))]
pub fn list_remote_checkpoints(
    _storage: &dyn PipelineStorage,
    _sync: &SyncConfig,
) -> Result<Vec<feldera_types::checkpoint::RemoteCheckpoint>, ControllerError> {
    Err(ControllerError::EnterpriseFeature("list S3 checkpoints"))
}

#[cfg(feature = "feldera-enterprise")]
pub fn continuous_pull<F>(
    storage: &dyn PipelineStorage,
    options: &StorageOptions,
    is_activated: F,
    host_info: Option<HostInfo>,
    pipeline: &PipelineIdentity,
) -> Result<(), ControllerError>
where
    F: Fn() -> bool,
{
    let StorageBackendConfig::File(ref file_cfg) = options.backend else {
        return Err(ControllerError::InvalidStandby(
            "standby mode requires file storage backend",
        ));
    };

    let FileBackendConfig {
        sync: Some(ref sync),
        ..
    } = **file_cfg
    else {
        return Err(ControllerError::InvalidStandby(
            "standby mode requires file storage backend to have synchronization configured",
        ));
    };

    sync.validate()
        .map_err(ControllerError::checkpoint_fetch_error)?;

    if sync.start_from_checkpoint.is_none() {
        return Err(ControllerError::InvalidStandby(
            "standby mode requires file storage backend to have synchronization configured to start from a checkpoint",
        ));
    }

    let mut cpm = None;
    let activation = checkpoint_sync(storage)
        .map_err(|e| ControllerError::checkpoint_fetch_error(format!("{e:?}")))?;

    if let Some(previous_activation) = activation.read_activation() {
        tracing::info!(
            "this pipeline was previously activated from {}, skipping standby mode",
            previous_activation
                .map(|p| format!("checkpoint '{}'", p.uuid))
                .unwrap_or_else(|| "scratch".to_owned())
        );
        return Ok(());
    }

    let mut prev = uuid::Uuid::nil();
    let mut pull_once_again_after_activation = false;

    // This should run at least once before checking for activation, to ensure
    // that we pull a checkpoint if one is available.
    // Also, if we receive an activation signal, we run one more iteration to
    // ensure that we have the latest checkpoint before activating.
    loop {
        match pull_and_gc(storage, sync, &mut prev, host_info, true, pipeline) {
            Err(err) => {
                // On our final attempt to pull the checkpoint after activation, if we fail, we should error out and not activate with a potentially stale or missing checkpoint.
                if pull_once_again_after_activation {
                    return Err(err);
                }
            }
            Ok(c) => {
                cpm = Some(c);
            }
        };

        if is_activated() {
            if pull_once_again_after_activation {
                // We've already done one iteration after activation, now we're done
                break;
            }
            pull_once_again_after_activation = true;
            // Continue to pull one more time to get the latest checkpoint
        }

        std::thread::sleep(sync.standby_pull_interval().into());
    }

    if let Err(marker_err) = activation
        .write_activation(&cpm)
        .context("failed to write activation marker file")
    {
        tracing::error!("{marker_err:?}");
        if sync.fail_if_no_checkpoint {
            return Err(ControllerError::checkpoint_fetch_error(format!(
                "{marker_err:?}"
            )));
        }
    }

    tracing::info!("pipeline activated");

    Ok(())
}
