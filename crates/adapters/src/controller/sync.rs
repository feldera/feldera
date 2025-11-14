#![allow(unused_imports)]
use anyhow::Context;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, LazyLock, Mutex, Weak,
};

use dbsp::circuit::{checkpointer::Checkpointer, CircuitStorageConfig};
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_storage::{
    checkpoint_synchronizer::CheckpointSynchronizer, histogram::ExponentialHistogram,
    StorageBackend, StoragePath,
};
use feldera_types::{
    checkpoint::CheckpointMetadata,
    config::{FileBackendConfig, StorageBackendConfig, SyncConfig},
    constants::ACTIVATION_MARKER_FILE,
};

use crate::server::ServerState;

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

/// Lazily resolves the checkpoint synchronizer.
///
/// This panic is safe as all enterprise builds must include the checkpoint-sync
/// crate.
pub(super) static SYNCHRONIZER: LazyLock<&'static dyn CheckpointSynchronizer> =
    LazyLock::new(|| {
        let Some(synchronizer) = inventory::iter::<&dyn CheckpointSynchronizer>
            .into_iter()
            .next()
        else {
            unreachable!("no checkpoint synchronizer found; are enterprise features enabled?");
        };

        *synchronizer
    });

/// Pulls the checkpoint specified by the sync config and garbage collects all
/// older checkpoints.
#[cfg(feature = "feldera-enterprise")]
fn pull_and_gc(
    storage: Arc<dyn StorageBackend>,
    sync: &SyncConfig,
    prev: &mut uuid::Uuid,
) -> Result<CheckpointMetadata, ControllerError> {
    match SYNCHRONIZER
        .pull(storage.clone(), sync.to_owned())
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

            let checkpointer = Checkpointer::new(storage.clone(), 0, false)?;
            checkpointer.gc_startup()?;

            Ok(cpm)
        }
    }
}

#[cfg(feature = "feldera-enterprise")]
pub fn is_pull_necessary(storage: &CircuitStorageConfig) -> Option<&SyncConfig> {
    let StorageBackendConfig::File(ref file_cfg) = storage.options.backend else {
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

#[cfg(feature = "feldera-enterprise")]
pub fn pull_once(storage: &CircuitStorageConfig, sync: &SyncConfig) -> Result<(), ControllerError> {
    if let Err(err) = pull_and_gc(storage.backend.clone(), sync, &mut uuid::Uuid::nil()) {
        if sync.fail_if_no_checkpoint {
            return Err(err);
        }
    }

    Ok(())
}

#[cfg(feature = "feldera-enterprise")]
pub fn continuous_pull<F>(
    storage: &CircuitStorageConfig,
    is_activated: F,
) -> Result<(), ControllerError>
where
    F: Fn() -> bool,
{
    let StorageBackendConfig::File(ref file_cfg) = storage.options.backend else {
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
    let activation_file = StoragePath::from(ACTIVATION_MARKER_FILE);
    let previously_activated = storage.backend.exists(&activation_file).unwrap_or(false);

    if previously_activated {
        let previous_activation = storage
            .backend
            .read_json::<Option<CheckpointMetadata>>(&activation_file)
            .ok()
            .flatten();

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
        match pull_and_gc(storage.backend.clone(), sync, &mut prev) {
            Err(err) => {
                if sync.fail_if_no_checkpoint {
                    return Err(err);
                }
            }
            Ok(c) => {
                cpm = Some(c);
            }
        };

        if !sync.standby {
            return Ok(());
        }

        if is_activated() {
            if pull_once_again_after_activation {
                // We've already done one iteration after activation, now we're done
                break;
            }
            pull_once_again_after_activation = true;
            // Continue to pull one more time to get the latest checkpoint
        }

        std::thread::sleep(std::time::Duration::from_secs(sync.pull_interval));
    }

    tracing::debug!("creating activation marker file: {}", activation_file);

    if let Err(marker_err) = storage
        .backend
        .write_json(&activation_file, &cpm)
        .and_then(|reader| reader.commit())
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
