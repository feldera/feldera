#![allow(unused_imports)]
use anyhow::Context;
use std::sync::{Arc, LazyLock, Weak};

use dbsp::circuit::{checkpointer::Checkpointer, CircuitStorageConfig};
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_storage::{
    checkpoint_synchronizer::{CheckpointSynchronizer, SYNCHRONIZER},
    StorageBackend, StoragePath,
};
use feldera_types::{
    checkpoint::CheckpointMetadata,
    config::{FileBackendConfig, StorageBackendConfig, SyncConfig},
    constants::ACTIVATION_MARKER_FILE,
};

use crate::server::ServerState;

/// Pulls the checkpoint specified by the sync config and garbage collects all
/// older checkpoints.
fn pull_and_gc(
    storage: Arc<dyn StorageBackend>,
    sync: &SyncConfig,
) -> Result<CheckpointMetadata, ControllerError> {
    match SYNCHRONIZER
        .pull(storage.clone(), sync.to_owned())
        .map_err(|e| ControllerError::checkpoint_fetch_error(format!("{e:?}")))
    {
        Err(err) => {
            tracing::error!("{:?}", err.to_string());
            return Err(err);
        }
        Ok(cpm) => {
            let checkpointer = Checkpointer::new(storage.clone(), 0, false)?;
            checkpointer.gc_startup()?;

            Ok(cpm)
        }
    }
}

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

    while !is_activated() {
        match pull_and_gc(storage.backend.clone(), sync) {
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

        std::thread::sleep(std::time::Duration::from_secs(sync.pull_interval));
    }

    tracing::debug!("creating activation marker file: {}", activation_file);

    if let Err(marker_err) = storage
        .backend
        .write_json(&activation_file, &cpm)
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
