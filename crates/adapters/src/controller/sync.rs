#![allow(unused_imports)]
use anyhow::Context;
use std::sync::{Arc, LazyLock, Weak};

use dbsp::circuit::CircuitStorageConfig;
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_storage::{
    checkpoint_synchronizer::CheckpointSynchronizer, StorageBackend, StoragePath,
};
use feldera_types::{
    config::{FileBackendConfig, StorageBackendConfig, SyncConfig},
    constants::ACTIVATION_MARKER_FILE,
};

use crate::server::ServerState;

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

#[cfg(feature = "feldera-enterprise")]
fn upgrade_state(weak: Weak<ServerState>) -> Result<Arc<ServerState>, ControllerError> {
    weak.upgrade()
        .ok_or(ControllerError::checkpoint_fetch_error(
            "unreachable: failed to upgrade server state".to_owned(),
        ))
}

/// Pulls the checkpoint specified by the sync config and garbage collects all
/// older checkpoints.
#[cfg(feature = "feldera-enterprise")]
fn pull_and_gc(storage: Arc<dyn StorageBackend>, sync: &SyncConfig) -> Result<(), ControllerError> {
    match SYNCHRONIZER
        .pull(storage.clone(), sync.to_owned())
        .map_err(|e| ControllerError::checkpoint_fetch_error(format!("{e:?}")))
    {
        Err(err) => {
            tracing::error!("{:?}", err.to_string());
            if sync.fail_if_no_checkpoint {
                return Err(err);
            }
        }
        Ok(cpm) => {
            _ = storage
                .gc_startup(&vec![cpm].into())
                .map_err(|e| ControllerError::dbsp_error(e.into()))?;
        }
    }

    Ok(())
}

#[cfg(feature = "feldera-enterprise")]
pub fn continuous_pull(
    storage: &CircuitStorageConfig,
    weak: Weak<ServerState>,
) -> Result<(), ControllerError> {
    let StorageBackendConfig::File(FileBackendConfig {
        sync: Some(ref sync),
        ..
    }) = storage.options.backend
    else {
        return Ok(());
    };

    sync.validate()
        .map_err(ControllerError::checkpoint_fetch_error)?;

    if sync.start_from_checkpoint.is_none() {
        return Ok(());
    }

    let state = upgrade_state(weak)?;
    while !state.activated() {
        let previously_activated = storage
            .backend
            .exists(&StoragePath::default().child(ACTIVATION_MARKER_FILE))
            .unwrap_or(false);

        if previously_activated {
            tracing::info!("time pipeline was previously activated, skipping standby mode");
            return Ok(());
        }

        pull_and_gc(storage.backend.clone(), sync)?;

        if !sync.standby {
            return Ok(());
        }

        std::thread::sleep(std::time::Duration::from_secs(sync.pull_interval));
    }

    if state.activated() {
        if let Err(marker_err) = storage
            .backend
            .write_json(&StoragePath::default().child(ACTIVATION_MARKER_FILE), &"")
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
    }

    Ok(())
}
