#![allow(unused_imports)]
use anyhow::Context;
use std::sync::{Arc, LazyLock, Weak};

use dbsp::circuit::{checkpointer::Checkpointer, CircuitStorageConfig};
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_storage::{
    checkpoint_synchronizer::CheckpointSynchronizer, StorageBackend, StoragePath,
};
use feldera_types::{
    checkpoint::CheckpointMetadata,
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

#[cfg(feature = "feldera-enterprise")]
pub fn continuous_pull(
    storage: &CircuitStorageConfig,
    weak: Weak<ServerState>,
) -> Result<(), ControllerError> {
    let StorageBackendConfig::File(ref file_cfg) = storage.options.backend else {
        return Ok(());
    };

    let FileBackendConfig {
        sync: Some(ref sync),
        ..
    } = **file_cfg
    else {
        return Ok(());
    };

    sync.validate()
        .map_err(ControllerError::checkpoint_fetch_error)?;

    if sync.start_from_checkpoint.is_none() {
        return Ok(());
    }

    let state = upgrade_state(weak)?;
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

    while !state.activated() {
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
