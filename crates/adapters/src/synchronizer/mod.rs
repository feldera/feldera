mod error;
mod rclone;

use anyhow::Result;
use dbsp::circuit::checkpointer::{CheckpointMetadata, CHECKPOINT_FILE_NAME};
use error::SyncError;
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_types::config::SyncConfig;
use rclone::{RcloneLocation, RcloneSync};
use serde::Deserialize;
use std::{
    ffi::OsStr,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    process::ExitStatus,
};
use tokio::io::AsyncReadExt;

use crate::controller::STATE_FILE;

#[derive(Debug, Deserialize)]
struct PSpineFiles {
    files: Vec<String>,
}

struct AllCheckpointFiles {
    metadata: Vec<String>,
    reference: Vec<String>,
}

impl AllCheckpointFiles {
    fn from_checkpoint(chk: &uuid::Uuid, storage_path: &Path) -> Result<Self, SyncError> {
        let metadata_files = std::fs::read_dir(storage_path.join(chk.to_string()))?;
        let metadata_files: Vec<PathBuf> = metadata_files
            .filter_map(|f| f.ok())
            .map(|f| f.path())
            .collect();

        let pspine = metadata_files.iter().filter(|f| {
            std::path::Path::new(f)
                .file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|n| n.starts_with("pspine-batches"))
        });

        let mut reference_files = vec![];

        for f in pspine {
            let f = BufReader::new(File::open(f)?);
            let value: PSpineFiles = serde_json::from_reader(f)?;

            reference_files.extend(value.files);
        }

        let mut metadata_files: Vec<String> = metadata_files
            .into_iter()
            .flat_map(|f| {
                f.strip_prefix(storage_path)
                    .ok()
                    .and_then(|f| f.to_str())
                    .map(|f| f.to_owned())
            })
            .collect();

        metadata_files.push(CHECKPOINT_FILE_NAME.to_string());
        metadata_files.push(STATE_FILE.to_string());

        Ok(AllCheckpointFiles {
            metadata: metadata_files,
            reference: reference_files,
        })
    }

    fn combined(mut self) -> Vec<String> {
        self.metadata.extend(self.reference);
        self.metadata
    }
}

pub(crate) async fn push(
    checkpoint: &CheckpointMetadata,
    storage_path: &Path,
    remote_config: SyncConfig,
) -> Result<(), ControllerError> {
    push_inner(checkpoint, storage_path, remote_config).await?;
    Ok(())
}

#[allow(clippy::result_large_err)]
fn handle_non_zero_exit_code(exit_code: ExitStatus) -> Result<(), ControllerError> {
    if !exit_code.success() {
        return Err(ControllerError::checkpoint_fetch_error(format!(
            "synchronizer exited with a non zero exit code: {}",
            exit_code
        )));
    }

    Ok(())
}

async fn push_inner(
    checkpoint: &CheckpointMetadata,
    storage_path: &Path,
    remote_config: SyncConfig,
) -> Result<(), SyncError> {
    let files = AllCheckpointFiles::from_checkpoint(&checkpoint.uuid, storage_path)?;

    let sync = RcloneSync::new(
        RcloneLocation::Local(storage_path.to_path_buf()),
        RcloneLocation::Remote(remote_config),
    );

    let exit_code = sync.sync(files.combined()).await?;

    handle_non_zero_exit_code(exit_code)?;

    Ok(())
}
pub(super) async fn pull(
    storage_path: &Path,
    remote_config: SyncConfig,
) -> Result<(), ControllerError> {
    pull_inner(storage_path, remote_config).await?;
    Ok(())
}

async fn pull_inner(storage_path: &Path, remote_config: SyncConfig) -> Result<(), SyncError> {
    if !remote_config.start_from_checkpoint {
        tracing::debug!("'start_from_checkpoint' set to false, no checkpoints will be pulled");
        return Ok(());
    };

    let sync = RcloneSync::new(
        RcloneLocation::Remote(remote_config),
        RcloneLocation::Local(storage_path.to_path_buf()),
    );

    let exit_code = sync
        .sync(vec![CHECKPOINT_FILE_NAME.to_owned(), STATE_FILE.to_owned()])
        .await?;
    handle_non_zero_exit_code(exit_code)?;
    let mut f = tokio::io::BufReader::new(
        tokio::fs::File::open(storage_path.join(CHECKPOINT_FILE_NAME)).await?,
    );

    let mut buf = vec![];
    f.read_to_end(&mut buf).await?;

    let mut to_pull = vec![];

    let chks: Vec<CheckpointMetadata> = serde_json::from_slice(&buf)?;
    for chk in chks.iter() {
        to_pull.push(format!("{}/*", chk.uuid));
    }

    let exit_code = sync
        .sync(to_pull)
        .await
        .map_err(|e| ControllerError::checkpoint_fetch_error(e.to_string()))?;

    handle_non_zero_exit_code(exit_code)?;

    let mut reference_files = vec![];

    for chk in chks {
        let AllCheckpointFiles { reference, .. } =
            AllCheckpointFiles::from_checkpoint(&chk.uuid, storage_path)?;
        reference_files.extend(reference);
    }

    let exit_code = sync
        .sync(reference_files)
        .await
        .map_err(|e| ControllerError::checkpoint_fetch_error(e.to_string()))?;

    handle_non_zero_exit_code(exit_code)?;

    Ok(())
}
