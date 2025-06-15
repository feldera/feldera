use feldera_types::config::SyncConfig;
use std::{io::Write, path::PathBuf, process::ExitStatus};
use tokio::process::Command;

use super::error::SyncError;

pub(super) type StoragePath = PathBuf;

/// Where to source from / send to.
#[derive(Debug)]
pub(super) enum RcloneLocation {
    Local(StoragePath),
    Remote(SyncConfig),
}

impl RcloneLocation {
    fn to_args(&self) -> String {
        match self {
            RcloneLocation::Remote(config) => {
                format!(":s3:{}", config.bucket.trim_start_matches("s3://"))
            }
            RcloneLocation::Local(path) => path.display().to_string(),
        }
    }
}

#[derive(Debug)]
pub struct RcloneSync {
    source: RcloneLocation,
    dest: RcloneLocation,
}

impl RcloneSync {
    pub(super) fn new(source: RcloneLocation, dest: RcloneLocation) -> Self {
        Self { source, dest }
    }

    fn set_options(&self, cmd: &mut Command) {
        let cfg = match (&self.source, &self.dest) {
            (RcloneLocation::Local(_), RcloneLocation::Local(_)) => return,
            (RcloneLocation::Local(_), RcloneLocation::Remote(cfg)) => cfg,
            (RcloneLocation::Remote(cfg), RcloneLocation::Local(_)) => cfg,
            (RcloneLocation::Remote(_), RcloneLocation::Remote(_)) => {
                unreachable!()
            }
        };

        if let Some(ref endpoint) = cfg.endpoint {
            cmd.arg("--s3-endpoint");
            cmd.arg(endpoint);
        }

        cmd.arg("--s3-provider");
        if let Some(ref provider) = cfg.provider {
            cmd.arg(provider);
        } else {
            cmd.arg("Other");
        }

        if let Some((access_key, secret_key)) = cfg.access_key.as_ref().zip(cfg.secret_key.as_ref())
        {
            cmd.env("RCLONE_S3_ACCESS_KEY_ID", access_key);
            cmd.env("RCLONE_S3_SECRET_ACCESS_KEY", secret_key);
        } else {
            cmd.arg("--s3-env-auth");
        }
    }

    #[tracing::instrument]
    pub(super) async fn sync(&self, files: Vec<String>) -> Result<ExitStatus, SyncError> {
        let source = self.source.to_args();
        let dest = self.dest.to_args();

        let mut temp = tempfile::NamedTempFile::new()?;
        let path = temp.path().display().to_string();
        write!(temp, "{}", files.join("\n"))?;
        temp.flush()?;

        let mut cmd = tokio::process::Command::new("rclone");
        cmd.args(["sync", "--include-from", &path, &source, &dest]);
        self.set_options(&mut cmd);

        Ok(cmd.spawn()?.wait().await?)
    }
}
