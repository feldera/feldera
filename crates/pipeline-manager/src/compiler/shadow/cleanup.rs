//! The shadow slot owns at most one unfinished cleanup.
use super::{Failure, Outcome};
use nix::{errno::Errno, sys::signal::killpg, unistd::Pid};
use std::{io, path::PathBuf};
use tokio::time::{Duration, sleep, timeout};
use tracing::{info, warn};

pub(super) struct Cleanup {
    pub directory: PathBuf,
    pub groups: [Option<u32>; 2],
}

impl Cleanup {
    async fn remove(&mut self) -> io::Result<()> {
        if !tokio::fs::try_exists(&self.directory).await? {
            return Ok(());
        }
        timeout(Duration::from_secs(5), async {
            for group in &mut self.groups {
                if let Some(id) = *group {
                    let pid = Pid::from_raw(i32::try_from(id).map_err(io::Error::other)?);
                    // The leader may already be reaped. Never signal its old group ID.
                    loop {
                        match killpg(pid, None) {
                            Err(Errno::ESRCH) => break,
                            Ok(()) | Err(Errno::EPERM) => sleep(Duration::from_millis(10)).await,
                            Err(e) => return Err(io::Error::from(e)),
                        }
                    }
                    *group = None;
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
        match tokio::fs::remove_dir_all(&self.directory).await {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            result => result,
        }
    }

    pub(super) async fn finish(mut self, outcome: Outcome, stale: &mut Option<Self>) -> Outcome {
        if let Err(e) = self.remove().await {
            warn!(
                kind = ?e.kind(),
                directory = %self.directory.display(),
                "SQL shadow cleanup failed. Check this directory. The next shadow will retry cleanup."
            );
            *stale = Some(self);
            if outcome != Outcome::Different {
                return Outcome::Failed(Failure::Cleanup);
            }
        }
        outcome
    }

    pub(super) async fn retry(stale: &mut Option<Self>) -> bool {
        if let Some(cleanup) = stale {
            if cleanup.remove().await.is_err() {
                return false;
            }
            *stale = None;
            info!("SQL shadow cleanup recovered. Shadow compilation can resume.");
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use tokio::process::Command;

    #[tokio::test(start_paused = true)]
    async fn cleanup_waits_for_the_last_group_member_before_removal() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("attempt");
        std::fs::create_dir(&directory).unwrap();
        let mut leader = Command::new("cat")
            .stdin(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let group = leader.id().unwrap();
        let mut helper = Command::new("cat")
            .stdin(Stdio::piped())
            .process_group(group.try_into().unwrap())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        leader.kill().await.unwrap();
        let mut stale = None;
        let cleanup = Cleanup {
            directory: directory.clone(),
            groups: [Some(group), None],
        };
        assert_eq!(
            cleanup.finish(Outcome::TimedOut, &mut stale).await,
            Outcome::Failed(Failure::Cleanup)
        );
        assert!(directory.is_dir());
        helper.kill().await.unwrap();
        assert!(Cleanup::retry(&mut stale).await);
        assert!(!directory.exists());
        assert!(stale.is_none());
    }

    #[tokio::test]
    async fn cleanup_retries_a_failed_removal_and_preserves_the_difference() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("attempt");
        std::fs::write(&directory, []).unwrap();
        let cleanup = Cleanup {
            directory: directory.clone(),
            groups: [None, None],
        };
        let mut stale = None;
        assert_eq!(
            cleanup.finish(Outcome::Different, &mut stale).await,
            Outcome::Different
        );
        assert!(!Cleanup::retry(&mut stale).await);
        std::fs::remove_file(&directory).unwrap();
        std::fs::create_dir(&directory).unwrap();
        assert!(Cleanup::retry(&mut stale).await);
        assert!(stale.is_none());
        assert!(!directory.exists());
    }
}
