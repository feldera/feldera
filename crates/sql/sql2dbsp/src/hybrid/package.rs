//! Identify the three deployed compiler files of a manager shadow. Paths are
//! installation-specific; only the ordered content digest leaves the worker.
use super::{file_digest, Failure, Supervisor};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive(PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    size: u64,
    change_time: (i64, i64),
}

impl FileIdentity {
    fn read(path: &Path) -> io::Result<Self> {
        let metadata = fs::metadata(path)?;
        if !metadata.is_file() {
            return Err(io::Error::other("expected a packaged compiler file"));
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
            change_time: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

pub(super) struct Package {
    files: Vec<(PathBuf, FileIdentity)>,
    digest: String,
}

impl Package {
    pub(super) fn identify(supervisor: &Supervisor, jar: &Path) -> Result<Self, Failure> {
        let worker = env::current_exe()?;
        #[cfg(target_os = "linux")]
        if FileIdentity::read(&worker)? != FileIdentity::read(Path::new("/proc/self/exe"))? {
            return Err(io::Error::other("running worker was replaced").into());
        }
        let name = format!(
            "{}feldera_sql_opt_jni{}",
            env::consts::DLL_PREFIX,
            env::consts::DLL_SUFFIX
        );
        let optimizer = worker.with_file_name(name);
        Self::from_files(supervisor, [worker, optimizer, jar.to_owned()])
    }

    fn from_files(supervisor: &Supervisor, paths: [PathBuf; 3]) -> Result<Self, Failure> {
        let mut files = Vec::with_capacity(paths.len());
        let mut hash = Sha256::new();
        for path in paths {
            let identity = FileIdentity::read(&path)?;
            hash.update(file_digest(supervisor, &path)?);
            files.push((path, identity));
        }
        Ok(Self {
            files,
            digest: hex::encode(hash.finalize()),
        })
    }

    pub(super) fn verified_digest(self) -> Option<String> {
        self.files
            .into_iter()
            .all(|(path, before)| FileIdentity::read(&path).is_ok_and(|after| after == before))
            .then_some(self.digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn replacing_any_component_changes_the_package_and_invalidates_an_inflight_sample() {
        let root = tempfile::tempdir().unwrap();
        let supervisor = Supervisor::new(root.path(), Duration::from_secs(10)).unwrap();
        let paths = ["worker", "optimizer", "jar"].map(|name| root.path().join(name));
        for path in &paths {
            fs::write(path, "original").unwrap();
        }
        let identify = || Package::from_files(&supervisor, paths.clone()).unwrap();
        let mut previous = identify().verified_digest().unwrap();
        for path in &paths {
            let inflight = identify();
            let next = root.path().join("replacement");
            fs::write(&next, "modified").unwrap();
            fs::rename(next, path).unwrap();
            assert!(inflight.verified_digest().is_none());
            let digest = identify().verified_digest().unwrap();
            assert_ne!(previous, digest);
            previous = digest;
        }
    }
}
