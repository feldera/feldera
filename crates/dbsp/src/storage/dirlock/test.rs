use super::LockedDirectory;
use crate::storage::backend::StorageError::{self};
use std::{fs, path::Path, process};

fn cant_relock(path: &Path) {
    let StorageError::StorageLocked(pid, dir) = LockedDirectory::new(path).unwrap_err() else {
        unreachable!();
    };
    assert_eq!(process::id(), pid);
    assert_eq!(dir, path.to_path_buf());
}

#[test]
fn test_pidlock_lifecycle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();
    let pidfile_path = temp_path.join(LockedDirectory::LOCKFILE_NAME);

    for _ in 0..2 {
        // Lock directory.
        let _pidfile = LockedDirectory::new(temp_path).unwrap();
        assert!(pidfile_path.exists());

        // Can't re-lock same directory.
        //
        // Because of `fcntl` weird behavior on file close, we try this twice in
        // case trying to lock the second time actually unlocks it for the third
        // try.
        for _ in 0..2 {
            cant_relock(temp_path);
        }

        // Drop the lock with the end of the block.
    }
}

#[test]
fn test_multiple_locks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();

    let a_path = temp_path.join("a");
    fs::create_dir(&a_path).unwrap();

    let b_path = temp_path.join("b");
    fs::create_dir(&b_path).unwrap();

    let c_path = temp_path.join("c");
    fs::create_dir(&c_path).unwrap();

    let _a = LockedDirectory::new(&a_path).unwrap();
    let _b = LockedDirectory::new(&b_path).unwrap();
    let _c = LockedDirectory::new(&c_path).unwrap();

    drop(_a);
    let _a = LockedDirectory::new(&a_path).unwrap();
    cant_relock(&a_path);
    drop(_a);

    drop(_b);
    let _a = LockedDirectory::new(&a_path).unwrap();

    drop(_c);
    drop(_a);
}
