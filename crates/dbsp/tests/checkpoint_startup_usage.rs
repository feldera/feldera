#[cfg(unix)]
mod unix {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use dbsp::Circuit;
    use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
    use dbsp::operator::Generator;
    use dbsp::typed_batch::OrdZSet;
    use dbsp::utils::Tup2;
    use dbsp::{DBSPHandle, Runtime};
    use feldera_types::config::{
        FileBackendConfig, StorageBackendConfig, StorageCacheConfig, StorageConfig, StorageOptions,
    };

    fn circuit_with_storage(path: &Path) -> Result<DBSPHandle, dbsp::Error> {
        let config =
            CircuitConfig::with_workers(2).with_storage(Some(CircuitStorageConfig::for_config(
                StorageConfig {
                    path: path.to_string_lossy().into_owned(),
                    cache: StorageCacheConfig::default(),
                },
                StorageOptions {
                    min_storage_bytes: Some(0),
                    backend: StorageBackendConfig::File(Box::new(FileBackendConfig::default())),
                    ..StorageOptions::default()
                },
            )?));

        let (handle, ()) = Runtime::init_circuit(config, |circuit| {
            let source = circuit.add_source(Generator::new(|| {
                let keys: Vec<Tup2<u64, i64>> = (0..256).map(|key| Tup2(key, 1)).collect();
                OrdZSet::from_keys((), keys)
            }));
            source.integrate_trace().apply(|_| ());
            Ok(())
        })?;
        Ok(handle)
    }

    #[test]
    fn runtime_restart_continues_after_unreadable_checkpoint_subdirectory() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let tempdir = tempfile::tempdir().unwrap();
        let storage = tempdir.path().join("storage");
        fs::create_dir(&storage).unwrap();
        let mut handle = circuit_with_storage(&storage).unwrap();
        handle.transaction().unwrap();
        handle.checkpoint().run().unwrap();
        handle.kill().unwrap();

        let checkpoint_dir = fs::read_dir(&storage)
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| uuid::Uuid::parse_str(&entry.file_name().to_string_lossy()).is_ok())
            .expect("checkpoint directory should exist")
            .path();
        let unreadable_dir = checkpoint_dir.join("unreadable");
        fs::create_dir(&unreadable_dir).unwrap();
        let mut permissions = fs::metadata(&unreadable_dir).unwrap().permissions();
        let original_mode = permissions.mode();
        permissions.set_mode(0);
        fs::set_permissions(&unreadable_dir, permissions).unwrap();
        assert!(fs::read_dir(&unreadable_dir).is_err());

        let restart = circuit_with_storage(&storage);
        let mut permissions = fs::metadata(&unreadable_dir).unwrap().permissions();
        permissions.set_mode(original_mode);
        fs::set_permissions(&unreadable_dir, permissions).unwrap();

        let restarted = restart.expect("runtime startup should skip the unreadable subtree");
        println!("DBSP runtime restarted with an unreadable checkpoint subdirectory");
        restarted.kill().unwrap();
    }
}
