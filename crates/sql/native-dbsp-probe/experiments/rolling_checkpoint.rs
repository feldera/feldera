//! Restore the selected generated rolling circuit with native decimal join keys.

use super::{circuit0, circuit0_native, input0, input4, input5, native_decimal};
use dbsp::{
    OrdZSet,
    circuit::{CircuitConfig, CircuitStorageConfig, Mode},
    utils::Tup2,
};
use feldera_sqllib::{append_to_collection_handle, read_output_spine};
use feldera_types::{
    config::{StorageCacheConfig, StorageConfig, StorageOptions},
    size::ConfigSize,
};
use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
};

fn storage(path: &Path) -> CircuitConfig {
    CircuitConfig::with_workers(1)
        .with_mode(Mode::Persistent)
        .with_storage(Some(
            CircuitStorageConfig::for_config(
                StorageConfig {
                    path: path.to_str().unwrap().into(),
                    cache: StorageCacheConfig::default(),
                },
                StorageOptions {
                    min_spill_size: Some(ConfigSize::ZERO),
                    min_step_spill_size: Some(ConfigSize::ZERO),
                    ..StorageOptions::default()
                },
            )
            .unwrap(),
        ))
}

fn retain_directory(source: &Path, destination: &Path) {
    std::fs::create_dir(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            retain_directory(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn selected_rolling_checkpoint_restores_native_decimal_keys() {
    let evidence = std::env::var_os("P0_ROLLING_CHECKPOINT").map(std::path::PathBuf::from);
    if let Some(path) = &evidence {
        std::fs::create_dir(path).unwrap();
    }
    let state = tempfile::tempdir().unwrap();
    let reference_state = tempfile::tempdir().unwrap();
    let (mut classic, (input, data, error)) = circuit0(storage(state.path())).unwrap();
    let (mut uninterrupted, (reference_input, reference_data, reference_error)) =
        circuit0(storage(reference_state.path())).unwrap();

    let (initial,) = input0();
    append_to_collection_handle(&initial, &input);
    append_to_collection_handle(&initial, &reference_input);
    classic.transaction().unwrap();
    uninterrupted.transaction().unwrap();
    let initial_output = OrdZSet::from_keys((), vec![Tup2(Tup2(10, 1), 1), Tup2(Tup2(10, 2), 1)]);
    assert_eq!(read_output_spine(&data), initial_output);
    assert_eq!(read_output_spine(&reference_data), initial_output);
    let empty = OrdZSet::from_keys((), vec![]);
    assert_eq!(read_output_spine(&error), empty);
    assert_eq!(read_output_spine(&reference_error), empty);
    let fingerprint = classic.fingerprint();
    let checkpoint = classic.checkpoint().run().unwrap();
    classic.kill().unwrap();
    if let Some(path) = &evidence {
        retain_directory(state.path(), &path.join("classic"));
    }

    let encoder = native_decimal::encoder();
    let weak = Arc::downgrade(&encoder);
    let mut config = storage(state.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let (mut native, (input, data, error)) = circuit0_native(config).unwrap();
    assert_eq!(native.fingerprint(), fingerprint);

    for (bag,) in [input4(), input5(), input0()] {
        append_to_collection_handle(&bag, &input);
        append_to_collection_handle(&bag, &reference_input);
        native.transaction().unwrap();
        uninterrupted.transaction().unwrap();
        assert_eq!(read_output_spine(&data), read_output_spine(&reference_data));
        assert_eq!(read_output_spine(&error), empty);
        assert_eq!(read_output_spine(&reference_error), empty);
    }
    let checkpoint = native.checkpoint().run().unwrap();
    native.kill().unwrap();
    drop((input, data, error, encoder));
    assert!(weak.upgrade().is_none(), "first native encoder unloaded");
    if let Some(path) = &evidence {
        retain_directory(state.path(), &path.join("native"));
    }
    let writes = native_decimal::WRITES.load(Ordering::Relaxed);
    let reads = native_decimal::READS.load(Ordering::Relaxed);
    let searches = native_decimal::SEARCHES.load(Ordering::Relaxed);
    assert!(writes > 0 && reads > 0 && searches > 0);

    let mut config = storage(state.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let (mut native, (input, data, error)) = circuit0_native(config).unwrap();
    assert_eq!(native.fingerprint(), fingerprint);
    let (retraction,) = input5();
    append_to_collection_handle(&retraction, &input);
    append_to_collection_handle(&retraction, &reference_input);
    native.transaction().unwrap();
    uninterrupted.transaction().unwrap();
    assert_eq!(read_output_spine(&data), read_output_spine(&reference_data));
    assert_eq!(read_output_spine(&error), empty);
    assert_eq!(read_output_spine(&reference_error), empty);
    native.kill().unwrap();
    uninterrupted.kill().unwrap();
    println!(
        "fingerprint={fingerprint:016x} archive_writes={writes} archive_reads={reads} searches={searches}"
    );
}
