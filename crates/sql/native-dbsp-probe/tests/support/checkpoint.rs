//! Shared typed-input join harness for bounded native-state archive adapters.
use dbsp::{
    DBData, DBSPHandle, OrdZSet, OutputHandle, Runtime, ZSetHandle,
    circuit::{CircuitConfig, CircuitStorageConfig},
    utils::Tup2,
};
use feldera_types::{
    config::{StorageCacheConfig, StorageConfig, StorageOptions},
    size::ConfigSize,
};
use std::path::Path;
pub trait Key<R: DBData>: DBData + From<R> {
    fn reference(&self) -> R;
}
impl<R: DBData> Key<R> for R {
    fn reference(&self) -> R {
        self.clone()
    }
}
pub struct Circuit<R: DBData> {
    pub runtime: DBSPHandle,
    left: ZSetHandle<R>,
    right: ZSetHandle<R>,
    output: OutputHandle<OrdZSet<R>>,
}
impl<R: DBData> Circuit<R> {
    pub fn new<K: Key<R>>(config: CircuitConfig) -> Self {
        let (runtime, (left, right, output)) = Runtime::init_circuit(config, |circuit| {
            let (left, l) = circuit.add_input_zset::<R>();
            let (right, r) = circuit.add_input_zset::<R>();
            let left = left.map_index(|key| (K::from(key.clone()), ()));
            let right = right.map_index(|key| (K::from(key.clone()), ()));
            let output = left.join(&right, |key, _, _| key.reference()).output();
            Ok((l, r, output))
        })
        .unwrap();
        Self {
            runtime,
            left,
            right,
            output,
        }
    }
    pub fn step(
        &mut self,
        mut left: Vec<Tup2<R, i64>>,
        mut right: Vec<Tup2<R, i64>>,
    ) -> OrdZSet<R> {
        self.left.append(&mut left);
        self.right.append(&mut right);
        self.runtime.transaction().unwrap();
        self.output.consolidate()
    }
    pub fn stop(self) {
        self.runtime.kill().unwrap();
    }
}
pub fn storage(path: &Path) -> CircuitConfig {
    CircuitConfig::with_workers(1).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.to_string_lossy().into_owned(),
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
pub fn retain_directory(source: &Path, destination: &Path) {
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
