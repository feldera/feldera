/// Generate new golden files by running:
/// `cargo run -p storage-test-compat --bin golden-writer`
///
/// whenever you increment `VERSION_NUMBER`
///
/// This writes both large (Tup65) and small (Tup10) batches, in compressed
/// and uncompressed variants, under `crates/storage-test-compat/golden-files/`.
use dbsp::dynamic::DynData;
use dbsp::storage::backend::StorageBackend;
use dbsp::storage::file::format::BatchMetadata;
use dbsp::storage::file::format::Compression;
use dbsp::storage::file::writer::{Parameters, Writer1, Writer2};
use dbsp::storage::file::BatchKeyFilter;
use dbsp::storage::file::{Factories, FilterPlan};
use dbsp::{dynamic::Erase, DBData};
use feldera_types::config::{StorageConfig, StorageOptions};

use storage_test_compat::{
    buffer_cache, golden_aux, golden_file_specs, golden_row, golden_row_small, golden_writer2_key,
    storage_base_and_path, GoldenFileSpec, GoldenRow, GoldenRowSmall, GoldenSize, GoldenWriterKind,
    DEFAULT_ROWS,
};

struct Config {
    rows: usize,
    spec: GoldenFileSpec,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            rows: DEFAULT_ROWS,
            spec: GoldenFileSpec {
                writer: GoldenWriterKind::Writer1Bloom,
                compression: Some(Compression::Snappy),
                size: GoldenSize::Large,
            },
        }
    }
}

fn write_writer1_golden<T>(
    output: &std::path::Path,
    rows: usize,
    compression: Option<Compression>,
    row_builder: fn(usize) -> T,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: dbsp::DBData,
{
    let (base_dir, output_storage_path) = storage_base_and_path(output);
    std::fs::create_dir_all(&base_dir)?;

    let storage_backend = <dyn StorageBackend>::new(
        &StorageConfig {
            path: base_dir.to_string_lossy().to_string(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )?;

    let factories = Factories::<DynData, DynData>::new::<T, i64>();
    let parameters = Parameters::default().with_compression(compression);
    let mut writer = Writer1::new(
        &factories,
        buffer_cache,
        &*storage_backend,
        parameters,
        FilterPlan::<DynData>::decide_filter(None, rows),
    )?;

    for row in 0..rows {
        let key = row_builder(row);
        let aux = golden_aux(row);
        writer.write0((&key, &aux))?;
    }

    let tmp_path = writer.path().clone();
    let (_file_handle, key_filter, _key_bounds) = writer.close(BatchMetadata::default())?;
    assert!(
        matches!(key_filter, Some(BatchKeyFilter::Bloom(_))),
        "writer1 golden files must persist a Bloom filter",
    );
    let content = storage_backend.read(&tmp_path)?;
    storage_backend.write(&output_storage_path, (*content).clone())?;
    storage_backend.delete(&tmp_path)?;

    println!("wrote {} rows to {}", rows, output.display());
    Ok(())
}

fn filter_plan_from_keys<K>(keys: &[K]) -> FilterPlan<DynData>
where
    K: DBData + Erase<DynData>,
{
    FilterPlan::from_bounds(keys.first().unwrap().erase(), keys.last().unwrap().erase())
}

fn write_writer2_golden<T>(
    output: &std::path::Path,
    rows: usize,
    compression: Option<Compression>,
    row_builder: fn(usize) -> T,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: dbsp::DBData,
{
    let (base_dir, output_storage_path) = storage_base_and_path(output);
    std::fs::create_dir_all(&base_dir)?;

    let storage_backend = <dyn StorageBackend>::new(
        &StorageConfig {
            path: base_dir.to_string_lossy().to_string(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )?;

    let factories0 = Factories::<DynData, DynData>::new::<i64, ()>();
    let factories1 = Factories::<DynData, DynData>::new::<T, i64>();
    let parameters = Parameters::default().with_compression(compression);
    let keys: Vec<i64> = (0..rows).map(golden_writer2_key).collect();
    let filter_plan = filter_plan_from_keys(&keys);
    let mut writer = Writer2::new(
        &factories0,
        &factories1,
        buffer_cache,
        &*storage_backend,
        parameters,
        FilterPlan::decide_filter(Some(&filter_plan), rows),
    )?;

    for row in 0..rows {
        let key0 = golden_writer2_key(row);
        let key1 = row_builder(row);
        let aux1 = golden_aux(row);
        writer.write1((&key1, &aux1))?;
        writer.write0((&key0, &()))?;
    }

    let tmp_path = writer.path().clone();
    let (_file_handle, key_filter, _key_bounds) = writer.close(BatchMetadata::default())?;
    assert!(
        matches!(key_filter, Some(BatchKeyFilter::RoaringU32(_))),
        "writer2 golden files must persist a roaring filter",
    );
    let content = storage_backend.read(&tmp_path)?;
    storage_backend.write(&output_storage_path, (*content).clone())?;
    storage_backend.delete(&tmp_path)?;

    println!("wrote {} rows to {}", rows, output.display());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    for spec in golden_file_specs() {
        config.spec = spec;
        let output = if config.spec.path().is_absolute() {
            config.spec.path()
        } else {
            std::env::current_dir()?.join(config.spec.path())
        };

        match (config.spec.writer, config.spec.size) {
            (GoldenWriterKind::Writer1Bloom, GoldenSize::Large) => {
                write_writer1_golden::<GoldenRow>(
                    &output,
                    config.rows,
                    config.spec.compression,
                    golden_row,
                )?
            }
            (GoldenWriterKind::Writer1Bloom, GoldenSize::Small) => {
                write_writer1_golden::<GoldenRowSmall>(
                    &output,
                    config.rows,
                    config.spec.compression,
                    golden_row_small,
                )?
            }
            (GoldenWriterKind::Writer2Roaring, GoldenSize::Large) => {
                write_writer2_golden::<GoldenRow>(
                    &output,
                    config.rows,
                    config.spec.compression,
                    golden_row,
                )?
            }
            (GoldenWriterKind::Writer2Roaring, GoldenSize::Small) => {
                write_writer2_golden::<GoldenRowSmall>(
                    &output,
                    config.rows,
                    config.spec.compression,
                    golden_row_small,
                )?
            }
        }
    }

    Ok(())
}
