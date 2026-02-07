/// Generate new golden files by running:
/// `cargo run -p storage-test-compat --bin golden-writer`
///
/// whenever you increment `VERSION_NUMBER`
///
/// This writes both large (Tup65) and small (Tup8) batches, in compressed
/// and uncompressed variants, plus checkpoint payloads under
/// `crates/storage-test-compat/golden-files/`.
use std::path::PathBuf;

use dbsp::circuit::checkpointer::Checkpoint;
use dbsp::dynamic::DynData;
use dbsp::storage::backend::StorageBackend;
use dbsp::storage::file::format::Compression;
use dbsp::storage::file::format::VERSION_NUMBER;
use dbsp::storage::file::writer::{Parameters, Writer1};
use dbsp::storage::file::Factories;
use feldera_types::config::{StorageConfig, StorageOptions};

use storage_test_compat::{
    buffer_cache, golden_row, golden_row_small, storage_base_and_path, GoldenRow, GoldenRowSmall,
    DEFAULT_ROWS,
};

#[derive(Copy, Clone)]
enum GoldenSize {
    Large,
    Small,
}

impl GoldenSize {
    fn suffix(self) -> &'static str {
        match self {
            GoldenSize::Large => "large",
            GoldenSize::Small => "small",
        }
    }
}

struct Config {
    rows: usize,
    compression: Option<Compression>,
    size: GoldenSize,
}

impl Config {
    fn output(&self) -> PathBuf {
        let mut file_name = format!("golden-batch-v{VERSION_NUMBER}");
        match self.compression {
            Some(Compression::Snappy) => {
                file_name += "-snappy";
            }
            None => (),
        }
        file_name += "-";
        file_name += self.size.suffix();
        file_name += ".feldera";

        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("golden-files")
            .join(file_name)
    }

    fn checkpoint_output(&self) -> PathBuf {
        let mut file_name = format!("golden-checkpoint-v{VERSION_NUMBER}");
        file_name += "-";
        file_name += self.size.suffix();
        file_name += ".feldera";

        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("golden-files")
            .join(file_name)
    }
}

impl Default for Config {
    fn default() -> Self {
        let rows = DEFAULT_ROWS;
        let compression = Some(Compression::Snappy);
        let size = GoldenSize::Large;

        Config {
            rows,
            compression,
            size,
        }
    }
}

fn write_golden<T>(
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

    let factories = Factories::<DynData, DynData>::new::<T, ()>();
    let parameters = Parameters::default().with_compression(compression);
    let mut writer = Writer1::new(
        &factories,
        buffer_cache,
        &*storage_backend,
        parameters,
        rows,
    )?;

    for row in 0..rows {
        let key = row_builder(row);
        let aux = ();
        writer.write0((&key, &aux))?;
    }

    let tmp_path = writer.path().clone();
    let (_file_handle, _bloom_filter) = writer.close()?;
    let content = storage_backend.read(&tmp_path)?;
    storage_backend.write(&output_storage_path, (*content).clone())?;
    storage_backend.delete(&tmp_path)?;

    println!("wrote {} rows to {}", rows, output.display());
    Ok(())
}

fn write_checkpoint<T>(
    output: &std::path::Path,
    row_builder: fn(usize) -> T,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: Checkpoint,
{
    if let Some(base_dir) = output.parent() {
        std::fs::create_dir_all(base_dir)?;
    }
    let row = row_builder(0);
    let bytes = row.checkpoint()?;
    std::fs::write(output, bytes)?;
    println!("wrote checkpoint to {}", output.display());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    for size in [GoldenSize::Large, GoldenSize::Small] {
        config.size = size;
        for compression in [None, Some(Compression::Snappy)] {
            config.compression = compression;

            let output = config.output();
            let output = if output.is_absolute() {
                output
            } else {
                std::env::current_dir()?.join(output)
            };

            match config.size {
                GoldenSize::Large => {
                    write_golden::<GoldenRow>(&output, config.rows, config.compression, golden_row)?
                }
                GoldenSize::Small => write_golden::<GoldenRowSmall>(
                    &output,
                    config.rows,
                    config.compression,
                    golden_row_small,
                )?,
            }
        }

        let checkpoint_output = config.checkpoint_output();
        let checkpoint_output = if checkpoint_output.is_absolute() {
            checkpoint_output
        } else {
            std::env::current_dir()?.join(checkpoint_output)
        };
        match config.size {
            GoldenSize::Large => write_checkpoint::<GoldenRow>(&checkpoint_output, golden_row)?,
            GoldenSize::Small => {
                write_checkpoint::<GoldenRowSmall>(&checkpoint_output, golden_row_small)?
            }
        }
    }

    Ok(())
}
