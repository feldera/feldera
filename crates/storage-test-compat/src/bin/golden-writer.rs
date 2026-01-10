/// Generate new golden files by running:
/// `cargo run -p feldera-sqllib-test --bin golden-writer`
///
/// whenever you increment `VERSION_NUMBER`
use std::path::PathBuf;

use dbsp::dynamic::DynData;
use dbsp::storage::backend::StorageBackend;
use dbsp::storage::file::Factories;
use dbsp::storage::file::format::Compression;
use dbsp::storage::file::format::VERSION_NUMBER;
use dbsp::storage::file::writer::{Parameters, Writer1};
use feldera_types::config::{StorageConfig, StorageOptions};

use storage_test_compat::{
    DEFAULT_ROWS, GoldenRow, buffer_cache, golden_row, storage_base_and_path,
};

struct Config {
    rows: usize,
    compression: Option<Compression>,
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

        Config { rows, compression }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    for compression in [None, Some(Compression::Snappy)] {
        config.compression = compression;

        let output = if config.output().is_absolute() {
            config.output()
        } else {
            std::env::current_dir()?.join(config.output())
        };
        let (base_dir, output_storage_path) = storage_base_and_path(&output);
        std::fs::create_dir_all(&base_dir)?;

        let storage_backend = <dyn StorageBackend>::new(
            &StorageConfig {
                path: base_dir.to_string_lossy().to_string(),
                cache: Default::default(),
            },
            &StorageOptions::default(),
        )?;

        let factories = Factories::<DynData, DynData>::new::<GoldenRow, ()>();
        let parameters = Parameters::default().with_compression(config.compression);
        let mut writer = Writer1::new(
            &factories,
            buffer_cache,
            &*storage_backend,
            parameters,
            config.rows,
        )?;

        for row in 0..config.rows {
            let key = golden_row(row);
            let aux = ();
            writer.write0((&key, &aux))?;
        }

        let tmp_path = writer.path().clone();
        let (_file_handle, _bloom_filter) = writer.close()?;
        let content = storage_backend.read(&tmp_path)?;
        storage_backend.write(&output_storage_path, (*content).clone())?;
        storage_backend.delete(&tmp_path)?;

        println!("wrote {} rows to {}", config.rows, output.display());
    }

    Ok(())
}
