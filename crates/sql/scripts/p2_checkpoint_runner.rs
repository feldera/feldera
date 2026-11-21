//! Runs alongside either compiler's generated grouped COUNT circuit.

use anyhow::{Context, Result, ensure};
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use feldera_types::{
    config::{StorageCacheConfig, StorageConfig, StorageOptions},
    format::json::JsonFlavor,
    size::ConfigSize,
};
use static_connectors::{CircuitCatalog, RecordFormat};
use std::{env, fs, path::Path};

fn step(
    catalog: &static_connectors::Catalog,
    runtime: &mut dbsp::DBSPHandle,
    seed: bool,
    check_watermark: bool,
) -> Result<Vec<(i64, i64)>> {
    let format = RecordFormat::Json(JsonFlavor::Default);
    let input = catalog
        .input_collection_handle(&"t".into())
        .context("missing input T")?;
    let mut writer = input.handle.configure_deserializer(format.clone())?;
    let view = catalog
        .output_handles(&"v".into())
        .context("missing view V")?;
    view.enable_count.enable();
    let row = br#"{"x":7}"#;
    if seed {
        writer.insert(row, &None)?;
        writer.insert(row, &None)?;
    } else {
        if check_watermark {
            // With the restored watermark at five, this input must produce no new group.
            writer.insert(br#"{"x":0}"#, &None)?;
        }
        writer.delete(row, &None)?;
    }
    writer.flush()?;
    runtime.transaction()?;
    let output = view.delta_handle.take_readers();
    let mut cursor = output.whole.cursor(format)?;
    let mut counts = Vec::new();
    while cursor.key_valid() {
        while cursor.val_valid() {
            let mut bytes = Vec::new();
            cursor.serialize_key(&mut bytes)?;
            let row: serde_json::Value = serde_json::from_slice(&bytes)?;
            ensure!(row["x"] == 7, "unexpected group {row}");
            counts.push((row["c"].as_i64().context("missing COUNT")?, cursor.weight()));
            cursor.step_val();
        }
        cursor.step_key()?;
    }
    counts.sort_unstable();
    let expected = if seed {
        vec![(2, 1)]
    } else {
        vec![(1, 1), (2, -1)]
    };
    ensure!(
        counts == expected,
        "COUNT deltas {counts:?}, expected {expected:?}"
    );
    Ok(counts)
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    ensure!(
        args.len() == 3,
        "usage: p2-checkpoint seed|restore|restore-watermark STATE_DIR"
    );
    let seed = match args[1].as_str() {
        "seed" => true,
        "restore" | "restore-watermark" => false,
        other => anyhow::bail!("unknown checkpoint operation {other}"),
    };
    let path = Path::new(&args[2]);
    let checkpoint = path.join("checkpoint-id");
    let mut storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: path.join("dbsp").to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions {
            min_spill_size: Some(ConfigSize::ZERO),
            min_step_spill_size: Some(ConfigSize::ZERO),
            ..StorageOptions::default()
        },
    )?;
    if !seed {
        storage.init_checkpoint = Some(fs::read_to_string(&checkpoint)?.trim().parse()?);
    }
    let config = CircuitConfig::with_workers(1).with_storage(Some(storage));
    let (mut runtime, catalog) = temp::circuit(config)?;
    let bootstrap = runtime.bootstrap_in_progress();
    ensure!(!bootstrap, "checkpoint requires bootstrap");
    let counts = step(&catalog, &mut runtime, seed, args[1] == "restore-watermark")?;
    if seed {
        let snapshot = runtime.checkpoint().run()?;
        fs::write(&checkpoint, snapshot.uuid.to_string())?;
    }
    runtime
        .kill()
        .map_err(|_| anyhow::anyhow!("DBSP worker panicked during shutdown"))?;
    println!(
        "{}",
        serde_json::json!({
            "package": env!("CARGO_PKG_NAME"), "checkpoint_id": fs::read_to_string(checkpoint)?,
            "bootstrap": bootstrap, "count_deltas": counts, "operation": args[1],
        })
    );
    Ok(())
}
