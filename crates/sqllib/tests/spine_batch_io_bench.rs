use std::env;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use dbsp::DBData;
use dbsp::DynZWeight;
use dbsp::Runtime;
use dbsp::ZWeight;
use dbsp::circuit::{
    CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
};
use dbsp::dynamic::{
    DowncastTrait, DynData, DynUnit, DynWeightedPairs, Erase, LeanVec, pair::DynPair,
};
use dbsp::trace::ord::{FallbackWSet, FallbackWSetFactories};
use dbsp::trace::{Batch, BatchFactories, BatchReader, BatchReaderFactories, Batcher, Cursor};
use dbsp::utils::{Tup1, Tup2};

feldera_macros::declare_tuple! { Tup64<
    T1, T2, T3, T4, T5, T6, T7, T8,
    T9, T10, T11, T12, T13, T14, T15, T16,
    T17, T18, T19, T20, T21, T22, T23, T24,
    T25, T26, T27, T28, T29, T30, T31, T32,
    T33, T34, T35, T36, T37, T38, T39, T40,
    T41, T42, T43, T44, T45, T46, T47, T48,
    T49, T50, T51, T52, T53, T54, T55, T56,
    T57, T58, T59, T60, T61, T62, T63, T64
> }

type U64Tuple = Tup64<
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
>;

type DynPairs = Box<DynWeightedPairs<DynPair<DynData, DynUnit>, DynZWeight>>;

const DEFAULT_BATCH_SIZE_BYTES: &[usize] = &[8 * 1024 * 1024];
const TUP1_KEY_BYTES: usize = std::mem::size_of::<u64>();
const TUP64_KEY_BYTES: usize = 64 * std::mem::size_of::<u64>();

const INPUT_TYPE_ALL: &str = "all";
const INPUT_TYPE_TUP1: &str = "tup1";
const INPUT_TYPE_TUP64: &str = "tup64";

const MODE_TUP1: &str = "tup1_u64";
const MODE_TUP64: &str = "tup64_u64";

#[derive(Copy, Clone, Debug)]
enum InputType {
    All,
    Tup1,
    Tup64,
}

#[derive(Debug)]
struct BatchPayload {
    tuples: DynPairs,
    len: usize,
}

#[derive(Debug)]
struct BenchResult {
    mode: &'static str,
    batch_size_bytes: u64,
    records: u64,
    key_bytes: u64,
    logical_bytes: u64,
    disk_bytes: u64,
    write_elapsed: Duration,
    read_elapsed: Duration,
}

impl BenchResult {
    fn to_csv(&self) -> String {
        let write_elapsed_s = self.write_elapsed.as_secs_f64();
        let read_elapsed_s = self.read_elapsed.as_secs_f64();
        let write_logical_bps = bytes_per_sec(self.logical_bytes, self.write_elapsed);
        let write_disk_bps = bytes_per_sec(self.disk_bytes, self.write_elapsed);
        let read_logical_bps = bytes_per_sec(self.logical_bytes, self.read_elapsed);

        format!(
            "{},{},{},{},{},{},{write_elapsed_s:.6},{write_logical_bps:.2},{write_disk_bps:.2},{read_elapsed_s:.6},{read_logical_bps:.2}",
            self.mode,
            self.batch_size_bytes,
            self.records,
            self.key_bytes,
            self.logical_bytes,
            self.disk_bytes,
        )
    }
}

#[test]
#[ignore]
fn spine_batch_io_file_storage() {
    let batch_size_bytes_list = parse_csv_env("BATCH_SIZE_BYTES", DEFAULT_BATCH_SIZE_BYTES);
    let input_type = parse_input_type();
    let storage_path = env::var("STORAGE_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let mut results = Vec::new();

    match input_type {
        InputType::All => {
            for &batch_size_bytes in &batch_size_bytes_list {
                results.push(run_one_case::<Tup1<u64>>(
                    MODE_TUP1,
                    TUP1_KEY_BYTES,
                    batch_size_bytes,
                    storage_path.as_deref(),
                    generate_batch_tup1,
                ));
                results.push(run_one_case::<U64Tuple>(
                    MODE_TUP64,
                    TUP64_KEY_BYTES,
                    batch_size_bytes,
                    storage_path.as_deref(),
                    generate_batch_tup64,
                ));
            }
        }
        InputType::Tup1 => {
            for &batch_size_bytes in &batch_size_bytes_list {
                results.push(run_one_case::<Tup1<u64>>(
                    MODE_TUP1,
                    TUP1_KEY_BYTES,
                    batch_size_bytes,
                    storage_path.as_deref(),
                    generate_batch_tup1,
                ));
            }
        }
        InputType::Tup64 => {
            for &batch_size_bytes in &batch_size_bytes_list {
                results.push(run_one_case::<U64Tuple>(
                    MODE_TUP64,
                    TUP64_KEY_BYTES,
                    batch_size_bytes,
                    storage_path.as_deref(),
                    generate_batch_tup64,
                ));
            }
        }
    }

    println!(
        "mode,batch_size_bytes,records,key_bytes,logical_bytes,disk_bytes,write_elapsed_s,write_logical_bytes_per_sec,write_disk_bytes_per_sec,read_elapsed_s,read_logical_bytes_per_sec"
    );
    for result in results {
        println!("{}", result.to_csv());
    }
}

fn run_one_case<K>(
    mode: &'static str,
    key_bytes: usize,
    batch_size_bytes: usize,
    storage_path: Option<&str>,
    generate_batch: fn(usize) -> BatchPayload,
) -> BenchResult
where
    K: DBData + Erase<DynData>,
{
    assert!(
        batch_size_bytes > 0,
        "BATCH_SIZE_BYTES entries must be positive"
    );
    let records = batch_size_bytes / key_bytes;
    let remainder = batch_size_bytes % key_bytes;
    assert!(
        records > 0,
        "BATCH_SIZE_BYTES value {batch_size_bytes} is too small for mode '{mode}' with key size {key_bytes}"
    );

    eprintln!(
        "starting experiment: mode={mode}, batch_size_bytes={batch_size_bytes}, records={records}, key_bytes={key_bytes}"
    );
    if remainder != 0 {
        eprintln!(
            "note: mode={mode} drops {remainder} trailing bytes because batch_size_bytes is not divisible by key_bytes"
        );
    }

    let (case_storage_path, _tempdir) = prepare_case_storage(storage_path, mode);

    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: case_storage_path.clone(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions {
            min_storage_bytes: Some(0),
            min_step_storage_bytes: Some(0),
            ..StorageOptions::default()
        },
    )
    .expect("storage config");

    let config = CircuitConfig::with_workers(1).with_storage(storage);

    let (tx, rx) = mpsc::channel::<BenchResult>();
    let case_storage_dir = PathBuf::from(&case_storage_path);

    let runtime = Runtime::run(config, move |_parker| {
        let factories = FallbackWSetFactories::<DynData, DynZWeight>::new::<K, (), ZWeight>();
        let mut payload = generate_batch(records);
        assert_eq!(payload.len, records, "unexpected payload length");

        let mut pairs = factories.weighted_items_factory().default_box();
        let logical_bytes = payload.len as u64 * key_bytes as u64;

        let disk_before_write = dir_size_bytes(&case_storage_dir).expect("storage directory size");

        let write_start = Instant::now();
        let mut batcher = <FallbackWSet<_, _> as Batch>::Batcher::new_batcher(&factories, ());
        pairs.from_pairs(payload.tuples.as_mut());
        batcher.push_batch(&mut pairs);
        let mut batch = batcher.seal();
        if let Some(persisted) = batch.persisted() {
            batch = persisted;
        }
        let write_elapsed = write_start.elapsed();

        let disk_after_write = dir_size_bytes(&case_storage_dir).expect("storage directory size");
        let disk_bytes = disk_after_write.saturating_sub(disk_before_write);

        let read_start = Instant::now();
        let mut cursor = batch.cursor();
        let mut read_records = 0u64;
        let mut total_weight = 0i64;

        while cursor.key_valid() {
            while cursor.val_valid() {
                cursor.map_times(&mut |_time, weight| {
                    let weight = unsafe { *weight.downcast::<ZWeight>() };
                    assert_eq!(weight, 1, "expected weight 1 for every row");
                    read_records += 1;
                    total_weight += weight;
                });
                cursor.step_val();
            }
            cursor.step_key();
        }

        let read_elapsed = read_start.elapsed();
        assert_eq!(read_records as usize, records, "row count mismatch");
        assert_eq!(total_weight, records as i64, "total weight mismatch");

        tx.send(BenchResult {
            mode,
            batch_size_bytes: batch_size_bytes as u64,
            records: records as u64,
            key_bytes: key_bytes as u64,
            logical_bytes,
            disk_bytes,
            write_elapsed,
            read_elapsed,
        })
        .expect("send benchmark result");
    })
    .expect("runtime");

    let result = rx.recv().expect("benchmark result");
    runtime.join().expect("runtime join");

    result
}

fn prepare_case_storage(
    storage_path: Option<&str>,
    mode: &str,
) -> (String, Option<tempfile::TempDir>) {
    match storage_path {
        Some(path) => {
            let trimmed = path.trim();
            assert!(!trimmed.is_empty(), "STORAGE_PATH must not be empty");
            let case_path = Path::new(trimmed).join(mode);
            prepare_storage_path(&case_path);
            (case_path.to_string_lossy().into_owned(), None)
        }
        None => {
            let tempdir = tempfile::tempdir().expect("tempdir");
            let case_path = tempdir.path().join(mode);
            prepare_storage_path(&case_path);
            (case_path.to_string_lossy().into_owned(), Some(tempdir))
        }
    }
}

fn prepare_storage_path(path: &Path) {
    assert!(path != Path::new("/"), "refusing to remove root directory");
    assert!(
        path != Path::new("."),
        "refusing to remove current directory"
    );

    if path.exists() {
        std::fs::remove_dir_all(path).expect("clear storage dir");
    }
    std::fs::create_dir_all(path).expect("create storage dir");
}

fn dir_size_bytes(path: &Path) -> std::io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total += metadata.len();
        } else if metadata.is_dir() {
            total += dir_size_bytes(&entry.path())?;
        }
    }

    Ok(total)
}

fn generate_batch_tup1(records: usize) -> BatchPayload {
    let mut tuples = Vec::with_capacity(records);
    for row in 0..records {
        let key = Tup1(row as u64);
        tuples.push(Tup2(Tup2(key, ()), 1));
    }

    BatchPayload {
        tuples: Box::new(LeanVec::from(tuples)).erase_box(),
        len: records,
    }
}

fn generate_batch_tup64(records: usize) -> BatchPayload {
    let mut tuples = Vec::with_capacity(records);
    for row in 0..records {
        let mut state = (row as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut cols = [0u64; 64];
        cols[0] = row as u64;
        for col in &mut cols[1..] {
            *col = next_random_u64(&mut state);
        }

        let key = Tup64(
            cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8],
            cols[9], cols[10], cols[11], cols[12], cols[13], cols[14], cols[15], cols[16],
            cols[17], cols[18], cols[19], cols[20], cols[21], cols[22], cols[23], cols[24],
            cols[25], cols[26], cols[27], cols[28], cols[29], cols[30], cols[31], cols[32],
            cols[33], cols[34], cols[35], cols[36], cols[37], cols[38], cols[39], cols[40],
            cols[41], cols[42], cols[43], cols[44], cols[45], cols[46], cols[47], cols[48],
            cols[49], cols[50], cols[51], cols[52], cols[53], cols[54], cols[55], cols[56],
            cols[57], cols[58], cols[59], cols[60], cols[61], cols[62], cols[63],
        );

        tuples.push(Tup2(Tup2(key, ()), 1));
    }

    BatchPayload {
        tuples: Box::new(LeanVec::from(tuples)).erase_box(),
        len: records,
    }
}

fn next_random_u64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn parse_input_type() -> InputType {
    match env::var("INPUT_TYPE") {
        Err(_) => InputType::All,
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            match value.as_str() {
                "all" => InputType::All,
                "u64" | "small" | "tup1" => InputType::Tup1,
                "u64x64" | "wide" | "tup64" => InputType::Tup64,
                _ => panic!(
                    "Invalid INPUT_TYPE '{value}', expected '{INPUT_TYPE_ALL}', '{INPUT_TYPE_TUP1}', or '{INPUT_TYPE_TUP64}'"
                ),
            }
        }
    }
}

fn parse_csv_env(name: &str, defaults: &[usize]) -> Vec<usize> {
    let Ok(value) = env::var(name) else {
        return defaults.to_vec();
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return defaults.to_vec();
    }
    trimmed
        .split(',')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| {
            let parsed = item
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("Invalid {name} entry: {item}"));
            if parsed == 0 {
                panic!("{name} entries must be positive: {item}");
            }
            parsed
        })
        .collect()
}

fn bytes_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    let elapsed_s = elapsed.as_secs_f64();
    if elapsed_s > 0.0 {
        bytes as f64 / elapsed_s
    } else {
        0.0
    }
}
