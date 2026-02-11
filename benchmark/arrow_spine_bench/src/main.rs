use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    mpsc, Arc, Barrier, Condvar, Mutex,
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{ArrayBuilder, UInt64Array, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam::channel::{bounded, TryRecvError, TrySendError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const DEFAULT_THREADS: &[usize] = &[1, 2, 4, 8, 12, 16, 20, 24];
const DEFAULT_BATCH_SIZE: usize = 10_000;
const DEFAULT_DURATION_SECS: u64 = 120;
const PRODUCERS_PER_CONSUMER: usize = 8;
const MAX_BUFFERED_RECORDS: usize = 500_000_000;
const DEFAULT_INPUT_SIZE_BYTES: usize = 8;
const RECORD_BYTES: u64 = 8;
const MERGE_INPUT_BATCH_ROWS: usize = 32_768;
const MERGE_OUTPUT_BATCH_ROWS: usize = 32_768;

const MAX_LEVELS: usize = 9;

#[derive(Debug)]
struct WorkerResult {
    records: u64,
    bytes: u64,
    elapsed: Duration,
}

#[derive(Clone)]
enum BatchStorage {
    InMemory(RecordBatch),
    OnDisk { path: PathBuf },
}

#[derive(Clone)]
struct ArrowBatch {
    storage: BatchStorage,
    len: usize,
}

impl ArrowBatch {
    fn from_sorted_pairs(pairs: Vec<(u64, u64)>) -> Result<Self> {
        let (values, weights) = pairs.into_iter().unzip();
        let batch = record_batch_from_vectors(values, weights)?;
        let len = batch.num_rows();
        Ok(Self {
            storage: BatchStorage::InMemory(batch),
            len,
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn reader(&self) -> Result<Box<dyn ArrowBatchReader>> {
        match &self.storage {
            BatchStorage::InMemory(batch) => Ok(Box::new(MemoryBatchReader {
                batch: batch.clone(),
                done: false,
            })),
            BatchStorage::OnDisk { path, .. } => Ok(Box::new(ParquetBatchReader::new(path)?)),
        }
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("weight", DataType::UInt64, false),
    ]))
}

fn record_batch_from_vectors(values: Vec<u64>, weights: Vec<u64>) -> Result<RecordBatch> {
    let values_array = UInt64Array::from(values);
    let weights_array = UInt64Array::from(weights);
    let batch = RecordBatch::try_new(
        schema(),
        vec![Arc::new(values_array), Arc::new(weights_array)],
    )
    .context("build record batch")?;
    Ok(batch)
}

trait ArrowBatchReader: Send {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

struct MemoryBatchReader {
    batch: RecordBatch,
    done: bool,
}

impl ArrowBatchReader for MemoryBatchReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            Ok(Some(self.batch.clone()))
        }
    }
}

struct ParquetBatchReader {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
}

impl ParquetBatchReader {
    fn new(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path).context("open parquet file")?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).context("parquet reader builder")?;
        let reader = builder
            .with_batch_size(MERGE_INPUT_BATCH_ROWS)
            .build()
            .context("build parquet reader")?;
        Ok(Self { reader })
    }
}

impl ArrowBatchReader for ParquetBatchReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.reader.next() {
            Some(batch) => Ok(Some(batch.context("read parquet batch")?)),
            None => Ok(None),
        }
    }
}

struct BatchCursor {
    reader: Box<dyn ArrowBatchReader>,
    batch: Option<RecordBatch>,
    row: usize,
    values: Option<Arc<UInt64Array>>,
    weights: Option<Arc<UInt64Array>>,
}

impl BatchCursor {
    fn new(reader: Box<dyn ArrowBatchReader>) -> Result<Self> {
        let mut cursor = Self {
            reader,
            batch: None,
            row: 0,
            values: None,
            weights: None,
        };
        cursor.load_next_batch()?;
        Ok(cursor)
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        let batch = self.reader.next_batch()?;
        if let Some(batch) = batch {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("value column")?
                .clone();
            let weights = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("weight column")?
                .clone();
            self.row = 0;
            self.values = Some(Arc::new(values));
            self.weights = Some(Arc::new(weights));
            self.batch = Some(batch);
            Ok(true)
        } else {
            self.batch = None;
            self.values = None;
            self.weights = None;
            Ok(false)
        }
    }

    fn current(&self) -> Option<(u64, u64)> {
        let values = self.values.as_ref()?;
        let weights = self.weights.as_ref()?;
        if self.row < values.len() {
            Some((values.value(self.row), weights.value(self.row)))
        } else {
            None
        }
    }

    fn advance(&mut self) -> Result<bool> {
        self.row += 1;
        if let Some(values) = &self.values {
            if self.row < values.len() {
                return Ok(true);
            }
        }
        self.load_next_batch()
    }
}

#[derive(Eq, PartialEq)]
struct HeapItem {
    value: u64,
    weight: u64,
    cursor: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value
            .cmp(&other.value)
            .then(self.cursor.cmp(&other.cursor))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_batches(
    storage_dir: &Path,
    file_id: u64,
    batches: Vec<Arc<ArrowBatch>>,
) -> Result<Arc<ArrowBatch>> {
    assert!(!batches.is_empty());
    let mut cursors = Vec::with_capacity(batches.len());
    for batch in batches {
        let reader = batch.reader()?;
        cursors.push(BatchCursor::new(reader)?);
    }

    let mut heap = BinaryHeap::new();
    for (cursor_index, cursor) in cursors.iter().enumerate() {
        if let Some((value, weight)) = cursor.current() {
            heap.push(std::cmp::Reverse(HeapItem {
                value,
                weight,
                cursor: cursor_index,
            }));
        }
    }

    let path = storage_dir.join(format!("batch_{file_id}.parquet"));
    let file = std::fs::File::create(&path).context("create parquet file")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer =
        ArrowWriter::try_new(file, schema(), Some(props)).context("create parquet writer")?;

    let mut out_values = UInt64Builder::with_capacity(MERGE_OUTPUT_BATCH_ROWS);
    let mut out_weights = UInt64Builder::with_capacity(MERGE_OUTPUT_BATCH_ROWS);
    let mut total_rows: usize = 0;

    while let Some(std::cmp::Reverse(item)) = heap.pop() {
        let current_value = item.value;
        let mut weight_sum = item.weight;

        if cursors[item.cursor].advance()? {
            if let Some((value, weight)) = cursors[item.cursor].current() {
                heap.push(std::cmp::Reverse(HeapItem {
                    value,
                    weight,
                    cursor: item.cursor,
                }));
            }
        }

        while let Some(std::cmp::Reverse(next)) = heap.peek() {
            if next.value != current_value {
                break;
            }
            let next = heap.pop().expect("peeked element").0;
            weight_sum = weight_sum.saturating_add(next.weight);
            if cursors[next.cursor].advance()? {
                if let Some((value, weight)) = cursors[next.cursor].current() {
                    heap.push(std::cmp::Reverse(HeapItem {
                        value,
                        weight,
                        cursor: next.cursor,
                    }));
                }
            }
        }

        out_values.append_value(current_value);
        out_weights.append_value(weight_sum);
        total_rows += 1;

        if out_values.len() >= MERGE_OUTPUT_BATCH_ROWS {
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(out_values.finish()),
                    Arc::new(out_weights.finish()),
                ],
            )
            .context("build output record batch")?;
            writer.write(&batch).context("write parquet batch")?;
            out_values = UInt64Builder::with_capacity(MERGE_OUTPUT_BATCH_ROWS);
            out_weights = UInt64Builder::with_capacity(MERGE_OUTPUT_BATCH_ROWS);
        }
    }

    if out_values.len() > 0 {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(out_values.finish()),
                Arc::new(out_weights.finish()),
            ],
        )
        .context("build final record batch")?;
        writer.write(&batch).context("write final parquet batch")?;
    }
    writer.close().context("close parquet writer")?;

    Ok(Arc::new(ArrowBatch {
        storage: BatchStorage::OnDisk { path },
        len: total_rows,
    }))
}

struct Slot {
    merging_batches: Option<Vec<Arc<ArrowBatch>>>,
    loose_batches: VecDeque<Arc<ArrowBatch>>,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            merging_batches: None,
            loose_batches: VecDeque::new(),
        }
    }
}

impl Slot {
    fn try_start_merge(&mut self, level: usize) -> Option<Vec<Arc<ArrowBatch>>> {
        const MERGE_COUNTS: [std::ops::RangeInclusive<usize>; MAX_LEVELS] = [
            8..=64,
            8..=64,
            3..=64,
            3..=64,
            3..=64,
            3..=64,
            2..=64,
            2..=64,
            2..=64,
        ];

        let merge_counts = &MERGE_COUNTS[level];
        if self.merging_batches.is_none() && self.loose_batches.len() >= *merge_counts.start() {
            let n = std::cmp::min(*merge_counts.end(), self.loose_batches.len());
            let batches = self.loose_batches.drain(..n).collect::<Vec<_>>();
            self.merging_batches = Some(batches.clone());
            Some(batches)
        } else {
            None
        }
    }
}

struct SharedState {
    slots: [Slot; MAX_LEVELS],
    request_exit: bool,
}

impl SharedState {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| Slot::default()),
            request_exit: false,
        }
    }

    fn add_batch(&mut self, batch: Arc<ArrowBatch>) {
        debug_assert!(!batch.is_empty());
        let level = ArrowSpine::size_to_level(batch.len());
        self.slots[level].loose_batches.push_back(batch);
    }

    fn add_batches(&mut self, batches: impl IntoIterator<Item = Arc<ArrowBatch>>) {
        for batch in batches {
            if !batch.is_empty() {
                self.add_batch(batch);
            }
        }
    }

    fn should_apply_backpressure(&self) -> bool {
        const HIGH_THRESHOLD: usize = 128;
        self.slots
            .iter()
            .map(|s| s.loose_batches.len())
            .sum::<usize>()
            >= HIGH_THRESHOLD
    }

    fn should_relieve_backpressure(&self) -> bool {
        const LOWER_THRESHOLD: usize = 127;
        self.slots
            .iter()
            .map(|s| s.loose_batches.len())
            .sum::<usize>()
            <= LOWER_THRESHOLD
    }

    fn try_start_merge_any(&mut self) -> Option<(usize, Vec<Arc<ArrowBatch>>)> {
        for (level, slot) in self.slots.iter_mut().enumerate() {
            if let Some(batches) = slot.try_start_merge(level) {
                return Some((level, batches));
            }
        }
        None
    }

    fn merge_complete(&mut self, level: usize, new_batch: Arc<ArrowBatch>) {
        let slot = &mut self.slots[level];
        let _batches = slot.merging_batches.take().expect("merge state");
        self.add_batches([new_batch]);
    }
}

struct AsyncMerger {
    state: Arc<Mutex<SharedState>>,
    wake: Arc<Condvar>,
    no_backpressure: Arc<Condvar>,
    merged_bytes: Arc<AtomicU64>,
    merged_nanos: Arc<AtomicU64>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl AsyncMerger {
    fn new(storage_dir: PathBuf) -> Self {
        let state = Arc::new(Mutex::new(SharedState::new()));
        let wake = Arc::new(Condvar::new());
        let no_backpressure = Arc::new(Condvar::new());
        let next_file_id = Arc::new(AtomicU64::new(0));
        let merged_bytes = Arc::new(AtomicU64::new(0));
        let merged_nanos = Arc::new(AtomicU64::new(0));

        let thread_state = Arc::clone(&state);
        let thread_wake = Arc::clone(&wake);
        let thread_no_backpressure = Arc::clone(&no_backpressure);
        let thread_dir = storage_dir.clone();
        let thread_file_id = Arc::clone(&next_file_id);
        let thread_merged_bytes = Arc::clone(&merged_bytes);
        let thread_merged_nanos = Arc::clone(&merged_nanos);

        let handle = std::thread::Builder::new()
            .name("arrow-spine-merger".to_string())
            .spawn(move || loop {
                let (level, batches) = {
                    let mut state = thread_state.lock().unwrap();
                    loop {
                        if state.request_exit {
                            return;
                        }
                        if let Some(merge) = state.try_start_merge_any() {
                            break merge;
                        }
                        state = thread_wake.wait(state).unwrap();
                    }
                };

                let start = Instant::now();
                let input_records: u64 = batches.iter().map(|b| b.len() as u64).sum();
                let file_id = thread_file_id.fetch_add(1, AtomicOrdering::Relaxed);
                let merged = match merge_batches(&thread_dir, file_id, batches) {
                    Ok(batch) => batch,
                    Err(error) => {
                        eprintln!("merge error: {error:?}");
                        return;
                    }
                };
                let elapsed = start.elapsed();
                thread_merged_bytes
                    .fetch_add(input_records * RECORD_BYTES, AtomicOrdering::Relaxed);
                thread_merged_nanos.fetch_add(elapsed.as_nanos() as u64, AtomicOrdering::Relaxed);

                let mut state = thread_state.lock().unwrap();
                state.merge_complete(level, merged);
                if state.should_relieve_backpressure() {
                    thread_no_backpressure.notify_all();
                }
                thread_wake.notify_one();
            })
            .expect("spawn merger thread");

        Self {
            state,
            wake,
            no_backpressure,
            merged_bytes,
            merged_nanos,
            handle: Some(handle),
        }
    }

    fn add_batch(&self, batch: Arc<ArrowBatch>) {
        let mut state = self.state.lock().unwrap();
        state.add_batch(batch);
        self.wake.notify_one();
        if state.should_apply_backpressure() {
            let state = self
                .no_backpressure
                .wait_while(state, |s| !s.should_relieve_backpressure())
                .unwrap();
            if state.should_apply_backpressure() {
                self.no_backpressure.notify_all();
            }
        }
    }

    fn shutdown(&mut self) {
        {
            let mut state = self.state.lock().unwrap();
            state.request_exit = true;
        }
        self.wake.notify_all();
        self.no_backpressure.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        let bytes = self.merged_bytes.load(AtomicOrdering::Relaxed);
        let nanos = self.merged_nanos.load(AtomicOrdering::Relaxed);
        if nanos > 0 {
            let mib_per_s = (bytes as f64 / (1024.0 * 1024.0)) / (nanos as f64 / 1_000_000_000.0);
            eprintln!("merge_throughput_mib_per_s={mib_per_s:.2}");
        } else {
            eprintln!("merge_throughput_mib_per_s=0.00");
        }
    }
}

struct ArrowSpine {
    merger: AsyncMerger,
}

impl ArrowSpine {
    fn new(storage_dir: PathBuf) -> Self {
        Self {
            merger: AsyncMerger::new(storage_dir),
        }
    }

    fn insert(&self, batch: ArrowBatch) {
        if batch.is_empty() {
            return;
        }
        self.merger.add_batch(Arc::new(batch));
    }

    fn shutdown(&mut self) {
        self.merger.shutdown();
    }

    fn size_to_level(len: usize) -> usize {
        match len {
            0..=9999 => 0,
            10_000..=99_999 => 1,
            100_000..=999_999 => 2,
            1_000_000..=9_999_999 => 3,
            10_000_000..=99_999_999 => 4,
            100_000_000..=999_999_999 => 5,
            1_000_000_000..=9_999_999_999 => 6,
            10_000_000_000..=99_999_999_999 => 7,
            _ => 8,
        }
    }
}

impl Drop for ArrowSpine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct BatchPayload {
    pairs: Vec<(u64, u64)>,
    len: usize,
}

fn generate_batch_u64(batch_size: usize, rng: &mut u64) -> BatchPayload {
    let mut pairs = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        *rng ^= *rng << 13;
        *rng ^= *rng >> 7;
        *rng ^= *rng << 17;
        pairs.push((*rng, 1u64));
    }
    BatchPayload {
        pairs,
        len: batch_size,
    }
}

fn prepare_storage_path(path: &str) -> String {
    let trimmed = path.trim();
    assert!(!trimmed.is_empty(), "STORAGE_PATH must not be empty");
    let storage_path = Path::new(trimmed);
    assert!(
        storage_path != Path::new("/"),
        "refusing to remove root directory"
    );
    assert!(
        storage_path != Path::new("."),
        "refusing to remove current directory"
    );

    if storage_path.exists() {
        std::fs::remove_dir_all(storage_path).expect("clear storage dir");
    }
    std::fs::create_dir_all(storage_path).expect("create storage dir");
    storage_path.to_string_lossy().into_owned()
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    match env::var(name) {
        Ok(value) => value.parse().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_batch_sizes() -> Vec<usize> {
    parse_csv_env("BATCH_SIZES", &[DEFAULT_BATCH_SIZE])
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

fn run_one_case(
    threads: usize,
    input_size_bytes: usize,
    duration_secs: u64,
    batch_size: usize,
    storage_path: Option<&str>,
) -> String {
    assert!(threads > 0, "threads must be positive");
    assert!(duration_secs > 0, "duration must be positive");
    assert!(batch_size > 0, "batch_size must be positive");

    let barrier = Arc::new(Barrier::new(threads));

    let (storage_path, _tempdir) = match storage_path {
        Some(path) => (prepare_storage_path(path), None),
        None => {
            let tempdir = tempfile::tempdir().expect("tempdir");
            (tempdir.path().to_string_lossy().into_owned(), Some(tempdir))
        }
    };

    let (tx, rx) = mpsc::channel::<WorkerResult>();
    let duration = Duration::from_secs(duration_secs);

    let mut worker_handles = Vec::with_capacity(threads);
    for worker_index in 0..threads {
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        let worker_storage = PathBuf::from(&storage_path).join(format!("worker_{worker_index}"));
        std::fs::create_dir_all(&worker_storage).expect("worker storage dir");

        worker_handles.push(
            std::thread::Builder::new()
                .name(format!("spine-worker-{worker_index}"))
                .spawn(move || {
                    let stop = Arc::new(AtomicBool::new(false));
                    let capacity_batches = std::cmp::max(1, MAX_BUFFERED_RECORDS / batch_size);
                    let (batch_tx, batch_rx) = bounded::<BatchPayload>(capacity_batches);

                    let mut producers = Vec::with_capacity(PRODUCERS_PER_CONSUMER);
                    for producer_idx in 0..PRODUCERS_PER_CONSUMER {
                        let stop = stop.clone();
                        let batch_tx = batch_tx.clone();
                        let log_queue = worker_index == 0 && producer_idx == 0;
                        let log_sender = batch_tx.clone();
                        let mut rng_state = (worker_index as u64)
                            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                            .wrapping_add(producer_idx as u64 + 1);
                        let thread_name = format!("b-producer-{worker_index}-{producer_idx}");
                        producers.push(
                            std::thread::Builder::new()
                                .name(thread_name)
                                .spawn(move || {
                                    let mut next_log = Instant::now() + Duration::from_secs(10);
                                    loop {
                                        if stop.load(AtomicOrdering::Acquire) {
                                            break;
                                        }
                                        if log_queue && Instant::now() >= next_log {
                                            let batches = log_sender.len();
                                            let records = batches * batch_size;
                                            eprintln!("buffered_records={records}");
                                            next_log += Duration::from_secs(10);
                                        }
                                        let payload =
                                            generate_batch_u64(batch_size, &mut rng_state);
                                        let mut payload = payload;
                                        loop {
                                            if stop.load(AtomicOrdering::Acquire) {
                                                return;
                                            }
                                            match batch_tx.try_send(payload) {
                                                Ok(()) => break,
                                                Err(TrySendError::Full(p)) => {
                                                    payload = p;
                                                }
                                                Err(TrySendError::Disconnected(_)) => return,
                                            }
                                        }
                                    }
                                })
                                .expect("spawn b-producer thread"),
                        );
                    }

                    let spine = ArrowSpine::new(worker_storage);

                    while batch_rx.len() < capacity_batches {
                        std::thread::yield_now();
                    }
                    if worker_index == 0 {
                        eprintln!("prefill complete (queue full: {capacity_batches} batches)");
                    }

                    barrier.wait();
                    let start = Instant::now();
                    let end_time = start + duration;
                    let mut records: u64 = 0;
                    let mut bytes: u64 = 0;

                    loop {
                        let now = Instant::now();
                        if now >= end_time {
                            break;
                        }
                        match batch_rx.try_recv() {
                            Ok(mut payload) => {
                                payload.pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                                let batch = ArrowBatch::from_sorted_pairs(payload.pairs)
                                    .expect("arrow batch");
                                spine.insert(batch);
                                records += payload.len as u64;
                                bytes += payload.len as u64 * input_size_bytes as u64;
                            }
                            Err(TryRecvError::Empty) => {
                                eprintln!(
                                    "warning: empty buffer, increase producers or pre-buffering"
                                );
                            }
                            Err(TryRecvError::Disconnected) => break,
                        }
                    }

                    stop.store(true, AtomicOrdering::Release);
                    drop(batch_rx);
                    for producer in producers {
                        let _ = producer.join();
                    }
                    let elapsed = start.elapsed();
                    tx.send(WorkerResult {
                        records,
                        bytes,
                        elapsed,
                    })
                    .expect("send worker result");
                })
                .expect("spawn worker"),
        );
    }

    drop(tx);

    let mut results = Vec::with_capacity(threads);
    for _ in 0..threads {
        results.push(rx.recv().expect("worker result"));
    }

    for handle in worker_handles {
        let _ = handle.join();
    }

    let total_bytes: u64 = results.iter().map(|r| r.bytes).sum();
    let total_records: u64 = results.iter().map(|r| r.records).sum();
    let elapsed = results
        .iter()
        .map(|r| r.elapsed)
        .max()
        .unwrap_or_else(|| Duration::from_secs(0));

    let elapsed_s = elapsed.as_secs_f64();
    let bytes_per_sec = if elapsed_s > 0.0 {
        total_bytes as f64 / elapsed_s
    } else {
        0.0
    };
    let records_per_sec = if elapsed_s > 0.0 {
        total_records as f64 / elapsed_s
    } else {
        0.0
    };

    format!(
        "{threads},{input_size_bytes},{total_records},{batch_size},{total_bytes},{elapsed_s:.6},{bytes_per_sec:.2},{records_per_sec:.2}"
    )
}

fn main() {
    let threads_list = parse_csv_env("THREADS", DEFAULT_THREADS);
    let batch_sizes = parse_batch_sizes();
    let duration_secs = parse_env_u64("DURATION", DEFAULT_DURATION_SECS);
    let input_size_bytes =
        parse_env_u64("INPUT_SIZE_BYTES", DEFAULT_INPUT_SIZE_BYTES as u64) as usize;
    let storage_path = env::var("STORAGE_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let mut csv_lines = Vec::new();
    for &threads in &threads_list {
        for &batch_size in &batch_sizes {
            eprintln!(
                "starting experiment: threads={threads}, batch_size={batch_size}, duration_s={duration_secs}"
            );
            let line = run_one_case(
                threads,
                input_size_bytes,
                duration_secs,
                batch_size,
                storage_path.as_deref(),
            );
            csv_lines.push(line);
        }
    }

    println!(
        "threads,input_size_bytes,records,batch_size,bytes,elapsed_s,bytes_per_sec,records_per_sec"
    );
    for line in csv_lines {
        println!("{line}");
    }
}
