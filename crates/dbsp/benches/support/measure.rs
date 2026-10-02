//! What the benchmarks that exercise a spine's negative weights share:
//! payloads of about two hundred bytes, made on threads of their own, and what
//! each phase of a run reports -- wall time, the CPU time of the circuit's
//! workers and of everything else, peak memory, storage, and how many
//! batches, records and negative weights each spine holds at each level --
//! and the circuit profiles those counts come from, kept as raw data.

use dbsp::{
    circuit::{
        CircuitConfig, CircuitStorageConfig, DBSPHandle, StorageCacheConfig, StorageConfig,
        StorageOptions,
    },
    profile::DbspProfile,
    utils::Tup4,
};
use feldera_types::config::DevTweaks;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// Levels a spine has.
pub const LEVELS: usize = 9;

/// How often [`settle`] asks the circuit whether any spine is merging.
const SETTLE_POLL: Duration = Duration::from_millis(500);

/// How many polls in a row have to find no spine merging before [`settle`]
/// believes the mergers are done.
const SETTLE_IDLE_POLLS: usize = 3;

/// Mixes the bits of `x`, so that neighbouring inputs give unrelated outputs.
/// This is SplitMix64's finalizer.
pub fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A record's payload: an identifier, a version, some text and a number,
/// about two hundred bytes in all.
pub type Payload = Tup4<i64, i64, String, i64>;

/// Bytes of text in a payload, roughly; the three numbers make up the rest of
/// its two hundred.
const TEXT_BYTES: usize = 170;

/// The words a payload's text is made of, so that it compresses about as well
/// as text does.
const WORDS: [&str; 64] = [
    "account",
    "balance",
    "customer",
    "delivery",
    "invoice",
    "order",
    "payment",
    "product",
    "quantity",
    "region",
    "shipment",
    "supplier",
    "warehouse",
    "priority",
    "status",
    "discount",
    "comment",
    "address",
    "street",
    "city",
    "country",
    "phone",
    "market",
    "segment",
    "nation",
    "container",
    "brand",
    "maker",
    "category",
    "return",
    "flag",
    "line",
    "item",
    "part",
    "size",
    "type",
    "clerk",
    "date",
    "receipt",
    "instruct",
    "mode",
    "total",
    "price",
    "tax",
    "extended",
    "commit",
    "ship",
    "open",
    "closed",
    "pending",
    "furiously",
    "quickly",
    "carefully",
    "blithely",
    "slyly",
    "regular",
    "special",
    "express",
    "final",
    "ironic",
    "bold",
    "even",
    "unusual",
    "silent",
];

/// The payload of record `id` at `version`.
///
/// # Arguments
///
/// * `id` - which record the payload belongs to.
/// * `version` - which of the record's values it is; an update moves a
///   record to its next version.
///
/// # Returns
///
/// A payload that no other `(id, version)` has.
pub fn payload(id: u64, version: u64) -> Payload {
    let mut state = mix(id ^ version.rotate_left(48));
    let mut text = String::with_capacity(TEXT_BYTES + 16);
    while text.len() < TEXT_BYTES {
        state = mix(state);
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(WORDS[(state % WORDS.len() as u64) as usize]);
    }
    Tup4(id as i64, version as i64, text, (state >> 16) as i64)
}

/// CPU time of record generators' threads that have exited, in nanoseconds,
/// which their names can no longer be found by.
static RETIRED_DATAGEN_NANOS: AtomicU64 = AtomicU64::new(0);

/// Adds the calling thread's CPU time to [`RETIRED_DATAGEN_NANOS`], as a
/// record generator's thread does as it exits.
fn retire_datagen_thread() {
    #[cfg(target_os = "linux")]
    // SAFETY: `getrusage` only writes the struct it is given.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_THREAD, &mut usage) == 0 {
            let nanos =
                |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
            RETIRED_DATAGEN_NANOS.fetch_add(
                nanos(usage.ru_utime) + nanos(usage.ru_stime),
                Ordering::Relaxed,
            );
        }
    }
}

/// Chunks of records made on threads of their own and handed out in order,
/// a few ahead of whoever consumes them, as a pipeline's input connectors
/// would make them.
pub struct Chunks<T> {
    receivers: Vec<mpsc::Receiver<T>>,
    threads: Vec<thread::JoinHandle<()>>,
    next: usize,
    count: usize,
}

impl<T: Send + 'static> Chunks<T> {
    /// Starts making `count` chunks on `threads` threads.
    ///
    /// # Arguments
    ///
    /// * `threads` - how many threads make chunks; each makes every
    ///   `threads`th one.
    /// * `count` - how many chunks there are.
    /// * `make` - makes the `index`th chunk.
    ///
    /// # Returns
    ///
    /// An iterator over the chunks, in order.
    pub fn new<F>(threads: usize, count: usize, make: F) -> Self
    where
        F: Fn(usize) -> T + Send + Sync + 'static,
    {
        let threads = threads.max(1);
        let make = Arc::new(make);
        let mut receivers = Vec::with_capacity(threads);
        let mut handles = Vec::with_capacity(threads);
        for first in 0..threads {
            let (sender, receiver) = mpsc::sync_channel(1);
            let make = make.clone();
            handles.push(
                thread::Builder::new()
                    .name(format!("datagen-{first}"))
                    .spawn(move || {
                        let mut index = first;
                        while index < count && sender.send(make(index)).is_ok() {
                            index += threads;
                        }
                        retire_datagen_thread();
                    })
                    .expect("failed to start a record generator"),
            );
            receivers.push(receiver);
        }
        Self {
            receivers,
            threads: handles,
            next: 0,
            count,
        }
    }
}

impl<T> Iterator for Chunks<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.next == self.count {
            // Joined before the caller can measure anything, so that a
            // generator's CPU time is counted once, as retired.
            for handle in self.threads.drain(..) {
                handle.join().expect("a record generator panicked");
            }
            return None;
        }
        let chunk = self.receivers[self.next % self.receivers.len()]
            .recv()
            .expect("a record generator stopped early");
        self.next += 1;
        Some(chunk)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.count - self.next;
        (left, Some(left))
    }
}

impl<T> ExactSizeIterator for Chunks<T> {}

/// CPU time the process has used, in seconds, split by who used it.
#[derive(Clone, Copy, Debug, Default)]
pub struct Cpu {
    /// Every thread of the process, including those that have exited.
    pub total: f64,
    /// The circuit's worker threads.  `None` where the system does not say
    /// how much each thread has used.
    pub workers: Option<f64>,
    /// The spine mergers' threads.
    pub mergers: Option<f64>,
    /// The threads that make records, which a pipeline would spend in its
    /// input connectors instead.
    pub datagen: Option<f64>,
}

impl Cpu {
    /// How much of each kind of CPU time was used between `earlier` and `self`.
    pub fn since(self, earlier: Self) -> Self {
        let delta = |now: Option<f64>, then: Option<f64>| Some(now? - then?);
        Self {
            total: self.total - earlier.total,
            workers: delta(self.workers, earlier.workers),
            mergers: delta(self.mergers, earlier.mergers),
            datagen: delta(self.datagen, earlier.datagen),
        }
    }

    /// CPU time used outside the workers, not counting the record
    /// generators, which are the benchmark's and not the circuit's.
    pub fn background(self) -> Option<f64> {
        Some(self.total - self.workers? - self.datagen.unwrap_or(0.0))
    }

    /// The sum of two spans of CPU time.
    fn plus(self, other: Self) -> Self {
        let add = |a: Option<f64>, b: Option<f64>| Some(a? + b?);
        Self {
            total: self.total + other.total,
            workers: add(self.workers, other.workers),
            mergers: add(self.mergers, other.mergers),
            datagen: add(self.datagen, other.datagen),
        }
    }
}

/// The CPU time the process has used so far.
pub fn cpu_now() -> Cpu {
    // SAFETY: `getrusage` only writes the struct it is given.
    let total = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        seconds(usage.ru_utime) + seconds(usage.ru_stime)
    };
    let retired = RETIRED_DATAGEN_NANOS.load(Ordering::Relaxed) as f64 / 1e9;
    let (workers, mergers, datagen) = match thread_cpu_by_name() {
        Some(threads) => {
            let sum = |prefix: &str| {
                threads
                    .iter()
                    .filter(|(name, _)| name.starts_with(prefix))
                    .map(|(_, seconds)| seconds)
                    .sum::<f64>()
            };
            (
                Some(sum("dbsp-worker-")),
                Some(sum("merger-")),
                Some(sum("datagen-") + retired),
            )
        }
        None => (None, None, None),
    };
    Cpu {
        total,
        workers,
        mergers,
        datagen,
    }
}

/// The name and CPU time of every live thread of this process.
///
/// # Returns
///
/// One entry per thread, or `None` where the system does not say.
#[cfg(target_os = "linux")]
fn thread_cpu_by_name() -> Option<Vec<(String, f64)>> {
    // SAFETY: `sysconf` reads a system constant.
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let mut threads = Vec::new();
    for entry in std::fs::read_dir("/proc/self/task").ok()?.flatten() {
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        // The name sits between the first '(' and the last ')', and may
        // itself hold either.
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let name = stat[open + 1..close].to_string();
        let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
        // After the name come the state and ten more fields, then user and
        // system time in clock ticks.
        let (Some(user), Some(system)) = (fields.get(11), fields.get(12)) else {
            continue;
        };
        let ticks = user.parse::<f64>().unwrap_or(0.0) + system.parse::<f64>().unwrap_or(0.0);
        threads.push((name, ticks / ticks_per_second));
    }
    Some(threads)
}

#[cfg(not(target_os = "linux"))]
fn thread_cpu_by_name() -> Option<Vec<(String, f64)>> {
    None
}

/// How many bytes of memory the process holds, where the system will say.
#[cfg(target_os = "linux")]
pub fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: `sysconf` reads a system constant.
    Some(pages * unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64)
}

#[cfg(not(target_os = "linux"))]
pub fn rss_bytes() -> Option<u64> {
    None
}

/// Whether `dir` is on a file system held in memory, which would make every
/// read of the storage a read of memory.
#[cfg(target_os = "linux")]
pub fn on_tmpfs(dir: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    const TMPFS_MAGIC: i64 = 0x0102_1994;
    let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `statfs` only writes the struct it is given.
    unsafe {
        let mut fs: libc::statfs = std::mem::zeroed();
        libc::statfs(path.as_ptr(), &mut fs) == 0 && fs.f_type as i64 == TMPFS_MAGIC
    }
}

#[cfg(not(target_os = "linux"))]
pub fn on_tmpfs(_dir: &Path) -> bool {
    false
}

/// How many bytes `dir` holds, counting what is under it.
pub fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_bytes(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}

pub fn gib(bytes: u64) -> f64 {
    bytes as f64 / (1u64 << 30) as f64
}

/// A circuit configuration with storage under `dir`.
///
/// # Arguments
///
/// * `workers` - how many worker threads the circuit runs on.
/// * `dir` - where the storage goes.
/// * `max_rss_gib` - the memory the circuit should keep within.
/// * `dev_tweaks` - development settings as a JSON object, or `None` for the
///   defaults.  A setting this version does not know is kept and ignored, so
///   one run can name a setting another version lacks.
pub fn circuit_config(
    workers: usize,
    dir: &Path,
    max_rss_gib: u64,
    dev_tweaks: Option<&str>,
) -> CircuitConfig {
    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: dir.to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions::default(),
    )
    .expect("failed to configure storage");
    let mut config = CircuitConfig::with_workers(workers)
        .with_storage(Some(storage))
        .with_max_rss_bytes(Some(max_rss_gib << 30));
    if let Some(json) = dev_tweaks {
        config.dev_tweaks = serde_json::from_str::<DevTweaks>(json)
            .unwrap_or_else(|error| panic!("--dev-tweaks {json}: {error}"));
    }
    config
}

/// The most memory and storage a phase used, and the storage it left behind.
#[derive(Clone, Copy, Debug, Default)]
pub struct Footprint {
    pub peak_rss: u64,
    pub peak_storage: u64,
    pub end_storage: u64,
}

/// When a phase began, so that what it cost can be measured when it ends.
#[derive(Clone, Copy, Debug)]
pub struct Mark {
    at: Instant,
    cpu: Cpu,
}

impl Mark {
    pub fn now() -> Self {
        Self {
            at: Instant::now(),
            cpu: cpu_now(),
        }
    }
}

/// What one phase of a run cost.
#[derive(Clone, Debug)]
pub struct PhaseCost {
    pub name: String,
    pub wall: f64,
    pub cpu: Cpu,
    pub footprint: Footprint,
}

impl PhaseCost {
    /// The cost of `phases` taken together: their times added up, the
    /// highest of their peaks, and the storage the last one left.
    pub fn combined(name: &str, phases: &[PhaseCost]) -> Self {
        let mut cpu = phases[0].cpu;
        for phase in &phases[1..] {
            cpu = cpu.plus(phase.cpu);
        }
        Self {
            name: name.to_string(),
            wall: phases.iter().map(|phase| phase.wall).sum(),
            cpu,
            footprint: Footprint {
                peak_rss: phases
                    .iter()
                    .map(|p| p.footprint.peak_rss)
                    .max()
                    .unwrap_or(0),
                peak_storage: phases
                    .iter()
                    .map(|p| p.footprint.peak_storage)
                    .max()
                    .unwrap_or(0),
                end_storage: phases.last().map_or(0, |p| p.footprint.end_storage),
            },
        }
    }

    /// Prints the phase's line of a report.
    pub fn print(&self) {
        let seconds = |value: Option<f64>| value.map_or("n/a".to_string(), |v| format!("{v:.0}"));
        println!(
            "  {:<14} {:>9.1} s wall   cpu: {:>7} fg {:>7} bg ({:>7} merger) {:>8.0} total   \
             rss peak {:>5.1} GiB   storage peak {:>6.1} GiB, end {:>6.1} GiB",
            self.name,
            self.wall,
            seconds(self.cpu.workers),
            seconds(self.cpu.background()),
            seconds(self.cpu.mergers),
            self.cpu.total,
            gib(self.footprint.peak_rss),
            gib(self.footprint.peak_storage),
            gib(self.footprint.end_storage),
        );
    }

    pub fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "wall_s": self.wall,
            "cpu_total_s": self.cpu.total,
            "cpu_fg_s": self.cpu.workers,
            "cpu_bg_s": self.cpu.background(),
            "cpu_merger_s": self.cpu.mergers,
            "cpu_datagen_s": self.cpu.datagen,
            "peak_rss_bytes": self.footprint.peak_rss,
            "peak_storage_bytes": self.footprint.peak_storage,
            "end_storage_bytes": self.footprint.end_storage,
        })
    }
}

/// Watches the process's memory and storage from a thread of its own,
/// keeping the peak of each since the last phase ended.
pub struct Sampler {
    stop: Arc<AtomicBool>,
    current: Arc<Mutex<Footprint>>,
    thread: Option<thread::JoinHandle<()>>,
    storage: PathBuf,
}

impl Sampler {
    /// Starts watching: memory five times a second, the storage under
    /// `storage` once a second.
    pub fn start(storage: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let current = Arc::new(Mutex::new(Footprint::default()));
        let thread = {
            let (stop, current, storage) = (stop.clone(), current.clone(), storage.clone());
            thread::Builder::new()
                .name("sampler".into())
                .spawn(move || {
                    let mut tick = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let rss = rss_bytes().unwrap_or(0);
                        let bytes = tick.is_multiple_of(5).then(|| dir_bytes(&storage));
                        let mut footprint = current.lock().unwrap();
                        footprint.peak_rss = footprint.peak_rss.max(rss);
                        if let Some(bytes) = bytes {
                            footprint.peak_storage = footprint.peak_storage.max(bytes);
                        }
                        drop(footprint);
                        tick += 1;
                        thread::sleep(Duration::from_millis(200));
                    }
                })
                .expect("failed to start the sampler")
        };
        Self {
            stop,
            current,
            thread: Some(thread),
            storage,
        }
    }

    /// Ends the running phase, which began at `mark`, and begins the next.
    ///
    /// # Returns
    ///
    /// What the phase cost.
    pub fn end_phase(&self, name: &str, mark: Mark) -> PhaseCost {
        let wall = mark.at.elapsed().as_secs_f64();
        let cpu = cpu_now().since(mark.cpu);
        let end_storage = dir_bytes(&self.storage);
        let mut footprint = std::mem::take(&mut *self.current.lock().unwrap());
        footprint.end_storage = end_storage;
        footprint.peak_storage = footprint.peak_storage.max(end_storage);
        PhaseCost {
            name: name.to_string(),
            wall,
            cpu,
            footprint,
        }
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// How one spine's batches are spread over its levels, added up over the
/// workers.
#[derive(Clone, Debug, Default)]
pub struct SpineLayout {
    /// The operator the spine belongs to, by the clusters it sits in.
    pub name: String,
    /// Batches at each level, loose or being merged.
    pub batches: [u64; LEVELS],
    /// Records at each level, loose or being merged.
    pub records: [u64; LEVELS],
    /// Records with negative weights at each level, loose or being merged.
    pub negative: [u64; LEVELS],
    pub bytes: u64,
}

impl SpineLayout {
    /// The records the spine holds at all its levels.
    pub fn total_records(&self) -> u64 {
        self.records.iter().sum()
    }

    /// The records with negative weights the spine holds at all its levels.
    pub fn total_negative(&self) -> u64 {
        self.negative.iter().sum()
    }
}

/// How every spine of a circuit was laid out when `profile` was taken.
///
/// # Arguments
///
/// * `profile` - the circuit's profile.
///
/// # Returns
///
/// One layout per spine that holds anything, by the name of its operator.
pub fn spine_layouts(profile: &DbspProfile) -> Vec<SpineLayout> {
    let profile = serde_json::to_value(profile).expect("failed to serialize the profile");
    let mut names = HashMap::new();
    name_nodes(&profile["graph"]["nodes"], &[], &mut names);

    let mut spines: BTreeMap<String, SpineLayout> = BTreeMap::new();
    for worker in profile["worker_profiles"].as_array().into_iter().flatten() {
        for (node, readings) in worker["metadata"].as_object().into_iter().flatten() {
            // The root adds up every spine of the circuit.
            if node == "n" {
                continue;
            }
            let spine = spines.entry(node.clone()).or_insert_with(|| SpineLayout {
                name: names.get(node).cloned().unwrap_or_else(|| node.clone()),
                ..SpineLayout::default()
            });
            for reading in readings.as_array().into_iter().flatten() {
                let value = reading["value"]["value"].as_u64().unwrap_or(0);
                let slot = reading["labels"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|label| label[0] == "slot")
                    .and_then(|label| label[1].as_str()?.parse::<usize>().ok())
                    .filter(|&slot| slot < LEVELS);
                match reading["metric_id"].as_str().unwrap_or("") {
                    // The spine also counts its merging batches as a whole,
                    // without a level; `add_at` leaves that count out.
                    "loose_batches_count" | "merging_batches_count" => {
                        add_at(&mut spine.batches, slot, value)
                    }
                    "loose_memory_records_count"
                    | "loose_storage_records_count"
                    | "merging_memory_records_count"
                    | "merging_storage_records_count" => add_at(&mut spine.records, slot, value),
                    "negative_weight_count" => add_at(&mut spine.negative, slot, value),
                    "spine_storage_size_bytes" => spine.bytes += value,
                    _ => {}
                }
            }
        }
    }
    let mut spines: Vec<(String, SpineLayout)> = spines
        .into_iter()
        .filter(|(_, spine)| spine.total_records() > 0 || spine.batches.iter().any(|&n| n > 0))
        .collect();
    // Operators of the same kind give their spines the same name, so those
    // are told apart by node.
    let mut uses: HashMap<String, usize> = HashMap::new();
    for (_, spine) in &spines {
        *uses.entry(spine.name.clone()).or_default() += 1;
    }
    for (node, spine) in &mut spines {
        if uses[&spine.name] > 1 {
            spine.name = format!("{} [{node}]", spine.name);
        }
    }
    spines.into_iter().map(|(_, spine)| spine).collect()
}

/// Snapshots of the circuit, taken as a run goes.
///
/// Each snapshot adds the layout of the circuit's spines to the run's
/// timeline and, when the run keeps profiles, saves the circuit's whole
/// profile, the raw data the layouts come from, in a file of its own.
pub struct Snapshots {
    /// The directory the profiles go to, if the run keeps them.
    profiles: Option<PathBuf>,
    /// When the run started, which the timeline counts from.
    started: Instant,
    /// Each snapshot's label, time and spine layouts.
    timeline: Vec<Value>,
    /// Wall time spent taking snapshots, in seconds.
    seconds: f64,
}

impl Snapshots {
    /// Starts a run's snapshots.
    ///
    /// # Arguments
    ///
    /// * `profiles` - the directory to save the circuit's profiles in,
    ///   created if it is missing, or `None` to save none.
    ///
    /// # Returns
    ///
    /// Snapshots with none taken yet, counting time from now.
    pub fn new(profiles: Option<PathBuf>) -> Self {
        if let Some(dir) = &profiles {
            std::fs::create_dir_all(dir)
                .unwrap_or_else(|error| panic!("failed to create {}: {error}", dir.display()));
        }
        Self {
            profiles,
            started: Instant::now(),
            timeline: Vec::new(),
            seconds: 0.0,
        }
    }

    /// Takes a snapshot of the circuit.
    ///
    /// When the run keeps profiles, the circuit's profile goes to
    /// `NNN-<label>.zip` in their directory, as `profile.json`; `NNN` numbers
    /// the snapshots in the order they were taken.
    ///
    /// # Arguments
    ///
    /// * `dbsp` - the circuit.
    /// * `label` - what the run had done, such as "after 1000 transactions".
    ///
    /// # Returns
    ///
    /// The layout of every spine that holds anything.
    pub fn take(&mut self, dbsp: &mut DBSPHandle, label: &str) -> Vec<SpineLayout> {
        let begun = Instant::now();
        let profile = dbsp.retrieve_profile().expect("failed to read the profile");
        if let Some(dir) = &self.profiles {
            let path = dir.join(format!("{:03}-{}.zip", self.timeline.len(), slug(label)));
            std::fs::write(&path, profile.as_json_zip())
                .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
        }
        let layouts = spine_layouts(&profile);
        self.timeline.push(json!({
            "when": label,
            "seconds": begun.duration_since(self.started).as_secs_f64(),
            "spines": layouts_json(&layouts),
        }));
        self.seconds += begun.elapsed().as_secs_f64();
        layouts
    }

    /// Prints how many snapshots the run took and the wall time they took.
    pub fn print(&self) {
        println!(
            "  snapshots: {} in {:.1} s{}",
            self.timeline.len(),
            self.seconds,
            match &self.profiles {
                Some(dir) => format!(", profiles in {}", dir.display()),
                None => String::new(),
            }
        );
    }

    /// The run's snapshots, for its result: how many it took, the wall time
    /// they took, and each one's label, time and spine layouts.
    pub fn to_json(&self) -> Value {
        json!({
            "count": self.timeline.len(),
            "seconds": self.seconds,
            "timeline": self.timeline,
        })
    }
}

/// `label` with each run of characters other than ASCII letters and digits
/// made one hyphen, for a file name.
fn slug(label: &str) -> String {
    label
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Waits until the circuit's mergers have nothing left to do.
///
/// Each level of a spine merges on its own, and starts its next merge as
/// soon as one ends or new batches arrive, so a spine that has merging left
/// to do is always merging.  The mergers are done once [`SETTLE_IDLE_POLLS`]
/// polls in a row, [`SETTLE_POLL`] apart, find no spine merging.
///
/// # Arguments
///
/// * `dbsp` - the circuit, between transactions.
pub fn settle(dbsp: &mut DBSPHandle) {
    let mut idle_polls = 0;
    loop {
        let profile = dbsp.retrieve_profile().expect("failed to read the profile");
        if merging_batches(&profile) == 0 {
            idle_polls += 1;
            if idle_polls == SETTLE_IDLE_POLLS {
                return;
            }
        } else {
            idle_polls = 0;
        }
        thread::sleep(SETTLE_POLL);
    }
}

/// How many batches the spines of a circuit were merging when `profile` was
/// taken, over all their levels and workers.
fn merging_batches(profile: &DbspProfile) -> u64 {
    let profile = serde_json::to_value(profile).expect("failed to serialize the profile");
    let mut merging = 0;
    for worker in profile["worker_profiles"].as_array().into_iter().flatten() {
        for (node, readings) in worker["metadata"].as_object().into_iter().flatten() {
            // The root adds up every spine of the circuit.
            if node == "n" {
                continue;
            }
            for reading in readings.as_array().into_iter().flatten() {
                // A spine reports its merging batches level by level and as a
                // whole; the levels carry a label.
                let by_level = reading["labels"]
                    .as_array()
                    .is_some_and(|labels| !labels.is_empty());
                if reading["metric_id"] == "merging_batches_count" && by_level {
                    merging += reading["value"]["value"].as_u64().unwrap_or(0);
                }
            }
        }
    }
    merging
}

/// Adds `value` to `counts` at `level`, if the reading named a level.
fn add_at(counts: &mut [u64; LEVELS], level: Option<usize>, value: u64) {
    if let Some(level) = level {
        counts[level] += value;
    }
}

/// Names every node of the circuit's graph by the clusters it sits in.
///
/// # Arguments
///
/// * `cluster` - a cluster of the graph, the root to begin with.
/// * `path` - the labels of the clusters around `cluster`.
/// * `names` - where each node's name goes, by its identifier.
fn name_nodes(cluster: &Value, path: &[String], names: &mut HashMap<String, String>) {
    let short = |label: &Value| {
        label
            .as_str()
            .unwrap_or("")
            .split(" @ ")
            .next()
            .unwrap_or("")
            .trim()
            .to_string()
    };
    for child in cluster["nodes"].as_array().into_iter().flatten() {
        if let Some(node) = child.get("Simple") {
            let mut name = path.to_vec();
            name.push(short(&node["label"]));
            if let Some(id) = node["id"].as_str() {
                names.insert(id.to_string(), name.join(" / "));
            }
        } else if let Some(inner) = child.get("Cluster") {
            let mut inner_path = path.to_vec();
            inner_path.push(short(&inner["label"]));
            name_nodes(inner, &inner_path, names);
        }
    }
}

/// Prints how the spines are laid out: each spine's batches per level,
/// averaged over the workers, and then the records and negative weights at
/// each of its levels, added up over the workers.
pub fn print_layouts(when: &str, layouts: &[SpineLayout], workers: usize) {
    println!(
        "  spines {when} (batches per level averaged over {workers} workers; \
         records added up over them):"
    );
    for spine in layouts {
        let levels: Vec<String> = spine
            .batches
            .iter()
            .enumerate()
            .filter(|(_, n)| **n > 0)
            .map(|(level, n)| format!("L{level} {:.1}", *n as f64 / workers as f64))
            .collect();
        println!(
            "    {:<60} {:<44} {:>14} records, {:>13} negative, {:>6.1} GiB",
            spine.name,
            if levels.is_empty() {
                "-".to_string()
            } else {
                levels.join("  ")
            },
            spine.total_records(),
            spine.total_negative(),
            gib(spine.bytes),
        );
        for level in (0..LEVELS).filter(|&level| spine.records[level] > 0) {
            println!(
                "        L{level} {:>14} records, {:>13} negative ({:.1}%)",
                spine.records[level],
                spine.negative[level],
                100.0 * spine.negative[level] as f64 / spine.records[level] as f64,
            );
        }
    }
}

pub fn layouts_json(layouts: &[SpineLayout]) -> Value {
    Value::Array(
        layouts
            .iter()
            .map(|spine| {
                json!({
                    "name": spine.name,
                    "batches_per_level": spine.batches,
                    "records_per_level": spine.records,
                    "negative_per_level": spine.negative,
                    "records": spine.total_records(),
                    "negative": spine.total_negative(),
                    "bytes": spine.bytes,
                })
            })
            .collect(),
    )
}
