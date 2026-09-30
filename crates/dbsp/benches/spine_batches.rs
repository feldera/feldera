//! What do extra batches in a spine cost the threads that read it?
//!
//! A level merges once it has gathered enough batches, so the count a level
//! waits for decides how many batches a spine carries: three is what the
//! built-in minimum leaves at levels two and above, ten is what a raised
//! threshold leaves.  Raising it buys fewer, wider merges -- the records stop
//! being rewritten twice per level -- and pays for them in batches that every
//! lookup has to consider.  This measures that price.
//!
//! Both layouts hold the same records: a level's share of them is divided into
//! however many batches the layout asks for, each level holding ten times what
//! the level below does, as a spine's levels do.  Every batch spans the whole
//! key space, so no batch can be dismissed by its key range alone.  The merge
//! threshold is set to the widest the levels allow, so neither layout merges
//! while it is being measured.
//!
//! A lookup's cost turns on how many batches hold the key, so three kinds are
//! timed:
//!
//! * `absent`, a key in no batch, which every batch answers from its
//!   membership filter;
//! * `in one`, a key one batch holds, where that batch reads a data block and
//!   the rest answer from their filters;
//! * `in all`, a key every batch holds, which is what an integral looks like
//!   where the same keys are updated again and again, and where every batch
//!   reads a data block.
//!
//! A scan is timed too, which reads every record once however they are
//! divided up, and so pays only for the merge that orders them.
//!
//! ```text
//! cargo bench -p dbsp --bench spine_batches -- [--per-level N] [--top-records N]
//!     [--probes N] [--cache-mib N] [--scale N] [--dir PATH] [--cold] [--direct]
//!     [--no-roaring] [--bloom-fp RATE] [--no-scan]
//! ```
//!
//! Keys here are integers packed into a range narrow enough for a batch to
//! hold them in a roaring bitmap, which is the filter a batch prefers where it
//! can: a lookup for a key the batch does not hold is then answered from the
//! bitmap.  `--no-roaring` leaves the Bloom filter in its place, which is what
//! a spine keyed by strings, tuples or wider integers has, and `--bloom-fp`
//! sets the rate of false positives that filter is built for -- the finer the
//! rate the larger the filter, until it is too large for the cache to hold and
//! reading it costs what reading the data would have.  A rate of zero leaves
//! the batches no membership filter at all, so every batch answers every
//! lookup by descending its index.
//!
//! What filter each run ended up with is reported with its size, and the hits
//! and misses the lookups drew from it, since a filter that is not there and a
//! filter that never rejects anything look the same in the times alone.
//!
//! The spine is written to a temporary directory under `--dir`, or under the
//! system's temporary directory if none is given.  Where that is a file
//! system held in memory, as `/tmp` often is, every read comes from memory and
//! `--direct` changes nothing, so a run that is meant to read a disk has to
//! name a directory on one.  `--cold` then drops the spine from the page
//! cache once it is built, so the lookups read it from the device, as a
//! pipeline does once its spine outgrows the machine's memory; each timed
//! pass asks about keys of its own, so that none finds blocks an earlier one
//! brought in.  Each run reports how much the lookups read and
//! how much of it came from a storage device, which says which kind of run it
//! was.
//!
//! Reads go through the operating system's page cache, as a pipeline's do,
//! unless `--direct` asks for Feldera's own cache, which on Linux opens the
//! files with `O_DIRECT` and so leaves `--cache-mib` as the only cache there
//! is.  That is the spine a pipeline has once it outgrows the machine's
//! memory, and the two runs bracket what a lookup costs: the page cache says
//! what the extra batches cost in processor time, direct reads say what they
//! cost in waiting for a disk.
//!
//! `--scale` divides every level's records, for trying the benchmark out
//! without waiting for the spine it is meant to measure.  The levels then hold
//! batches too small for them, so only the full-size run says anything about
//! the layouts.
//!
//! ```text
//! ```

use dbsp::circuit::metadata::{
    BLOOM_FILTER_BITS_PER_KEY, BLOOM_FILTER_HITS_COUNT, BLOOM_FILTER_MISSES_COUNT,
    BLOOM_FILTER_SIZE_BYTES, LOOSE_BATCHES_COUNT, MERGING_BATCHES_COUNT, MetaItem, MetricId,
    OperatorMeta, RANGE_FILTER_HITS_COUNT, RANGE_FILTER_MISSES_COUNT, RANGE_FILTER_SIZE_BYTES,
    ROARING_FILTER_HITS_COUNT, ROARING_FILTER_MISSES_COUNT, ROARING_FILTER_SIZE_BYTES,
};
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::dynamic::{DowncastTrait, Erase};
use dbsp::trace::BatchLocation;
use dbsp::{
    OrdZSet, Runtime, ZWeight,
    trace::{
        Batch, BatchReader as DynBatchReader, BatchReaderFactories, Builder, Cursor as DynCursor,
        Spine, Trace, TraceRole,
    },
    typed_batch::BatchReader as TypedBatchReader,
};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::{tempdir, tempdir_in};

type TypedBatchType = OrdZSet<u64>;
type Inner = <TypedBatchType as TypedBatchReader>::Inner;

/// How many records each level below the top holds, lowest level first.
///
/// A level holds ten times what the level below does, and these totals are
/// divided among however many batches the layout asks for.  They are chosen so
/// that each batch lands at the level meant for it whether the layout is three
/// batches or ten: a level takes a batch by its record count, and the counts a
/// level accepts span a ten-fold range.
///
/// Levels zero and one are left empty on purpose.  They keep their own
/// minimums whatever the threshold says -- level one merges at eight -- so a
/// layout of ten batches cannot be held there, and the merging would move the
/// spine under the measurement.
const LOWER_LEVEL_RECORDS: [usize; 3] = [2_700_000, 27_000_000, 270_000_000];

/// How many keys every batch holds a copy of, at most.
///
/// These are what the `in all` lookups ask for.  They are a small share of
/// even the shortest batch, so they do not move a batch to another level; a
/// run small enough for them not to be gives each batch as many as it can
/// spread out instead.
const SHARED_KEYS: usize = 10_000;

#[derive(Clone)]
struct Args {
    per_level: usize,
    top_records: usize,
    probes: usize,
    cache_mib: usize,
    scale: usize,
    dir: Option<String>,
    cold: bool,
    direct: bool,
    roaring: bool,
    bloom_fp: f64,
    scan: bool,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            per_level: 3,
            top_records: 1_000_000_000,
            probes: 10_000,
            cache_mib: 512,
            scale: 1,
            dir: None,
            cold: false,
            direct: false,
            roaring: true,
            bloom_fp: 0.0001,
            scan: true,
        };
        let argv: Vec<String> = std::env::args().collect();
        let mut i = 1;
        let rate = |i: &mut usize| -> f64 {
            *i += 1;
            argv[*i]
                .parse()
                .unwrap_or_else(|_| panic!("{} wants a rate", argv[*i - 1]))
        };
        let number = |i: &mut usize| -> usize {
            *i += 1;
            argv[*i]
                .parse()
                .unwrap_or_else(|_| panic!("{} wants a number", argv[*i - 1]))
        };
        while i < argv.len() {
            match argv[i].as_str() {
                "--per-level" => args.per_level = number(&mut i),
                "--top-records" => args.top_records = number(&mut i),
                "--probes" => args.probes = number(&mut i),
                "--cache-mib" => args.cache_mib = number(&mut i),
                "--scale" => args.scale = number(&mut i),
                "--dir" => {
                    i += 1;
                    args.dir = Some(argv[i].clone());
                }
                "--direct" => args.direct = true,
                "--cold" => args.cold = true,
                "--no-roaring" => args.roaring = false,
                "--bloom-fp" => args.bloom_fp = rate(&mut i),
                "--no-scan" => args.scan = false,
                "--bench" => {}
                other => panic!("unknown argument {other}"),
            }
            i += 1;
        }
        assert!(args.per_level > 0, "--per-level wants at least one");
        assert!(args.scale > 0, "--scale wants at least one");
        args
    }

    /// How many records each level holds, lowest level first.
    ///
    /// # Returns
    ///
    /// One record count per level, the last being the top level's.
    fn level_records(&self) -> Vec<usize> {
        let mut records = LOWER_LEVEL_RECORDS.to_vec();
        records.push(self.top_records);
        records.iter().map(|level| level / self.scale).collect()
    }

    /// How many batches the whole spine holds.
    fn batches(&self) -> usize {
        self.per_level * (LOWER_LEVEL_RECORDS.len() + 1)
    }
}

/// The key space, which every batch spans.
///
/// It stops short of what a 32-bit offset holds, because a batch whose keys
/// fit in that range can hold them in a roaring bitmap and answer a lookup for
/// an absent key from that alone, without reading a data block.  A wider space
/// turns the filter off for every batch, which would hide what it is worth.
const SPAN: u64 = 4_000_000_000;

/// Keys are laid out so that every batch spans the whole key space.
///
/// A batch takes every `stride`th key from an offset of its own, with the
/// stride set by how many keys the batch holds: a batch of a million keys
/// steps through the space a hundred times as fast as a batch of a hundred
/// million, and both end up spanning it.  A batch's offset is its ordinal, so
/// a key's remainder says which batch it came from and no two batches hold the
/// same key.
///
/// That leaves every remainder spoken for, so the keys a lookup asks about
/// that no batch holds, and the ones every batch holds, come from the gaps
/// instead: half a stride above where the shortest batch puts its own keys.
struct KeySpace {
    batches: u64,
    shortest: u64,
    shared: usize,
}

impl KeySpace {
    /// The key space for a spine of `batches` batches, the shortest holding
    /// `shortest` records.
    fn new(batches: usize, shortest: usize) -> Self {
        assert!(
            SPAN <= u32::MAX as u64,
            "the key space has to fit a 32-bit offset for a batch to filter on it"
        );
        assert!(batches >= 2, "the gaps need two batches to sit beside");
        let keys = Self {
            batches: batches as u64,
            shortest: shortest as u64,
            shared: SHARED_KEYS.min(shortest),
        };
        assert!(
            keys.gap() > 0,
            "the shortest batch's keys are too close together to leave a gap between them"
        );
        keys
    }

    /// The distance between the keys of a batch holding `len` records.
    fn stride(&self, len: usize) -> u64 {
        (SPAN / (len as u64 * self.batches)).max(1) * self.batches
    }

    /// How many keys every batch holds a copy of.
    fn shared_count(&self) -> usize {
        self.shared
    }

    /// How far above the shortest batch's keys the gaps sit.
    ///
    /// Half of that batch's stride, rounded down to a whole number of batches
    /// so that a gap keeps the remainder it is given.
    fn gap(&self) -> u64 {
        self.stride(self.shortest as usize) / 2 / self.batches * self.batches
    }

    /// The `ordinal`th key of the `index`th batch, which holds `len` records.
    ///
    /// # Arguments
    ///
    /// * `index` - which batch holds the key, which decides its remainder.
    /// * `ordinal` - the key's place among that batch's keys.  A caller
    ///   looking keys up spreads these across the batch, so that the lookups
    ///   do not all land in the same few data blocks.
    /// * `len` - how many records that batch holds.
    fn own(&self, index: usize, ordinal: usize, len: usize) -> u64 {
        ordinal as u64 % len as u64 * self.stride(len) + index as u64
    }

    /// A key that no batch holds.
    ///
    /// It sits in a gap of the first batch's keys, so no other batch's
    /// remainder can match it and that batch's own stride steps over it.  The
    /// gaps used are spread across the space, so that the lookups do not all
    /// land in the same few data blocks.
    fn absent(&self, index: usize) -> u64 {
        index as u64 % self.shared as u64
            * (self.shortest / self.shared as u64)
            * self.stride(self.shortest as usize)
            + self.gap()
    }

    /// The `index`th key that every batch holds a copy of.
    ///
    /// These sit in the gaps of the second batch's keys, spread across the
    /// space, so a batch holds them in as many different data blocks as it
    /// has.  The shared key of a slot lies one above the absent key of the
    /// same slot, in the same data blocks, so the slots are counted from half
    /// way round: lookups for the first absent keys and for the first shared
    /// keys then read different blocks.
    fn shared(&self, index: usize) -> u64 {
        (index as u64 + self.shared as u64 / 2) % self.shared as u64
            * (self.shortest / self.shared as u64)
            * self.stride(self.shortest as usize)
            + self.gap()
            + 1
    }
}

/// The `index`th of `modulus` numbers, in an order that is not theirs.
///
/// Looking keys up in the order a batch stores them would let each lookup warm
/// the cache for the next, which is not what an operator's lookups do.
fn scatter(index: usize, modulus: usize) -> usize {
    index.wrapping_mul(0x9E37_79B9) % modulus
}

/// One batch: `len` of its own keys, and a copy of every shared key.
///
/// The batch is built straight to storage, told up front how many keys to
/// expect, because that count is what sizes its membership filter.  A builder
/// left to choose for itself starts in memory and moves to storage once it
/// grows past a threshold, and the filter it then writes is sized for the keys
/// it held when it moved -- at a threshold of zero, a single key -- so it
/// answers "maybe" to every lookup.  A merge tells its builder the key counts
/// of its inputs, so a batch built this way is the one a spine holds.
///
/// The batch's own keys and the shared ones are two ascending runs, which go
/// in merged, since a builder takes keys in order.
///
/// # Arguments
///
/// * `index` - the batch's ordinal in the spine, which decides its keys.
/// * `len` - how many of its own keys the batch holds.
/// * `keys` - the key space the batch takes its keys from.
/// * `factories` - the factories for the batch being built.
///
/// # Returns
///
/// The batch, and how many records went into it.
fn batch(
    index: usize,
    len: usize,
    keys: &KeySpace,
    factories: &<Inner as DynBatchReader>::Factories,
) -> (Inner, usize) {
    let mut shared: Vec<u64> = (0..keys.shared_count()).map(|i| keys.shared(i)).collect();
    shared.sort_unstable();
    shared.dedup();

    let mut builder = <Inner as Batch>::Builder::with_capacity_in_location(
        factories,
        len + shared.len(),
        len + shared.len(),
        Some(BatchLocation::Storage),
    );
    let unit = ();
    let weight: ZWeight = 1;
    let mut own = 0;
    let mut taken = 0;
    let mut records = 0;
    let mut last = None;
    loop {
        let mine = (own < len).then(|| keys.own(index, own, len));
        let theirs = shared.get(taken).copied();
        let next = match (mine, theirs) {
            (Some(mine), Some(theirs)) if mine <= theirs => {
                own += 1;
                mine
            }
            (Some(mine), None) => {
                own += 1;
                mine
            }
            (_, Some(theirs)) => {
                taken += 1;
                theirs
            }
            (None, None) => break,
        };
        // A key the builder has already taken cannot go in again.  The two
        // runs are laid out not to collide, so this drops nothing.
        if last == Some(next) {
            continue;
        }
        last = Some(next);
        let mut next = next;
        builder.push_val_diff(unit.erase(), weight.erase());
        builder.push_key(next.erase_mut());
        records += 1;
    }
    (builder.done(), records)
}

/// The batches the spine holds, by level.
///
/// A layout asked for is not a layout achieved: a level takes a batch by its
/// record count, so a batch meant for one level lands at another if its length
/// says so.  Reporting what the spine ended up with is what makes the two runs
/// comparable.
///
/// # Arguments
///
/// * `spine` - the spine to read.
///
/// # Returns
///
/// The loose batch count per level, and how many are merging in total.
fn layout(spine: &Spine<Inner>) -> (Vec<usize>, usize) {
    let mut meta = OperatorMeta::default();
    spine.metadata(&mut meta);
    let mut per_level = vec![0usize; 9];
    for (labels, item) in meta.readings(&LOOSE_BATCHES_COUNT) {
        let slot: usize = labels
            .iter()
            .find(|(name, _)| name == "slot")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        if let MetaItem::Count(n) = item
            && slot < per_level.len()
        {
            per_level[slot] = *n;
        }
    }
    let merging = meta
        .readings(&MERGING_BATCHES_COUNT)
        .filter_map(|(_, item)| match item {
            MetaItem::Count(n) => Some(*n),
            _ => None,
        })
        .sum();
    (per_level, merging)
}

/// What the spine reports for `metric`, added up over its levels.
///
/// # Arguments
///
/// * `spine` - the spine to read.
/// * `metric` - which reading to add up.
///
/// # Returns
///
/// The total, or zero where the spine reports no such reading.
fn reading(spine: &Spine<Inner>, metric: &MetricId) -> u64 {
    let mut meta = OperatorMeta::default();
    spine.metadata(&mut meta);
    meta.readings(metric)
        .filter_map(|(_, item)| match item {
            MetaItem::Count(n) | MetaItem::Int(n) => Some(*n as u64),
            MetaItem::Bytes(bytes) => Some(bytes.bytes),
            _ => None,
        })
        .sum()
}

/// What the filters the batches were built with cost and did.
///
/// # Arguments
///
/// * `spine` - the spine to read.
///
/// # Returns
///
/// A line naming the filter the batches ended up with, how large it is, and
/// the hits and misses the lookups so far have drawn from it.
fn filters(spine: &Spine<Inner>) -> String {
    let mut kinds = Vec::new();
    for (name, size, hits, misses) in [
        (
            "bloom",
            BLOOM_FILTER_SIZE_BYTES,
            BLOOM_FILTER_HITS_COUNT,
            BLOOM_FILTER_MISSES_COUNT,
        ),
        (
            "roaring",
            ROARING_FILTER_SIZE_BYTES,
            ROARING_FILTER_HITS_COUNT,
            ROARING_FILTER_MISSES_COUNT,
        ),
        (
            "range",
            RANGE_FILTER_SIZE_BYTES,
            RANGE_FILTER_HITS_COUNT,
            RANGE_FILTER_MISSES_COUNT,
        ),
    ] {
        let size = reading(spine, &size);
        let hits = reading(spine, &hits);
        let misses = reading(spine, &misses);
        if size > 0 || hits > 0 || misses > 0 {
            kinds.push(format!(
                "{name} {:.0} MiB, {hits} hits, {misses} misses",
                size as f64 / (1 << 20) as f64,
            ));
        }
    }
    if kinds.is_empty() {
        String::from("none")
    } else {
        kinds.join("; ")
    }
}

/// How many bytes `dir` holds, counting what is under it.
fn dir_bytes(dir: &Path) -> u64 {
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

/// Drops the files under `dir` from the page cache, so that what reads them
/// next fetches them from the device.
///
/// A file's dirty pages cannot be dropped, so each is written out first.
///
/// # Arguments
///
/// * `dir` - the directory whose files to drop.
///
/// # Returns
///
/// How many bytes of files it dropped, or `None` where the system offers no
/// way to drop them.
#[cfg(target_os = "linux")]
fn drop_from_page_cache(dir: &Path) -> Option<u64> {
    use std::os::fd::AsRawFd;
    let mut dropped = 0;
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.is_dir() {
            dropped += drop_from_page_cache(&entry.path()).unwrap_or(0);
        } else if let Ok(file) = std::fs::File::open(entry.path()) {
            let _ = file.sync_data();
            // SAFETY: `file` is open, and advice about a range of it cannot
            // touch memory.
            unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            dropped += meta.len();
        }
    }
    Some(dropped)
}

#[cfg(not(target_os = "linux"))]
fn drop_from_page_cache(_dir: &Path) -> Option<u64> {
    None
}

/// How many bytes this process has read, and how many of those came from a
/// storage device rather than from the page cache, where the system will say.
///
/// # Returns
///
/// The bytes read by any means and the bytes fetched from a device, or `None`
/// where the system does not keep these counts.
fn io_read_bytes() -> Option<(u64, u64)> {
    let io = std::fs::read_to_string("/proc/self/io").ok()?;
    let field = |name: &str| -> Option<u64> {
        io.lines()
            .find_map(|line| line.strip_prefix(name))?
            .trim()
            .parse()
            .ok()
    };
    Some((field("rchar:")?, field("read_bytes:")?))
}

/// How many bytes of memory this process holds, where the system will say.
fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}

/// How long a run of lookups took, per lookup, in microseconds.
///
/// # Arguments
///
/// * `lookups` - how many lookups were made.
/// * `elapsed` - how long they took altogether.
fn per_lookup(lookups: usize, elapsed: std::time::Duration) -> f64 {
    elapsed.as_secs_f64() * 1e6 / lookups as f64
}

/// Looks for `keys` every way an operator asks a spine, and reports the time
/// each way took per key.
///
/// Four ways, because two choices each change what extra batches cost.  One is
/// how many cursors: an operator that looks up the keys of an input batch
/// opens one cursor on its trace per step and seeks through it, so the cost of
/// opening a cursor on every batch is paid once, while a lookup that stands
/// alone pays it every time.  The other is which call: [`DynCursor::seek_key`]
/// has to find the first key at or past the one asked for, so an absent key
/// still costs a descent in every batch, while
/// [`DynCursor::seek_key_exact`] asks only whether the key is there and lets a
/// batch answer no from its membership filter.
///
/// The one-cursor lookups go in key order, as an operator's do, and the ones
/// that open their own cursor go in an order of no relation to how the batches
/// store them, so that no lookup warms the cache for the next.
///
/// # Arguments
///
/// * `name` - what to call this kind of key in the report.
/// * `spine` - the spine to look in.
/// * `sorted` - the keys to look for, in order, without repeats.
/// * `scattered` - the same keys, out of order.
/// * `expect` - whether the spine holds these keys, which the counts the
///   lookups return are checked against.
/// * `cool` - what to do before each pass, untimed: drop the spine from the
///   page cache for a cold run, nothing for a warm one.
fn probe(
    name: &str,
    spine: &Spine<Inner>,
    passes: &[Vec<u64>; PASSES],
    expect: bool,
    cool: &dyn Fn(),
) {
    let [
        one_seek_keys,
        one_exact_keys,
        fresh_seek_keys,
        fresh_exact_keys,
    ] = passes;
    let mut found = [0usize; PASSES];

    cool();
    let mut cursor = spine.cursor();
    let one_seek = Instant::now();
    for &key in one_seek_keys {
        let mut key = key;
        cursor.seek_key(key.erase_mut());
        if cursor.key_valid() && *cursor.key().downcast_checked::<u64>() == key {
            found[0] += 1;
        }
    }
    let one_seek = one_seek.elapsed();

    cool();
    let mut cursor = spine.cursor();
    let one_exact = Instant::now();
    for &key in one_exact_keys {
        if cursor.seek_key_exact(key.erase(), None) {
            found[1] += 1;
        }
    }
    let one_exact = one_exact.elapsed();

    cool();
    let fresh_seek = Instant::now();
    for &key in fresh_seek_keys {
        let mut key = key;
        let mut cursor = spine.cursor();
        cursor.seek_key(key.erase_mut());
        if cursor.key_valid() && *cursor.key().downcast_checked::<u64>() == key {
            found[2] += 1;
        }
    }
    let fresh_seek = fresh_seek.elapsed();

    cool();
    let fresh_exact = Instant::now();
    for &key in fresh_exact_keys {
        let mut cursor = spine.cursor();
        if cursor.seek_key_exact(key.erase(), None) {
            found[3] += 1;
        }
    }
    let fresh_exact = fresh_exact.elapsed();

    let asked: Vec<usize> = passes.iter().map(Vec::len).collect();
    let wanted: Vec<usize> = asked.iter().map(|&n| if expect { n } else { 0 }).collect();
    println!(
        "  {name:7}{:>10.2}{:>10.2}{:>10.2}{:>10.2}   {}",
        per_lookup(asked[0], one_seek),
        per_lookup(asked[1], one_exact),
        per_lookup(asked[2], fresh_seek),
        per_lookup(asked[3], fresh_exact),
        if found.as_slice() == wanted.as_slice() {
            format!("{} of {}", wanted[0], asked[0])
        } else {
            format!("WRONG: found {found:?}, wanted {wanted:?}")
        },
    );
}

/// How many timed passes each kind of key gets: one cursor and a fresh cursor
/// per key, each with `seek_key` and with `seek_key_exact`.
const PASSES: usize = 4;

/// The keys each timed pass looks for: in order for the passes that use one
/// cursor, out of order for those that open a cursor per key.
///
/// Each pass gets keys of its own, the `pass`th run of `probes` of them, so
/// that no pass looks up a key whose blocks an earlier pass has already
/// brought into a cache -- which matters when the spine is read from a device,
/// and changes nothing when it is in memory.
///
/// # Arguments
///
/// * `probes` - how many keys to ask for in each pass.
/// * `key_at` - the `index`th key.
///
/// # Returns
///
/// The keys for each pass, without repeats.
fn probe_keys(probes: usize, key_at: impl Fn(usize) -> u64) -> [Vec<u64>; PASSES] {
    std::array::from_fn(|pass| {
        let mut sorted: Vec<u64> = (pass * probes..(pass + 1) * probes).map(&key_at).collect();
        sorted.sort_unstable();
        sorted.dedup();
        // The first two passes use one cursor, which an operator walks in key
        // order; the other two open a cursor per key.
        if pass < 2 {
            sorted
        } else {
            (0..sorted.len())
                .map(|i| sorted[scatter(i, sorted.len())])
                .collect()
        }
    })
}

fn main() {
    let args = Args::parse();
    let temp = match &args.dir {
        Some(dir) => tempdir_in(dir),
        None => tempdir(),
    }
    .expect("failed to create temp directory");
    let mut config = CircuitConfig::with_workers(1).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: if args.direct {
                    StorageCacheConfig::FelderaCache
                } else {
                    StorageCacheConfig::default()
                },
            },
            StorageOptions {
                // Every batch on storage, which is where a spine this size
                // keeps them, and which keeps memory pressure from starting a
                // merge the layout did not ask for.
                min_storage_bytes: Some(0),
                min_step_storage_bytes: Some(0),
                cache_mib: Some(args.cache_mib),
                ..StorageOptions::default()
            },
        )
        .expect("failed to configure POSIX storage"),
    ));
    // The widest the levels allow, so the layout under measurement stays put.
    config.dev_tweaks.integral_merge_threshold_batches = Some(64);
    config.dev_tweaks.merger_threads = Some(2);
    // With the filter off, a lookup for a key a batch does not hold costs the
    // same descent as one it does, which is what a spine keyed by anything a
    // roaring bitmap cannot hold -- a string, a tuple, a wider integer -- has
    // to pay.
    config.dev_tweaks.enable_roaring = Some(args.roaring);
    config.dev_tweaks.bloom_false_positive_rate = Some(args.bloom_fp);

    let path = temp.path().to_path_buf();
    let handle = Runtime::run(config, move |_parker| {
        let factories = BatchReaderFactories::new::<u64, (), ZWeight>();

        let mut spine = Spine::<Inner>::new(
            &factories,
            Arc::new(String::from("spine_batches")),
            TraceRole::Integral,
        );

        let level_records = args.level_records();
        let batches = args.batches();
        let top_len = level_records[level_records.len() - 1] / args.per_level;
        let keys = KeySpace::new(batches, level_records[0] / args.per_level);

        let mut records = 0;
        let mut index = 0;
        let build = Instant::now();
        for &level in &level_records {
            let len = level / args.per_level;
            for _ in 0..args.per_level {
                let (batch, in_batch) = batch(index, len, &keys, &factories);
                spine.insert_without_blocking(batch);
                records += in_batch;
                index += 1;
            }
        }
        let built = build.elapsed();

        let distinct: usize = level_records
            .iter()
            .map(|level| level / args.per_level * args.per_level)
            .sum::<usize>()
            + SHARED_KEYS;
        println!(
            "{batches} batches ({} per level x {} levels), {records} records \
             ({distinct} distinct keys), {} cache {} MiB, built in {:.0}s",
            args.per_level,
            level_records.len(),
            if args.direct { "direct," } else { "page" },
            args.cache_mib,
            built.as_secs_f64(),
        );
        println!(
            "  batch lengths: {}",
            level_records
                .iter()
                .map(|level| format!("{}", level / args.per_level + SHARED_KEYS))
                .collect::<Vec<_>>()
                .join(" ")
        );
        let (per_level, merging) = layout(&spine);
        println!(
            "  spine holds:   {}{}",
            per_level
                .iter()
                .enumerate()
                .filter(|(_, n)| **n > 0)
                .map(|(level, n)| format!("L{level}:{n}"))
                .collect::<Vec<_>>()
                .join(" "),
            if merging > 0 {
                format!("  ({merging} merging -- the layout is moving under the measurement)")
            } else {
                String::new()
            },
        );
        println!(
            "  storage:       {:.1} GiB   rss {:.1} GiB",
            dir_bytes(&path) as f64 / (1 << 30) as f64,
            rss_bytes().unwrap_or(0) as f64 / (1 << 30) as f64,
        );
        println!(
            "  filter:        {}   ({:.1} bits/key asked of a bloom at fp {})",
            filters(&spine),
            reading(&spine, &BLOOM_FILTER_BITS_PER_KEY) as f64,
            args.bloom_fp,
        );

        println!(
            "  {:7}{:^20}{:^20}   (us per lookup)",
            "", "one cursor", "fresh cursor per key"
        );
        println!(
            "  {:7}{:>10}{:>10}{:>10}{:>10}   found",
            "", "seek", "exact", "seek", "exact"
        );
        if args.cold {
            // Each pass has to look up keys no earlier pass did, or it finds
            // their blocks in Feldera's own cache, which a cold run keeps.
            assert!(
                PASSES * args.probes <= keys.shared_count(),
                "a cold run looks up {} keys of each kind, more than the {} there are \
                 that no batch holds and that every batch holds; ask for fewer --probes",
                PASSES * args.probes,
                keys.shared_count(),
            );
            match drop_from_page_cache(&path) {
                Some(bytes) => println!(
                    "  cold:          dropping {:.1} GiB of the spine from the page cache \
                     before each pass",
                    bytes as f64 / (1 << 30) as f64
                ),
                None => {
                    println!("  cold:          this system offers no way to drop the page cache")
                }
            }
        }
        let cool = || {
            if args.cold {
                drop_from_page_cache(&path);
            }
        };
        let io_before = io_read_bytes();
        // How many questions the membership filters have answered, and how
        // many of those they answered "no".
        let answers = || {
            let rejected = reading(&spine, &BLOOM_FILTER_MISSES_COUNT)
                + reading(&spine, &ROARING_FILTER_MISSES_COUNT);
            let passed = reading(&spine, &BLOOM_FILTER_HITS_COUNT)
                + reading(&spine, &ROARING_FILTER_HITS_COUNT);
            (rejected + passed, rejected)
        };
        let mut absent_rejected = 0;
        let mut absent_asked = 0;
        for (name, passes, expect) in [
            ("absent", probe_keys(args.probes, |i| keys.absent(i)), false),
            (
                "in one",
                probe_keys(args.probes, |i| {
                    keys.own(batches - 1, i * (top_len / (PASSES * args.probes)), top_len)
                }),
                true,
            ),
            ("in all", probe_keys(args.probes, |i| keys.shared(i)), true),
        ] {
            let (asked_before, rejected_before) = answers();
            probe(name, &spine, &passes, expect, &cool);
            if !expect {
                let (asked, rejected) = answers();
                absent_asked = asked - asked_before;
                absent_rejected = rejected - rejected_before;
            }
        }

        println!("  filter now:    {}", filters(&spine));
        if let (Some((all_before, device_before)), Some((all_after, device_after))) =
            (io_before, io_read_bytes())
        {
            println!(
                "  lookups read:  {:.1} MiB, {:.1} MiB of it from a storage device",
                (all_after - all_before) as f64 / (1 << 20) as f64,
                (device_after - device_before) as f64 / (1 << 20) as f64,
            );
        }
        let filter_bytes =
            reading(&spine, &BLOOM_FILTER_SIZE_BYTES) + reading(&spine, &ROARING_FILTER_SIZE_BYTES);
        if filter_bytes > 0 {
            println!(
                "  filters rejected {absent_rejected} of {absent_asked} questions about \
                 absent keys ({:.1}%)",
                absent_rejected as f64 * 100.0 / absent_asked.max(1) as f64,
            );
            // A working filter rejects nearly every one, whatever rate it was
            // built for; fewer than half means most batches' filters are not
            // doing their job.
            if absent_rejected * 2 < absent_asked {
                println!(
                    "  WARNING: most batches' membership filters are not rejecting keys \
                     they do not hold"
                );
            }
        }

        // A scan reads every record once however they are divided up.
        if args.scan {
            let start = Instant::now();
            let mut seen = 0u64;
            let mut cursor = spine.cursor();
            while cursor.key_valid() {
                seen += 1;
                cursor.step_key();
            }
            let elapsed = start.elapsed();
            println!(
                "  scan:   {seen} keys in {:.1}s  ({:.1} M keys/s)",
                elapsed.as_secs_f64(),
                seen as f64 / elapsed.as_secs_f64() / 1e6,
            );
        }
    })
    .expect("failed to start the runtime");
    handle.join().unwrap();
}
