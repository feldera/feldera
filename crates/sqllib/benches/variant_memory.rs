//! Heap footprint of VARIANT-heavy rows: legacy [`Variant`] vs [`FlatVariant`].
//!
//! The row type mirrors what the SQL compiler generates for
//!
//! ```sql
//! create table user_props (
//!     bsin varchar not null primary key,
//!     user_id varchar,
//!     email varchar,
//!     contacts variant,
//!     properties variant,
//!     site_id varchar,
//!     replaced_by varchar,
//!     email_md5 varchar,
//!     sub_site_ids variant,
//!     scoped_properties variant,
//!     scoped_contacts variant,
//!     consent variant,
//!     external_ids variant,
//!     merged_bsins variant)
//! ```
//!
//! and the sample rows are records that OOMed a 64 GB pipeline built on that
//! table. Rows are parsed exactly as the JSON input connector parses them, held
//! alive in a `Vec`, and measured four ways:
//!
//! - bytes requested from the allocator and not yet freed, the true heap cost;
//! - the same rounded to jemalloc size classes, the cost the pipeline pays,
//!   since the generated binary links `tikv_jemallocator`;
//! - the number of live blocks the row is scattered across;
//! - `SizeOf::total_bytes`, the number the runtime's memory accounting sees and
//!   uses to decide when to spill a batch to disk.
//!
//! Run with `cargo bench -p feldera-sqllib --bench variant_memory`. Optional
//! positional argument: how many times to replay the sample (default 20).
//! `VARIANT_BENCH_DATA=path` reads rows from another `.jsonl`/`.jsonl.gz` file.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use feldera_sqllib::flat_variant::DocSet;
use feldera_sqllib::{FlatVariant, SqlString, Variant};
use feldera_types::format::json::JsonFlavor;
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use size_of::SizeOf;

// ------------------------------------------------------------------ allocator

/// Wraps the system allocator to track live bytes and allocation counts.
struct Counting;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

static LIVE_REQUESTED: AtomicUsize = AtomicUsize::new(0);
static LIVE_RESERVED: AtomicUsize = AtomicUsize::new(0);
static LIVE_BLOCKS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        on_dealloc(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            on_dealloc(layout.size());
            on_alloc(new_size);
        }
        new_ptr
    }
}

fn on_alloc(size: usize) {
    LIVE_REQUESTED.fetch_add(size, Relaxed);
    LIVE_RESERVED.fetch_add(size_class(size), Relaxed);
    LIVE_BLOCKS.fetch_add(1, Relaxed);
    ALLOCATIONS.fetch_add(1, Relaxed);
}

fn on_dealloc(size: usize) {
    LIVE_REQUESTED.fetch_sub(size, Relaxed);
    LIVE_RESERVED.fetch_sub(size_class(size), Relaxed);
    LIVE_BLOCKS.fetch_sub(1, Relaxed);
}

/// Bytes jemalloc reserves for a `size`-byte request.
///
/// jemalloc rounds a request up to a size class: 8 bytes, then multiples of the
/// 16-byte quantum through 128, then four classes per power of two. A
/// representation that makes many small allocations pays this rounding on every
/// one of them, so it belongs in the comparison.
fn size_class(size: usize) -> usize {
    match size {
        0 => 0,
        1..=8 => 8,
        9..=128 => size.next_multiple_of(16),
        _ => {
            let group = 1usize << (usize::BITS - 1 - size.leading_zeros());
            size.next_multiple_of(group / 4)
        }
    }
}

/// Guards [`size_class`] against spot values from jemalloc's size-class table
/// (`lg_quantum = 4`). A bench with `harness = false` never runs `#[test]`
/// functions, so the check runs at startup instead.
fn check_size_classes() {
    for (request, class) in [
        (1, 8),
        (8, 8),
        (9, 16),
        (17, 32),
        (128, 128),
        (129, 160),
        (160, 160),
        (200, 224),
        (256, 256),
        (257, 320),
        (1000, 1024),
        (1025, 1280),
    ] {
        assert_eq!(size_class(request), class, "request of {request} bytes");
    }
}

/// Allocator counters at one instant.
#[derive(Clone, Copy)]
struct Snapshot {
    requested: usize,
    reserved: usize,
    blocks: usize,
    allocations: usize,
}

impl Snapshot {
    fn take() -> Self {
        Self {
            requested: LIVE_REQUESTED.load(Relaxed),
            reserved: LIVE_RESERVED.load(Relaxed),
            blocks: LIVE_BLOCKS.load(Relaxed),
            allocations: ALLOCATIONS.load(Relaxed),
        }
    }

    /// Growth between `earlier` and `self`.
    fn since(self, earlier: Snapshot) -> Snapshot {
        Snapshot {
            requested: self.requested - earlier.requested,
            reserved: self.reserved - earlier.reserved,
            blocks: self.blocks - earlier.blocks,
            allocations: self.allocations - earlier.allocations,
        }
    }
}

// ----------------------------------------------------------------- row types

feldera_macros::declare_tuple! {
    Tup14<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>
}

static JSON_CONFIG: LazyLock<SqlSerdeConfig> =
    LazyLock::new(|| SqlSerdeConfig::from(JsonFlavor::Default));

/// Declares the types the SQL compiler generates for `user_props`, once per
/// VARIANT representation: the deserialization struct, the `TupN` row the
/// circuit stores, and the conversion between them. Both representations get
/// the same declaration, so the comparison cannot drift.
///
/// The nested per-column modules each declare a single VARIANT column. Parsing
/// an unmodified record into one of them attributes cost to that column,
/// because the deserializer ignores columns the struct does not declare;
/// splitting the record text instead would reorder its keys, and a legacy
/// `Variant` map costs more when its keys arrive sorted.
macro_rules! user_props_row {
    ($repr:ident, $variant:ident, $(($column:ident, $column_name:tt)),*) => {
        mod $repr {
            use super::*;

            #[derive(Clone, Debug, Eq, PartialEq, Default, PartialOrd, Ord)]
            pub struct UserProps {
                field0: SqlString,
                field1: Option<SqlString>,
                field2: Option<SqlString>,
                field3: Option<$variant>,
                field4: Option<$variant>,
                field5: Option<SqlString>,
                field6: Option<SqlString>,
                field7: Option<SqlString>,
                field8: Option<$variant>,
                field9: Option<$variant>,
                field10: Option<$variant>,
                field11: Option<$variant>,
                field12: Option<$variant>,
                field13: Option<$variant>,
            }

            /// The row as the circuit stores it.
            pub type Row = Tup14<
                SqlString,
                Option<SqlString>,
                Option<SqlString>,
                Option<$variant>,
                Option<$variant>,
                Option<SqlString>,
                Option<SqlString>,
                Option<SqlString>,
                Option<$variant>,
                Option<$variant>,
                Option<$variant>,
                Option<$variant>,
                Option<$variant>,
                Option<$variant>,
            >;

            impl From<UserProps> for Row {
                fn from(t: UserProps) -> Self {
                    Tup14::new(
                        t.field0, t.field1, t.field2, t.field3, t.field4, t.field5, t.field6,
                        t.field7, t.field8, t.field9, t.field10, t.field11, t.field12, t.field13,
                    )
                }
            }

            feldera_types::deserialize_table_record!(UserProps["user_props", Variant, 14] {
                (field0, "bsin", false, SqlString, |_| None),
                (field1, "user_id", false, Option<SqlString>, |_| Some(None)),
                (field2, "email", false, Option<SqlString>, |_| Some(None)),
                (field3, "contacts", false, Option<$variant>, |_| Some(None)),
                (field4, "properties", false, Option<$variant>, |_| Some(None)),
                (field5, "site_id", false, Option<SqlString>, |_| Some(None)),
                (field6, "replaced_by", false, Option<SqlString>, |_| Some(None)),
                (field7, "email_md5", false, Option<SqlString>, |_| Some(None)),
                (field8, "sub_site_ids", false, Option<$variant>, |_| Some(None)),
                (field9, "scoped_properties", false, Option<$variant>, |_| Some(None)),
                (field10, "scoped_contacts", false, Option<$variant>, |_| Some(None)),
                (field11, "consent", false, Option<$variant>, |_| Some(None)),
                (field12, "external_ids", false, Option<$variant>, |_| Some(None)),
                (field13, "merged_bsins", false, Option<$variant>, |_| Some(None))
            });

            /// Parses one record the way the JSON input connector parses it.
            pub fn parse_row(json: &str) -> Row {
                UserProps::deserialize_with_context(
                    &mut serde_json::Deserializer::from_str(json),
                    &JSON_CONFIG,
                )
                .expect("sample record parses")
                .into()
            }

            /// Bytes the row occupies once serialized for storage.
            pub fn archived_bytes(row: &Row) -> usize {
                rkyv::to_bytes::<_, 8192>(row)
                    .expect("row serializes")
                    .len()
            }

            $(
                pub mod $column {
                    use super::*;

                    #[derive(Clone, Debug, Eq, PartialEq, Default, PartialOrd, Ord)]
                    pub struct OneColumn {
                        field0: Option<$variant>,
                    }

                    feldera_types::deserialize_table_record!(OneColumn["user_props", Variant, 1] {
                        (field0, $column_name, false, Option<$variant>, |_| Some(None))
                    });

                    /// Parses just this column out of an unmodified record.
                    pub fn parse(json: &str) -> Option<$variant> {
                        OneColumn::deserialize_with_context(
                            &mut serde_json::Deserializer::from_str(json),
                            &JSON_CONFIG,
                        )
                        .expect("sample record parses")
                        .field0
                    }
                }
            )*

            /// Every VARIANT column, in declaration order.
            pub const COLUMNS: &[(&str, fn(&str) -> Option<$variant>)] =
                &[$(($column_name, $column::parse)),*];
        }
    };
}

/// Names the VARIANT columns once, for every representation under comparison.
macro_rules! user_props_rows {
    ($(($repr:ident, $variant:ident)),*) => {
        $(user_props_row!($repr, $variant,
            (contacts, "contacts"),
            (properties, "properties"),
            (sub_site_ids, "sub_site_ids"),
            (scoped_properties, "scoped_properties"),
            (scoped_contacts, "scoped_contacts"),
            (consent, "consent"),
            (external_ids, "external_ids"),
            (merged_bsins, "merged_bsins")
        );)*
    };
}

user_props_rows!((legacy, Variant), (flat, FlatVariant));

/// Move every VARIANT column of `rows` onto one shared dictionary, the way a
/// batch builder will.
///
/// All eight columns share one dictionary rather than one each, because a
/// batch is one dictionary and the columns of a row repeat each other's field
/// names.
fn intern_rows(rows: &[flat::Row]) -> Vec<flat::Row> {
    intern_rows_with(rows, DocSet::new())
}

/// The same, packing documents into shared chunks instead of one each. Fewer
/// allocations, at the cost of coupling the documents' lifetimes.
fn intern_rows_packed(rows: &[flat::Row]) -> Vec<flat::Row> {
    intern_rows_with(rows, DocSet::packed())
}

fn intern_rows_with(rows: &[flat::Row], mut set: DocSet) -> Vec<flat::Row> {
    for row in rows {
        for doc in [
            &row.3, &row.4, &row.8, &row.9, &row.10, &row.11, &row.12, &row.13,
        ]
        .into_iter()
        .flatten()
        {
            set.push(doc);
        }
    }
    let mut moved = set.finish().into_iter();
    rows.iter()
        .map(|row| {
            let mut row = row.clone();
            for column in [
                &mut row.3,
                &mut row.4,
                &mut row.8,
                &mut row.9,
                &mut row.10,
                &mut row.11,
                &mut row.12,
                &mut row.13,
            ] {
                if column.is_some() {
                    *column = Some(moved.next().expect("one rewired document per column"));
                }
            }
            row
        })
        .collect()
}

// --------------------------------------------------------------- measurement

/// What one representation costs for a batch of rows.
struct Footprint {
    rows: usize,
    /// Bytes of the row itself, replicated per row inside a batch.
    inline: usize,
    /// Bytes requested from the allocator and still live.
    heap: usize,
    /// The same, rounded to jemalloc size classes.
    reserved: usize,
    /// Live allocations the rows are scattered across.
    blocks: usize,
    /// Allocations performed, including the parser's temporaries.
    allocations: usize,
    /// Bytes `SizeOf` reports, which is what the runtime's memory accounting
    /// sees.
    reported: usize,
    /// Bytes the rows occupy once serialized for storage.
    archived: usize,
    parse_time: Duration,
}

impl Footprint {
    fn per_row(&self, total: usize) -> f64 {
        total as f64 / self.rows as f64
    }
}

/// Parses `repeats` copies of `records`, holding every row alive, and reports
/// what they cost.
fn measure<R: SizeOf>(
    records: &[String],
    repeats: usize,
    parse: fn(&str) -> R,
    archived_bytes: fn(&R) -> usize,
) -> Footprint {
    let rows = records.len() * repeats;
    // Allocated before the snapshot so the vector itself stays out of the delta.
    let mut parsed: Vec<R> = Vec::with_capacity(rows);

    let before = Snapshot::take();
    let start = Instant::now();
    for _ in 0..repeats {
        for record in records {
            parsed.push(parse(record));
        }
    }
    let parse_time = start.elapsed();
    let delta = Snapshot::take().since(before);

    Footprint {
        rows,
        inline: size_of::<R>(),
        heap: delta.requested,
        reserved: delta.reserved,
        blocks: delta.blocks,
        allocations: delta.allocations,
        // One context for the whole batch, so a buffer shared by several
        // rows counts once. Summing per row would count a shared dictionary
        // once per row, which is how a batch of 700 rows first reported 54000%
        // of its own heap.
        reported: parsed.size_of().total_bytes(),
        archived: parsed.iter().map(archived_bytes).sum(),
        parse_time,
    }
}

/// Parses `repeats` copies of `records` and then moves every VARIANT column of
/// every `scope` consecutive rows onto one shared dictionary, the way a batch
/// builder will. Reports what the rewired rows cost.
///
/// `scope` is how many rows share a dictionary. A batch holds thousands, but
/// the sweep runs small scopes too because that is where interning stops
/// paying for itself.
fn measure_interned(
    records: &[String],
    repeats: usize,
    scope: usize,
    intern: fn(&[flat::Row]) -> Vec<flat::Row>,
) -> Footprint {
    let rows = records.len() * repeats;
    let mut parsed: Vec<flat::Row> = Vec::with_capacity(rows);
    for _ in 0..repeats {
        for record in records {
            parsed.push(flat::parse_row(record));
        }
    }

    // Only the rewired rows are measured: the sources above are the caller's
    // and would be dropped as each batch seals.
    let mut interned: Vec<flat::Row> = Vec::with_capacity(rows);
    let before = Snapshot::take();
    let start = Instant::now();
    for group in parsed.chunks(scope.max(1)) {
        interned.extend(intern(group));
    }
    let parse_time = start.elapsed();
    let delta = Snapshot::take().since(before);
    drop(parsed);

    Footprint {
        rows,
        inline: size_of::<flat::Row>(),
        heap: delta.requested,
        reserved: delta.reserved,
        blocks: delta.blocks,
        allocations: delta.allocations,
        reported: interned.size_of().total_bytes(),
        archived: interned.iter().map(flat::archived_bytes).sum(),
        parse_time,
    }
}

/// Live heap of one column of `repeats` copies of `records`.
fn measure_column<V>(records: &[String], repeats: usize, parse: fn(&str) -> Option<V>) -> usize {
    let mut parsed: Vec<Option<V>> = Vec::with_capacity(records.len() * repeats);

    let before = Snapshot::take();
    for _ in 0..repeats {
        for record in records {
            parsed.push(parse(record));
        }
    }
    Snapshot::take().since(before).requested
}

// ---------------------------------------------------------------- input data

/// Sample rows, one JSON record per line, gzipped.
const SAMPLE: &[u8] = include_bytes!("data/user_props.jsonl.gz");

fn load_records(path: Option<&str>) -> Vec<String> {
    let raw = match path {
        Some(path) => std::fs::read(path).expect("data file readable"),
        None => SAMPLE.to_vec(),
    };
    let text = if raw.starts_with(&[0x1f, 0x8b]) {
        let mut text = String::new();
        flate2::read::GzDecoder::new(raw.as_slice())
            .read_to_string(&mut text)
            .expect("data file decompresses");
        text
    } else {
        String::from_utf8(raw).expect("data file is UTF-8")
    };

    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_string)
        .collect()
}

/// JSON text bytes each VARIANT column contributes to an average record.
///
/// Re-serializing through `serde_json::Value` reorders keys but preserves
/// length, which is all this is used for.
fn column_json_bytes(records: &[String], column: &str) -> usize {
    let total: usize = records
        .iter()
        .filter_map(|record| {
            let record: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(record).expect("record parses");
            record.get(column).map(|value| value.to_string().len())
        })
        .sum();
    total / records.len()
}

/// Guards the comparison: both representations must hold the same value, or the
/// footprints below describe different data. Also warms every parser, so
/// one-time costs such as the deserializer's field map land outside the
/// measurements.
fn check_representations_agree(records: &[String]) {
    for record in records {
        for (&(column, parse_legacy), &(_, parse_flat)) in legacy::COLUMNS.iter().zip(flat::COLUMNS)
        {
            assert_eq!(
                parse_flat(record).as_ref().map(Variant::from),
                parse_legacy(record),
                "representations disagree on column {column}"
            );
        }
    }
    drop((legacy::parse_row(&records[0]), flat::parse_row(&records[0])));
}

// ---------------------------------------------------------------- reporting

fn kib(bytes: f64) -> String {
    format!("{:.1}", bytes / 1024.0)
}

fn report(name: &str, footprint: &Footprint, json_bytes: f64) {
    let heap = footprint.per_row(footprint.heap);
    println!(
        "{name:<16} {inline:>8} {heap:>8} {reserved:>9} {blocks:>7} {allocations:>7} \
         {archived:>9} {reported:>8} {seen:>7} {amplification:>8} {rate:>8}",
        inline = footprint.inline,
        heap = kib(heap),
        reserved = kib(footprint.per_row(footprint.reserved)),
        blocks = format!("{:.0}", footprint.per_row(footprint.blocks)),
        allocations = format!("{:.0}", footprint.per_row(footprint.allocations)),
        archived = kib(footprint.per_row(footprint.archived)),
        reported = kib(footprint.per_row(footprint.reported)),
        seen = format!(
            "{:.0}%",
            100.0 * footprint.reported as f64 / footprint.heap as f64
        ),
        amplification = format!("{:.1}x", heap / json_bytes),
        rate = format!(
            "{:.0}",
            footprint.rows as f64 / footprint.parse_time.as_secs_f64()
        ),
    );
}

/// Time the operations a batch performs on its keys: comparison, hashing, and
/// the sort a builder runs.
///
/// Interning has to pay for itself here as well as in memory. Comparing two
/// references from one dictionary is an integer compare against a `memcmp`,
/// which should make sorting inside a batch faster, while comparing across two
/// dictionaries has to resolve both sides.
fn report_comparison_costs(records: &[String], repeats: usize) {
    let parsed: Vec<flat::Row> = (0..repeats)
        .flat_map(|_| records.iter().map(|r| flat::parse_row(r)))
        .collect();
    let one_dict = intern_rows(&parsed);
    // Two dictionaries, so a comparison has to resolve both sides.
    let half = parsed.len() / 2;
    let split: Vec<flat::Row> = intern_rows(&parsed[..half])
        .into_iter()
        .chain(intern_rows(&parsed[half..]))
        .collect();

    fn time(label: &str, rows: &[flat::Row], baseline: Option<Duration>) -> Duration {
        // `properties` is the column that dominates the row.
        let docs: Vec<&FlatVariant> = rows.iter().filter_map(|r| r.4.as_ref()).collect();
        let start = Instant::now();
        let mut ord = 0usize;
        for pair in docs.windows(2) {
            ord += (pair[0].cmp(pair[1]) as i8 as isize + 1) as usize;
        }
        let cmp_time = start.elapsed();

        let start = Instant::now();
        let mut sorted: Vec<&FlatVariant> = docs.clone();
        sorted.sort_unstable();
        let sort_time = start.elapsed();

        let start = Instant::now();
        let mut hashes = 0u64;
        for doc in &docs {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            doc.hash(&mut h);
            hashes ^= h.finish();
        }
        let hash_time = start.elapsed();
        std::hint::black_box((ord, sorted.len(), hashes));

        let total = cmp_time + sort_time + hash_time;
        println!(
            "{label:<24} {:>10} {:>10} {:>10} {:>10}",
            format!("{:.0}", docs.len() as f64 / cmp_time.as_secs_f64()),
            format!("{:.0}", docs.len() as f64 / sort_time.as_secs_f64()),
            format!("{:.0}", docs.len() as f64 / hash_time.as_secs_f64()),
            match baseline {
                None => "-".to_string(),
                Some(b) => format!("{:.2}x", b.as_secs_f64() / total.as_secs_f64()),
            },
        );
        total
    }

    println!("comparison cost, documents per second");
    println!(
        "{:<24} {:>10} {:>10} {:>10} {:>10}",
        "keys", "cmp", "sort", "hash", "vs flat",
    );
    let baseline = time("inline (flat)", &parsed, None);
    time("one dictionary", &one_dict, Some(baseline));
    time("two dictionaries", &split, Some(baseline));
    println!();
}

fn main() {
    // `cargo bench` passes `--bench`; take the first non-flag argument as the
    // number of times to replay the sample.
    let repeats: usize = std::env::args()
        .skip(1)
        .find(|arg| !arg.starts_with('-'))
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(20);
    let path = std::env::var("VARIANT_BENCH_DATA").ok();
    let records = load_records(path.as_deref());
    assert!(!records.is_empty(), "no sample records");

    check_size_classes();
    check_representations_agree(&records);

    let json_bytes = records.iter().map(String::len).sum::<usize>() as f64 / records.len() as f64;
    println!(
        "\n{} sample records, {:.0} B of JSON text each, replayed {repeats}x\n",
        records.len(),
        json_bytes,
    );

    println!("per-row footprint, KiB unless noted");
    println!(
        "{:<16} {:>8} {:>8} {:>9} {:>7} {:>7} {:>9} {:>8} {:>7} {:>8} {:>8}",
        "representation",
        "inline B",
        "heap",
        "jemalloc",
        "blocks",
        "allocs",
        "archived",
        "SizeOf",
        "seen",
        "vs JSON",
        "rows/s",
    );

    let legacy = measure(&records, repeats, legacy::parse_row, legacy::archived_bytes);
    report("legacy Variant", &legacy, json_bytes);

    let flat = measure(&records, repeats, flat::parse_row, flat::archived_bytes);
    report("FlatVariant", &flat, json_bytes);

    // Rows per dictionary. A batch holds thousands; the small scopes are
    // where interning stops paying for itself.
    let rows = records.len() * repeats;
    let mut scopes: Vec<usize> = [1, 8, 64, 512]
        .into_iter()
        .filter(|&scope| scope < rows)
        .collect();
    scopes.push(rows);
    let mut interned = Vec::new();
    for &scope in &scopes {
        let footprint = measure_interned(&records, repeats, scope, intern_rows);
        report(&format!("  dict/{scope} rows"), &footprint, json_bytes);
        interned.push((scope, footprint));
    }
    // Packing is a separate milestone; report it once so its upside is visible
    // without conflating it with what the dictionary itself buys.
    let widest_scope = *scopes.last().expect("at least one scope");
    let packed = measure_interned(&records, repeats, widest_scope, intern_rows_packed);
    report("  + packed chunks", &packed, json_bytes);

    println!(
        "\nflat/legacy: heap {:.2}x, jemalloc {:.2}x, live blocks {:.4}x, archived {:.2}x, \
         parse {:.2}x",
        flat.heap as f64 / legacy.heap as f64,
        flat.reserved as f64 / legacy.reserved as f64,
        flat.blocks as f64 / legacy.blocks as f64,
        flat.archived as f64 / legacy.archived as f64,
        flat.parse_time.as_secs_f64() / legacy.parse_time.as_secs_f64(),
    );
    let widest = interned.last().expect("at least one scope");
    println!(
        "dict/flat at {} rows per dictionary: heap {:.2}x, jemalloc {:.2}x, live blocks {:.2}x\n",
        widest.0,
        flat.heap as f64 / widest.1.heap as f64,
        flat.reserved as f64 / widest.1.reserved as f64,
        flat.blocks as f64 / widest.1.blocks as f64,
    );

    report_comparison_costs(&records, repeats);

    println!("per-column heap, bytes per row");
    println!(
        "{:<20} {:>8} {:>10} {:>10} {:>11}",
        "column", "json B", "legacy", "flat", "flat/legacy",
    );
    let rows = (records.len() * repeats) as f64;
    let mut columns: Vec<_> = legacy::COLUMNS
        .iter()
        .zip(flat::COLUMNS)
        .map(|(&(column, parse_legacy), &(_, parse_flat))| {
            let json = column_json_bytes(&records, column);
            let legacy = measure_column(&records, repeats, parse_legacy);
            let flat = measure_column(&records, repeats, parse_flat);
            (column, json, legacy, flat)
        })
        .collect();
    columns.sort_by_key(|(_, _, legacy, _)| std::cmp::Reverse(*legacy));
    for (column, json, legacy, flat) in columns {
        println!(
            "{column:<20} {json:>8} {:>10.0} {:>10.0} {:>11}",
            legacy as f64 / rows,
            flat as f64 / rows,
            format!("{:.2}x", flat as f64 / legacy as f64),
        );
    }

    println!(
        "\nheap: live bytes requested. jemalloc: the same rounded to size classes. \
         blocks: live allocations.\nallocs: allocations made, parser temporaries included. \
         archived: rkyv bytes. seen: SizeOf/heap,\nthe share of the row the runtime's memory \
         accounting observes.\n"
    );
}
