//! What does splicing a layer file's values save over re-encoding them?
//!
//! A spine merge decodes every value out of its inputs and encodes it again
//! into its output.  Where one input alone supplies a stretch of the output,
//! the bytes already say what the output has to say, and they can be copied
//! instead.  This benchmark isolates that saving: it copies one layer file to
//! another twice, once through the ordinary write path and once by splicing,
//! over exactly the same data.
//!
//! The copy is not a merge, so the figure here is an upper bound on what a
//! merge could save: a merge still has to compare keys, step its cursors and
//! compress and write its output, none of which a splice touches.
//!
//!     cargo bench --bench splice_copy -- [--values N] [--keys N] [--repeat N]

use std::sync::Arc;
use std::time::{Duration, Instant};

use dbsp::{
    dynamic::{DynData, DynWeight, Erase},
    storage::buffer_cache::BufferCache,
    storage::{
        backend::StorageBackend,
        file::{
            Factories,
            format::{BatchMetadata, Compression},
            reader::Reader,
            writer::{Parameters, Writer2},
        },
    },
};
use feldera_types::config::{StorageConfig, StorageOptions};
use tempfile::{TempDir, tempdir};

type K0 = String;
type A0 = ();
type K1 = u64;
type A1 = i64;

type Cols = (
    &'static DynData,
    &'static DynData,
    (&'static DynData, &'static DynWeight, ()),
);

struct Args {
    keys: usize,
    values: usize,
    repeat: usize,
    compression: Option<Compression>,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            keys: 200_000,
            values: 1,
            repeat: 3,
            compression: Some(Compression::Snappy),
        };
        let argv: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < argv.len() {
            let flag = argv[i].clone();
            let number = |i: &mut usize| -> usize {
                *i += 1;
                argv.get(*i)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| panic!("{flag} needs a number"))
            };
            match argv[i].as_str() {
                "--keys" => args.keys = number(&mut i),
                "--values" => args.values = number(&mut i),
                "--repeat" => args.repeat = number(&mut i),
                "--no-compression" => args.compression = None,
                _ => {}
            }
            i += 1;
        }
        args
    }
}

/// The writer caches the blocks it produces, so it needs somewhere to put
/// them.  One cache per thread is all this single-threaded benchmark wants.
fn buffer_cache() -> Option<Arc<BufferCache>> {
    thread_local! {
        static CACHE: Arc<BufferCache> = Arc::new(BufferCache::new(256 << 20));
    }
    Some(CACHE.with(|cache| cache.clone()))
}

/// A key whose length varies, so items do not all encode to one size.
fn key0(row: usize) -> K0 {
    format!("key-{row:08}-{}", "y".repeat(row % 29))
}

fn value(row: usize, i: usize) -> (K1, A1) {
    ((row * 16 + i) as u64, (row as i64) % 7 - 3)
}

fn parameters(compression: Option<Compression>) -> Parameters {
    Parameters {
        compression,
        ..Parameters::default()
    }
}

fn backend(dir: &TempDir) -> Arc<dyn StorageBackend> {
    <dyn StorageBackend>::new(
        &StorageConfig {
            path: dir.path().to_string_lossy().to_string(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )
    .unwrap()
}

fn writer(
    backend: &dyn StorageBackend,
    args: &Args,
) -> Writer2<DynData, DynData, DynData, DynWeight> {
    Writer2::new(
        &Factories::<DynData, DynData>::new::<K0, A0>(),
        &Factories::<DynData, DynWeight>::new::<K1, A1>(),
        buffer_cache,
        backend,
        parameters(args.compression),
        dbsp::storage::file::FilterPlan::<DynData>::decide_filter(None, args.keys),
    )
    .unwrap()
}

/// A writer with no membership filter, which splicing keys needs: the filter
/// hashes a decoded key and a splice never decodes one.
fn writer_unfiltered(
    backend: &dyn StorageBackend,
    args: &Args,
) -> Writer2<DynData, DynData, DynData, DynWeight> {
    Writer2::new(
        &Factories::<DynData, DynData>::new::<K0, A0>(),
        &Factories::<DynData, DynWeight>::new::<K1, A1>(),
        buffer_cache,
        backend,
        parameters(args.compression),
        None,
    )
    .unwrap()
}

fn build(backend: &dyn StorageBackend, args: &Args) -> Reader<Cols> {
    let mut w = writer(backend, args);
    for row in 0..args.keys {
        for i in 0..args.values {
            let (mut k, mut a) = value(row, i);
            w.write1((k.erase_mut(), a.erase_mut())).unwrap();
        }
        let (mut k, mut a) = (key0(row), ());
        w.write0((k.erase_mut(), a.erase_mut())).unwrap();
    }
    w.into_reader(BatchMetadata::default()).unwrap().0
}

/// Copies `source` by decoding each item and encoding it again.
///
/// Walks both columns with one cursor each, exactly as the spliced copies do,
/// so that the only difference between them is how the items get written.  A
/// version that reached each key through the tree instead would be paying for
/// the descent, not for the encoding, and would flatter the splice.
fn copy_decoded(source: &Reader<Cols>, backend: &dyn StorageBackend, args: &Args) -> Duration {
    let start = Instant::now();
    let mut w = writer_unfiltered(backend, args);
    let rows0 = source.rows();
    let rows1 = source.all_value_rows();
    let mut value_cursor = unsafe { rows1.first() }.unwrap();
    let mut values_at = 0u64;
    let mut keys = unsafe { rows0.first() }.unwrap();
    let (mut k0, mut a0) = (K0::default(), A0::default());
    let (mut k1, mut a1) = (K1::default(), A1::default());
    while keys.has_value() {
        let needed = keys.raw_run_with_row_groups().unwrap().1[1];
        while values_at < needed {
            let item = unsafe { value_cursor.item((k1.erase_mut(), a1.erase_mut())) }.unwrap();
            w.write1((item.0, item.1)).unwrap();
            values_at += 1;
            unsafe { value_cursor.move_next() }.unwrap();
        }
        let item = unsafe { keys.item((k0.erase_mut(), a0.erase_mut())) }.unwrap();
        w.write0((item.0, item.1)).unwrap();
        unsafe { keys.move_next() }.unwrap();
    }
    w.into_reader(BatchMetadata::default()).unwrap();
    start.elapsed()
}

/// Copies `source` by moving runs of both columns as they stand, which is what
/// a merge that found a stretch of the output to one input would do.
///
/// A run stops at a key block's edge, so this is the shape of a merge whose
/// inputs hold long disjoint stretches of the key space, not of one whose
/// inputs interleave.
fn copy_spliced_both(source: &Reader<Cols>, backend: &dyn StorageBackend, args: &Args) -> Duration {
    let start = Instant::now();
    let mut w = writer_unfiltered(backend, args);
    let rows0 = source.rows();
    // One cursor over the whole value column, so a run of values spanning many
    // keys goes in one call instead of one call a key.
    let rows1 = source.all_value_rows();
    let mut values = unsafe { rows1.first() }.unwrap();
    let mut values_at = 0u64;
    let mut keys = unsafe { rows0.first() }.unwrap();
    let mut at = 0u64;
    while keys.has_value() {
        let (wanted, needed) = {
            let (items, boundaries) = keys.raw_run_with_row_groups().unwrap();
            (items.roots.len(), boundaries[boundaries.len() - 1])
        };
        while values_at < needed {
            let run = values.raw_run().unwrap();
            let took = w.write1_raw(&run).unwrap() as u64;
            assert!(took > 0, "the value splice stalled");
            values_at += took;
            unsafe { values.move_to_row(values_at) }.unwrap();
        }
        let mut took = 0usize;
        while took < wanted {
            unsafe { keys.move_to_row(at + took as u64) }.unwrap();
            let more = {
                let (items, boundaries) = keys.raw_run_with_row_groups().unwrap();
                w.write0_raw(&items, &boundaries).unwrap()
            };
            assert!(more > 0, "the key splice stalled");
            took += more;
        }
        at += took as u64;
        unsafe { keys.move_to_row(at) }.unwrap();
    }
    w.into_reader(BatchMetadata::default()).unwrap();
    start.elapsed()
}

/// Copies `source` by moving each run of encoded values as it stands.
fn copy_spliced(source: &Reader<Cols>, backend: &dyn StorageBackend, args: &Args) -> Duration {
    let start = Instant::now();
    let mut w = writer(backend, args);
    let rows0 = source.rows();
    for row in 0..args.keys {
        let keys = rows0.nth(row as u64).unwrap();
        let rows1 = keys.next_column().unwrap();
        let mut c = unsafe { rows1.first() }.unwrap();
        let mut taken = 0u64;
        while c.has_value() {
            let run = c
                .raw_run()
                .expect("a block this writer wrote can be spliced");
            let took = w.write1_raw(&run).unwrap();
            assert!(took > 0, "the splice stalled");
            taken += took as u64;
            unsafe { c.move_to_row(taken) }.unwrap();
        }
        let mut k = key0(row);
        let mut a = ();
        w.write0((k.erase_mut(), a.erase_mut())).unwrap();
    }
    w.into_reader(BatchMetadata::default()).unwrap();
    start.elapsed()
}

fn main() {
    let args = Args::parse();
    let dir = tempdir().unwrap();
    let backend = backend(&dir);
    let source = build(&*backend, &args);
    let tuples = args.keys * args.values;
    println!(
        "copying {} keys x {} values ({} tuples), compression {}",
        args.keys,
        args.values,
        tuples,
        if args.compression.is_some() {
            "on"
        } else {
            "off"
        },
    );

    let mut decoded = Duration::MAX;
    let mut values_only = Duration::MAX;
    let mut both = Duration::MAX;
    for _ in 0..args.repeat {
        decoded = decoded.min(copy_decoded(&source, &*backend, &args));
        values_only = values_only.min(copy_spliced(&source, &*backend, &args));
        both = both.min(copy_spliced_both(&source, &*backend, &args));
    }
    let per = |d: Duration| d.as_secs_f64() * 1e9 / tuples as f64;
    let saving = |d: Duration| 100.0 * (1.0 - d.as_secs_f64() / decoded.as_secs_f64());
    println!(
        "  re-encoded        {:>8.2} ns/tuple  ({:.3}s)",
        per(decoded),
        decoded.as_secs_f64()
    );
    println!(
        "  spliced values    {:>8.2} ns/tuple  ({:.3}s)  {:+.1}%",
        per(values_only),
        values_only.as_secs_f64(),
        saving(values_only),
    );
    println!(
        "  spliced both      {:>8.2} ns/tuple  ({:.3}s)  {:+.1}%",
        per(both),
        both.as_secs_f64(),
        saving(both),
    );
}
