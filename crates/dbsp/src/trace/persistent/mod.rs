//! This module implements logic and datastructures to provide a trace that is
//! using on-disk storage with the help of RocksDB.

use std::cmp::Ordering;

use once_cell::sync::Lazy;
use rkyv::{validation::validators::DefaultValidator, Archive, Deserialize, Serialize};
use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options, DB};
use uuid::Uuid;

mod cursor;
mod tests;
mod trace;

/// A single value with many time and weight tuples.
type ValueTimeWeights<T, R> = Vec<(T, R)>;

/// A collection of values with time and weight tuples, this is the type that we
/// persist in RocksDB under values.
type Values<T, R> = ValueTimeWeights<T, R>;

/// The cursor for the persistent trace.
pub use cursor::PersistentTraceCursor;
/// The persistent trace itself, it should be equivalent to the [`Spine`].
pub use trace::PersistentTrace;

use crate::DBData;

use super::Deserializable;

/// DB in-memory cache size [bytes].
///
/// # TODO
/// Set to 1 GiB for now, in the future we should probably make this
/// configurable or determine it based on the system parameters.
const DB_DRAM_CACHE_SIZE: usize = 4 * 1024 * 1024 * 1024;

/// Path of the RocksDB database file on disk.
///
/// # TODO
/// Eventually should be supplied as a command-line argument or provided in a
/// config file.
static DB_PATH: Lazy<String> = Lazy::new(|| format!("/tmp/{}.db", Uuid::new_v4()));

/// Options for the RocksDB database
static DB_OPTS: Lazy<Options> = Lazy::new(|| {
    let cache = Cache::new_lru_cache(DB_DRAM_CACHE_SIZE);
    let mut global_opts = Options::default();
    // Create the database file if it's missing (the default behavior)
    global_opts.create_if_missing(true);
    // Disable compression for now, this will help with benchmarking but
    // probably not ideal for a production setting
    global_opts.set_compression_type(DBCompressionType::None);
    // Ensure we use a shared cache for all column families
    global_opts.set_row_cache(&cache);
    // RocksDB doesn't like to close files by default, if we set this it limits
    // the number of open files by closing them again (should be set in
    // accordance with ulimit)
    global_opts.set_max_open_files(9000);
    global_opts.set_use_direct_reads(true);

    let mut bbo = BlockBasedOptions::default();
    bbo.set_format_version(5);
    //let cache = Cache::new_lru_cache(DB_DRAM_CACHE_SIZE);
    //bbo.set_block_cache(&cache);
    //bbo.set_block_size(16 * 4096);
    //bbo.set_cache_index_and_filter_blocks(true);
    //bbo.set_pin_l0_filter_and_index_blocks_in_cache(true);
    //bbo.set_pin_top_level_index_and_filter(true);
    global_opts.set_block_based_table_factory(&bbo);

    global_opts.set_enable_blob_files(true);

    // Some options (that seem to hurt more than help -- needs more
    // experimentation):
    //global_opts.increase_parallelism(2);
    //global_opts.set_max_background_jobs(2);
    //global_opts.set_max_write_buffer_number(2);
    //global_opts.set_write_buffer_size(1024*1024*4);
    //global_opts.set_target_file_size_base(1024*1024*8);

    global_opts
});

/// The RocksDB instance that holds all traces (in different columns).
static ROCKS_DB_INSTANCE: Lazy<DB> = Lazy::new(|| {
    // Open the database (or create it if it doesn't exist
    DB::open(&DB_OPTS, DB_PATH.clone()).unwrap()
});

/// The format of the 'key' we store in RocksDB.
///
/// The ordering is important for Ord/PartialOrd to work correctly:
/// https://doc.rust-lang.org/reference/items/enumerations.html#implicit-discriminants
#[derive(Debug, Archive, PartialEq, Eq, Deserialize, PartialOrd, Serialize)]
#[archive_attr(derive(Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "K: std::cmp::Eq, <K as super::Deserializable>::ArchivedDeser: std::cmp::Eq + Ord, V: std::cmp::Eq, <V as super::Deserializable>::ArchivedDeser: std::cmp::Eq + Ord"
))]
#[archive_attr(repr(align(4)))]
pub(self) enum PersistedKey<K: DBData, V: DBData> {
    /// A (phantom) marker to indicate the start of a key's values.
    ///
    /// This is used by [`rocksdb_key_comparator`] to find the beginning of a key.
    /// It should never be stored itself as a key.
    ///
    /// TODO: Would be nice to be able to mark this as non-serializable for rykv
    /// and panics if we attempt to do it.
    StartMarker(K),
    /// A dbsp key--value pair that is persisted in a RocksDB key.
    Key(K, V),
    /// A (phantom marker) to indicate the end of a key's values.
    ///
    /// See [`StartMarker`] for more details.
    EndMarker(K),
}

impl<K, V> TryFrom<PersistedKey<K, V>> for (K, V)
where
    K: DBData,
    V: DBData,
{
    type Error = &'static str;

    fn try_from(value: PersistedKey<K, V>) -> Result<Self, Self::Error> {
        match value {
            PersistedKey::Key(k, v) => Ok((k, v)),
            _ => Err("Cannot convert non-key to (key, value)"),
        }
    }
}
/*
impl<K, V> PartialOrd for ArchivedPersistedKey<K, V>
where
    K: DBData,
    V: DBData,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
*/
impl<K, V> Ord for ArchivedPersistedKey<K, V>
where
    K: DBData,
    V: DBData,
    <K as Deserializable>::ArchivedDeser: Ord,
    <V as Deserializable>::ArchivedDeser: Ord,
{
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ArchivedPersistedKey::StartMarker(k1), ArchivedPersistedKey::StartMarker(k2)) => {
                k1.cmp(k2)
            }
            (ArchivedPersistedKey::StartMarker(k1), ArchivedPersistedKey::Key(k2, _)) => {
                k1.cmp(k2).then(Ordering::Less)
            }
            (ArchivedPersistedKey::StartMarker(_), ArchivedPersistedKey::EndMarker(_)) => {
                Ordering::Less
            }
            (ArchivedPersistedKey::Key(k1, _), ArchivedPersistedKey::StartMarker(k2)) => {
                k1.cmp(k2).then(Ordering::Greater)
            }
            (ArchivedPersistedKey::Key(k1, v1), ArchivedPersistedKey::Key(k2, v2)) => {
                k1.cmp(k2).then(v1.cmp(v2))
            }
            (ArchivedPersistedKey::Key(k1, _), ArchivedPersistedKey::EndMarker(k2)) => {
                k1.cmp(k2).then(Ordering::Less)
            }
            (ArchivedPersistedKey::EndMarker(k1), ArchivedPersistedKey::EndMarker(k2)) => {
                k1.cmp(k2)
            }

            (ArchivedPersistedKey::EndMarker(k1), ArchivedPersistedKey::Key(k2, _)) => {
                k1.cmp(k2).then(Ordering::Greater)
            }
            (ArchivedPersistedKey::EndMarker(_), ArchivedPersistedKey::StartMarker(_)) => {
                Ordering::Greater
            }
        }
    }
}

impl<K, V> Ord for PersistedKey<K, V>
where
    K: DBData,
    V: DBData,
{
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (PersistedKey::StartMarker(k1), PersistedKey::StartMarker(k2)) => k1.cmp(k2),
            (PersistedKey::StartMarker(k1), PersistedKey::Key(k2, _)) => {
                k1.cmp(k2).then(Ordering::Less)
            }
            (PersistedKey::StartMarker(_), PersistedKey::EndMarker(_)) => Ordering::Less,
            (PersistedKey::Key(k1, _), PersistedKey::StartMarker(k2)) => {
                k1.cmp(k2).then(Ordering::Greater)
            }
            (PersistedKey::Key(k1, v1), PersistedKey::Key(k2, v2)) => k1.cmp(k2).then(v1.cmp(v2)),
            (PersistedKey::Key(k1, _), PersistedKey::EndMarker(k2)) => {
                k1.cmp(k2).then(Ordering::Less)
            }
            (PersistedKey::EndMarker(k1), PersistedKey::EndMarker(k2)) => k1.cmp(k2),
            (PersistedKey::EndMarker(k1), PersistedKey::Key(k2, _)) => {
                k1.cmp(k2).then(Ordering::Greater)
            }
            (PersistedKey::EndMarker(_), PersistedKey::StartMarker(_)) => Ordering::Greater,
        }
    }
}

/// Wrapper function for doing key comparison in RockDB.
///
/// It works by deserializing the keys and then comparing it (as opposed to the
/// byte-wise comparison which is the default in RocksDB).
pub(self) fn rocksdb_key_comparator<K, V>(a: &[u8], b: &[u8]) -> Ordering
where
    K: DBData,
    V: DBData,
    <K as super::Deserializable>::ArchivedDeser: Ord,
    <V as super::Deserializable>::ArchivedDeser: Ord,
{
    log::info!("Comparing keys: {:?} {:?}", a, b);
    let a = unsafe { rkyv::archived_root::<PersistedKey<K, V>>(a) };
    log::info!("parsed a");
    let b = unsafe { rkyv::archived_root::<PersistedKey<K, V>>(b) };
    log::info!("compared:");

    a.cmp(b)
    //let a: PersistedKey<K, V> = super::unaligned_deserialize(a);
    //let b: PersistedKey<K, V> = super::unaligned_deserialize(b);
    //a.cmp(&b)
}
