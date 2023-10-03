//! This module implements logic and datastructures to provide a trace that is
//! using on-disk storage with the help of RocksDB.

use std::cmp::Ordering;

use once_cell::sync::Lazy;
use rocksdb::{Cache, DBCompressionType, Options, DB};
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

use super::{unaligned_deserialize, Deserializable};

/// DB in-memory cache size [bytes].
///
/// # TODO
/// Set to 1 GiB for now, in the future we should probably make this
/// configurable or determine it based on the system parameters.
const DB_DRAM_CACHE_SIZE: usize = 1024 * 1024 * 1024;

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
pub(self) type PersistedKey<K, V> = (K, Option<V>);

/// Wrapper function for doing key comparison in RockDB.
///
/// It works by deserializing the keys and then comparing it (as opposed to the
/// byte-wise comparison which is the default in RocksDB).
pub(self) fn rocksdb_key_comparator<K: Deserializable + Ord, V: Deserializable + Ord>(
    a: &[u8],
    b: &[u8],
) -> Ordering {
    let (key_a, val_a): PersistedKey<K, V> = unaligned_deserialize(a);
    let (key_b, val_b): PersistedKey<K, V> = unaligned_deserialize(b);
    key_a.cmp(&key_b).then(val_a.cmp(&val_b))
}
