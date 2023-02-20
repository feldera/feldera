//! This module implements logic and datastructures to provide a trace that is
//! using on-disk storage with the help of RocksDB.

use std::cmp::Ordering;

use bincode::{
    config::{BigEndian, Fixint},
    decode_from_slice,
    enc::write::Writer,
    error::EncodeError,
    Decode, Encode,
};
use once_cell::sync::Lazy;
use rocksdb::{Cache, DBCompressionType, Options, DB};
use uuid::Uuid;

mod cursor;
mod tests;
mod trace;

/// A single value with many time and weight tuples.
type ValueTimeWeights<V, T, R> = (V, Vec<(T, R)>);

/// A collection of values with time and weight tuples, this is the type that we
/// persist in RocksDB under values.
type Values<V, T, R> = Vec<ValueTimeWeights<V, T, R>>;

/// The cursor for the persistent trace.
pub use cursor::PersistentTraceCursor;
/// The persistent trace itself, it should be equivalent to the [`Spine`].
pub use trace::PersistentTrace;

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
    let cache = Cache::new_lru_cache(DB_DRAM_CACHE_SIZE).expect("Can't create cache for DB");
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

/// Configuration we use for encodings/decodings to/from RocksDB data.
static BINCODE_CONFIG: bincode::config::Configuration<BigEndian, Fixint> =
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_big_endian();

/// Wrapper function for doing key comparison in RockDB.
///
/// It works by deserializing the keys and then comparing it (as opposed to the
/// byte-wise comparison which is the default in RocksDB).
pub(self) fn rocksdb_key_comparator<K: Decode + Ord>(a: &[u8], b: &[u8]) -> Ordering {
    let (key_a, _) = decode_from_slice::<K, _>(a, BINCODE_CONFIG).expect("Can't decode_from_slice");
    let (key_b, _) = decode_from_slice::<K, _>(b, BINCODE_CONFIG).expect("Can't decode_from_slice");
    key_a.cmp(&key_b)
}

/// A buffer that holds an encoded value.
///
/// Useful to keep around in code where serialization happens repeatedly as it
/// can avoid repeated [`Vec`] allocations.
#[derive(Default)]
struct ReusableEncodeBuffer(Vec<u8>);

impl ReusableEncodeBuffer {
    /// Creates a buffer with initial capacity of `cap` bytes.
    fn with_capacity(cap: usize) -> Self {
        ReusableEncodeBuffer(Vec::with_capacity(cap))
    }

    /// Encodes `val` into the buffer owned by this struct.
    ///
    /// # Returns
    /// - An error if encoding failed.
    /// - A reference to the buffer where `val` was encoded into. Makes sure the
    ///   buffer won't change until the reference out-of-scope again.
    fn encode<T: Encode>(&mut self, val: &T) -> Result<&[u8], EncodeError> {
        self.0.clear();
        bincode::encode_into_writer(val, &mut *self, BINCODE_CONFIG)?;
        Ok(&self.0)
    }
}

/// We can get the internal storage if we don't need the ReusableEncodeBuffer
/// anymore with the `From` trait.
impl From<ReusableEncodeBuffer> for Vec<u8> {
    fn from(r: ReusableEncodeBuffer) -> Vec<u8> {
        r.0
    }
}

impl Writer for &mut ReusableEncodeBuffer {
    /// Allows bincode to write into the buffer.
    ///
    /// # Note
    /// Client needs to ensure that the buffer is cleared in advance if we store
    /// something new. When possible use the [`Self::encode`] method instead
    /// which takes care of that.
    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.0.extend(bytes);
        Ok(())
    }
}
