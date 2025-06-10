use std::{
    cell::RefCell,
    collections::VecDeque,
    hash::{BuildHasherDefault, Hasher},
    panic::Location,
    sync::{LazyLock, RwLock},
};

use dbsp::{
    dynamic::{DowncastTrait, DynData},
    operator::communication::new_exchange_operators,
    trace::{
        BatchReader, BatchReaderFactories, Cursor, OrdIndexedWSet as DynOrdIndexedWSet,
        OrdIndexedWSetFactories, SpineSnapshot,
    },
    utils::Tup1,
    Circuit, DynZWeight, OrdZSet, RootCircuit, Runtime, Stream, ZWeight,
};
use quick_cache::{
    sync::{Cache, DefaultLifecycle, GuardResult},
    OptionsBuilder, Weighter,
};

use crate::{SqlString, Uuid};

// TODO: experiment with shard counts.

/// Estimated number of cache entries used by quick_cache to provision internal resources.
const CACHE_CAPACITY: usize = 1 << 26;

/// Maximum weight of the cache, in bytes.
///
/// We use memory occupied by each cache entry as weight; so this roughly limits cache to 1GiB
/// plus new entries created during the last two steps, which are pinned in the cache by setting
/// their weight to 0.
const WEIGHT_CAPACITY: u64 = 1 << 30;

/// FIXME. We use 128-bit integers to represent interned strings.
/// The Uuid type is the closest thing we have in SQL. Once we have compiler support for
/// string interning we will be able to use a separate type for this.
pub type InternedStringId = Uuid;

/// String and a flag that indicates whether the string should be pinned in the cache.
type InternedString = (SqlString, bool);

/// Keys in the interned string cache are already hashes; we don't need to hash them again.
/// This hasher just uses the first 8 bytes of the 128-bit key as the hash.
#[derive(Default)]
struct IdentityHasher {
    hash: u64,
}

impl Hasher for IdentityHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        debug_assert_eq!(bytes.len(), 16, "Expected 16 bytes");
        self.hash = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}
type BuildIdentityHasher = BuildHasherDefault<IdentityHasher>;

/// Weighter for the interned string cache. It uses the length of the string plus the size of the
/// key as the weight. If the string is pinned, its weight is 0, so it will not be evicted from the
/// cache.
#[derive(Clone)]
struct StringWeighter;

impl Weighter<InternedStringId, InternedString> for StringWeighter {
    fn weight(&self, _key: &InternedStringId, val: &InternedString) -> u64 {
        if val.1 {
            0
        } else {
            (val.0.len() + size_of::<InternedStringId>()) as u64
                + size_of::<InternedString>() as u64
        }
    }
}

thread_local! {
    /// Current step number, used to pin recently created strings in the cache.
    /// Incremented by each worker on each step.
    static CURRENT_STEP: RefCell<u64> = const { RefCell::new(0) };

    /// List of pinned strings. On each step, each worker thread unpins and removes
    /// from the list strings pinned >= 2 steps ago.
    static PINNED_STRINGS: RefCell<VecDeque<(InternedStringId, (SqlString, u64))>> = const { RefCell::new(VecDeque::new()) };
}

/// Indexed Z-set that maps interned string IDs back to strings for use by `unintern`.
///
/// Each worker maintains its own shard of this Z-set.
/// Worker 0 collects references to all batches and stores them in this global variable at each step.
static INTERNED_STRING_BY_ID: LazyLock<
    RwLock<SpineSnapshot<DynOrdIndexedWSet<DynData, DynData, DynZWeight>>>,
> = LazyLock::new(|| RwLock::new(empty_by_id()));

/// In-memory cache of interned strings.
///
/// This cache serves two purposes.
/// 1. Fast interned string lookup. The backing store for interned strings is a spine snapshot,
///    where point lookups can require I/O and are generally more expensive than a hash map lookup
///    used here.
/// 2. Provide access to recently interned strings (specifically strings interned during the last
///    2 steps) that are not yet guaranteed to be in the indexed Z-set.
///
// We use the `quick_cache` crate, which in my very limited benchmarking is faster than `lru`,
// `moka`, and `foyer`.
type InternedStringCache = Cache<
    InternedStringId,
    InternedString,
    StringWeighter,
    BuildIdentityHasher,
    DefaultLifecycle<InternedStringId, InternedString>,
>;

fn init_interned_string_cache() -> InternedStringCache {
    Cache::with_options(
        OptionsBuilder::new()
            .estimated_items_capacity(CACHE_CAPACITY)
            .weight_capacity(WEIGHT_CAPACITY)
            .build()
            .unwrap(),
        StringWeighter,
        BuildIdentityHasher::default(),
        DefaultLifecycle::default(),
    )
}

#[allow(clippy::type_complexity)]
static INTERNED_STRING_CACHE: LazyLock<InternedStringCache> =
    LazyLock::new(|| init_interned_string_cache());

/// Hash a string to a 128-bit (probabilistically) unique id.
fn hash_string(s: &SqlString) -> Uuid {
    // Use the first 16 bytes of the BLAKE3 hash. 16 bytes is enough to prevent cache
    // collisions for up to 2^64 strings.
    let hash = blake3::hash(s.str().as_bytes());
    let bytes = hash.as_bytes();
    Uuid::from_bytes(bytes[0..16].try_into().unwrap())
}

/// Intern a string.
///
/// Returns an opaque ID that can be passed to `unintern_string` to retrieve the original string.
///
/// IMPORTANT: this function only adds the string to INTERNED_STRING_CACHE, where it can get evicted
/// after two steps. To store the mapping permanently, it needs to be added to a spine, which is
/// what `build_string_interner` does.
pub fn intern_string(s: &SqlString) -> InternedStringId {
    let id = hash_string(s);

    // Insert string into the cache with pinned flag set to true, so it doesn't get evicted.
    // Record the string in the PINNED_STRINGS list, so it can be unpinned later.
    if let GuardResult::Guard(g) = INTERNED_STRING_CACHE.get_value_or_guard(&id, None) {
        let current_step = CURRENT_STEP.with_borrow(|step| *step);
        let val = (s.clone(), true);
        // The record should be admitted, as it has weight 0.
        g.insert(val)
            .expect("Failed to insert into interned string cache");
        PINNED_STRINGS
            .with_borrow_mut(|pinned| pinned.push_back((id.clone(), (s.clone(), current_step))));
    };

    id
}

/// Returns the original string given its interned id.
pub fn unintern_string(id: &InternedStringId) -> Option<SqlString> {
    // Lookup the string in the cache first.
    // If the string is not in the cache, look it up in the spine.
    INTERNED_STRING_CACHE
        .get(id)
        .map(|(string, _step)| string)
        .or_else(|| {
            let mut cursor = INTERNED_STRING_BY_ID.read().unwrap().cursor();
            if cursor.seek_key_exact(id) {
                let val = unsafe { cursor.val().downcast::<Tup1<SqlString>>().0.clone() };
                // Insert the string into the cache with pinned flag set to false, so it can be evicted.
                INTERNED_STRING_CACHE.insert(id.clone(), (val.clone(), false));
                Some(val)
            } else {
                None
            }
        })
}

/// Create an empty spine snapshot.
fn empty_by_id() -> SpineSnapshot<DynOrdIndexedWSet<DynData, DynData, DynZWeight>> {
    let factories: OrdIndexedWSetFactories<DynData, DynData, DynZWeight> =
        BatchReaderFactories::new::<InternedStringId, Tup1<SqlString>, ZWeight>();

    SpineSnapshot::<DynOrdIndexedWSet<DynData, DynData, DynZWeight>>::new(factories)
}

/// Build the string interner circuit.
///
/// Takes a stream that contains strings to be interned, and sets up a spine snapshot
/// that maps interned string IDs back to strings. The spine snapshot is stored in the
/// `INTERNED_STRING_BY_ID` global variable, and is updated on each step of the circuit.
///
// ```text
//                                               ┌──────────┐                      ┌────────────────────────┐
//                                               │integrate │                      │ Global interner state  │
//                                               │  ┌───┐   │       ┌───────┐by_id │                        │
//                                               │  │Z-1├───┼──────►│gather ├─────►│ INTERNED_STRINGS_BY_ID │
//                                               │  └──┬┘   │       └───────┘      │ CURRENT_STEP           │
//                                               │   ▲ │    │                      │ PINNED_STRINGS         │
//                                               │   │ ▼    │                      └────────────────────────┘
//   feldera_interned_strings ┌─────────┐        │  ┌┴──┐   │
// ──────────────────────────►│map_index├────────┼─►│ + │   │
//                            └─────────┘        │  └───┘   │
//                                               └──────────┘
// ```
pub fn build_string_interner(interned_strings: Stream<RootCircuit, OrdZSet<Tup1<SqlString>>>) {
    // Intern input strings, index them by interned string ID, and store them in a spine.
    //
    // The last step below `.delay_trace()` makes sure that we work with the spine snapshot
    // from the previous step, hence any entries deleted at the current step will still be
    // present. This is important, because even after the string is removed from the spine,
    // it may still be used during the current step. At the same time, all newly added strings
    // that are not in the spine are guaranteed to be in the cache.
    let by_id = interned_strings
        .map_index(|s| (intern_string(&s.0), s.clone()))
        .shard()
        .set_persistent_id(Some("feldera_interned_string_by_id"))
        .integrate_trace()
        .inner()
        .delay_trace();

    // Collect spine snapshots from all workers and merge them into a single spine snapshot in worker 0.
    let by_id = if let Some(runtime) = Runtime::runtime() {
        let num_workers = runtime.num_workers();
        let (sender, receiver) = new_exchange_operators(
            &runtime,
            Runtime::worker_index(),
            Some(Location::caller()),
            empty_by_id,
            move |spine: SpineSnapshot<_>, outputs| {
                outputs.push(spine.clone());
                for _ in 1..num_workers {
                    outputs.push(empty_by_id());
                }
            },
            |snapshot, remote_snapshot| {
                if Runtime::worker_index() == 0 {
                    snapshot.extend(remote_snapshot);
                }
            },
        );
        interned_strings
            .circuit()
            .add_exchange(sender, receiver, &by_id)
    } else {
        by_id
    };

    // Update global interner state:
    // - CURRENT_STEP - increment by 1.
    // - PINNED_STRINGS - unpin strings that were pinned 2 steps ago or more.
    //   These strings should now be in the spine. The reason we need to go back 2 steps is that
    //   the spine snapshot contains string from the previous steps; in addition, the snapshot
    //   is updated by worker 0, which may not have processed the current step yet.
    // - INTERNED_STRING_BY_ID - set to the latest spine snapshot in the `by_id` stream.
    by_id.apply(|spine| {
        let current_step = CURRENT_STEP.with_borrow_mut(|step| {
            *step += 1;
            *step
        });

        if Runtime::worker_index() == 0 {
            // println!(
            //     "cache capacity: {}, cache size: {}, hits: {}, misses: {}, weight: {}, shard capacity: {}, pinned: {}",
            //     INTERNED_STRING_CACHE.capacity(),
            //     INTERNED_STRING_CACHE.len(),
            //     INTERNED_STRING_CACHE.hits(),
            //     INTERNED_STRING_CACHE.misses(),
            //     INTERNED_STRING_CACHE.weight(),
            //     INTERNED_STRING_CACHE.shard_capacity(),
            //     PINNED_STRINGS.with_borrow(|pinned| pinned.len())
            // );
            *INTERNED_STRING_BY_ID.write().unwrap() = spine.clone();
        }

        PINNED_STRINGS.with_borrow_mut(|pinned| {
            let first_pinned =
                pinned.partition_point(|(_, (_, step))| *step <= current_step.saturating_sub(2));
            for (id, val) in pinned.drain(..first_pinned) {
                let _ = INTERNED_STRING_CACHE.replace(id, (val.0, false), true);
            }
            pinned.shrink_to(pinned.len() * 2);
        });
    });
}

#[cfg(test)]
mod interned_string_test {

    use crate::{build_string_interner, SqlString};
    use crate::{intern_string, unintern_string};
    use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, StorageConfig, StorageOptions};
    use dbsp::trace::{BatchReader, Cursor};
    use dbsp::utils::{Tup1, Tup2};
    use dbsp::{DBSPHandle, OrdZSet, OutputHandle, Runtime, ZSetHandle};
    use serial_test::serial;
    use std::path::Path;
    use uuid::Uuid;

    /// The first input stream contains strings to be interned.
    /// The second input stream contains queries for interned strings. It is joined with the first stream
    /// to produce an output stream of un-interned strings.
    pub fn interner_test_circuit(
        path: &Path,
        checkpoint: Option<Uuid>,
    ) -> (
        DBSPHandle,
        (
            ZSetHandle<SqlString>,
            ZSetHandle<SqlString>,
            OutputHandle<OrdZSet<SqlString>>,
        ),
    ) {
        let (circuit, handles) = Runtime::init_circuit(
            CircuitConfig::with_workers(8).with_storage(
                CircuitStorageConfig::for_config(
                    StorageConfig {
                        path: path.display().to_string(),
                        cache: Default::default(),
                    },
                    StorageOptions::default(),
                )
                .unwrap()
                .with_init_checkpoint(checkpoint),
            ),
            move |circuit| {
                let (input_strings, hinput_strings) = circuit.add_input_zset::<SqlString>();
                input_strings.set_persistent_mir_id(&"input_strings".to_string());

                let (queries, hqueries) = circuit.add_input_zset::<SqlString>();
                queries.set_persistent_mir_id(&"queries".to_string());

                build_string_interner(input_strings.map(|s| Tup1(s.clone())));

                let output_strings = input_strings
                    .map_index(|s| (s.clone(), intern_string(s)))
                    .join(
                        &queries.map_index(|q| (q.clone(), ())),
                        |_, intern_string_id, _| unintern_string(intern_string_id).unwrap(),
                    );

                Ok((hinput_strings, hqueries, output_strings.output()))
            },
        )
        .unwrap();
        (circuit, handles)
    }

    /// Push queries to the circuit, force a step, and check that the output matches the queries.
    fn query<'a, I>(
        circuit: &mut DBSPHandle,
        hqueries: &ZSetHandle<SqlString>,
        houtput_strings: &OutputHandle<OrdZSet<SqlString>>,
        queries: I,
    ) where
        I: IntoIterator<Item = &'a str>,
    {
        let mut queries = queries.into_iter().map(SqlString::from).collect::<Vec<_>>();

        let mut tuples = queries
            .iter()
            .map(|s| Tup2(s.clone(), 1))
            .collect::<Vec<_>>();
        hqueries.append(&mut tuples);

        circuit.step().unwrap();
        let output = houtput_strings.consolidate();
        let mut output = output.iter().map(|(s, _, _)| s.clone()).collect::<Vec<_>>();
        output.sort();

        queries.sort();
        assert_eq!(output, queries);

        let mut tuples = queries
            .iter()
            .map(|s| Tup2(s.clone(), -1))
            .collect::<Vec<_>>();
        hqueries.append(&mut tuples);

        circuit.step().unwrap();
    }

    #[test]
    #[serial]
    fn test_interner_basic() {
        let path = tempfile::tempdir().unwrap().keep();

        let (mut circuit, (hinput_strings, hqueries, houtput_strings)) =
            interner_test_circuit(path.as_path(), None);

        hinput_strings.push(SqlString::from("1"), 1);
        query(&mut circuit, &hqueries, &houtput_strings, ["1"]);

        hinput_strings.push(SqlString::from("2"), 1);
        query(&mut circuit, &hqueries, &houtput_strings, ["2"]);

        hinput_strings.push(SqlString::from("3"), 1);
        query(&mut circuit, &hqueries, &houtput_strings, ["3"]);

        hinput_strings.push(SqlString::from("4"), 1);
        query(&mut circuit, &hqueries, &houtput_strings, ["4"]);

        hinput_strings.push(SqlString::from("5"), 1);
        query(&mut circuit, &hqueries, &houtput_strings, ["5"]);

        query(
            &mut circuit,
            &hqueries,
            &houtput_strings,
            ["1", "2", "3", "4", "5"],
        );

        let checkpoint = circuit.commit().unwrap();
        circuit.kill().unwrap();
        super::INTERNED_STRING_CACHE.clear();

        let (mut circuit, (hinput_strings, hqueries, houtput_strings)) =
            interner_test_circuit(path.as_path(), Some(checkpoint.uuid));

        query(
            &mut circuit,
            &hqueries,
            &houtput_strings,
            ["1", "2", "3", "4", "5"],
        );

        hinput_strings.push(SqlString::from("6"), 1);
        hinput_strings.push(SqlString::from("7"), 1);

        query(
            &mut circuit,
            &hqueries,
            &houtput_strings,
            ["1", "2", "3", "4", "5", "6", "7"],
        );
        circuit.kill().unwrap();
    }

    /// Intern a small number (1,000) of strings repeatedly.
    #[test]
    #[serial]
    fn test_interner_small() {
        // INTERNED_STRING_CACHE is a global variable, so we need to reset it
        // before each test to avoid interference.
        super::INTERNED_STRING_CACHE.clear();

        // The above doesn't reset the misses counter, so we store it here
        // and compute delta at the end.
        let old_missed = super::INTERNED_STRING_CACHE.misses();

        let path = tempfile::tempdir().unwrap().keep();

        let (mut circuit, (hinput_strings, hqueries, houtput_strings)) =
            interner_test_circuit(path.as_path(), None);

        for _batch in 0..1_000 {
            let values = (0..1_000).map(|i| i.to_string()).collect::<Vec<_>>();
            let mut chunk = values
                .iter()
                .map(|i| Tup2(SqlString::from(i.as_str()), 1))
                .collect::<Vec<_>>();
            hinput_strings.append(&mut chunk);

            query(
                &mut circuit,
                &hqueries,
                &houtput_strings,
                values.iter().map(String::as_str),
            );
        }

        let checkpoint = circuit.commit().unwrap();
        circuit.kill().unwrap();
        super::INTERNED_STRING_CACHE.clear();

        let (mut circuit, (_hinput_strings, hqueries, houtput_strings)) =
            interner_test_circuit(path.as_path(), Some(checkpoint.uuid));

        query(
            &mut circuit,
            &hqueries,
            &houtput_strings,
            (0..1_000)
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .iter()
                .map(|s| s.as_str()),
        );

        assert_eq!(super::INTERNED_STRING_CACHE.len(), 1000);
        assert!(super::INTERNED_STRING_CACHE.misses() - old_missed <= 10000);
        circuit.kill().unwrap();
    }

    /// Insert and then delete some strings.
    /// INTERNED_STRING_BY_ID should be empty in the end.
    #[test]
    #[serial]
    fn test_interner_deletions() {
        let path = tempfile::tempdir().unwrap().keep();

        let (mut circuit, (hinput_strings, hqueries, houtput_strings)) =
            interner_test_circuit(path.as_path(), None);

        for batch in 0..1_000 {
            let values = (batch * 100..(batch + 1) * 100)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();
            let mut chunk = values
                .iter()
                .map(|i| Tup2(SqlString::from(i.as_str()), 1))
                .collect::<Vec<_>>();
            hinput_strings.append(&mut chunk);

            query(
                &mut circuit,
                &hqueries,
                &houtput_strings,
                values.iter().map(String::as_str),
            );
        }

        let checkpoint = circuit.commit().unwrap();
        circuit.kill().unwrap();
        super::INTERNED_STRING_CACHE.clear();

        let (mut circuit, (hinput_strings, _hqueries, _houtput_strings)) =
            interner_test_circuit(path.as_path(), Some(checkpoint.uuid));

        for batch in 0..1_000 {
            let values = (batch * 100..(batch + 1) * 100)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();
            let mut chunk = values
                .iter()
                .map(|i| Tup2(SqlString::from(i.as_str()), -1))
                .collect::<Vec<_>>();
            hinput_strings.append(&mut chunk);
            circuit.step().unwrap();
        }
        circuit.step().unwrap();
        circuit.step().unwrap();

        let mut cursor = super::INTERNED_STRING_BY_ID.read().unwrap().cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                assert_eq!(**cursor.weight(), 0);
                //println!("weight: {}", **cursor.weight());
                cursor.step_val();
            }
            cursor.step_key();
        }
        circuit.kill().unwrap();
    }

    #[test]
    #[serial]
    fn test_interner_bulk() {
        let path = tempfile::tempdir().unwrap().keep();

        let (mut circuit, (hinput_strings, hqueries, houtput_strings)) =
            interner_test_circuit(path.as_path(), None);

        for batch in 0..100 {
            let values = (batch * 1_000..(batch + 1) * 1_000)
                .map(|i| i.to_string())
                .collect::<Vec<_>>();
            let mut chunk = values
                .iter()
                .map(|i| Tup2(SqlString::from(i.as_str()), 1))
                .collect::<Vec<_>>();
            hinput_strings.append(&mut chunk);

            query(
                &mut circuit,
                &hqueries,
                &houtput_strings,
                values.iter().map(String::as_str),
            );
        }

        let checkpoint = circuit.commit().unwrap();
        circuit.kill().unwrap();
        super::INTERNED_STRING_CACHE.clear();

        let (mut circuit, (_hinput_strings, hqueries, houtput_strings)) =
            interner_test_circuit(path.as_path(), Some(checkpoint.uuid));

        query(
            &mut circuit,
            &hqueries,
            &houtput_strings,
            (0..100 * 1_000)
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .iter()
                .map(|s| s.as_str()),
        );
        circuit.kill().unwrap();
    }
}
