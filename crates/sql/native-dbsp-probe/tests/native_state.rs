//! Actual DBSP checkpoint restore with a different native-serialized key type.
//! The prebuilt archived adapter covers one tuple layout, not arbitrary schemas.
#![cfg(target_os = "linux")]
#[allow(dead_code)]
#[path = "native_context/mod.rs"]
mod context;
#[path = "native_state/mod.rs"]
mod native;
use dbsp::{OrdZSet, utils::Tup2};
#[path = "support/checkpoint.rs"]
mod checkpoint;
use checkpoint::{Circuit, Key, retain_directory, storage};
use native::{ARCHIVED_SEARCHES, NATIVE_READS, NATIVE_WRITES, NativePair};
use std::sync::{Arc, atomic::Ordering};

type Pair = Tup2<i32, i64>;
impl Key<Pair> for NativePair {
    fn reference(&self) -> Pair {
        self.tuple()
    }
}
fn delta(entries: &[(i32, i64, i64)]) -> Vec<Tup2<Pair, i64>> {
    entries
        .iter()
        .map(|(a, b, w)| Tup2(Tup2(*a, *b), *w))
        .collect()
}
fn verify_native_archive_contract() {
    use dbsp::dynamic::{HashRepr, OrdRepr};
    use rkyv::Archived;
    use std::hash::{Hash, Hasher};
    use xxhash_rust::xxh3::Xxh3Default;
    assert_eq!(
        std::mem::size_of::<Archived<NativePair>>(),
        std::mem::size_of::<Archived<Pair>>()
    );
    assert_eq!(
        std::mem::align_of::<Archived<NativePair>>(),
        std::mem::align_of::<Archived<Pair>>()
    );
    let tuples = [
        Tup2(i32::MIN, i64::MIN),
        Tup2(-1, i64::MAX),
        Tup2(0, -1),
        Tup2(0, 0),
        Tup2(i32::MAX, i64::MAX),
    ];
    for tuple in &tuples {
        let row = NativePair::from(*tuple);
        let bytes = rkyv::to_bytes::<_, 256>(&row).unwrap();
        assert_eq!(
            bytes.as_slice(),
            rkyv::to_bytes::<_, 256>(tuple).unwrap().as_slice()
        );
        // SAFETY: byte equality with trusted reference serialization and the
        // transparent archived wrapper establish the archived representation;
        // rkyv's AlignedVec supplies the required alignment and the full row.
        let archived = unsafe { rkyv::archived_root::<NativePair>(&bytes) };
        let mut actual = Xxh3Default::default();
        archived.hash_repr(&mut actual);
        let mut expected = Xxh3Default::default();
        tuple.hash(&mut expected);
        assert_eq!(actual.finish(), expected.finish());
        for other in &tuples {
            assert_eq!(
                archived.ord_cmp(&NativePair::from(*other)),
                tuple.cmp(other)
            );
        }
    }
}
#[test]
fn rust_tuple_checkpoint_restores_native_keys_and_writes_native_spills() {
    let retained = std::env::var_os("P0_NATIVE_STATE_EVIDENCE").map(std::path::PathBuf::from);
    if let Some(path) = &retained {
        std::fs::create_dir(path).unwrap();
    }
    let oracle_dir = tempfile::tempdir().unwrap();
    let original_dir = tempfile::tempdir().unwrap();
    let mut oracle = Circuit::new::<Pair>(storage(oracle_dir.path()));
    let mut original = Circuit::new::<Pair>(storage(original_dir.path()));
    let initial_left = delta(&[(-1, i64::MIN, 2), (0, 0, 1), (i32::MAX, i64::MAX, 1)]);
    let initial_right = delta(&[(-1, i64::MIN, 3), (0, 0, 1)]);
    let expected = OrdZSet::from_keys((), delta(&[(-1, i64::MIN, 6), (0, 0, 1)]));
    assert_eq!(
        original.step(initial_left.clone(), initial_right.clone()),
        expected
    );
    assert_eq!(oracle.step(initial_left, initial_right), expected);
    let original_fingerprint = original.runtime.fingerprint();
    let checkpoint = original.runtime.checkpoint().run().unwrap();
    original.stop();
    if let Some(path) = &retained {
        retain_directory(original_dir.path(), &path.join("rust-checkpoint"));
    }
    assert_eq!(
        NATIVE_WRITES.load(Ordering::Relaxed),
        0,
        "original state must use generated tuple serialization"
    );
    verify_native_archive_contract();
    let writes_before_restore = NATIVE_WRITES.load(Ordering::Relaxed);
    let reads_before_restore = NATIVE_READS.load(Ordering::Relaxed);
    let searches_before_restore = ARCHIVED_SEARCHES.load(Ordering::Relaxed);
    let mut config = storage(original_dir.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let keep_layout = native::layout();
    let weak = Arc::downgrade(&keep_layout);
    let mut restored = Circuit::new::<NativePair>(config);
    assert_eq!(restored.runtime.fingerprint(), original_fingerprint);
    let steps = [
        (vec![], vec![], vec![]),
        (
            vec![],
            delta(&[(-1, i64::MIN, -1)]),
            delta(&[(-1, i64::MIN, -2)]),
        ),
        (
            delta(&[(-1, i64::MIN, -1), (7, 9, 1)]),
            delta(&[(7, 9, 2)]),
            delta(&[(-1, i64::MIN, -2), (7, 9, 2)]),
        ),
        (
            delta(&[(-1, i64::MIN, -1)]),
            delta(&[(7, 9, -1)]),
            delta(&[(-1, i64::MIN, -2), (7, 9, -1)]),
        ),
        (vec![], delta(&[(0, 0, -1)]), delta(&[(0, 0, -1)])),
    ];
    for (i, (left, right, expected)) in steps.into_iter().enumerate() {
        let expected = OrdZSet::from_keys((), expected);
        assert_eq!(
            oracle.step(left.clone(), right.clone()),
            expected,
            "oracle transaction {i}"
        );
        assert_eq!(
            restored.step(left, right),
            expected,
            "native transaction {i}"
        );
    }
    let checkpoint = restored.runtime.checkpoint().run().unwrap();
    let writes = NATIVE_WRITES.load(Ordering::Relaxed) - writes_before_restore;
    let reads = NATIVE_READS.load(Ordering::Relaxed) - reads_before_restore;
    let searches = ARCHIVED_SEARCHES.load(Ordering::Relaxed) - searches_before_restore;
    assert!(
        writes > 0,
        "native code must serialize actual persisted keys"
    );
    assert!(reads > 0, "native key adapter must decode archived state");
    assert!(searches > 0, "storage must search archived native keys");
    restored.stop();
    if let Some(path) = &retained {
        retain_directory(original_dir.path(), &path.join("native-checkpoint"));
    }
    drop(keep_layout);
    assert!(
        weak.upgrade().is_none(),
        "first native library retained after shutdown"
    );
    let fresh_layout = native::layout();
    let fresh_weak = Arc::downgrade(&fresh_layout);
    let mut config = storage(original_dir.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let mut resumed = Circuit::new::<NativePair>(config);
    let left = delta(&[(7, 9, -1)]);
    let expected = OrdZSet::from_keys((), delta(&[(7, 9, -1)]));
    assert_eq!(oracle.step(left.clone(), vec![]), expected);
    assert_eq!(resumed.step(left, vec![]), expected);
    resumed.stop();
    oracle.stop();
    drop(fresh_layout);
    assert!(
        fresh_weak.upgrade().is_none(),
        "native descriptor leaked after runtimes stopped"
    );
    if let Some(path) = &retained {
        let report = format!(
            concat!(
                "{{\"native_serializations\":{writes},",
                "\"native_deserializations\":{reads},",
                "\"archived_searches\":{searches},",
                "\"post_restore_transactions\":6,",
                "\"scope\":\"One prebuilt tuple-layout adapter; not arbitrary runtime schemas\"}}\n"
            ),
            writes = writes,
            reads = reads,
            searches = searches
        );
        std::fs::write(path.join("result.json"), report).unwrap();
    }
    println!(
        "native state counters: serialized={writes}, decoded={reads}, archived_searches={searches}; 6 checked post-restore transactions"
    );
}
