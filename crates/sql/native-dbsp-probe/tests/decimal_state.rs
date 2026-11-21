//! Actual storage restoration for the frozen nullable DECIMAL(6,2) key shape.
#![cfg(target_os = "linux")]
#[path = "support/checkpoint.rs"]
mod checkpoint;
#[path = "native_decimal/mod.rs"]
mod native;
use checkpoint::{Circuit, Key, retain_directory, storage};
use dbsp::{
    OrdZSet,
    utils::{Tup1, Tup2},
};
use native::{NativeDecimal, READS, Row, SEARCHES, WRITES};
use std::sync::{Arc, atomic::Ordering};
impl Key<Row> for NativeDecimal {
    fn reference(&self) -> Row {
        self.value
    }
}
fn delta(values: &[(Option<&str>, i64)]) -> Vec<Tup2<Row, i64>> {
    values
        .iter()
        .map(|(v, w)| Tup2(Tup1(v.map(|s| s.parse().unwrap())), *w))
        .collect()
}
#[test]
fn nullable_decimal_tuple_checkpoint_restores_native_archive_adapter() {
    let retained = std::env::var_os("P0_DECIMAL_STATE_EVIDENCE").map(std::path::PathBuf::from);
    if let Some(path) = &retained {
        std::fs::create_dir(path).unwrap();
    }
    let oracle_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let mut oracle = Circuit::new::<Row>(storage(oracle_dir.path()));
    let mut original = Circuit::new::<Row>(storage(state_dir.path()));
    let left = delta(&[(None, 2), (Some("-9999.99"), 1), (Some("0.01"), 1)]);
    let right = delta(&[(None, 3), (Some("-9999.99"), 1)]);
    let expected = OrdZSet::from_keys((), delta(&[(None, 6), (Some("-9999.99"), 1)]));
    assert_eq!(original.step(left.clone(), right.clone()), expected);
    assert_eq!(oracle.step(left, right), expected);
    let fingerprint = original.runtime.fingerprint();
    let saved = original.runtime.checkpoint().run().unwrap();
    original.stop();
    if let Some(path) = &retained {
        retain_directory(state_dir.path(), &path.join("rust-checkpoint"));
    }
    assert_eq!(WRITES.load(Ordering::Relaxed), 0);
    let keep = native::encoder();
    let weak = Arc::downgrade(&keep);
    let mut config = storage(state_dir.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(saved.uuid);
    let mut restored = Circuit::new::<NativeDecimal>(config);
    assert_eq!(restored.runtime.fingerprint(), fingerprint);
    let steps = [
        (vec![], vec![], vec![]),
        (vec![], delta(&[(None, -1)]), delta(&[(None, -2)])),
        (
            delta(&[(None, -1), (Some("9999.99"), 1)]),
            delta(&[(Some("9999.99"), 2)]),
            delta(&[(None, -2), (Some("9999.99"), 2)]),
        ),
        (
            delta(&[(None, -1)]),
            delta(&[(Some("-9999.99"), -1)]),
            delta(&[(None, -2), (Some("-9999.99"), -1)]),
        ),
    ];
    for (left, right, expected) in steps {
        let expected = OrdZSet::from_keys((), expected);
        assert_eq!(oracle.step(left.clone(), right.clone()), expected);
        assert_eq!(restored.step(left, right), expected);
    }
    let saved = restored.runtime.checkpoint().run().unwrap();
    restored.stop();
    if let Some(path) = &retained {
        retain_directory(state_dir.path(), &path.join("native-checkpoint"));
    }
    assert!(WRITES.load(Ordering::Relaxed) > 0);
    assert!(READS.load(Ordering::Relaxed) > 0);
    assert!(SEARCHES.load(Ordering::Relaxed) > 0);
    drop(keep);
    assert!(weak.upgrade().is_none());
    let fresh = native::encoder();
    let fresh_weak = Arc::downgrade(&fresh);
    let mut config = storage(state_dir.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(saved.uuid);
    let mut resumed = Circuit::new::<NativeDecimal>(config);
    let left = delta(&[(Some("9999.99"), -1)]);
    let expected = OrdZSet::from_keys((), delta(&[(Some("9999.99"), -2)]));
    assert_eq!(oracle.step(left.clone(), vec![]), expected);
    assert_eq!(resumed.step(left, vec![]), expected);
    resumed.stop();
    oracle.stop();
    drop(fresh);
    assert!(fresh_weak.upgrade().is_none());
    println!(
        "nullable decimal native state: writes={}, reads={}, archived searches={}; five post-restore checked transactions",
        WRITES.load(Ordering::Relaxed),
        READS.load(Ordering::Relaxed),
        SEARCHES.load(Ordering::Relaxed)
    );
}
