//! Persist and reopen a DBSP trace with native nullable VARCHAR archive keys.
#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "support/checkpoint.rs"]
mod checkpoint;
#[path = "native_string/mod.rs"]
mod native;
#[path = "../experiments/paired_string.rs"]
mod paired_string;

use dbsp::{
    DBSPHandle, OrdZSet, OutputHandle, Runtime, ZSetHandle,
    circuit::{CircuitConfig, Mode},
    utils::{Tup1, Tup2},
};
use feldera_sqllib::SqlString;
use native::{NativeString, READS, Row, SEARCHES, WRITES};
use rkyv::ser::{Serializer, serializers::AllocSerializer};
use std::sync::{Arc, atomic::Ordering::Relaxed};

impl<const TRACE: bool> checkpoint::Key<Row> for native::StringKey<TRACE> {
    fn reference(&self) -> Row {
        self.value.clone()
    }
}

struct Join {
    runtime: DBSPHandle,
    left: ZSetHandle<Row>,
    right: ZSetHandle<Row>,
    output: OutputHandle<OrdZSet<Row>>,
}

impl Join {
    fn start<K: checkpoint::Key<Row>>(config: CircuitConfig) -> Self {
        let (runtime, (left, right, output)) = Runtime::init_circuit(config, |circuit| {
            let (left, left_input) = circuit.add_input_zset::<Row>();
            let (right, right_input) = circuit.add_input_zset::<Row>();
            let left = left
                .map_index(|key| (K::from(key.clone()), ()))
                .set_persistent_id(Some("text-key-left"));
            let right = right
                .map_index(|key| (K::from(key.clone()), ()))
                .set_persistent_id(Some("text-key-right"));
            let output = left
                .join(&right, |key, _, _| key.reference())
                .output_persistent(Some("text-join-output"));
            Ok((left_input, right_input, output))
        })
        .unwrap();
        Self {
            runtime,
            left,
            right,
            output,
        }
    }

    fn step(
        &mut self,
        mut left: Vec<Tup2<Row, i64>>,
        mut right: Vec<Tup2<Row, i64>>,
    ) -> OrdZSet<Row> {
        self.left.append(&mut left);
        self.right.append(&mut right);
        self.runtime.transaction().unwrap();
        self.output.consolidate()
    }
}

fn bag(values: &[(Option<&str>, i64)]) -> Vec<Tup2<Row, i64>> {
    values
        .iter()
        .map(|(text, weight)| Tup2(Tup1(text.map(SqlString::from_ref)), *weight))
        .collect()
}

fn check_interleaved_archives() {
    let _encoder = native::encoder();
    let encode = |mut serializer: AllocSerializer<256>, row: Row, other: Row| {
        serializer.write(&[42; 3]).unwrap();
        let position = serializer.serialize_value(&Tup2(row, other)).unwrap();
        (position, serializer.into_serializer().into_inner())
    };
    let reference_a = Tup1(Some(SqlString::from_ref("a long UTF-8 🐍 string")));
    let reference_b = Tup1(Some(SqlString::from_ref("another long \0 string")));
    let (position, expected) = encode(
        AllocSerializer::default(),
        reference_a.clone(),
        reference_b.clone(),
    );
    let mut serializer = AllocSerializer::<256>::default();
    serializer.write(&[42; 3]).unwrap();
    let actual_position = serializer
        .serialize_value(&Tup2(
            NativeString::from(reference_a),
            NativeString::from(reference_b),
        ))
        .unwrap();
    let actual = serializer.into_serializer().into_inner();
    assert_eq!(actual_position, position);
    assert_eq!(actual.as_slice(), expected.as_slice());
    // The bounded encoder refuses larger payloads. The reference fallback
    // still has to produce the same bytes and position.
    let large = "x".repeat((1 << 20) + 1);
    let reference = Tup1(Some(SqlString::from_ref(&large)));
    let expected = rkyv::to_bytes::<_, 256>(&reference).unwrap();
    let actual = rkyv::to_bytes::<_, 256>(&NativeString::from(reference)).unwrap();
    assert_eq!(actual.as_slice(), expected.as_slice());
}

#[test]
fn persistent_string_trace_interchanges_with_reference() {
    check_interleaved_archives();

    let state = tempfile::tempdir().unwrap();
    let oracle_state = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| {
        CircuitConfig::with_workers(4)
            .with_mode(Mode::Persistent)
            .with_storage(checkpoint::storage(path).storage)
    };
    let mut original = Join::start::<Row>(config(state.path()));
    let mut oracle = Join::start::<Row>(config(oracle_state.path()));
    let long = "text".repeat(7) + "\0🐍";
    let long = Some(long.as_str());
    let left = bag(&[(None, 2), (Some(""), 1), (long, 2)]);
    let right = bag(&[(None, 1), (Some(""), 2), (long, 3)]);
    let expected = OrdZSet::from_keys((), bag(&[(None, 2), (Some(""), 2), (long, 6)]));
    assert_eq!(original.step(left.clone(), right.clone()), expected);
    assert_eq!(oracle.step(left, right), expected);

    let fingerprint = original.runtime.fingerprint();
    let first = original.runtime.checkpoint().run().unwrap();
    original.runtime.kill().unwrap();
    if let Some(path) = std::env::var_os("P0_NATIVE_STRING_STATE") {
        checkpoint::retain_directory(state.path(), &std::path::Path::new(&path).join("classic"));
    }

    let owner = native::encoder();
    let weak = Arc::downgrade(&owner);
    let restore = |checkpoint| {
        let mut config = config(state.path());
        config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint);
        config
    };
    let writes_before = WRITES.load(Relaxed);
    let mut native = Join::start::<NativeString>(restore(first.uuid));
    assert_eq!(native.runtime.fingerprint(), fingerprint);
    for (left, right, expected) in [
        (vec![], bag(&[(long, -1)]), bag(&[(long, -2)])),
        (
            bag(&[(None, -1), (Some("é"), 3)]),
            bag(&[(Some("é"), 2)]),
            bag(&[(None, -1), (Some("é"), 6)]),
        ),
    ] {
        let expected = OrdZSet::from_keys((), expected);
        assert_eq!(oracle.step(left.clone(), right.clone()), expected);
        assert_eq!(native.step(left, right), expected);
    }
    let next = native.runtime.checkpoint().run().unwrap();
    native.runtime.kill().unwrap();
    if let Some(path) = std::env::var_os("P0_NATIVE_STRING_STATE") {
        checkpoint::retain_directory(state.path(), &std::path::Path::new(&path).join("native"));
    }
    drop(owner);
    assert!(weak.upgrade().is_none());
    assert!(WRITES.load(Relaxed) > writes_before);
    assert!(READS.load(Relaxed) > 0);
    assert!(SEARCHES.load(Relaxed) > 0);

    let mut classic = Join::start::<Row>(restore(next.uuid));
    assert_eq!(classic.runtime.fingerprint(), fingerprint);
    let left = bag(&[(long, -1)]);
    let expected = OrdZSet::from_keys((), bag(&[(long, -2)]));
    assert_eq!(oracle.step(left.clone(), vec![]), expected);
    assert_eq!(classic.step(left, vec![]), expected);
    oracle.runtime.kill().unwrap();
    classic.runtime.kill().unwrap();
    println!(
        "workers=4 fingerprint={fingerprint} archive_writes={} archive_reads={} archived_searches={}",
        WRITES.load(Relaxed) - writes_before,
        READS.load(Relaxed),
        SEARCHES.load(Relaxed),
    );
}
