//! Tests to ensure correctness of the persistent trace implementation.
//!
//! This module has:
//!  - a few unit test that check interesting cases (mostly found by proptests)
//!  - proptests that check that the persistent trace behaves like a spine
#![cfg(test)]

mod proptests;

use super::PersistentTrace;
use crate::time::NestedTimestamp32;
use crate::trace::cursor::Cursor;
use crate::trace::ord::{OrdIndexedZSet, OrdKeyBatch, OrdValBatch, OrdZSet};
use crate::trace::{Batch, BatchReader, Batcher, Trace};
use proptests::{spine_ptrace_are_equal, ComplexKey};

#[test]
fn vals_are_sorted() {
    let mut val_builder = <OrdIndexedZSet<String, usize, usize> as Batch>::Batcher::new_batcher(());
    let mut b = vec![((String::from(""), 9777), 1)];
    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut val_builder = <OrdIndexedZSet<String, usize, usize> as Batch>::Batcher::new_batcher(());
    let mut b = vec![((String::from(""), 0), 1), ((String::from(""), 0), 1)];
    val_builder.push_batch(&mut b);
    let vset2 = val_builder.seal();

    let mut spine =
        crate::trace::spine_fueled::Spine::<OrdIndexedZSet<String, usize, usize>>::new(None);
    spine.insert(vset1.clone());
    spine.insert(vset2.clone());
    let mut fuel = 10000;
    spine.apply_fuel(&mut fuel);

    let mut ptrace = PersistentTrace::<OrdIndexedZSet<String, usize, usize>>::new(None);
    ptrace.insert(vset1);
    ptrace.insert(vset2);

    assert!(spine_ptrace_are_equal(&spine, &ptrace));
}

#[test]
fn empty_batch_ptrace() {
    let val_builder = <OrdValBatch<String, String, u32, usize> as Batch>::Batcher::new_batcher(0);
    let vset1 = val_builder.seal();
    let mut spine =
        crate::trace::spine_fueled::Spine::<OrdValBatch<String, String, u32, usize>>::new(None);
    spine.insert(vset1.clone());
    let mut ptrace = PersistentTrace::<OrdValBatch<String, String, u32, usize>>::new(None);
    ptrace.insert(vset1);

    assert!(spine_ptrace_are_equal(&spine, &ptrace));
}

#[test]
fn insert_bug() {
    let mut val_builder =
        <OrdValBatch<String, String, u32, usize> as Batch>::Batcher::new_batcher(0);
    let mut b = vec![((String::from("a"), String::from("b")), 1)];
    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut val_builder =
        <OrdValBatch<String, String, u32, usize> as Batch>::Batcher::new_batcher(0);
    let mut b = vec![
        ((String::from("a"), String::from("b")), 1),
        ((String::from("a"), String::from("c")), 1),
    ];
    val_builder.push_batch(&mut b);
    let vset2 = val_builder.seal();

    let mut spine =
        crate::trace::spine_fueled::Spine::<OrdValBatch<String, String, u32, usize>>::new(None);
    spine.insert(vset1.clone());
    spine.insert(vset2.clone());
    let mut fuel = 10000;
    spine.apply_fuel(&mut fuel);

    let mut ptrace = PersistentTrace::<OrdValBatch<String, String, u32, usize>>::new(None);
    ptrace.insert(vset1);
    ptrace.insert(vset2);

    assert!(spine_ptrace_are_equal(&spine, &ptrace));
}

#[test]
fn times_are_sorted() {
    let mut val_builder =
        <OrdValBatch<String, String, u32, usize> as Batch>::Batcher::new_batcher(473291538);
    let mut b = vec![((String::from("y"), String::from("")), 1)];
    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut val_builder =
        <OrdValBatch<String, String, u32, usize> as Batch>::Batcher::new_batcher(0);
    let mut b = vec![((String::from("y"), String::from("")), 1)];
    val_builder.push_batch(&mut b);
    let vset2 = val_builder.seal();

    let mut spine =
        crate::trace::spine_fueled::Spine::<OrdValBatch<String, String, u32, usize>>::new(None);
    spine.insert(vset1.clone());
    spine.insert(vset2.clone());
    let mut fuel = 10000;
    spine.apply_fuel(&mut fuel);

    let mut ptrace = PersistentTrace::<OrdValBatch<String, String, u32, usize>>::new(None);
    ptrace.insert(vset1);
    ptrace.insert(vset2);

    assert!(spine_ptrace_are_equal(&spine, &ptrace));
}

#[test]
fn recede_to_bug() {
    let mut val_builder =
        <OrdValBatch<String, usize, u32, usize> as Batch>::Batcher::new_batcher(2);
    let mut b = vec![((String::from("a"), 1), 1)];
    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut val_builder =
        <OrdValBatch<String, usize, u32, usize> as Batch>::Batcher::new_batcher(0);
    let mut b = vec![((String::from("a"), 1), 1)];
    val_builder.push_batch(&mut b);
    let vset2 = val_builder.seal();

    let mut spine =
        crate::trace::spine_fueled::Spine::<OrdValBatch<String, usize, u32, usize>>::new(None);
    spine.insert(vset1.clone());
    spine.insert(vset2.clone());
    spine.recede_to(&1);

    let mut ptrace = PersistentTrace::<OrdValBatch<String, usize, u32, usize>>::new(None);
    ptrace.insert(vset1);
    ptrace.insert(vset2);
    ptrace.recede_to(&1);

    assert!(spine_ptrace_are_equal(&spine, &ptrace));
}

#[test]
fn weights_cancellation() {
    // [(ComplexKey { _a: 0, ord: "" }, -8)])), Insert(((), [(ComplexKey { _a: 0,
    // ord: "" }, 8)]))]

    let mut val_builder =
        <OrdValBatch<String, usize, u32, isize> as Batch>::Batcher::new_batcher(0);
    let mut b = vec![((String::from("a"), 1), -8)];
    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut val_builder =
        <OrdValBatch<String, usize, u32, isize> as Batch>::Batcher::new_batcher(0);
    let mut b = vec![((String::from("a"), 1), 8)];
    val_builder.push_batch(&mut b);
    let vset2 = val_builder.seal();

    let mut spine =
        crate::trace::spine_fueled::Spine::<OrdValBatch<String, usize, u32, isize>>::new(None);
    spine.insert(vset1.clone());
    spine.insert(vset2.clone());
    let mut fuel = 10000;
    spine.apply_fuel(&mut fuel);

    let mut ptrace = PersistentTrace::<OrdValBatch<String, usize, u32, isize>>::new(None);
    ptrace.insert(vset1);
    ptrace.insert(vset2);

    assert!(spine_ptrace_are_equal(&spine, &ptrace));
}

#[test]
fn timestamp_aggregation() {
    // actions = [
    //    Insert((NestedTimestamp32(2147483649), [(ComplexKey { _a: 0, ord: "" },
    // 1)])),    Insert((NestedTimestamp32(2), [(ComplexKey { _a: 0, ord: "" },
    // 1)])),    Insert((NestedTimestamp32(1), [(ComplexKey { _a: 0, ord: "" },
    // 1)])),    RecedeTo(NestedTimestamp32(2))
    //]

    let mut val_builder =
        <OrdKeyBatch<ComplexKey, NestedTimestamp32, isize> as Batch>::Batcher::new_batcher(
            NestedTimestamp32::new(true, 1),
        );
    let mut b = vec![(
        ComplexKey {
            _a: 0,
            ord: String::from(""),
        },
        1,
    )];
    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut val_builder =
        <OrdKeyBatch<ComplexKey, NestedTimestamp32, isize> as Batch>::Batcher::new_batcher(
            NestedTimestamp32::new(false, 2),
        );
    let mut b = vec![(
        ComplexKey {
            _a: 0,
            ord: String::from(""),
        },
        1,
    )];
    val_builder.push_batch(&mut b);
    let vset2 = val_builder.seal();

    let mut val_builder =
        <OrdKeyBatch<ComplexKey, NestedTimestamp32, isize> as Batch>::Batcher::new_batcher(
            NestedTimestamp32::new(false, 1),
        );
    let mut b = vec![(
        ComplexKey {
            _a: 0,
            ord: String::from(""),
        },
        1,
    )];
    val_builder.push_batch(&mut b);
    let vset3 = val_builder.seal();

    let mut spine = crate::trace::spine_fueled::Spine::<
        OrdKeyBatch<ComplexKey, NestedTimestamp32, isize>,
    >::new(None);
    spine.insert(vset1.clone());
    let mut fuel = isize::MAX;
    spine.exert(&mut fuel);
    spine.insert(vset2.clone());
    let mut fuel = isize::MAX;
    spine.exert(&mut fuel);
    spine.insert(vset3.clone());
    let mut fuel = isize::MAX;
    spine.exert(&mut fuel);
    spine.recede_to(&NestedTimestamp32::new(false, 2));

    let mut ptrace =
        PersistentTrace::<OrdKeyBatch<ComplexKey, NestedTimestamp32, isize>>::new(None);
    ptrace.insert(vset1);
    ptrace.insert(vset2);
    ptrace.insert(vset3);
    ptrace.recede_to(&NestedTimestamp32::new(false, 2));

    assert!(spine_ptrace_are_equal(&spine, &ptrace));
}

#[test]
fn weights_cancellation2() {
    //    [
    //        Insert((NestedTimestamp32(0), [(ComplexKey { _a: 0, ord: "" }, -1)])),
    //        Insert((NestedTimestamp32(4), [(ComplexKey { _a: 0, ord: "" }, -9)])),
    //        Insert((NestedTimestamp32(4), [(ComplexKey { _a: 0, ord: "" }, 9)])),
    //    ]
    let mut val_builder =
        <OrdKeyBatch<ComplexKey, NestedTimestamp32, isize> as Batch>::Batcher::new_batcher(
            NestedTimestamp32::new(false, 0),
        );
    let mut b = vec![(
        ComplexKey {
            _a: 0,
            ord: String::from(""),
        },
        -1,
    )];
    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut val_builder =
        <OrdKeyBatch<ComplexKey, NestedTimestamp32, isize> as Batch>::Batcher::new_batcher(
            NestedTimestamp32::new(false, 4),
        );
    let mut b = vec![(
        ComplexKey {
            _a: 0,
            ord: String::from(""),
        },
        -9,
    )];
    val_builder.push_batch(&mut b);
    let vset2 = val_builder.seal();

    let mut val_builder =
        <OrdKeyBatch<ComplexKey, NestedTimestamp32, isize> as Batch>::Batcher::new_batcher(
            NestedTimestamp32::new(false, 4),
        );
    let mut b = vec![(
        ComplexKey {
            _a: 0,
            ord: String::from(""),
        },
        9,
    )];
    val_builder.push_batch(&mut b);
    let vset3 = val_builder.seal();

    let mut spine = crate::trace::spine_fueled::Spine::<
        OrdKeyBatch<ComplexKey, NestedTimestamp32, isize>,
    >::new(None);
    spine.insert(vset1.clone());
    let mut fuel = isize::MAX;
    spine.exert(&mut fuel);
    spine.insert(vset2.clone());
    let mut fuel = isize::MAX;
    spine.exert(&mut fuel);
    spine.insert(vset3.clone());
    let mut fuel = isize::MAX;
    spine.exert(&mut fuel);
    spine.recede_to(&NestedTimestamp32::new(false, 2));

    let mut ptrace =
        PersistentTrace::<OrdKeyBatch<ComplexKey, NestedTimestamp32, isize>>::new(None);
    ptrace.insert(vset1);
    ptrace.insert(vset2);
    ptrace.insert(vset3);
    ptrace.recede_to(&NestedTimestamp32::new(false, 2));

    assert!(spine_ptrace_are_equal(&spine, &ptrace));
}

#[test]
fn cursor_weight() {
    // Use a unit-test for weight() -- in proptests we don't call it it's
    // specific for things with () timestamps

    let mut val_builder = <OrdZSet<String, i32> as Batch>::Batcher::new_batcher(());
    let mut b = vec![(String::from("a"), -8)];
    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut spine = crate::trace::spine_fueled::Spine::<OrdZSet<String, i32>>::new(None);
    spine.insert(vset1.clone());
    let mut fuel = 10000;
    spine.apply_fuel(&mut fuel);

    let mut ptrace = PersistentTrace::<OrdZSet<String, i32>>::new(None);
    ptrace.insert(vset1);

    let mut ptrace_cursor = ptrace.cursor();
    let mut spine_cursor = spine.cursor();

    assert_eq!(ptrace_cursor.weight(), -8);
    assert_eq!(ptrace_cursor.weight(), spine_cursor.weight());
}

#[test]
fn cursor_weight_multiple_values_bug() {
    // Use a unit-test for weight() -- in proptests we don't call it it's
    // specific for things with () timestamps
    //
    // Checks that if we have multiple values with weights we need to report the
    // right weights when stepping through them

    let mut val_builder = <OrdIndexedZSet<String, String, i32> as Batch>::Batcher::new_batcher(());
    let mut b = vec![
        ((String::from("k"), String::from("v")), 1),
        ((String::from("k"), String::from("v2")), 2),
    ];

    val_builder.push_batch(&mut b);
    let vset1 = val_builder.seal();

    let mut spine =
        crate::trace::spine_fueled::Spine::<OrdIndexedZSet<String, String, i32>>::new(None);
    spine.insert(vset1.clone());
    let mut fuel = 10000;
    spine.apply_fuel(&mut fuel);

    let mut ptrace = PersistentTrace::<OrdIndexedZSet<String, String, i32>>::new(None);
    ptrace.insert(vset1);

    let mut ptrace_cursor = ptrace.cursor();
    let mut spine_cursor = spine.cursor();

    assert_eq!(ptrace_cursor.weight(), spine_cursor.weight());
    ptrace_cursor.step_val();
    spine_cursor.step_val();
    assert_eq!(ptrace_cursor.weight(), spine_cursor.weight());
}
