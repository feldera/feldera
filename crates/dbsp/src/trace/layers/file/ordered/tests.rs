#![cfg(test)]

use crate::trace::layers::{Builder, TupleBuilder};

/*
fn empty_consumer() -> FileOrderedLayerConsumer<usize, usize, isize, usize> {
    FileOrderedLayerConsumer::from(
        FileOrderedBuilder::<usize, ColumnLayerBuilder<usize, isize>, usize>::new().done(),
    )
}

#[test]
fn consumer_smoke_test() {
    let expected = [0, 10, 20, 1000, 10000];

    let mut builder: FileOrderedBuilder<usize, ColumnLayerBuilder<usize, isize>, usize> =
        FileOrderedBuilder::with_capacity(10000);
    for key in 0..20000 {
        builder.push_tuple((key, (100, -100)));
    }
    let mut consumer = FileOrderedLayerConsumer::from(builder.done());
    assert!(consumer.key_valid());

    for expected in expected {
        assert!(consumer.key_valid());
        consumer.seek_key(&expected);
        assert!(consumer.key_valid());
        assert_eq!(*consumer.peek_key(), expected);

        let (key, mut values) = consumer.next_key();
        assert_eq!(key, expected);

        assert!(values.value_valid());
        let (value, diff, ()) = values.next_value();
        assert!(!values.value_valid());
        assert_eq!(value, 100);
        assert_eq!(diff, -100);
    }
}
*/

/*
#[test]
fn empty_seek() {
    let mut consumer = empty_consumer();
    assert!(!consumer.key_valid());
    consumer.seek_key(&1000);
    assert!(!consumer.key_valid());
}
 */

#[test]
fn remaining_values() {
    let mut builder = FileOrderedBuilder::<usize, ColumnLayerBuilder<usize, isize>, usize>::new();
    for idx in 0..10 {
        builder.push_tuple((0, (idx, 1)));
    }
    let mut consumer = FileOrderedLayerConsumer::from(builder.done());

    assert!(consumer.key_valid());
    let (key, mut values) = consumer.next_key();
    assert_eq!(key, 0);

    for remaining in (1..=10).rev() {
        assert_eq!(values.remaining_values(), remaining);
        let _ = values.next_value();
        assert_eq!(values.remaining_values(), remaining - 1);
    }
}

#[test]
#[should_panic]
fn next_empty_consumer() {
    let mut consumer = empty_consumer();
    assert!(!consumer.key_valid());
    let _ = consumer.next_key();
}

#[test]
#[should_panic]
fn peek_empty_consumer() {
    let consumer = empty_consumer();
    assert!(!consumer.key_valid());
    let _ = consumer.peek_key();
}

#[test]
#[should_panic]
fn next_empty_values() {
    let mut builder = FileOrderedBuilder::<usize, ColumnLayerBuilder<usize, isize>, usize>::new();
    builder.push_tuple((1, (1, 1)));
    let mut consumer = FileOrderedLayerConsumer::from(builder.done());

    let (key, mut values) = consumer.next_key();
    assert_eq!(key, 1);
    assert!(values.value_valid());
    assert_eq!(values.remaining_values(), 1);

    let (value, diff, ()) = values.next_value();
    assert_eq!(value, 1);
    assert_eq!(diff, 1);
    assert!(!values.value_valid());
    assert_eq!(values.remaining_values(), 0);

    let _ = values.next_value();
}
