use crate::{
    algebra::{HasOne, HasZero},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        OwnershipPreference, Scope,
    },
    trace::{cursor::Cursor, Batch, Trace},
};
use std::{borrow::Cow, marker::PhantomData, mem::swap, ops::Neg};

pub struct Upsert<K, VI, F, V, T, B> {
    val_func: F,
    phantom: PhantomData<(K, VI, V, T, B)>,
}

impl<K, VI, F, V, T, B> Upsert<K, VI, F, V, T, B> {
    pub fn new(val_func: F) -> Self {
        Self {
            val_func,
            phantom: PhantomData,
        }
    }
}

/// Internal implementation of `Circuit::add_set` and `Circuit::add_map`.
///
/// Applies a vector of upsert commands to a trace (an upsert inserts a
/// key/value pair, replacing existing value for the given key, or deletes
/// a key/value pair if it exists).  Outputs a batch of changes to the
/// trace.
impl<K, VI, F, V, T, B> Operator for Upsert<K, VI, F, V, T, B>
where
    B: 'static,
    T: 'static,
    V: 'static,
    F: 'static,
    VI: 'static,
    K: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Upsert")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

fn skip_zero_weights<'s, K, V: 's, R, C>(cursor: &mut C) -> bool
where
    C: Cursor<'s, K, V, (), R>,
    R: HasZero,
{
    while cursor.val_valid() && cursor.weight().is_zero() {
        cursor.step_val();
    }
    cursor.val_valid()
}

impl<K, VI, F, V, T, B> BinaryOperator<T, Vec<(K, VI)>, B> for Upsert<K, VI, F, V, T, B>
where
    T: Trace<Key = K, Val = V, Batch = B, Time = (), R = B::R> + 'static,
    B: Batch<Key = K, Val = V, Time = ()> + 'static,
    F: Fn(VI) -> Option<V> + 'static,
    K: Ord + Clone + 'static,
    VI: Eq + 'static,
    B::R: HasOne + Neg<Output = B::R> + 'static,
    V: Eq + Clone + 'static,
{
    fn eval(&mut self, _trace: &T, _upserts: &Vec<(K, VI)>) -> B {
        panic!("Upsert::eval(): cannot accept upserts by reference")
    }

    fn eval_owned_and_ref(&mut self, mut _trace: T, _upserts: &Vec<(K, VI)>) -> B {
        panic!("Upsert::eval_owned_and_ref(): cannot accept upserts by reference")
    }

    fn eval_ref_and_owned(&mut self, trace: &T, mut upserts: Vec<(K, VI)>) -> B {
        let mut updates = Vec::with_capacity(upserts.len());
        let mut cursor = trace.cursor();

        // Sort the vector by key, preserving the history of updates for each key.
        // Upserts cannot be merged or reordered, therefore we cannot use unstable sort.
        upserts.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Find the last upsert for each key, that's the only one that matters.
        upserts.dedup_by(|(k1, v1), (k2, v2)| {
            if k1 == k2 {
                swap(v1, v2);
                true
            } else {
                false
            }
        });

        for (k, val) in upserts.into_iter() {
            cursor.seek_key(&k);
            let val = (self.val_func)(val);

            if cursor.key_valid() && cursor.key() == &k && skip_zero_weights(&mut cursor) {
                // Key already present in the trace.
                match val {
                    // New value for existing key - delete old value, insert the new one.
                    Some(val) if &val != cursor.val() => {
                        updates.push((
                            B::item_from(cursor.key().clone(), cursor.val().clone()),
                            B::R::one().neg(),
                        ));
                        updates.push((B::item_from(k, val), B::R::one()));
                    }
                    // New value is `None` - remove existing key/value pair.
                    None => {
                        updates.push((B::item_from(k, cursor.val().clone()), B::R::one().neg()));
                    }
                    // Otherwise, the new value is the same as the old value - do nothing.
                    _ => {}
                }
            } else {
                // Key not in the trace.

                if let Some(val) = val {
                    updates.push((B::item_from(k, val), HasOne::one()));
                }
            }
        }

        B::from_tuples((), updates)
    }

    fn eval_owned(&mut self, trace: T, upserts: Vec<(K, VI)>) -> B {
        self.eval_ref_and_owned(&trace, upserts)
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::STRONGLY_PREFER_OWNED,
        )
    }
}
