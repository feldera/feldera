use std::marker::PhantomData;

use crate::{
    dynamic::{DataTrait, Erase, Factory},
    trace::Cursor,
    DynZWeight, Timestamp,
};

/// When `SATURATE` is `true`, behaves as if the underlying cursor contained exactly
/// one value equal to `V::default()` for each key not present in it.
///
/// When `SATURATE` is `false`, behaves as the underlying cursor.
///
/// Currently implements just enough of the cursor API to be used in
/// inner join operators; in particular, it only implements searching
/// for a key with `seek_key_exact`.
pub struct SaturatingCursor<'a, K, V, T, const SATURATE: bool>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
{
    /// The underlying cursor.
    cursor: Box<dyn Cursor<K, V, T, DynZWeight> + 'a>,

    /// The cursor is pointing to a ghost key that is not
    /// present in the underlying cursor.
    on_ghost_key: bool,

    /// Only used if `on_ghost_key` is `true`.
    ghost_key: Box<K>,

    /// Contains the default value of `V`.
    ghost_val: Box<V>,

    /// Only used if `on_ghost_key` is `true`.
    ghost_val_valid: bool,

    phantom: PhantomData<fn(&K, &V, &T)>,
}

impl<'a, K, V, T, const SATURATE: bool> SaturatingCursor<'a, K, V, T, SATURATE>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
{
    pub fn new(
        cursor: Box<dyn Cursor<K, V, T, DynZWeight> + 'a>,
        key_factory: &'static dyn Factory<K>,
        val_factory: &'static dyn Factory<V>,
    ) -> Self {
        let on_ghost_key = if SATURATE { !cursor.key_valid() } else { false };

        Self {
            cursor,
            on_ghost_key,
            ghost_val_valid: false,
            ghost_key: key_factory.default_box(),
            ghost_val: val_factory.default_box(),
            phantom: PhantomData,
        }
    }
}

impl<'a, K, V, T, const SATURATE: bool> Cursor<K, V, T, DynZWeight>
    for SaturatingCursor<'a, K, V, T, SATURATE>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
{
    fn weight_factory(&self) -> &'static dyn Factory<DynZWeight> {
        self.cursor.weight_factory()
    }

    fn key_valid(&self) -> bool {
        if SATURATE {
            true
        } else {
            self.cursor.key_valid()
        }
    }

    fn val_valid(&self) -> bool {
        if self.on_ghost_key {
            self.ghost_val_valid
        } else {
            self.cursor.val_valid()
        }
    }

    fn key(&self) -> &K {
        if self.on_ghost_key {
            &self.ghost_key
        } else {
            self.cursor.key()
        }
    }

    fn val(&self) -> &V {
        if self.on_ghost_key {
            &self.ghost_val
        } else {
            self.cursor.val()
        }
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &DynZWeight)) {
        if self.on_ghost_key {
            logic(&T::default(), 1.erase());
        } else {
            self.cursor.map_times(logic);
        }
    }

    fn map_times_through(&mut self, _upper: &T, _logic: &mut dyn FnMut(&T, &DynZWeight)) {
        unimplemented!()
    }

    fn weight(&mut self) -> &DynZWeight
    where
        T: PartialEq<()>,
    {
        if self.on_ghost_key {
            1.erase()
        } else {
            self.cursor.weight()
        }
    }

    fn map_values(&mut self, _logic: &mut dyn FnMut(&V, &DynZWeight))
    where
        T: PartialEq<()>,
    {
        unimplemented!()
    }

    fn step_key(&mut self) {
        debug_assert!(!SATURATE);
        self.cursor.step_key();
    }

    fn step_key_reverse(&mut self) {
        debug_assert!(!SATURATE);
        self.cursor.step_key_reverse();
    }

    fn seek_key(&mut self, _key: &K) {
        unimplemented!()
    }

    fn seek_key_exact(&mut self, key: &K, hash: Option<u64>) -> bool {
        //println!("SaturatingCursor seek_key_exact: key: {:?}", key);
        if self.cursor.seek_key_exact(key, hash) {
            self.on_ghost_key = false;
            true
        } else if SATURATE {
            key.clone_to(self.ghost_key.as_mut());
            self.on_ghost_key = true;
            self.ghost_val_valid = true;
            true
        } else {
            false
        }
    }

    fn seek_key_with(&mut self, _predicate: &dyn Fn(&K) -> bool) {
        unimplemented!()
    }

    fn seek_key_with_reverse(&mut self, _predicate: &dyn Fn(&K) -> bool) {
        unimplemented!()
    }

    fn seek_key_reverse(&mut self, _key: &K) {
        unimplemented!()
    }

    fn step_val(&mut self) {
        if self.on_ghost_key {
            self.ghost_val_valid = false;
        } else {
            self.cursor.step_val();
        }
    }

    fn step_val_reverse(&mut self) {
        if self.on_ghost_key {
            self.ghost_val_valid = false;
        } else {
            self.cursor.step_val_reverse();
        }
    }

    fn seek_val(&mut self, val: &V) {
        if self.on_ghost_key {
            self.ghost_val_valid = val == self.ghost_val.as_ref();
        } else {
            self.cursor.seek_val(val);
        }
    }

    fn seek_val_reverse(&mut self, val: &V) {
        if self.on_ghost_key {
            self.ghost_val_valid = val == self.ghost_val.as_ref();
        } else {
            self.cursor.seek_val_reverse(val);
        }
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        if self.on_ghost_key {
            self.ghost_val_valid = predicate(self.ghost_val.as_ref());
        } else {
            self.cursor.seek_val_with(predicate);
        }
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        if self.on_ghost_key {
            self.ghost_val_valid = predicate(self.ghost_val.as_ref());
        } else {
            self.cursor.seek_val_with_reverse(predicate);
        }
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
        self.on_ghost_key = if SATURATE {
            !self.cursor.key_valid()
        } else {
            false
        };
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward_keys();
        self.on_ghost_key = if SATURATE {
            !self.cursor.key_valid()
        } else {
            false
        };
    }

    fn rewind_vals(&mut self) {
        if self.on_ghost_key {
            self.ghost_val_valid = true;
        } else {
            self.cursor.rewind_vals();
        }
    }

    fn fast_forward_vals(&mut self) {
        if self.on_ghost_key {
            self.ghost_val_valid = true;
        } else {
            self.cursor.fast_forward_vals();
        }
    }

    fn position(&self) -> Option<super::Position> {
        self.cursor.position()
    }
}
