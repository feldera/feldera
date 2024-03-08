use dyn_clone::clone_box;

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    ops::Range,
};

use crate::storage::file::reader::Cursor as FileCursor;

use crate::{
    dynamic::{DataTrait, WeightTrait},
    trace::{layers::Cursor, ord::file::StorageBackend},
};

use super::FileColumnLayer;

/// A cursor for walking through a [`FileColumnLayer`].
#[derive(Debug)]
pub struct FileColumnLayerCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub(crate) storage: &'s FileColumnLayer<K, R>,
    key: Box<K>,
    diff: Box<R>,
    valid: bool,
    cursor: FileCursor<'s, StorageBackend, K, R, (), (&'static K, &'static R, ())>,
}

impl<'s, K, R> Clone for FileColumnLayerCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage,
            key: clone_box(&self.key),
            diff: clone_box(&self.diff),
            valid: self.valid,
            cursor: self.cursor.clone(),
        }
    }
}

impl<'s, K, R> FileColumnLayerCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new(storage: &'s FileColumnLayer<K, R>, bounds: Range<u64>) -> Self {
        let mut key = storage.factories.file_factories.key_factory.default_box();
        let mut diff = storage.factories.diff_factory.default_box();

        let cursor = storage.file.rows().subset(bounds).first().unwrap();
        let valid = unsafe { cursor.item((key.as_mut(), diff.as_mut())) }.is_some();

        Self {
            cursor,
            storage,
            key,
            diff,
            valid,
        }
    }
    pub fn current_key(&self) -> &K {
        debug_assert!(self.valid);
        &self.key
    }

    pub fn current_diff(&self) -> &R {
        debug_assert!(self.valid);
        &self.diff
    }

    pub fn current_item(&self) -> (&K, &R) {
        debug_assert!(self.valid);
        (&self.key, &self.diff)
    }

    pub fn get_current_item(&self) -> Option<(&K, &R)> {
        if self.valid {
            Some((&self.key, &self.diff))
        } else {
            None
        }
    }

    fn get_item(&mut self) {
        self.valid = unsafe { self.cursor.item((&mut self.key, &mut self.diff)) }.is_some();
    }

    pub fn seek_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        unsafe { self.cursor.seek_forward_until(predicate) }.unwrap();
        self.get_item();
    }

    pub fn seek_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        unsafe { self.cursor.seek_backward_until(predicate) }.unwrap();
        self.get_item();
    }

    pub fn move_to_row(&mut self, row: usize) {
        self.cursor.move_to_row(row as u64).unwrap();
        self.get_item();
    }
}

impl<'s, K, R> Cursor<'s> for FileColumnLayerCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Item<'k> = (&'k K, &'k R)
        where
            Self: 'k;

    type Key = K;

    type ValueCursor = ();

    fn keys(&self) -> usize {
        self.cursor.len() as usize
    }

    fn item(&self) -> Self::Item<'_> {
        self.current_item()
    }

    fn values(&self) {}

    fn step(&mut self) {
        self.cursor.move_next().unwrap();
        self.get_item();
    }

    fn step_reverse(&mut self) {
        self.cursor.move_prev().unwrap();
        self.get_item();
    }

    fn seek(&mut self, key: &Self::Key) {
        unsafe { self.cursor.advance_to_value_or_larger(key) }.unwrap();
        self.get_item();
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        unsafe { self.cursor.rewind_to_value_or_smaller(key) }.unwrap();
        self.get_item();
    }

    fn valid(&self) -> bool {
        self.valid
    }

    fn rewind(&mut self) {
        self.cursor.move_first().unwrap();
        self.get_item();
    }

    fn fast_forward(&mut self) {
        self.cursor.move_last().unwrap();
        self.get_item();
    }

    fn position(&self) -> usize {
        self.cursor.absolute_position() as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.cursor = self
            .storage
            .file
            .rows()
            .subset(lower as u64..upper as u64)
            .first()
            .unwrap();
        self.get_item();
    }
}

impl<'a, K, R> Display for FileColumnLayerCursor<'a, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let mut cursor: FileColumnLayerCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.item();
            writeln!(f, "{key:?} -> {val:?}")?;
            cursor.step();
        }

        Ok(())
    }
}
