mod builders;
pub(crate) mod cursor;

use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{DataTrait, DynVec, Erase, Factory, WeightTrait, WeightTraitTyped, WithFactory},
    storage::file::{
        reader::{FallibleEq, Reader},
        Factories as FileFactories,
    },
    trace::{
        layers::{Builder, Cursor, Trie, TupleBuilder},
        ord::file::StorageBackend,
    },
    DBData, DBWeight, NumEntries, Runtime,
};
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::path::PathBuf;
use std::{
    cmp::min,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    ops::{Add, AddAssign, Neg},
};

pub use self::{builders::FileColumnLayerBuilder, cursor::FileColumnLayerCursor};

pub struct FileLeafFactories<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    pub(crate) file_factories: FileFactories<K, R>,
    pub(crate) diff_factory: &'static dyn Factory<R>,
}

impl<K, R> FileLeafFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new<KType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        RType: DBData + Erase<R>,
    {
        Self {
            file_factories: FileFactories::new::<KType, RType>(),
            diff_factory: WithFactory::<RType>::FACTORY,
        }
    }
}

impl<K, R> Clone for FileLeafFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            file_factories: self.file_factories.clone(),
            diff_factory: self.diff_factory,
        }
    }
}

pub struct FileColumnLayer<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    pub factories: FileLeafFactories<K, R>,
    file: Reader<StorageBackend, (&'static K, &'static R, ())>,
    lower_bound: usize,
}

impl<K, R> Clone for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            file: self.file.clone(),
            lower_bound: self.lower_bound,
        }
    }
}

impl<K, R> FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn len(&self) -> u64 {
        self.file.n_rows(0)
    }

    pub fn is_empty(&self) -> bool {
        self.file.n_rows(0) == 0
    }

    pub fn empty(factories: &FileLeafFactories<K, R>) -> Self {
        Self {
            factories: factories.clone(),
            file: Reader::empty(&Runtime::storage()).unwrap(),
            lower_bound: 0,
        }
    }

    pub fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<K>)
    where
        RG: Rng,
    {
        let size = self.keys();
        let mut cursor = self.cursor();
        if sample_size >= size {
            output.reserve(size);

            while let Some((key, _)) = cursor.get_current_item() {
                output.push_ref(key);
                cursor.step();
            }
        } else {
            output.reserve(sample_size);

            let mut indexes = sample(rng, size, sample_size).into_vec();
            indexes.sort_unstable();
            for index in indexes.into_iter() {
                cursor.move_to_row(index);
                output.push_ref(cursor.current_key());
            }
        }
    }

    /// Remove keys smaller than `lower_bound` from the batch.
    pub fn truncate_keys_below(&mut self, lower_bound: &K) {
        let mut cursor = self.file.rows().before();
        unsafe { cursor.advance_to_value_or_larger(lower_bound) }.unwrap();
        self.truncate_below(cursor.absolute_position() as usize);
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.file.path()
    }
}

impl<K, R> Debug for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "FileColumnLayer(lower_bound={}):", self.lower_bound)?;
        let mut cursor = self.cursor();
        while let Some((key, diff)) = cursor.get_current_item() {
            write!(f, " ({key:?}, {diff:+?})")?;
            cursor.step();
        }
        Ok(())
    }
}

impl<K, R> Display for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Display::fmt(&self.cursor(), f)
    }
}

impl<K, R> Trie for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Item<'a> = (&'a mut K, &'a mut R);

    type ItemRef<'a> = (&'a K, &'a R);
    type Factories = FileLeafFactories<K, R>;

    type Cursor<'s> = FileColumnLayerCursor<'s, K, R> where K: 's, R: 's;
    type MergeBuilder = FileColumnLayerBuilder<K, R>;
    type TupleBuilder = FileColumnLayerBuilder<K, R>;

    fn keys(&self) -> usize {
        self.file.rows().len() as usize - self.lower_bound
    }

    fn tuples(&self) -> usize {
        self.file.rows().len() as usize - self.lower_bound
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        FileColumnLayerCursor::new(self, lower as u64..upper as u64)
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.file.rows().len() as usize);
        }
    }

    fn lower_bound(&self) -> usize {
        self.lower_bound
    }
}

impl<K, R> Archive for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, R, S> Serialize<S> for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, R, D> Deserialize<FileColumnLayer<K, R>, D> for Archived<FileColumnLayer<K, R>>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileColumnLayer<K, R>, D::Error> {
        unimplemented!();
    }
}

impl<K, R> SizeOf for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, R> PartialEq for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.file.equals(&other.file).unwrap()
    }
}

impl<K, R> Eq for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, R> NumEntries for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.len() as usize
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        // FIXME: Doesn't take element sizes into account
        self.len() as usize
    }
}

impl<K, R> Add<Self> for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        if self.is_empty() {
            rhs
        } else if rhs.is_empty() {
            self
        } else {
            self.merge(&rhs)
        }
    }
}

impl<K, R> AddAssign<Self> for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
    }
}

impl<K, R> AddAssignByRef for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            *self = self.merge(other);
        }
    }
}

impl<K, R> AddByRef for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, R> NegByRef for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    fn neg_by_ref(&self) -> Self {
        let mut tuple_builder = <Self as Trie>::TupleBuilder::new(&self.factories);
        let mut cursor = self.cursor();
        while let Some((key, diff)) = cursor.get_current_item() {
            let diff = diff.neg_by_ref();
            tuple_builder.push_refs((key, diff.erase()));
            cursor.step();
        }
        tuple_builder.done()
    }
}

impl<K, R> Neg for FileColumnLayer<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        let mut tuple_builder = <Self as Trie>::TupleBuilder::new(&self.factories);
        let mut cursor = self.cursor();
        while let Some((key, diff)) = cursor.get_current_item() {
            let diff = diff.deref().neg_by_ref();
            tuple_builder.push_refs((key, diff.erase()));
            cursor.step();
        }
        tuple_builder.done()
    }
}
