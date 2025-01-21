use crate::dynamic::DataTrait;
use crate::storage::backend::{FileReader, Storage, StorageError};
use crate::storage::file::cache::FileCache;
use crate::storage::file::reader::{ImmutableFileRef, Reader};
use crate::storage::file::{AnyFactories, Factories};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

/// Configuration parameters for writing a layer file.
///
/// The default parameters should usually be good enough.
pub struct Parameters {}

impl Parameters {}

impl Default for Parameters {
    fn default() -> Self {
        Self {}
    }
}

/// General-purpose layer file writer.
///
/// A `Writer` can write a layer file with any number of columns.  It lacks type
/// safety to ensure that the data and auxiliary values written to columns are
/// all the same type.  Thus, [`Writer1`] and [`Writer2`] exist for writing
/// 1-column and 2-column layer files, respectively, with added type safety.
struct Writer {
    arrow_writer: ArrowWriter<File>,
}

impl Writer {
    pub fn new(
        factories: &[&AnyFactories],
        writer: &Arc<FileCache>,
        parameters: Parameters,
        n_columns: usize,
    ) -> Result<Self, StorageError> {
        let file = writer.create()?;
        let schema = parquet::schema::types::Type::group_type_builder("root")
            .with_fields(factories.iter().map(|f| f.schema()))
            .build()
            .unwrap();

        Ok(Self {
            arrow_writer: ArrowWriter::try_new(writer.as_file(), schema, parameters.into())?,
        })
    }

    pub fn write<K, A>(&mut self, column: usize, item: (&K, &A)) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        unimplemented!()
    }

    pub fn finish_column<K, A>(&mut self, column: usize) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        unimplemented!()
    }

    pub fn close(mut self) -> Result<(Arc<dyn FileReader>, PathBuf), StorageError> {
        unimplemented!()
    }

    pub fn n_columns(&self) -> usize {
        unimplemented!()
    }

    pub fn n_rows(&self) -> u64 {
        unimplemented!()
    }

    pub fn storage(&self) -> &Arc<FileCache> {
        unimplemented!()
    }
}

/// 1-column layer file writer.
///
/// `Writer1<K0, A0>` writes a new 1-column layer file in which column 0 has
/// key and auxiliary data types `(K0, A0)`.
///
/// # Example
///
/// The following code writes 1000 rows in column 0 with values `(0, ())`
/// through `(999, ())`.
///
/// ```
/// # use dbsp::dynamic::{DynData, Erase, DynUnit};
/// # use dbsp::storage::file::{writer::{Parameters, Writer1}};
/// use dbsp::storage::file::cache::default_cache;
/// use dbsp::storage::file::Factories;
/// let factories = Factories::<DynData, DynUnit>::new::<u32, ()>();
/// let mut file =
///     Writer1::new(&factories, &default_cache(), Parameters::default()).unwrap();
/// for i in 0..1000_u32 {
///     file.write0((i.erase(), ().erase())).unwrap();
/// }
/// file.close().unwrap();
/// ```
pub struct Writer1<K0, A0>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
{
    inner: Writer,
    pub(crate) factories: Factories<K0, A0>,
    _phantom: PhantomData<fn(&K0, &A0)>,
    #[cfg(debug_assertions)]
    prev0: Option<Box<K0>>,
}

impl<K0, A0> Writer1<K0, A0>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
{
    /// Creates a new writer with the given parameters.
    pub fn new(
        factories: &Factories<K0, A0>,
        storage: &Arc<FileCache>,
        parameters: Parameters,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            factories: factories.clone(),
            inner: Writer::new(&[&factories.any_factories()], storage, parameters, 1)?,
            _phantom: PhantomData,
            #[cfg(debug_assertions)]
            prev0: None,
        })
    }
    /// Writes `item` to column 0.  `item.0` must be greater than passed in the
    /// previous call to this function (if any).
    pub fn write0(&mut self, item: (&K0, &A0)) -> Result<(), StorageError> {
        unimplemented!()
    }

    /// Returns the number of calls to [`write0`](Self::write0) so far.
    pub fn n_rows(&self) -> u64 {
        unimplemented!()
    }

    /// Finishes writing the layer file and returns the writer passed to
    /// [`new`](Self::new).
    pub fn close(mut self) -> Result<(Arc<dyn FileReader>, PathBuf), StorageError> {
        unimplemented!()
    }

    /// Returns the storage used for this writer.
    pub fn storage(&self) -> &Arc<FileCache> {
        self.inner.storage()
    }

    /// Finishes writing the layer file and returns a reader for it.
    pub fn into_reader(
        self,
    ) -> Result<Reader<(&'static K0, &'static A0, ())>, super::reader::Error> {
        let storage = self.storage().clone();
        let any_factories = self.factories.any_factories();

        let (file_handle, path) = self.close()?;
        Reader::new(
            &[&any_factories],
            Arc::new(ImmutableFileRef::new(&storage, file_handle, path)),
        )
    }
}

/// 2-column layer file writer.
///
/// `Writer2<K0, A0, K1, A1>` writes a new 2-column layer file in which
/// column 0 has key and auxiliary data types `(K0, A0)` and column 1 has `(K1,
/// A1)`.
///
/// Each row in column 0 must be associated with a group of one or more rows in
/// column 1.  To form the association, first write the rows to column 1 using
/// [`write1`](Self::write1) then the row to column 0 with
/// [`write0`](Self::write0).
///
/// # Example
///
/// The following code writes 1000 rows in column 0 with values `(0, ())`
/// through `(999, ())`, each associated with 10 rows in column 1 with values
/// `(0, ())` through `(9, ())`.
///
/// ```
/// # use dbsp::dynamic::{DynData, DynUnit};
/// use dbsp::storage::file::{cache::default_cache, writer::{Parameters, Writer2}};
/// # use dbsp::storage::file::Factories;
/// let factories = Factories::<DynData, DynUnit>::new::<u32, ()>();
/// let mut file =
///     Writer2::new(&factories, &factories, &default_cache(), Parameters::default()).unwrap();
/// for i in 0..1000_u32 {
///     for j in 0..10_u32 {
///         file.write1((&j, &())).unwrap();
///     }
///     file.write0((&i, &())).unwrap();
/// }
/// file.close().unwrap();
/// ```
pub struct Writer2<K0, A0, K1, A1>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: DataTrait + ?Sized,
{
    inner: Writer,
    pub(crate) factories0: Factories<K0, A0>,
    pub(crate) factories1: Factories<K1, A1>,
    #[cfg(debug_assertions)]
    prev0: Option<Box<K0>>,
    #[cfg(debug_assertions)]
    prev1: Option<Box<K1>>,
    _phantom: PhantomData<fn(&K0, &A0, &K1, &A1)>,
}

impl<K0, A0, K1, A1> Writer2<K0, A0, K1, A1>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: DataTrait + ?Sized,
{
    /// Creates a new writer with the given parameters.
    pub fn new(
        factories0: &Factories<K0, A0>,
        factories1: &Factories<K1, A1>,
        storage: &Arc<FileCache>,
        parameters: Parameters,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            factories0: factories0.clone(),
            factories1: factories1.clone(),
            inner: Writer::new(
                &[&factories0.any_factories(), &factories1.any_factories()],
                storage,
                parameters,
                2,
            )?,
            #[cfg(debug_assertions)]
            prev0: None,
            #[cfg(debug_assertions)]
            prev1: None,
            _phantom: PhantomData,
        })
    }
    /// Writes `item` to column 0.  All of the items previously written to
    /// column 1 since the last call to this function (if any) become the row
    /// group associated with `item`.  There must be at least one such item.
    ///
    /// `item.0` must be greater than passed in the previous call to this
    /// function (if any).
    pub fn write0(&mut self, item: (&K0, &A0)) -> Result<(), StorageError> {
        unimplemented!()
    }

    /// Writes `item` to column 1.  `item.0` must be greater than passed in the
    /// previous call to this function (if any) since the last call to
    /// [`write0`](Self::write0) (if any).
    pub fn write1(&mut self, item: (&K1, &A1)) -> Result<(), StorageError> {
        unimplemented!()
    }

    /// Returns the number of calls to [`write0`](Self::write0) so far.
    pub fn n_rows(&self) -> u64 {
        unimplemented!()
    }

    /// Finishes writing the layer file and returns the writer passed to
    /// [`new`](Self::new).
    ///
    /// This function will panic if [`write1`](Self::write1) has been called
    /// without a subsequent call to [`write0`](Self::write0).
    pub fn close(mut self) -> Result<(Arc<dyn FileReader>, PathBuf), StorageError> {
        unimplemented!()
    }

    /// Returns the storage used for this writer.
    pub fn storage(&self) -> &Arc<FileCache> {
        self.inner.storage()
    }

    /// Finishes writing the layer file and returns a reader for it.
    #[allow(clippy::type_complexity)]
    pub fn into_reader(
        self,
    ) -> Result<
        Reader<(&'static K0, &'static A0, (&'static K1, &'static A1, ()))>,
        super::reader::Error,
    > {
        unimplemented!()
    }
}
