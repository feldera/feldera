use std::hash::{Hash, Hasher};
use dbsp_adapters::Catalog;
use pyo3::prelude::*;
use rkyv;
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize};
use rkyv::ser::Serializer;
use size_of::SizeOf;

pub use dbsp;
pub use dbsp::circuit::CircuitConfig;
pub use dbsp::DBSPHandle;
pub use dbsp::Error;
use pipeline_types::{deserialize_without_context, serialize_without_context};

#[pyclass]
struct Runtime {
    handle: DBSPHandle,
    catalog: Catalog,
}

#[derive(
    Clone,
    Default,
    Debug,
    SizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
struct WrapperPyObject {
    #[serde(skip)]
    #[size_of(skip)]
    inner: Option<PyObject>,
}

deserialize_without_context!(WrapperPyObject);
serialize_without_context!(WrapperPyObject);
impl Hash for WrapperPyObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        //self.inner.hash(state)
        todo!()
    }
}

impl Ord for WrapperPyObject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        //self.inner.cmp(&other.inner)
        todo!()
    }
}

impl PartialEq for WrapperPyObject {
    fn eq(&self, other: &Self) -> bool {
        //self.inner == other.inner
        todo!()
    }
}

impl PartialOrd for WrapperPyObject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        //self.inner.partial_cmp(&other.inner)
        todo!()
    }
}

impl Eq for WrapperPyObject {}

impl Archive for WrapperPyObject
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<S: Serializer + ?Sized> Serialize<S> for WrapperPyObject {
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<D: Fallible> Deserialize<WrapperPyObject, D> for Archived<WrapperPyObject> {
    fn deserialize(&self, _deserializer: &mut D) -> Result<WrapperPyObject, D::Error> {
        unimplemented!();
    }
}

#[pymethods]
impl Runtime {
    #[new]
    fn new(workers: usize) -> PyResult<Self> {
        let cconf = CircuitConfig::with_workers(workers);
        let (handle, catalog) = dbsp::Runtime::init_circuit(cconf, |circuit| {
            let mut catalog = Catalog::new();

            let (stream14, handle14) = circuit.add_input_zset::<WrapperPyObject>();
            catalog.register_input_zset::<_, WrapperPyObject>(stream14.clone(), handle14, r#"{"name":"USERS","case_sensitive":false,"fields":[{"name":"NAME","case_sensitive":false,"columntype":{"type":"VARCHAR","nullable":false,"precision":-1}}]}"#);
            catalog.register_output_zset::<_, WrapperPyObject>(stream14.clone(), r#"{"name":"OUTPUT_USERS","case_sensitive":false,"fields":[{"name":"NAME","case_sensitive":false,"columntype":{"type":"INTEGER","nullable":false,"precision":-1}}]}"#);

            Ok(catalog)
        }).expect("Failed to initialize circuit");

        Ok(Runtime { handle, catalog })
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn pydbsp(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Runtime>()?;
    Ok(())
}
