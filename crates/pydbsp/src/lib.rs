use pyo3::prelude::*;

pub use dbsp;
pub use dbsp::circuit::CircuitConfig;
pub use dbsp::DBSPHandle;
pub use dbsp::Error;
use dbsp_adapters::Catalog;

#[pyclass]
struct Runtime {
    handle: DBSPHandle,
    catalog: Catalog,
}

#[pymethods]
impl Runtime {
    #[new]
    fn new(workers: usize) -> PyResult<Self> {
        let cconf = CircuitConfig::with_workers(workers);
        let (handle, catalog) = dbsp::Runtime::init_circuit(cconf, |circuit| {
            let mut catalog = Catalog::new();

            let (stream14, handle14) = circuit.add_input_zset::<i64>();
            catalog.register_input_zset::<_, i64>(stream14.clone(), handle14, r#"{"name":"USERS","case_sensitive":false,"fields":[{"name":"NAME","case_sensitive":false,"columntype":{"type":"VARCHAR","nullable":false,"precision":-1}}]}"#);
            catalog.register_output_zset::<_, i64>(stream14.clone(), r#"{"name":"OUTPUT_USERS","case_sensitive":false,"fields":[{"name":"NAME","case_sensitive":false,"columntype":{"type":"INTEGER","nullable":false,"precision":-1}}]}"#);

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
