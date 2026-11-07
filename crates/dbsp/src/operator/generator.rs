//! Defines an operator that generates an infinite output stream from a single
//! seed value.

use super::require_persistent_id;
use crate::{
    Error,
    circuit::{
        GlobalNodeId, Runtime, Scope,
        checkpointer::checkpoint_invalid_data_error,
        operator_traits::{
            Data, FileOperatorCheckpoint, Operator, OperatorCheckpoint, SourceOperator,
        },
    },
    storage::file::to_bytes,
};
use feldera_storage::StoragePath;
use rkyv::bytecheck;
use std::{borrow::Cow, marker::PhantomData};

/// A source operator that yields an infinite output stream
/// from a generator function.
pub struct Generator<T, F> {
    generator: F,
    _t: PhantomData<T>,
}

impl<T, F> Generator<T, F> {
    /// Creates a generator
    pub fn new(g: F) -> Self {
        Self {
            generator: g,
            _t: Default::default(),
        }
    }
}

impl<T, F> Operator for Generator<T, F>
where
    T: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Generator")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }
    fn is_input(&self) -> bool {
        true
    }
}

impl<T, F> SourceOperator<T> for Generator<T, F>
where
    F: FnMut() -> T + 'static,
    T: 'static,
{
    async fn eval(&mut self) -> T {
        (self.generator)()
    }
}

/// A version of Generator that passes a flag to the generator function when `flush` has been called, giving it a chance
/// to produce one output per transaction.
pub struct TransactionGenerator<T, F> {
    generator: F,
    flush: bool,
    _t: PhantomData<T>,
}

impl<T, F> TransactionGenerator<T, F> {
    /// Creates a generator
    pub fn new(g: F) -> Self {
        Self {
            generator: g,
            flush: false,
            _t: Default::default(),
        }
    }
}

impl<T, F> Operator for TransactionGenerator<T, F>
where
    T: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Generator")
    }
    fn flush(&mut self) {
        self.flush = true;
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }
    fn is_input(&self) -> bool {
        true
    }
}

impl<T, F> SourceOperator<T> for TransactionGenerator<T, F>
where
    F: FnMut(bool) -> T + 'static,
    T: 'static,
{
    async fn eval(&mut self) -> T {
        let result = (self.generator)(self.flush);
        self.flush = false;
        result
    }
}

/// Generator operator for nested circuits.
///
/// At each parent clock tick, invokes a user-provided reset closure, which
/// returns a generator closure, that yields a nested stream.
pub struct GeneratorNested<T> {
    reset: Box<dyn FnMut() -> Box<dyn FnMut() -> T>>,
    generator: Option<Box<dyn FnMut() -> T>>,
    flush: bool,
    empty: T,
}

impl<T> GeneratorNested<T>
where
    T: Clone,
{
    /// Creates a nested generator with specified `reset` closure.
    pub fn new(reset: Box<dyn FnMut() -> Box<dyn FnMut() -> T>>, empty: T) -> Self {
        Self {
            reset,
            generator: None,
            flush: false,
            empty,
        }
    }
}

impl<T> Operator for GeneratorNested<T>
where
    T: Data,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("GeneratorNested")
    }
    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            self.generator = Some((self.reset)());
        }
    }

    fn is_input(&self) -> bool {
        true
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // TODO: do we want a version of `GeneratorNested` that
        // can inform the circuit that it's reached a fixedpoint?
        false
    }

    fn flush(&mut self) {
        self.flush = true;
    }
}

impl<T> SourceOperator<T> for GeneratorNested<T>
where
    T: Data,
{
    async fn eval(&mut self) -> T {
        if self.flush {
            self.flush = false;
            (self.generator.as_mut().unwrap())()
        } else {
            self.empty.clone()
        }
    }
}

/// A source operator that yields an infinite output stream
/// from a constant value.  Note: only worker 0 generates the data;
/// the other workers generate empty streams.
pub struct ConstantGenerator<T> {
    generator: T,
}

impl<T> ConstantGenerator<T>
where
    T: Clone,
{
    /// Creates a constant generator
    pub fn new(g: T) -> Self {
        Self { generator: g }
    }
}

impl<T> Operator for ConstantGenerator<T>
where
    T: Data + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ConstantGenerator")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        // Always a fixed-point
        true
    }
    fn is_input(&self) -> bool {
        true
    }
    fn is_deterministic_source(&self) -> bool {
        // A constant source has no external input: the bootstrap circuit copy
        // reproduces it by re-running, so it needs no replay source.
        true
    }
}

impl<T> SourceOperator<T> for ConstantGenerator<T>
where
    T: Data + 'static + Clone + Default,
{
    async fn eval(&mut self) -> T {
        if Runtime::worker_index() == 0 {
            self.generator.clone()
        } else {
            T::default()
        }
    }
}

/// A [`ConstantGenerator`] followed by a differentiator, as one operator: it
/// yields the value at the first step of the clock and `T::default()` at the
/// other steps.  Only worker 0 generates the value; the other workers generate
/// empty streams.  The state is one flag instead of the differentiator's copy
/// of the value; like the delay in the differentiator, the operator clears its
/// state at the end of the clock.
pub struct IncrementalConstantGenerator<T> {
    value: T,
    /// True after the first step of the clock.
    emitted: bool,
    global_id: GlobalNodeId,
}

#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[archive_attr(derive(rkyv::CheckBytes))]
struct CommittedIncrementalConstant {
    emitted: bool,
}

impl<T> IncrementalConstantGenerator<T> {
    /// Creates an incremental constant generator
    pub fn new(value: T) -> Self {
        Self {
            value,
            emitted: false,
            global_id: GlobalNodeId::root(),
        }
    }

    fn checkpoint_file<P: AsRef<str>>(persistent_id: P) -> String {
        format!("incremental-constant-{}.dat", persistent_id.as_ref())
    }
}

impl<T> Operator for IncrementalConstantGenerator<T>
where
    T: Data + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("IncrementalConstantGenerator")
    }
    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
    }
    fn clock_end(&mut self, _scope: Scope) {
        self.emitted = false;
    }
    fn fixedpoint(&self, scope: Scope) -> bool {
        // After the first step the output is empty
        scope > 0 || self.emitted
    }
    fn is_input(&self) -> bool {
        true
    }
    fn is_deterministic_source(&self) -> bool {
        // The bootstrap circuit copy reproduces the output by re-running the
        // operator from a clear state, so it needs no replay source.
        true
    }
    fn checkpoint(
        &mut self,
        persistent_id: Option<&str>,
    ) -> Result<Option<Box<dyn OperatorCheckpoint>>, Error> {
        let committed = CommittedIncrementalConstant {
            emitted: self.emitted,
        };
        Ok(Some(Box::new(FileOperatorCheckpoint {
            name: Self::checkpoint_file(require_persistent_id(persistent_id, &self.global_id)?),
            content: to_bytes(&committed)
                .expect("Serializing CommittedIncrementalConstant should work."),
        })))
    }
    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;
        let path = base.clone().join(Self::checkpoint_file(persistent_id));
        let content = Runtime::storage_backend().unwrap().read(&path)?;
        let committed = rkyv::check_archived_root::<CommittedIncrementalConstant>(&content)
            .map_err(|e| {
                checkpoint_invalid_data_error(
                    "IncrementalConstantGenerator checkpoint validation failed",
                    format!("{path}: {e}"),
                )
            })?;
        self.emitted = committed.emitted;
        Ok(())
    }
    fn clear_state(&mut self) -> Result<(), Error> {
        self.emitted = false;
        Ok(())
    }
    fn swap_state(&mut self, other: &mut Self) -> Result<(), Error> {
        std::mem::swap(&mut self.emitted, &mut other.emitted);
        Ok(())
    }
}

impl<T> SourceOperator<T> for IncrementalConstantGenerator<T>
where
    T: Data + 'static + Clone + Default,
{
    async fn eval(&mut self) -> T {
        let first = !self.emitted;
        self.emitted = true;
        if first && Runtime::worker_index() == 0 {
            self.value.clone()
        } else {
            T::default()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Circuit, OrdZSet, RootCircuit, Runtime,
        circuit::{
            CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
            operator_traits::{Operator, SourceOperator},
        },
        operator::{ConstantGenerator, IncrementalConstantGenerator, OutputHandle},
        zset,
    };
    use anyhow::Result as AnyResult;
    use futures::executor::block_on;
    use std::path::Path;
    use tempfile::tempdir;

    /// The output of a differentiated constant, and the output of an incremental constant.
    fn constants(
        circuit: &mut RootCircuit,
    ) -> AnyResult<(OutputHandle<OrdZSet<u64>>, OutputHandle<OrdZSet<u64>>)> {
        let value = zset! { 1u64 => 1, 2 => -1 };
        let differentiated = circuit
            .add_source(ConstantGenerator::new(value.clone()))
            .differentiate()
            .output();
        let incremental = circuit
            .add_source(IncrementalConstantGenerator::new(value))
            .output();
        Ok((differentiated, incremental))
    }

    #[test]
    fn incremental_constant_is_differentiated_constant() {
        let (mut dbsp, (differentiated, incremental)) =
            Runtime::init_circuit(2, constants).unwrap();
        for _ in 0..3 {
            dbsp.transaction().unwrap();
            assert_eq!(differentiated.consolidate(), incremental.consolidate());
        }
        dbsp.kill().unwrap();
    }

    fn circuit_config(path: &Path) -> CircuitConfig {
        let storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions::default(),
        )
        .expect("storage backend");
        CircuitConfig::from(2).with_storage(Some(storage))
    }

    fn incremental_constant(circuit: &mut RootCircuit) -> AnyResult<OutputHandle<OrdZSet<u64>>> {
        let stream = circuit.add_source(IncrementalConstantGenerator::new(zset! { 1u64 => 1 }));
        stream.set_persistent_id(Some("incremental"));
        Ok(stream.output_persistent(Some("output")))
    }

    /// A circuit restored from a checkpoint does not produce the value again.
    #[test]
    fn incremental_constant_restores_state() {
        let storage_dir = tempdir().expect("temp dir for circuit storage");
        let (mut dbsp, output) =
            Runtime::init_circuit(circuit_config(storage_dir.path()), incremental_constant)
                .unwrap();
        dbsp.transaction().unwrap();
        assert_eq!(output.consolidate(), zset! { 1u64 => 1 });
        let checkpoint = dbsp.checkpoint().run().unwrap();
        dbsp.kill().unwrap();

        let mut config = circuit_config(storage_dir.path());
        config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
        let (mut dbsp, output) = Runtime::init_circuit(config, incremental_constant).unwrap();
        dbsp.transaction().unwrap();
        let empty: OrdZSet<u64> = zset! {};
        assert_eq!(output.consolidate(), empty);
        dbsp.kill().unwrap();
    }

    /// In a nested circuit the clock of the operator ends after each step of the parent;
    /// like the delay of a differentiator, the operator then produces the value again.
    #[test]
    fn incremental_constant_restarts_with_its_clock() {
        let value = zset! { 1u64 => 1 };
        let empty: OrdZSet<u64> = zset! {};
        let mut constant = IncrementalConstantGenerator::new(value.clone());
        assert!(!constant.fixedpoint(0));
        assert_eq!(block_on(constant.eval()), value);
        assert!(constant.fixedpoint(0));
        assert_eq!(block_on(constant.eval()), empty);
        constant.clock_end(0);
        assert!(!constant.fixedpoint(0));
        assert_eq!(block_on(constant.eval()), value);
    }
}
