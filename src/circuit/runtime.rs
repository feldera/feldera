//! A multithreaded runtime for evaluating DBSP circuits in a data-parallel fashion.

use std::{
    sync::Arc,
    thread::{Builder, JoinHandle, Result as ThreadResult},
};
use typedmap::{TypedDashMap, TypedMapKey};

pub struct LocalStoreMarker;

/// Local data store shared by all workers in a runtime.
pub type LocalStore = TypedDashMap<LocalStoreMarker>;

struct RuntimeInner {
    nworkers: usize,
    store: LocalStore,
}

impl RuntimeInner {
    fn new(nworkers: usize) -> Self {
        Self {
            nworkers,
            store: TypedDashMap::new(),
        }
    }
}

/// A multithreaded runtime that hosts `N` circuits running in parallel worker threads.
/// Typically, all `N` circuits are identical, but this is not required or enforced.
#[repr(transparent)]
#[derive(Clone)]
pub struct Runtime(Arc<RuntimeInner>);

impl Runtime {
    /// Create a new runtime with `nworkers` worker threads and run a user-provided
    /// closure in each thread.  The closure takes a reference to the `Runtime`
    /// as an argument, so that workers can access shared services provided by the
    /// runtime.
    ///
    /// Returns a handle through which the caller can interact with the runtime.
    ///
    /// # Arguments
    ///
    /// * `nworkers` - the number of worker threads to spawn.
    ///
    /// * `f` - closure that will be invoked in each worker thread.  Normally,
    ///     this closure builds and runs a circuit.  The first argument of the
    ///     closure is a reference to the `Runtime`, the second argument is the
    ///     index of the worker thread within the runtime.
    ///
    /// # Examples
    /// ```
    /// use dbsp::circuit::{Root, Runtime,};
    ///
    /// // Create a runtime with 4 worker threads.
    /// let hruntime = Runtime::run(4, |runtime, index| {
    ///     // This closure runs within each worker thread.
    ///
    ///     let root = Root::build(move |circuit| {
    ///         // Populate `circuit` with operators.
    ///     });
    ///
    ///     // Run circuit for 100 clock cycles.
    ///     for _ in 0..100 {
    ///         root.step();
    ///     }
    /// });
    ///
    /// // Wait for all worker threads to terminate.
    /// hruntime.join().unwrap();
    /// ```
    pub fn run<F>(nworkers: usize, f: F) -> RuntimeHandle
    where
        F: FnOnce(&Runtime, usize) + Clone + Send + 'static,
    {
        let mut workers = Vec::with_capacity(nworkers);

        let runtime = Self(Arc::new(RuntimeInner::new(nworkers)));

        for i in 0..nworkers {
            let runtime = runtime.clone();
            let f = f.clone();
            let builder = Builder::new().name(format!("worker{}", i));

            let worker = builder
                .spawn(move || {
                    f(&runtime, i);
                })
                .unwrap_or_else(|_| panic!("failed to spawn worker thread {}", i));
            workers.push(worker);
        }

        RuntimeHandle::new(runtime, workers)
    }

    fn inner(&self) -> &RuntimeInner {
        &*self.0
    }

    /// Returns the number of workers in this runtime.
    pub fn num_workers(&self) -> usize {
        self.inner().nworkers
    }

    /// Returns reference to the data store shared by all workers within the runtime.
    ///
    /// This low-level mechanism can be used by various services that
    /// require common state shared across all workers.
    ///
    /// The [`LocalStore`] type is an alias to [`typedmap::TypedDashMap`], a concurrent
    /// map type that can store key/value pairs of different types.  See `typedmap`
    /// crate documentation for details.
    pub fn local_store(&self) -> &LocalStore {
        &self.inner().store
    }

    /// A per-worker sequential counter.
    ///
    /// This method can be used to generate unique identifiers that will be the same
    /// across all worker threads.  Repeated calls to this function with the same
    /// worker index generate numbers 0, 1, 2, ...
    pub fn sequence_next(&self, worker_index: usize) -> usize {
        debug_assert!(worker_index < self.inner().nworkers);
        let mut entry = self
            .local_store()
            .entry(WorkerId(worker_index))
            .or_insert(0);
        let result = *entry;
        *entry += 1;
        result
    }
}

/// Handle returned by `Runtime::run`.
pub struct RuntimeHandle {
    runtime: Runtime,
    workers: Vec<JoinHandle<()>>,
}

impl RuntimeHandle {
    fn new(runtime: Runtime, workers: Vec<JoinHandle<()>>) -> Self {
        Self { runtime, workers }
    }

    /// Returns reference to the runtime.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Wait for all workers in the runtime to terminate.
    ///
    /// The calling thread blocks until all worker threads have terminated.
    pub fn join(self) -> ThreadResult<()> {
        // Insist on joining all threads even if some of them fail.
        #[allow(clippy::needless_collect)]
        let results: Vec<ThreadResult<()>> = self.workers.into_iter().map(|h| h.join()).collect();
        results.into_iter().collect::<ThreadResult<()>>()
    }
}

#[derive(Hash, PartialEq, Eq)]
struct WorkerId(usize);

impl TypedMapKey<LocalStoreMarker> for WorkerId {
    type Value = usize;
}

#[cfg(test)]
mod tests {

    use super::Runtime;
    use crate::circuit::{
        operator::{Generator, Inspect},
        schedule::{DynamicScheduler, Scheduler, StaticScheduler},
        Root,
    };
    use std::{cell::RefCell, rc::Rc};

    #[test]
    fn test_runtime_static() {
        test_runtime::<StaticScheduler>();
    }

    #[test]
    fn test_runtime_dynamic() {
        test_runtime::<DynamicScheduler>();
    }

    fn test_runtime<S>()
    where
        S: Scheduler + 'static,
    {
        let hruntime = Runtime::run(4, |runtime, index| {
            let data = Rc::new(RefCell::new(vec![]));
            let data_clone = data.clone();
            let runtime = runtime.clone();
            let root = Root::build_with_scheduler::<_, S>(move |circuit| {
                let rtclone = runtime.clone();
                // Generator that produces values using `sequence_next`.
                let source = circuit.add_source(Generator::new(
                    rtclone.sequence_next(index),
                    move |n: &mut usize| *n = rtclone.sequence_next(index),
                ));
                circuit.add_sink(
                    Inspect::new(move |n: &usize| data_clone.borrow_mut().push(*n)),
                    &source,
                );
            });

            for _ in 0..100 {
                root.step();
            }

            assert_eq!(&*data.borrow(), &(0..100).collect::<Vec<usize>>());
        });

        hruntime.join().unwrap();
    }
}
