//! Bound in-flight Delta object-store GETs so a slow HTTP body cannot OOM the pipeline.
//!
//! `object_store` copies each GET body across the runtime boundary through a
//! deliberately unbounded channel (`client/http/spawn.rs`: "We use an unbounded
//! channel to prevent backpressure across the runtime boundary"). On a slow link
//! the bodies accumulate faster than the pipeline drains them. Hold a reader slot
//! for the whole life of the body stream so snapshot fan-out stays finite.
//!
//! Why not `object_store::limit::LimitStore`, which also permit-gates GETs: it
//! builds a private semaphore per store instance, so every Delta connector would
//! get its own budget and the process-wide bound this needs would not hold. The
//! semaphore here is shared by every wrapped store (see
//! `separately_wrapped_stores_share_one_global_budget`). This wrapper also gates
//! reads only, leaving list/put paths untouched.

use anyhow::{Result as AnyResult, bail};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, Stream, StreamExt};
// Use the `object_store` that `deltalake` re-exports: the wrapper is handed back
// to delta-rs and DataFusion, so it must implement *their* `ObjectStore` trait.
use deltalake::logstore::object_store::path::Path;
use deltalake::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use std::fmt::{self, Debug, Display};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default number of concurrently buffered object-store reads.
pub(super) const DEFAULT_MAX_CONCURRENT_READERS: usize = 6;

/// The process-wide read budget: a semaphore plus the value a connector set on it.
///
/// Kept together so tests can build their own budget instead of mutating the
/// global one, which only ever moves 0 -> N once per process.
pub(super) struct ReaderBudget {
    semaphore: Arc<Semaphore>,
    /// Configured `max_concurrent_readers` (0 = not set by any connector).
    configured: AtomicUsize,
}

impl ReaderBudget {
    pub(super) fn new(permits: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            configured: AtomicUsize::new(0),
        }
    }

    pub(super) fn semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.semaphore)
    }

    /// Apply `max_concurrent_readers`. Returns `true` if this caller set the value.
    pub(super) fn apply(&self, max_concurrent_readers: usize) -> AnyResult<bool> {
        if max_concurrent_readers == 0 {
            bail!(
                "invalid 'max_concurrent_readers' value: 'max_concurrent_readers' must be greater than 0"
            );
        }

        let first_setter = match self.configured.compare_exchange(
            0,
            max_concurrent_readers,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(current) if current == max_concurrent_readers => false,
            Err(_) => {
                bail!(
                    "found conflicting values of the `max_concurrent_readers` attribute: this is a global setting that affects all Delta Lake connectors, and not just the connector where it is specified; if multiple connectors specify `max_concurrent_readers`, they must all use the same value."
                );
            }
        };

        if first_setter {
            // Tokens have not been acquired yet: connectors initialize before the first GET.
            let available = self.semaphore.available_permits();
            if max_concurrent_readers > available {
                self.semaphore
                    .add_permits(max_concurrent_readers - available);
            } else if max_concurrent_readers < available {
                self.semaphore
                    .forget_permits(available - max_concurrent_readers);
            }
        }

        Ok(first_setter)
    }
}

static DELTA_READER_BUDGET: LazyLock<ReaderBudget> =
    LazyLock::new(|| ReaderBudget::new(DEFAULT_MAX_CONCURRENT_READERS));

/// Apply `max_concurrent_readers` to the process-wide read budget.
pub(super) fn apply_max_concurrent_readers(max_concurrent_readers: usize) -> AnyResult<bool> {
    DELTA_READER_BUDGET.apply(max_concurrent_readers)
}

/// Wrap `inner` with the process-wide Delta GET limiter.
pub(super) fn bound_delta_reads(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(BoundedObjectStore::with_semaphore(
        inner,
        DELTA_READER_BUDGET.semaphore(),
    ))
}

/// Holds a reader slot until a streaming GET body is dropped.
struct BoundedObjectStore {
    inner: Arc<dyn ObjectStore>,
    semaphore: Arc<Semaphore>,
    inflight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl BoundedObjectStore {
    fn with_semaphore(inner: Arc<dyn ObjectStore>, semaphore: Arc<Semaphore>) -> Self {
        Self {
            inner,
            semaphore,
            inflight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[cfg(test)]
    fn in_flight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn peak_in_flight(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    async fn acquire(&self) -> (OwnedSemaphorePermit, InflightGuard) {
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("Delta reader semaphore closed");
        (permit, InflightGuard::enter(&self.inflight, &self.peak))
    }
}

impl Display for BoundedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BoundedObjectStore({})", self.inner)
    }
}

impl Debug for BoundedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedObjectStore")
            .field("inner", &self.inner.to_string())
            .finish()
    }
}

struct InflightGuard {
    inflight: Arc<AtomicUsize>,
}

impl InflightGuard {
    fn enter(inflight: &Arc<AtomicUsize>, peak: &Arc<AtomicUsize>) -> Self {
        let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(n, Ordering::SeqCst);
        Self {
            inflight: Arc::clone(inflight),
        }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Keeps the reader slot alive until the GET body stream is dropped.
struct PermitStream<S> {
    inner: S,
    _permit: OwnedSemaphorePermit,
    _guard: InflightGuard,
}

impl<S: Stream + Unpin> Stream for PermitStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

fn permit_get_result(
    result: GetResult,
    permit: OwnedSemaphorePermit,
    guard: InflightGuard,
) -> GetResult {
    match result.payload {
        GetResultPayload::Stream(stream) => GetResult {
            payload: GetResultPayload::Stream(
                PermitStream {
                    inner: stream,
                    _permit: permit,
                    _guard: guard,
                }
                .boxed(),
            ),
            ..result
        },
        payload => {
            // Local-file payloads are not HTTP bodies; release the slot now.
            drop(permit);
            drop(guard);
            GetResult { payload, ..result }
        }
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for BoundedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let (permit, guard) = self.acquire().await;
        match self.inner.get_opts(location, options).await {
            Ok(result) => Ok(permit_get_result(result, permit, guard)),
            Err(e) => Err(e),
        }
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let (_permit, _guard) = self.acquire().await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::logstore::object_store::memory::InMemory;
    use deltalake::logstore::object_store::{ObjectStoreExt, PutPayload};
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio::time::timeout;

    const FILE: &str = "part-00000.parquet";

    async fn seeded_memory() -> InMemory {
        let store = InMemory::new();
        store
            .put(&Path::from(FILE), PutPayload::from_static(b"delta-row"))
            .await
            .unwrap();
        store
    }

    /// Inner store that counts how many GETs passed the outer limiter.
    /// Without a bound this counter would track every concurrent caller.
    #[derive(Debug)]
    struct GatedStore {
        inner: InMemory,
        started: Arc<AtomicUsize>,
        gate: watch::Receiver<bool>,
    }

    impl Display for GatedStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "GatedStore")
        }
    }

    #[async_trait]
    #[deny(clippy::missing_trait_methods)]
    impl ObjectStore for GatedStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.started.fetch_add(1, Ordering::SeqCst);
            let mut gate = self.gate.clone();
            let _ = gate.wait_for(|open| *open).await;
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    #[test]
    fn budget_rejects_zero() {
        let budget = ReaderBudget::new(DEFAULT_MAX_CONCURRENT_READERS);
        let err = budget.apply(0).unwrap_err().to_string();
        assert!(err.contains("must be greater than 0"), "{err}");
    }

    #[test]
    fn budget_first_setter_resizes_then_agrees_with_itself() {
        let budget = ReaderBudget::new(DEFAULT_MAX_CONCURRENT_READERS);
        assert!(budget.apply(32).unwrap(), "first caller sets the value");
        assert_eq!(budget.semaphore().available_permits(), 32);
        assert!(!budget.apply(32).unwrap(), "same value is not a new setter");
        assert_eq!(budget.semaphore().available_permits(), 32);
    }

    #[test]
    fn budget_shrinks_when_configured_below_default() {
        let budget = ReaderBudget::new(DEFAULT_MAX_CONCURRENT_READERS);
        assert!(budget.apply(2).unwrap());
        assert_eq!(budget.semaphore().available_permits(), 2);
    }

    #[test]
    fn budget_rejects_conflicting_values() {
        let budget = ReaderBudget::new(DEFAULT_MAX_CONCURRENT_READERS);
        budget.apply(8).unwrap();
        let err = budget.apply(9).unwrap_err().to_string();
        assert!(err.contains("conflicting values"), "{err}");
        assert_eq!(
            budget.semaphore().available_permits(),
            8,
            "a rejected value must not resize the semaphore"
        );
    }

    #[tokio::test]
    async fn concurrent_gets_never_exceed_reader_cap() {
        let cap = 2;
        let started = Arc::new(AtomicUsize::new(0));
        let (gate_tx, gate_rx) = watch::channel(false);
        let gated = GatedStore {
            inner: seeded_memory().await,
            started: Arc::clone(&started),
            gate: gate_rx,
        };
        let bounded = Arc::new(BoundedObjectStore::with_semaphore(
            Arc::new(gated),
            Arc::new(Semaphore::new(cap)),
        ));
        let path = Path::from(FILE);

        let mut joins = Vec::new();
        for _ in 0..16 {
            let store = Arc::clone(&bounded);
            let path = path.clone();
            joins.push(tokio::spawn(async move { store.get(&path).await }));
        }

        timeout(Duration::from_secs(2), async {
            loop {
                if started.load(Ordering::SeqCst) >= cap {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("inner GETs should start");

        // Without the wrapper, all 16 tasks would have entered GatedStore.
        assert_eq!(started.load(Ordering::SeqCst), cap);
        assert_eq!(bounded.peak_in_flight(), cap);
        assert_eq!(bounded.in_flight(), cap);

        gate_tx.send(true).unwrap();
        for join in joins {
            join.await.unwrap().unwrap();
        }
        assert_eq!(bounded.peak_in_flight(), cap);
        assert_eq!(bounded.in_flight(), 0);
    }

    /// Two independently wrapped stores must contend for ONE process-wide budget.
    ///
    /// This is the property `object_store::limit::LimitStore` cannot provide: it
    /// builds a private semaphore per store, so N Delta connectors would get N
    /// separate budgets and the process-wide bound would not hold.
    ///
    /// This pins the sharing property only. It does not cover the `input.rs`
    /// registration sites, which need an integration test against a real object
    /// store.
    #[tokio::test]
    async fn separately_wrapped_stores_share_one_global_budget() {
        let cap = 3;
        let budget = ReaderBudget::new(cap);

        let started = Arc::new(AtomicUsize::new(0));
        let (gate_tx, gate_rx) = watch::channel(false);

        let mut stores = Vec::new();
        for _ in 0..2 {
            stores.push(Arc::new(BoundedObjectStore::with_semaphore(
                Arc::new(GatedStore {
                    inner: seeded_memory().await,
                    started: Arc::clone(&started),
                    gate: gate_rx.clone(),
                }),
                budget.semaphore(),
            )));
        }

        // 2 * cap callers race for cap global slots.
        let path = Path::from(FILE);
        let mut joins = Vec::new();
        for store in &stores {
            for _ in 0..cap {
                let store = Arc::clone(store);
                let path = path.clone();
                joins.push(tokio::spawn(async move { store.get(&path).await }));
            }
        }

        timeout(Duration::from_secs(5), async {
            loop {
                if started.load(Ordering::SeqCst) >= cap {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("inner GETs should start");

        // Let any unbounded extra GETs slip through before asserting.
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }

        // With a per-store semaphore this would be 2 * cap.
        assert_eq!(started.load(Ordering::SeqCst), cap);

        gate_tx.send(true).unwrap();
        for join in joins {
            join.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn get_stream_holds_slot_until_dropped() {
        let inner = seeded_memory().await;
        let bounded =
            BoundedObjectStore::with_semaphore(Arc::new(inner), Arc::new(Semaphore::new(1)));
        let path = Path::from(FILE);

        let first = bounded.get(&path).await.unwrap();
        let blocked = timeout(Duration::from_millis(80), bounded.get(&path)).await;
        assert!(blocked.is_err(), "second GET must wait on the live body");

        drop(first);
        let second = bounded
            .get(&path)
            .await
            .expect("slot should free when the stream is dropped");
        assert_eq!(bounded.in_flight(), 1);
        drop(second);
        assert_eq!(bounded.in_flight(), 0);
    }
}
