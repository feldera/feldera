//! A process-wide cap on concurrent object-store part uploads.
//!
//! delta-rs drives ten concurrent parts per writer and a flush runs one writer per key range,
//! so `threads` ranges keep `threads * 10` uploads in flight.  Past the host's socket budget
//! the store fails with ENOBUFS, and because the retry rewrites the whole flush, each attempt
//! orphans the parquet the last one wrote: eight attempts left 113 GB behind for a 20 GB
//! table.  The cap turns that overrun into backpressure.
//!
//! It covers every store this connector builds, so `append` and `cdc` mode get it too: they
//! write one Parquet file per range through the same writer, and so run the same overrun.
//!
//! The bin-packing half of `OPTIMIZE` escapes it, because delta-rs opens that table itself
//! and handing it a bounded store means rebuilding the table from a root store this connector
//! assembles -- which is what `uc://` credential vending exists to avoid.  Its two rewrite
//! tasks are a tenth of the concurrency that exhausted the socket budget, so the gap is
//! recorded rather than closed.

use std::sync::Arc;

use deltalake::logstore::ObjectStoreRef;
use deltalake::logstore::object_store::path::Path;
use deltalake::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use futures::stream::BoxStream;
use tokio::sync::Semaphore;

/// At 5 MiB a part, this carries a flush's ~110 MB/s over a 100 ms round trip with room to
/// spare, and stays well under the concurrency that exhausted the socket budget.
const MAX_CONCURRENT_UPLOADS: usize = 16;

fn upload_budget() -> Arc<Semaphore> {
    static BUDGET: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
    BUDGET
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS)))
        .clone()
}

/// Wrap `inner` so that its part uploads draw on the process-wide budget.
pub(super) fn bound_uploads(inner: ObjectStoreRef) -> ObjectStoreRef {
    with_budget(inner, upload_budget())
}

fn with_budget(inner: ObjectStoreRef, budget: Arc<Semaphore>) -> ObjectStoreRef {
    Arc::new(BoundedUploadStore { inner, budget })
}

#[derive(Debug)]
struct BoundedUploadStore {
    inner: ObjectStoreRef,
    budget: Arc<Semaphore>,
}

impl std::fmt::Display for BoundedUploadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BoundedUpload({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for BoundedUploadStore {
    // Unbounded: a whole-object put is one request, and the commits and deletion vectors that
    // use it are few.  Holding a permit here would only queue a commit behind bulk uploads.
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
        // The budget is spent by the parts, not by opening the upload: this call does no I/O
        // beyond creating the upload, and holding a permit across its whole life would cap
        // concurrent *files* rather than concurrent requests.
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(BoundedUpload {
            inner,
            budget: self.budget.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
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

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct BoundedUpload {
    inner: Box<dyn MultipartUpload>,
    budget: Arc<Semaphore>,
}

#[async_trait::async_trait]
impl MultipartUpload for BoundedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // `UploadPart` is `'static`, so the permit is taken when the caller polls the part
        // rather than when it builds it: the writer may build ten and await them together.
        let budget = self.budget.clone();
        let part = self.inner.put_part(data);
        Box::pin(async move {
            let _permit = budget
                .acquire_owned()
                .await
                .expect("budget is never closed");
            part.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use deltalake::logstore::object_store::ObjectStoreExt as _;
    use deltalake::logstore::object_store::memory::InMemory;
    use futures::future::try_join_all;

    use super::*;

    /// Counts the part uploads running at once, so a test can see the cap hold.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<InMemory>,
        live: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
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
            Ok(Box::new(CountingUpload {
                inner: self.inner.put_multipart_opts(location, opts).await?,
                live: self.live.clone(),
                peak: self.peak.clone(),
            }))
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
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

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[derive(Debug)]
    struct CountingUpload {
        inner: Box<dyn MultipartUpload>,
        live: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for CountingUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let live = self.live.clone();
            let peak = self.peak.clone();
            let part = self.inner.put_part(data);
            Box::pin(async move {
                let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                // Long enough that every part the test starts overlaps without the cap.
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let result = part.await;
                live.fetch_sub(1, Ordering::SeqCst);
                result
            })
        }

        async fn complete(&mut self) -> Result<PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> Result<()> {
            self.inner.abort().await
        }
    }

    /// Start `parts` uploads at once and report how many ever ran together.
    async fn peak_concurrency(budget: Option<usize>, parts: usize) -> usize {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let counting: ObjectStoreRef = Arc::new(CountingStore {
            inner: Arc::new(InMemory::new()),
            live,
            peak: peak.clone(),
        });
        let store = match budget {
            Some(permits) => with_budget(counting, Arc::new(Semaphore::new(permits))),
            None => counting,
        };

        let mut upload = store
            .put_multipart_opts(&Path::from("part-file"), PutMultipartOptions::default())
            .await
            .unwrap();
        // Built first, awaited together: this is how delta-rs drives a writer's parts.
        let pending: Vec<_> = (0..parts)
            .map(|i| upload.put_part(vec![i as u8; 8].into()))
            .collect();
        try_join_all(pending).await.unwrap();
        upload.complete().await.unwrap();

        peak.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn parts_awaited_together_all_run_at_once_unbounded() {
        // The baseline the cap has to change: without it every part is in flight together.
        assert_eq!(peak_concurrency(None, 12).await, 12);
    }

    #[tokio::test]
    async fn the_budget_caps_the_parts_in_flight() {
        assert_eq!(peak_concurrency(Some(4), 12).await, 4);
    }

    #[tokio::test]
    async fn a_capped_upload_still_writes_every_part() {
        let store = with_budget(Arc::new(InMemory::new()), Arc::new(Semaphore::new(2)));
        let path = Path::from("whole-file");
        let mut upload = store
            .put_multipart_opts(&path, PutMultipartOptions::default())
            .await
            .unwrap();
        let parts: Vec<_> = (0..6u8)
            .map(|i| upload.put_part(vec![i; 4].into()))
            .collect();
        try_join_all(parts).await.unwrap();
        upload.complete().await.unwrap();

        let written = store.get(&path).await.unwrap().bytes().await.unwrap();
        let expected: Vec<u8> = (0..6u8).flat_map(|i| vec![i; 4]).collect();
        assert_eq!(written.as_ref(), expected.as_slice());
    }
}
