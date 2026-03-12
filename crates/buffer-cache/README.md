# feldera-buffer-cache

`feldera-buffer-cache` provides thread-safe in-memory caches used by Feldera's storage layer:

`S3FifoCache` wraps `quick_cache`'s SOSP'23 S3-FIFO implementation behind a
Feldera-oriented API:
<https://dl.acm.org/doi/10.1145/3600006.3613147>

`LruCache` provides a mutex-protected LRU backend with the same
high-level API.

`BufferCacheBuilder` constructs the foreground/background cache layout used by
DBSP circuits.

## API sketch

```rust
use feldera_buffer_cache::{CacheEntry, LruCache};

#[derive(Clone)]
struct Page(Vec<u8>);

impl CacheEntry for Page {
    fn cost(&self) -> usize {
        self.0.len()
    }
}

let cache = LruCache::<u64, Page>::new(64 << 20);
cache.insert(7, Page(vec![0; 4096]));

let page = cache.get(&7).expect("entry should be present");
assert_eq!(page.0.len(), 4096);
assert_eq!(cache.total_charge(), 4096);
```

## Benchmarks

The throughput benchmark comparing `LruCache` and `S3FifoCache` lives under the
`dbsp` bench targets:

```bash
cargo bench -p dbsp --bench buffer_cache -- --threads 1,2,4,8 --max-duration 10
```
