//! How a flush divides its budgets between key ranges. The `delta_merge` connector suite drives
//! the merge writer with random sequences of changes against an in-memory oracle.

/// The budgets a flush is configured with are for the flush, not for each of its ranges.
///
/// Left undivided, `threads` would multiply both the rows the writers buffer and the files
/// the lookup reads at once, so a thread count would quietly change how much memory a flush
/// needs and how hard it leans on the object store.
#[test]
fn ranges_divide_the_flush_budgets_between_them() {
    use super::flush::{
        APPEND_CHUNK_ROWS, MIN_RANGE_CHUNK_ROWS, MIN_RANGE_LOOKUP_BYTES, RangeBudget,
    };
    const LOOKUP_BYTES: usize = 256 * 1024 * 1024;

    let whole = RangeBudget::for_test(16, LOOKUP_BYTES, 1);
    assert_eq!(whole.chunk_rows, APPEND_CHUNK_ROWS);
    assert_eq!(whole.probes, 16);
    assert_eq!(whole.lookup_bytes, LOOKUP_BYTES);

    let split = RangeBudget::for_test(16, LOOKUP_BYTES, 4);
    assert_eq!(split.chunk_rows, APPEND_CHUNK_ROWS / 4);
    assert_eq!(
        split.probes, 4,
        "four ranges of four probes is the configured sixteen"
    );
    assert_eq!(
        split.lookup_bytes,
        LOOKUP_BYTES / 4,
        "four ranges holding a quarter of the keys each is the configured budget"
    );

    // Divided past what is useful, each range still buffers enough to be worth a write,
    // still reads one file at a time, and still holds enough keys to be worth a pass.
    let thin = RangeBudget::for_test(2, LOOKUP_BYTES, 64);
    assert_eq!(thin.chunk_rows, MIN_RANGE_CHUNK_ROWS);
    assert_eq!(thin.probes, 1);
    assert_eq!(thin.lookup_bytes, MIN_RANGE_LOOKUP_BYTES);

    // The floor applies to the division, not over the setting: an operator who caps what a
    // flush may hold gets that cap, not eight megabytes a range.
    let capped = RangeBudget::for_test(16, 1024, 4);
    assert_eq!(
        capped.lookup_bytes, 1024,
        "the floor raised a range past the configured budget"
    );
}
