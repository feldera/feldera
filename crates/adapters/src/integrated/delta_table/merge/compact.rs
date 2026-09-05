//! Compaction driven by the connector, for tables nothing else maintains.
//!
//! Merge mode marks a superseded row deleted rather than rewriting its file, so old versions
//! stay in the table until something rewrites that file. Compacting is normally the table
//! administrator's job, so `optimize_interval_secs` is off by default; it is there for tables
//! where Feldera is the only writer.
//!
//! Compaction runs in the background because its cost follows the size of the table, not the
//! size of the flush, so running it inline would stall output. A flush that loses the race is
//! redone against the new files, which merge mode must handle anyway to survive an
//! administrator's own `OPTIMIZE`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dbsp::circuit::tokio::TOKIO;
use deltalake::open_table_with_storage_options;
use deltalake::table::builder::ensure_table_uri;
use feldera_types::transport::delta_table::DeltaTableWriterConfig;
use parking_lot::Mutex;
use tracing::{info, warn};

/// Whether a compaction is in flight, and when the next one may start.
struct Schedule {
    running: bool,
    next_run: Instant,
}

/// Runs `OPTIMIZE` on the target table, one at a time and no more often than asked.
pub struct Compactor {
    uri: String,
    endpoint_name: String,
    storage_options: HashMap<String, String>,
    interval: Duration,
    schedule: Arc<Mutex<Schedule>>,
}

impl Compactor {
    /// Build a compactor, or `None` when the connector was not asked to compact.
    pub fn new(config: &DeltaTableWriterConfig, endpoint_name: &str) -> Option<Self> {
        Self::starting_at(config, endpoint_name, Instant::now())
    }

    /// Split from [`Self::new`] so a test can pin the instant the cadence counts from.
    fn starting_at(
        config: &DeltaTableWriterConfig,
        endpoint_name: &str,
        now: Instant,
    ) -> Option<Self> {
        let interval = Duration::from_secs(config.optimize_interval_secs?);
        Some(Self {
            uri: config.uri.clone(),
            endpoint_name: endpoint_name.to_string(),
            storage_options: config.object_store_config.clone(),
            interval,
            schedule: Arc::new(Mutex::new(Schedule {
                running: false,
                // One interval away, not immediately: naming a cadence asks for compaction
                // on that cadence, not a rewrite of the whole table at startup.
                next_run: now + interval,
            })),
        })
    }

    /// Take the next compaction slot if one is due and none is in flight.
    ///
    /// Split from [`Self::maybe_start`] so a test can drive the clock instead of waiting.
    fn claim(&self, now: Instant) -> bool {
        let mut schedule = self.schedule.lock();
        if schedule.running || now < schedule.next_run {
            return false;
        }
        schedule.running = true;
        // Set here as well as on release, so a compaction that fails immediately does not
        // retry on the very next flush.
        schedule.next_run = now + self.interval;
        true
    }

    /// Give the slot back, one interval from now.
    fn release(schedule: &Mutex<Schedule>, interval: Duration, now: Instant) {
        let mut schedule = schedule.lock();
        schedule.running = false;
        schedule.next_run = now + interval;
    }

    /// Start a compaction if one is due. Called after every successful flush, so the common
    /// path is one lock and one clock read; never blocks on the compaction itself.
    pub fn maybe_start(&self) {
        if !self.claim(Instant::now()) {
            return;
        }

        let schedule = self.schedule.clone();
        let uri = self.uri.clone();
        let endpoint_name = self.endpoint_name.clone();
        let storage_options = self.storage_options.clone();
        let interval = self.interval;

        TOKIO.spawn(async move {
            let started = Instant::now();
            match compact(&uri, storage_options).await {
                Ok(outcome) => info!(
                    "delta_table {endpoint_name}: compacted '{uri}' in {:.1}s, \
                     {} file(s) rewritten into {}",
                    started.elapsed().as_secs_f64(),
                    outcome.files_removed,
                    outcome.files_added,
                ),
                // A failed compaction leaves the table as it was, so warn rather than fail
                // the endpoint: the connector keeps writing.
                Err(e) => warn!(
                    "delta_table {endpoint_name}: unable to compact '{uri}': {e}. The table \
                     is unchanged; the connector will try again in {}s. Run OPTIMIZE \
                     externally if this persists.",
                    interval.as_secs(),
                ),
            }

            Self::release(&schedule, interval, Instant::now());
        });
    }
}

/// What one compaction did, for the log line.
struct Outcome {
    files_added: u64,
    files_removed: u64,
}

/// Open the table and compact it. No target file size is set, so delta-rs takes the table's
/// own `delta.targetFileSize` rather than the connector second-guessing it.
async fn compact(uri: &str, storage_options: HashMap<String, String>) -> anyhow::Result<Outcome> {
    let url = ensure_table_uri(uri)?;

    // Opened fresh, not shared with the writer: the writer's snapshot only advances with its
    // own commits, and this must see everything committed now.
    let table = open_table_with_storage_options(url, storage_options).await?;
    let (_table, metrics) = table.optimize().await?;

    Ok(Outcome {
        files_added: metrics.num_files_added,
        files_removed: metrics.num_files_removed,
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
    use deltalake::operations::create::CreateBuilder;
    use feldera_types::transport::delta_table::{
        DeltaTableUpdateMode, DeltaTableWriteMode, DeltaTableWriterConfig, DeltaVariantEncoding,
    };
    use tempfile::TempDir;

    use super::*;

    fn config_with(uri: &str, interval: Option<u64>) -> DeltaTableWriterConfig {
        DeltaTableWriterConfig {
            uri: uri.to_string(),
            mode: DeltaTableWriteMode::Append,
            variant_encoding: DeltaVariantEncoding::default(),
            update_mode: DeltaTableUpdateMode::Merge,
            lookup_chunk_bytes: 1 << 20,
            max_concurrent_probes: 4,
            checkpoint_interval: None,
            log_retention_duration: None,
            enable_expired_log_cleanup: None,
            max_retries: Some(0),
            threads: Some(1),
            optimize_interval_secs: interval,
            object_store_config: Default::default(),
        }
    }

    /// No interval means the connector must not compact at all.
    #[test]
    fn compaction_is_off_unless_asked_for() {
        assert!(Compactor::new(&config_with("memory://", None), "e").is_none());
        assert!(Compactor::new(&config_with("memory://", Some(60)), "e").is_some());
    }

    /// The first compaction waits a full interval rather than firing on the first flush.
    ///
    /// A pipeline adopting a large table would otherwise rewrite it at startup, which is not
    /// what naming a cadence asks for.
    #[test]
    fn the_first_compaction_waits_one_interval() {
        let start = Instant::now();
        let compactor =
            Compactor::starting_at(&config_with("memory://", Some(60)), "e", start).unwrap();

        assert!(!compactor.claim(start), "must not compact at startup");
        assert!(
            !compactor.claim(start + Duration::from_secs(59)),
            "must not compact before the interval elapses"
        );
        assert!(
            compactor.claim(start + Duration::from_secs(60)),
            "must compact once the interval has elapsed"
        );
    }

    /// One compaction at a time, and a full interval after the last one finishes.
    #[test]
    fn one_compaction_at_a_time() {
        let compactor = Compactor::new(&config_with("memory://", Some(60)), "e").unwrap();
        let start = Instant::now();
        let due = start + Duration::from_secs(60);
        assert!(compactor.claim(due));

        // While it runs, no second compaction starts however long it takes.
        assert!(
            !compactor.claim(due + Duration::from_secs(600)),
            "a compaction must not start while one is in flight"
        );

        // The gap is measured from the end, so a compaction that outran its interval does
        // not immediately trigger the next one.
        let finished = due + Duration::from_secs(600);
        Compactor::release(&compactor.schedule, compactor.interval, finished);
        assert!(
            !compactor.claim(finished + Duration::from_secs(59)),
            "the interval must be measured from when the last compaction finished"
        );
        assert!(compactor.claim(finished + Duration::from_secs(60)));
    }

    /// The compaction itself: it must rewrite files and lose no rows.
    ///
    /// Exercises the real path the connector uses, `ensure_table_uri` and
    /// `open_table_with_storage_options` included, rather than calling delta-rs directly.
    #[tokio::test]
    async fn compacting_rewrites_files_and_keeps_every_row() {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap();

        let columns = vec![
            StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), false),
            StructField::new("s", DeltaDataType::Primitive(PrimitiveType::String), true),
        ];
        let mut table = CreateBuilder::new()
            .with_location(uri)
            .with_columns(columns)
            .await
            .unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ]));
        // Three files, so there is something to bin-pack together.
        for file in 0..3i64 {
            let ids: Vec<i64> = (file * 10..file * 10 + 10).collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(StringArray::from(
                        ids.iter().map(|i| format!("v{i}")).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            table = table.write(vec![batch]).await.unwrap();
        }
        assert_eq!(table.snapshot().unwrap().log_data().into_iter().count(), 3);

        let outcome = compact(uri, Default::default()).await.unwrap();
        assert_eq!(outcome.files_removed, 3, "all three files should bin-pack");
        assert_eq!(outcome.files_added, 1);

        let compacted =
            open_table_with_storage_options(ensure_table_uri(uri).unwrap(), Default::default())
                .await
                .unwrap();
        let rows: usize = compacted
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .filter_map(|f| f.num_records())
            .sum();
        assert_eq!(rows, 30, "compaction must not lose or duplicate a row");
    }
}
