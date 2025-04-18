//! A datagen input adapter that generates random data based on a schema and config.

use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::hash::Hasher;
use std::num::NonZeroU32;
use std::ops::{Range, RangeInclusive};
use std::sync::{Arc, LazyLock};
use std::thread::Thread;
use std::time::Duration as StdDuration;

use anyhow::{anyhow, bail, Result as AnyResult};
use async_channel::Receiver as AsyncReceiver;
use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, Days, Duration, NaiveDate, NaiveTime, Timelike};
use feldera_types::config::FtModel;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Jitter, Quota, RateLimiter};
use num_traits::{clamp, Bounded, ToPrimitive};
use rand::distributions::{Alphanumeric, Uniform};
use rand::rngs::SmallRng;
use rand::{thread_rng, Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use range_set::RangeSet;
use serde::{Deserialize, Serialize};
use serde_json::{to_writer, Map, Value};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::{sync::mpsc::UnboundedSender, time::Instant as TokioInstant};

use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::format::{InputBuffer, Parser};
use feldera_adapterlib::transport::{
    InputCommandReceiver, InputConsumer, InputEndpoint, InputReader, InputReaderCommand, Resume,
    TransportInputEndpoint,
};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier, SqlType};
use feldera_types::transport::datagen::{
    DatagenInputConfig, DatagenStrategy, GenerationPlan, RngFieldSettings,
};
use xxhash_rust::xxh3::Xxh3Default;

fn range_as_i64(
    field: &SqlIdentifier,
    range: &Option<(Value, Value)>,
) -> AnyResult<Option<(i64, i64)>> {
    match range {
        None => Ok(None),
        Some((Value::Number(a), Value::Number(b))) => {
            let a = a
                .as_i64()
                .ok_or_else(|| anyhow!("Invalid min range for field {:?}", field.sql_name()))?;
            let b = b
                .as_i64()
                .ok_or_else(|| anyhow!("Invalid max range for field {:?}", field.sql_name()))?;
            Ok(Some((a, b)))
        }
        _ => Err(anyhow!(
            "Range values must be integers for field {:?}",
            field.sql_name()
        )),
    }
}

fn range_as_f64(
    field: &SqlIdentifier,
    range: &Option<(Value, Value)>,
) -> AnyResult<Option<(f64, f64)>> {
    match range {
        None => Ok(None),
        Some((Value::Number(a), Value::Number(b))) => {
            let a = a
                .as_f64()
                .ok_or_else(|| anyhow!("Invalid min range for field {:?}", field.sql_name()))?;
            let b = b
                .as_f64()
                .ok_or_else(|| anyhow!("Invalid max range for field {:?}", field.sql_name()))?;
            Ok(Some((a, b)))
        }
        _ => Err(anyhow!(
            "Range values must be floating point numbers for field {:?}",
            field.sql_name()
        )),
    }
}

fn field_is_json_array(field: &Field, value: &Value) -> AnyResult<()> {
    if !matches!(value, Value::Array(_)) {
        return Err(anyhow!(
            "Invalid type found in `values` for field {:?} with type {:?}",
            field.name,
            field.columntype.typ
        ));
    }
    Ok(())
}

fn field_is_string(field: &Field, value: &Value) -> AnyResult<()> {
    if !matches!(value, Value::String(_)) {
        return Err(anyhow!(
            "Invalid type found in `values` for field {:?} with type {:?}",
            field.name,
            field.columntype.typ
        ));
    }
    Ok(())
}

fn field_is_number(field: &Field, value: &Value) -> AnyResult<()> {
    if !matches!(value, Value::Number(_)) {
        return Err(anyhow!(
            "Invalid type found in `values` for field {:?} with type {:?}",
            field.name,
            field.columntype.typ
        ));
    }
    Ok(())
}

fn field_is_uuid(field: &Field, value: &Value) -> AnyResult<()> {
    match value {
        Value::String(s) => {
            uuid::Uuid::parse_str(s).map_err(|e| {
                anyhow!(
                    "Invalid UUID string `{s}` found in `values` for field {:?}: {e}",
                    field.name
                )
            })?;
        }
        _ => bail!(
            "Invalid type found in `values` for field {:?} with type {:?}",
            field.name,
            field.columntype.typ
        ),
    }

    Ok(())
}

/// Tries to parse a range as a UUID range.
fn parse_range_for_uuid(
    field: &SqlIdentifier,
    range: &Option<(Value, Value)>,
) -> AnyResult<Option<(u128, u128)>> {
    match range {
        Some((min, max)) => {
            let min = serde_json::from_value::<uuid::Uuid>(min.clone()).map_err(|e| {
                anyhow!(
                    "Failed to parse the left bound of the range for the field {:?} as a UUID: {e}",
                    field.sql_name()
                )
            })?;
            let max = serde_json::from_value::<uuid::Uuid>(max.clone()).map_err(|e| {
                anyhow!(
                    "Failed to parse the right bound of the range for the field {:?} as a UUID: {e}",
                    field.sql_name()
                )
            })?;
            Ok(Some((min.as_u128(), max.as_u128())))
        }
        None => Ok(None),
    }
}

/// Tries to parse a range as a date range which is days since UNIX epoch
/// but we can specify either as a number or a string.
fn parse_range_for_date(
    field: &SqlIdentifier,
    range: &Option<(Value, Value)>,
) -> AnyResult<Option<(i64, i64)>> {
    match range {
        None => Ok(None),
        Some((Value::Number(a), Value::Number(b))) => {
            let a = a
                .as_i64()
                .ok_or_else(|| anyhow!("Invalid min range for field {:?}", field.sql_name()))?;
            let b = b
                .as_i64()
                .ok_or_else(|| anyhow!("Invalid max range for field {:?}", field.sql_name()))?;
            Ok(Some((a, b)))
        }
        Some((Value::String(a), Value::String(b))) => {
            let unix_date: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let since = NaiveDate::signed_duration_since;
            let a = NaiveDate::parse_from_str(a, "%Y-%m-%d").map_err(|e| {
                anyhow!(
                    "Invalid min date range for field {:?}: {}",
                    field.sql_name(),
                    e
                )
            })?;
            let b = NaiveDate::parse_from_str(b, "%Y-%m-%d").map_err(|e| {
                anyhow!(
                    "Invalid max date range for field {:?}: {}",
                    field.sql_name(),
                    e
                )
            })?;
            Ok(Some((
                since(a, unix_date).num_days(),
                since(b, unix_date).num_days(),
            )))
        }
        _ => Err(anyhow!(
            "Range values must be integers or strings for field {:?}",
            field.sql_name()
        )),
    }
}

fn parse_range_for_time(
    field: &SqlIdentifier,
    range: &Option<(Value, Value)>,
) -> AnyResult<Option<(i64, i64)>> {
    match range {
        None => Ok(None),
        Some((Value::Number(a), Value::Number(b))) => {
            let a = a
                .as_i64()
                .ok_or_else(|| anyhow!("Invalid min range for field {:?}", field.sql_name()))?;
            let b = b
                .as_i64()
                .ok_or_else(|| anyhow!("Invalid max range for field {:?}", field.sql_name()))?;
            Ok(Some((a, b)))
        }
        Some((Value::String(a), Value::String(b))) => {
            let a = NaiveTime::parse_from_str(a, "%H:%M:%S").map_err(|e| {
                anyhow!(
                    "Invalid min time range for field {:?}: {}",
                    field.sql_name(),
                    e
                )
            })?;
            let b = NaiveTime::parse_from_str(b, "%H:%M:%S").map_err(|e| {
                anyhow!(
                    "Invalid max time range for field {:?}: {}",
                    field.sql_name(),
                    e
                )
            })?;
            Ok(Some((
                a.num_seconds_from_midnight() as i64 * 1000,
                b.num_seconds_from_midnight() as i64 * 1000,
            )))
        }
        _ => Err(anyhow!(
            "Range values must be integers or strings for field {:?}",
            field.sql_name()
        )),
    }
}

fn parse_range_for_datetime(
    field: &SqlIdentifier,
    range: &Option<(Value, Value)>,
) -> AnyResult<Option<(i64, i64)>> {
    match range {
        None => Ok(None),
        Some((Value::Number(a), Value::Number(b))) => {
            let a = a
                .as_i64()
                .ok_or_else(|| anyhow!("Invalid min range for field {:?}", field.sql_name()))?;
            let b = b
                .as_i64()
                .ok_or_else(|| anyhow!("Invalid max range for field {:?}", field.sql_name()))?;
            Ok(Some((a, b)))
        }
        Some((Value::String(a), Value::String(b))) => {
            let a = DateTime::parse_from_rfc3339(a).map_err(|e| {
                anyhow!(
                    "Invalid min datetime range for field {:?}: {}",
                    field.sql_name(),
                    e
                )
            })?;
            let b = DateTime::parse_from_rfc3339(b).map_err(|e| {
                anyhow!(
                    "Invalid max datetime range for field {:?}: {}",
                    field.sql_name(),
                    e
                )
            })?;
            Ok(Some((a.timestamp_millis(), b.timestamp_millis())))
        }
        _ => Err(anyhow!(
            "Range values must be integers or strings for field {:?}",
            field.sql_name()
        )),
    }
}

pub struct GeneratorEndpoint {
    config: DatagenInputConfig,
}

impl GeneratorEndpoint {
    pub fn new(config: DatagenInputConfig) -> Self {
        Self { config }
    }
}

impl InputEndpoint for GeneratorEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for GeneratorEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(InputGenerator::new(
            consumer,
            parser,
            self.config.clone(),
            schema,
        )?))
    }
}

struct InputGenerator {
    command_sender: UnboundedSender<InputReaderCommand>,
    datagen_thread: Thread,
}

impl InputGenerator {
    fn new(
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        mut config: DatagenInputConfig,
        schema: Relation,
    ) -> AnyResult<Self> {
        let (command_sender, command_receiver) = unbounded_channel();

        // Deal with case sensitivity in field names, make sure we can find the field in settings.
        // return an error if have a field in datagen that's not in the table.
        for plan in config.plan.iter_mut() {
            let mut normalized_plan_names: HashMap<String, Box<RngFieldSettings>> =
                HashMap::with_capacity(plan.fields.len());
            for (name, settings) in plan.fields.iter() {
                if let Some(field) = schema.fields.iter().find(|f| f.name == name) {
                    // Replace the settings name with the field name which is either all lower case
                    // (case_sensitive = false) or the original case (case_sensitive = true) to store the settings.
                    normalized_plan_names.insert(field.name.name(), settings.clone());
                } else {
                    return Err(anyhow!(
                        "Field `{}` specified in datagen does not exist in the table schema.",
                        name
                    ));
                }
            }
            plan.fields = normalized_plan_names;
        }

        let join_handle = std::thread::spawn(move || {
            if let Err(error) =
                Self::datagen_thread(command_receiver, consumer.clone(), parser, config, schema)
            {
                consumer.error(true, error);
            }
        });
        let datagen_thread = join_handle.thread().clone();

        Ok(Self {
            command_sender,
            datagen_thread,
        })
    }

    fn datagen_thread(
        command_receiver: UnboundedReceiver<InputReaderCommand>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        config: DatagenInputConfig,
        schema: Relation,
    ) -> AnyResult<()> {
        let rate_limiters = Arc::new(
            config
                .plan
                .iter()
                .map(|p| {
                    RateLimiter::direct(Quota::per_second(
                        p.rate.and_then(NonZeroU32::new).unwrap_or(NonZeroU32::MAX),
                    ))
                })
                .collect::<Vec<_>>(),
        );

        // Generate initial seed.  If we start from a checkpoint, we'll change
        // it to use the seed from that checkpoint.
        let mut seed = config.seed.unwrap_or(thread_rng().gen());

        // Long-running tasks with high CPU usage don't work well on cooperative runtimes,
        // so we use a separate blocking task if we think this will happen for our workload.
        let needs_blocking_tasks = config.plan.iter().any(|p| {
            u32::from(p.rate.and_then(NonZeroU32::new).unwrap_or(NonZeroU32::MAX))
                > (250_000 * config.workers as u32)
                && p.limit.unwrap_or(usize::MAX) > 10_000_000
        });

        // Start our worker threads or tasks.
        let (work_sender, work_receiver) = async_channel::bounded(config.workers * 2);
        let (completion_sender, mut completion_receiver) = tokio::sync::mpsc::unbounded_channel();
        for _ in 0..config.workers {
            let work_receiver = work_receiver.clone();
            let completion_sender = completion_sender.clone();
            let config = config.clone();
            let schema = schema.clone();
            let consumer = consumer.clone();
            let parser = parser.fork();
            let rate_limiters = rate_limiters.clone();

            let task = Self::worker_thread(
                work_receiver,
                completion_sender,
                config,
                schema,
                consumer,
                parser,
                rate_limiters,
                std::thread::current(),
            );
            if needs_blocking_tasks {
                std::thread::spawn(move || {
                    TOKIO.block_on(task);
                });
            } else {
                TOKIO.spawn(task);
            }
        }
        drop(work_receiver);
        drop(completion_sender);

        let mut command_receiver = InputCommandReceiver::<RawMetadata, ()>::new(command_receiver);

        // Tracks work that needs to be sent to a worker for execution.
        let mut unassigned = if let Some(metadata) = command_receiver.blocking_recv_seek()? {
            seed = metadata.seed;
            range_sets_from_vecs(metadata.todo)
        } else {
            config
                .plan
                .iter()
                .map(|plan| match plan.limit.unwrap_or(usize::MAX) {
                    0 => RowRangeSet::new(),
                    limit => RowRangeSet::from_ranges(&[0..=limit - 1]),
                })
                .collect()
        };

        // Tracks work that has been sent to a worker but for which we have not received a completion.
        let mut in_flight = vec![RowRangeSet::new(); config.plan.len()];

        // Replay steps.
        while let Some((metadata, _data)) = command_receiver.blocking_recv_replay()? {
            let metadata: Metadata = metadata.into();
            seed = metadata.seed;
            let mut rows = metadata.rows;
            unassigned = metadata.todo;
            let mut num_records = 0;
            let mut hasher = Xxh3Default::new();

            fn complete_replay(
                completion: Completion,
                in_flight: &mut [RowRangeSet],
                consumer: &dyn InputConsumer,
                hasher: &mut Xxh3Default,
            ) {
                let Completion {
                    batch,
                    mut buffer,
                    num_bytes,
                } = completion;
                in_flight[batch.plan_idx].remove_range(batch.rows.start..=batch.rows.end - 1);
                consumer.buffered(buffer.len(), num_bytes);
                buffer.hash(hasher);
                buffer.flush();
            }

            while let Some(work) = assign_work(&mut rows, &mut in_flight, &config, seed, false) {
                num_records += work.batch.rows.len();
                let _ = work_sender.send_blocking(work);
                while let Ok(completion) = completion_receiver.try_recv() {
                    complete_replay(completion, &mut in_flight, &*consumer, &mut hasher);
                }
            }
            while !in_flight.iter().all(|set| set.is_empty()) {
                let Some(completion) = completion_receiver.blocking_recv() else {
                    unreachable!()
                };
                complete_replay(completion, &mut in_flight, &*consumer, &mut hasher);
            }
            consumer.replayed(num_records, hasher.finish());
        }

        let mut running = false;
        let mut eoi = false;
        let mut completed = VecDeque::new();
        loop {
            match command_receiver.try_recv()? {
                Some(command @ InputReaderCommand::Seek(_))
                | Some(command @ InputReaderCommand::Replay { .. }) => {
                    unreachable!("{command:?} must be at the beginning of the command stream")
                }
                Some(InputReaderCommand::Extend) => running = true,
                Some(InputReaderCommand::Pause) => running = false,
                Some(InputReaderCommand::Queue) => {
                    let mut num_records = 0;
                    let mut hasher = consumer.hasher();
                    let n = consumer.max_batch_size();
                    let mut consumed = vec![RowRangeSet::new(); config.plan.len()];
                    while num_records < n {
                        let Some(Completion {
                            batch: Batch { plan_idx, rows },
                            mut buffer,
                            num_bytes: _,
                        }) = completed.pop_front()
                        else {
                            break;
                        };
                        let mut taken = buffer.take_some(n - num_records);
                        let flushed = taken.len();
                        if flushed > 0 {
                            num_records += flushed;
                            if let Some(hasher) = hasher.as_mut() {
                                taken.hash(hasher);
                            }
                            consumed[plan_idx].insert_range(rows.start..=rows.start + flushed - 1);
                            taken.flush();
                        }
                        if !buffer.is_empty() {
                            completed.push_front(Completion {
                                batch: Batch {
                                    plan_idx,
                                    rows: rows.start + flushed..rows.end,
                                },
                                buffer,
                                num_bytes: 0,
                            });
                            break;
                        }
                    }
                    let mut metadata_todo = unassigned.clone();
                    for (idx, set) in in_flight.iter().enumerate() {
                        for rows in set.as_ref().iter() {
                            metadata_todo[idx].insert_range(rows.clone());
                        }
                    }
                    for completion in &completed {
                        let batch = &completion.batch;
                        metadata_todo[batch.plan_idx]
                            .insert_range(batch.rows.start..=batch.rows.end - 1);
                    }
                    let metadata: RawMetadata = Metadata {
                        rows: consumed,
                        todo: metadata_todo,
                        seed,
                    }
                    .into();
                    let resume =
                        Resume::new_metadata_only(serde_json::to_value(metadata).unwrap(), hasher);
                    consumer.extended(num_records, Some(resume));
                }
                Some(InputReaderCommand::Disconnect) => break,
                None => (),
            }

            while let Ok(completion) = completion_receiver.try_recv() {
                let batch = &completion.batch;
                in_flight[batch.plan_idx].remove_range(batch.rows.start..=batch.rows.end - 1);
                consumer.buffered(completion.buffer.len(), 0);
                completed.push_back(completion);
            }

            if running && !work_sender.is_full() {
                if let Some(work) =
                    assign_work(&mut unassigned, &mut in_flight, &config, seed, true)
                {
                    let _ = work_sender.send_blocking(work);
                } else if in_flight.iter().all(|set| set.is_empty()) && !eoi {
                    eoi = true;
                    running = false;
                    consumer.eoi();
                }
            } else {
                std::thread::park();
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn worker_thread(
        work_receiver: AsyncReceiver<Work>,
        completion_sender: UnboundedSender<Completion>,
        config: DatagenInputConfig,
        schema: Relation,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        rate_limiters: Arc<Vec<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>>,
        datagen_thread: Thread,
    ) {
        let mut buffer = Vec::new();
        let mut buffer_start = 0;
        static START_ARR: &[u8; 1] = b"[";
        static END_ARR: &[u8; 1] = b"]";
        static REC_DELIM: &[u8; 1] = b",";

        while let Ok(work) = work_receiver.recv().await {
            datagen_thread.unpark();

            let Work {
                batch: Batch { plan_idx, rows },
                rate_limit,
                seed,
            } = work;
            let plan = &config.plan[plan_idx];
            let batch_size: usize = min(plan.rate.unwrap_or(u32::MAX) as usize, 10_000);
            let schema = schema.clone();
            let mut generator = RecordGenerator::new(seed, plan, &schema);

            // Count how long we took to so far to create a batch
            // If we end up taking too long we send a batch earlier even if we don't reach `batch_size`
            const BATCH_CREATION_TIMEOUT: StdDuration = StdDuration::from_secs(1);
            let mut batch_creation_duration = TokioInstant::now();

            // Number of generated records.
            let mut n_records = 0;

            for idx in rows.clone() {
                match generator.make_json_record(idx) {
                    Ok(record) => {
                        if n_records == 0 {
                            buffer_start = idx;
                            buffer.clear();
                            buffer.extend(START_ARR);
                        } else {
                            buffer.extend(REC_DELIM);
                        }
                        //eprintln!("Record: {}", record);
                        to_writer(&mut buffer, &record).unwrap();
                        n_records += 1;

                        if n_records >= batch_size
                            || batch_creation_duration.elapsed() > BATCH_CREATION_TIMEOUT
                        {
                            buffer.extend(END_ARR);
                            let num_bytes = buffer.len();
                            let (buffer, errors) = parser.parse(&buffer);
                            consumer.parse_errors(errors);
                            let _ = completion_sender.send(Completion {
                                batch: Batch {
                                    plan_idx,
                                    rows: buffer_start..idx + 1,
                                },
                                buffer,
                                num_bytes,
                            });
                            datagen_thread.unpark();
                            n_records = 0;
                            batch_creation_duration = TokioInstant::now();
                        }
                    }
                    Err(e) => {
                        consumer.error(true, e);
                        return;
                    }
                }

                if rate_limit {
                    rate_limiters[plan_idx]
                        .until_ready_with_jitter(Jitter::up_to(StdDuration::from_millis(20)))
                        .await;
                }
            }
            if n_records > 0 {
                buffer.extend(END_ARR);
                let num_bytes = buffer.len();
                let (buffer, errors) = parser.parse(&buffer);
                consumer.parse_errors(errors);
                let _ = completion_sender.send(Completion {
                    batch: Batch {
                        plan_idx,
                        rows: buffer_start..rows.end,
                    },
                    buffer,
                    num_bytes,
                });
                datagen_thread.unpark();
            }
        }
    }
}

struct Work {
    batch: Batch,
    rate_limit: bool,
    seed: u64,
}

struct Completion {
    batch: Batch,
    buffer: Option<Box<dyn InputBuffer>>,
    num_bytes: usize,
}

struct Batch {
    plan_idx: usize,
    rows: Range<usize>,
}

type RowRangeSet = RangeSet<[RangeInclusive<usize>; 4]>;

struct Metadata {
    /// Work done in this step.
    rows: Vec<RowRangeSet>,

    /// Work that remains to be done after this step.
    todo: Vec<RowRangeSet>,

    /// Random seed used for generating data.
    seed: u64,
}

fn range_sets_from_vecs(vecs: Vec<Vec<RangeInclusive<usize>>>) -> Vec<RowRangeSet> {
    vecs.into_iter()
        .map(|ranges| RowRangeSet::from_ranges(&ranges))
        .collect()
}

impl From<RawMetadata> for Metadata {
    fn from(value: RawMetadata) -> Self {
        Self {
            rows: range_sets_from_vecs(value.rows),
            todo: range_sets_from_vecs(value.todo),
            seed: value.seed,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RawMetadata {
    rows: Vec<Vec<RangeInclusive<usize>>>,
    todo: Vec<Vec<RangeInclusive<usize>>>,
    seed: u64,
}

fn vecs_from_range_sets(range_sets: Vec<RowRangeSet>) -> Vec<Vec<RangeInclusive<usize>>> {
    range_sets
        .into_iter()
        .map(|set| set.into_smallvec().into_vec())
        .collect()
}

impl From<Metadata> for RawMetadata {
    fn from(value: Metadata) -> Self {
        Self {
            rows: vecs_from_range_sets(value.rows),
            todo: vecs_from_range_sets(value.todo),
            seed: value.seed,
        }
    }
}

fn assign_work(
    unassigned: &mut [RowRangeSet],
    in_flight: &mut [RowRangeSet],
    config: &DatagenInputConfig,
    seed: u64,
    rate_limit: bool,
) -> Option<Work> {
    for (plan_idx, (set, plan)) in unassigned.iter_mut().zip(config.plan.iter()).enumerate() {
        if let Some(rows) = set.as_ref().iter().next().cloned() {
            let (first, last) = rows.into_inner();
            let batch_size: usize = min(plan.rate.unwrap_or(u32::MAX) as usize, 10_000);
            let per_thread_chunk: usize = plan.worker_chunk_size.unwrap_or(batch_size);
            let n = per_thread_chunk.min(last - first + 1);
            let batch = Batch {
                plan_idx,
                rows: first..first + n,
            };
            set.remove_range(first..=first + n - 1);
            in_flight[plan_idx].insert_range(batch.rows.start..=batch.rows.end - 1);
            return Some(Work {
                batch,
                rate_limit,
                seed,
            });
        }
    }
    None
}

impl InputReader for InputGenerator {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
        self.datagen_thread.unpark();
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}

impl Drop for InputGenerator {
    fn drop(&mut self) {
        self.request(InputReaderCommand::Disconnect);
    }
}

static YMD_FORMAT: LazyLock<Vec<Item<'static>>> = LazyLock::new(|| {
    StrftimeItems::new("%Y-%m-%d")
        .parse_to_owned()
        .expect("%Y-%m-%d is a valid date format")
});

struct RecordGenerator<'a> {
    config: &'a GenerationPlan,
    schema: &'a Relation,
    /// The current record number.
    current: usize,
    seed: u64,
    json_obj: Option<Value>,
}

impl<'a> RecordGenerator<'a> {
    fn new(seed: u64, config: &'a GenerationPlan, schema: &'a Relation) -> Self {
        Self {
            config,
            schema,
            current: 0,
            seed,
            json_obj: Some(Value::Object(Map::with_capacity(8))),
        }
    }

    fn typ_to_default(typ: SqlType) -> Value {
        match typ {
            SqlType::Boolean => Value::Bool(false),
            SqlType::Uuid => Value::String("00000000-0000-0000-0000-000000000000".to_string()),
            SqlType::TinyInt
            | SqlType::SmallInt
            | SqlType::Int
            | SqlType::BigInt
            | SqlType::Real
            | SqlType::Double
            | SqlType::Decimal => Value::Number(serde_json::Number::from(0)),
            SqlType::Binary | SqlType::Varbinary => Value::Array(Vec::new()),
            SqlType::Char
            | SqlType::Varchar
            | SqlType::Timestamp
            | SqlType::Date
            | SqlType::Variant
            | SqlType::Time => Value::String(String::new()),
            SqlType::Interval(_unit) => Value::Null,
            SqlType::Array => Value::Array(Vec::new()),
            SqlType::Map | SqlType::Struct => Value::Object(Map::new()),
            SqlType::Null => Value::Null,
        }
    }

    fn generate_fields(
        &self,
        fields: &[Field],
        settings: &HashMap<String, Box<RngFieldSettings>>,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        let map = obj.as_object_mut().unwrap();

        let default_settings = Box::<RngFieldSettings>::default();
        for field in fields {
            let field_settings = settings
                .get(&field.name.to_string())
                .unwrap_or(&default_settings);
            let obj = map
                .entry(field.name.to_string())
                .and_modify(|v| {
                    // If a `null_percentage` is set it can happen that a field in the
                    // map got set to NULL previously, however our generator methods
                    // depend on the field being set to the correct type, so we
                    // need reallocate a new default value again.
                    if v.is_null() {
                        *v = Self::typ_to_default(field.columntype.typ)
                    }
                })
                .or_insert_with(|| Self::typ_to_default(field.columntype.typ));
            self.generate_field(field, field_settings, incr, rng, obj)?;
        }

        Ok(())
    }

    fn generate_field(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        match field.columntype.typ {
            SqlType::Boolean => self.generate_boolean(field, settings, incr, rng, obj),
            SqlType::Uuid => self.generate_uuid(field, settings, incr, rng, obj),
            SqlType::TinyInt => self.generate_integer::<i8>(field, settings, incr, rng, obj),
            SqlType::SmallInt => self.generate_integer::<i16>(field, settings, incr, rng, obj),
            SqlType::Int => self.generate_integer::<i32>(field, settings, incr, rng, obj),
            SqlType::BigInt => self.generate_integer::<i64>(field, settings, incr, rng, obj),
            SqlType::Real => self.generate_real::<f64>(field, settings, incr, rng, obj),
            SqlType::Double => self.generate_real::<f64>(field, settings, incr, rng, obj),
            SqlType::Decimal => self.generate_real::<f64>(field, settings, incr, rng, obj),
            SqlType::Binary | SqlType::Varbinary => {
                let mut field = field.clone();
                let mut columntype = Box::new(ColumnType::tinyint(false));
                // Hack to indicate we're dealing with an u8 and not an i8
                // in `generate_integer`
                columntype.scale = Some(1);
                field.columntype.component = Some(columntype);
                self.generate_array(&field, settings, incr, rng, obj)
            }
            SqlType::Char | SqlType::Varchar => {
                self.generate_string(field, settings, incr, rng, obj)
            }
            SqlType::Timestamp => self.generate_timestamp(field, settings, incr, rng, obj),
            SqlType::Date => self.generate_date(field, settings, incr, rng, obj),
            SqlType::Time => self.generate_time(field, settings, incr, rng, obj),
            SqlType::Interval(_unit) => {
                // I don't think this can show up in a table schema
                *obj = Value::Null;
                Ok(())
            }
            SqlType::Variant => {
                // I don't think this can show up in a table schema
                *obj = Value::Null;
                Ok(())
            }
            SqlType::Array => self.generate_array(field, settings, incr, rng, obj),
            SqlType::Map => self.generate_map(field, settings, incr, rng, obj),
            SqlType::Struct => {
                if let Some(nl) = Self::maybe_null(field, settings, rng) {
                    *obj = nl;
                    return Ok(());
                }

                self.generate_fields(
                    field.columntype.fields.as_ref().unwrap(),
                    settings.fields.as_ref().unwrap_or(&HashMap::new()),
                    incr,
                    rng,
                    obj,
                )
            }
            SqlType::Null => {
                *obj = Value::Null;
                Ok(())
            }
        }
    }

    /// Generates NULL values with the specified probability for this field.
    fn maybe_null(
        _field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> Option<Value> {
        settings.null_percentage.and_then(|null_percentage| {
            let between = Uniform::from(1..=100);
            if between.sample(rng) <= null_percentage {
                Some(Value::Null)
            } else {
                None
            }
        })
    }

    fn generate_map(
        &self,
        _field: &Field,
        _settings: &RngFieldSettings,
        _incr: usize,
        _rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        *obj = Value::Object(Map::new());
        Ok(())
    }

    fn generate_array(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        if let Value::Array(arr) = obj {
            let range = range_as_i64(&field.name, &settings.range)?;
            if range.iter().any(|(a, b)| *a < 0 || *b < 0) {
                return Err(anyhow!(
                    "Range for field `{:?}` must be positive.",
                    field.name
                ));
            }
            let (min, max) = range
                .map(|(a, b)| (a.try_into().unwrap_or(0), b.try_into().unwrap_or(5)))
                .unwrap_or((0usize, 5usize));
            if min >= max {
                return Err(anyhow!(
                    "Empty range, min >= max for field {:?}",
                    field.name
                ));
            }
            let scale = settings.scale.try_into().unwrap_or(1usize);

            let value_settings = settings.value.clone().unwrap_or_default();
            let columntype = *field.columntype.component.as_ref().unwrap().clone();
            let arr_field = Field {
                name: SqlIdentifier::from("array_element"),
                columntype,
                lateness: None,
                watermark: None,
                default: None,
                unused: false,
            };

            if let Some(nl) = Self::maybe_null(field, settings, rng) {
                *obj = nl;
                return Ok(());
            }

            match (&settings.strategy, &settings.values) {
                (DatagenStrategy::Increment, None) => {
                    let val = incr * scale;
                    let range = max - min;
                    let len = min + (val % range);
                    arr.resize_with(len, || Self::typ_to_default(arr_field.columntype.typ));
                    for (idx, e) in arr.iter_mut().enumerate() {
                        self.generate_field(&arr_field, &value_settings, idx, rng, e)?;
                    }
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    let new_value = values[self.current % values.len()].clone();
                    field_is_json_array(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Uniform, None) => {
                    let len = rng.sample(Uniform::from(min..max)) * scale;
                    arr.resize_with(len, || Self::typ_to_default(arr_field.columntype.typ));
                    for (idx, e) in arr.iter_mut().enumerate() {
                        self.generate_field(&arr_field, &value_settings, idx, rng, e)?;
                    }
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    let new_value = values[rng.sample(Uniform::from(0..values.len()))].clone();
                    field_is_json_array(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Zipf, None) => {
                    let range = max - min;
                    let zipf = Zipf::new(range as u64, settings.e as f64).unwrap();
                    let len = rng.sample(zipf) as usize - 1;
                    arr.resize_with(len, || Self::typ_to_default(arr_field.columntype.typ));
                    for (idx, e) in arr.iter_mut().enumerate() {
                        self.generate_field(&arr_field, &value_settings, idx, rng, e)?;
                    }
                }
                (DatagenStrategy::Zipf, Some(values)) => {
                    let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    let new_value = values[idx].clone();
                    field_is_json_array(field, &new_value)?;
                    *obj = new_value;
                }
                (m, _) => {
                    return Err(anyhow!(
                        "Invalid strategy `{m:?}` for field: `{:?}` with type {:?}",
                        field.name,
                        field.columntype.typ
                    ));
                }
            };

            Ok(())
        } else {
            unreachable!("Value is not an array");
        }
    }

    fn generate_time(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        const MAX_TIME_VALUE: u64 = 86400000; // 24h in milliseconds
        if let Value::String(str) = obj {
            str.clear();
            let range = parse_range_for_time(&field.name, &settings.range)?;
            if range.iter().any(|(a, b)| *a < 0 || *b < 0) {
                return Err(anyhow!(
                    "Range for field `{:?}` must be positive.",
                    field.name
                ));
            }
            let (min, max) = range
                .map(|(a, b)| {
                    (
                        a.try_into().unwrap_or(0u64),
                        b.try_into().unwrap_or(MAX_TIME_VALUE),
                    )
                })
                .unwrap_or((0, MAX_TIME_VALUE));
            if min >= max {
                return Err(anyhow!(
                    "Empty range, min >= max for field {:?}",
                    field.name
                ));
            }
            let scale = settings.scale.try_into().unwrap_or(1u64);

            let start_time = NaiveTime::MIN;

            match (&settings.strategy, &settings.values) {
                (DatagenStrategy::Increment, None) => {
                    let range = max - min;
                    let val_in_range = (incr as u64 * scale) % range;
                    let val = min + val_in_range;
                    debug_assert!(val >= min && val < max);

                    let t = start_time + Duration::milliseconds(val as i64);
                    write!(str, "{}", t)?;
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    let new_value = values[incr % values.len()].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Uniform, None) => {
                    let dist = Uniform::from(min..max);
                    let val = rng.sample(dist) * scale;
                    let t = start_time + Duration::milliseconds(val as i64);
                    write!(str, "{}", t)?;
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    let new_value = values[rng.sample(Uniform::from(0..values.len()))].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Zipf, None) => {
                    let range = max - min;
                    let zipf = Zipf::new(range, settings.e as f64).unwrap();
                    let val = rng.sample(zipf) as u64 - 1;
                    let t = start_time + Duration::milliseconds(val as i64);
                    write!(str, "{}", t)?;
                }
                (DatagenStrategy::Zipf, Some(values)) => {
                    let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    let new_value = values[idx].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (m, _) => {
                    return Err(anyhow!(
                        "Invalid strategy `{m:?}` for field: `{:?}` with type {:?}",
                        field.name,
                        field.columntype.typ
                    ));
                }
            };

            Ok(())
        } else {
            unreachable!("Value is not a string");
        }
    }

    fn generate_date(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        const MAX_DATE_VALUE: i64 = 54787; // 2100-01-01
        if let Value::String(str) = obj {
            str.clear();

            let range = parse_range_for_date(&field.name, &settings.range)?;
            let (min, max) = range.unwrap_or((0, MAX_DATE_VALUE));
            if min >= max {
                return Err(anyhow!(
                    "Empty range, min >= max for field {:?}",
                    field.name
                ));
            }
            let unix_date: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

            if let Some(nl) = Self::maybe_null(field, settings, rng) {
                *obj = nl;
                return Ok(());
            }
            let scale = settings.scale;
            let checked_days_arith_err = || {
                anyhow!(
                    "Unable calculate date for '{}'. Make sure it fits within Jan 1, 262145 BCE to Dec 31, 262143 CE. If the start and end dates are represented as numbers, they will be interpreted as the number of days since 1970-01-01.",
                    field.name
                )
            };

            match (&settings.strategy, &settings.values) {
                (DatagenStrategy::Increment, None) => {
                    let range = max - min;
                    let val_in_range = (incr as i64 * scale) % range;
                    let val = min + val_in_range;
                    debug_assert!(val >= min && val < max);
                    let d = unix_date
                        .checked_add_days(Days::new(val as u64))
                        .ok_or_else(checked_days_arith_err)?;
                    write!(str, "{}", d.format_with_items(YMD_FORMAT.as_slice().iter()))?;
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    let new_value = values[incr % values.len()].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Uniform, None) => {
                    let dist = Uniform::from(min..max);
                    let days = rng.sample(dist);
                    let d = if days > 0 {
                        unix_date
                            .checked_add_days(Days::new(days.unsigned_abs() * scale.unsigned_abs()))
                            .ok_or_else(checked_days_arith_err)?
                    } else {
                        unix_date
                            .checked_sub_days(Days::new(days.unsigned_abs() * scale.unsigned_abs()))
                            .ok_or_else(checked_days_arith_err)?
                    };
                    write!(str, "{}", d.format_with_items(YMD_FORMAT.as_slice().iter()))?;
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    let new_value = values[rng.sample(Uniform::from(0..values.len()))].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Zipf, None) => {
                    let range = max - min;
                    let zipf = Zipf::new(range as u64, settings.e as f64).unwrap();
                    let val = rng.sample(zipf) as i64 - 1;
                    let days = clamp(min + val, min, max);
                    let d = if days > 0 {
                        unix_date
                            .checked_add_days(Days::new(days as u64))
                            .ok_or_else(checked_days_arith_err)?
                    } else {
                        unix_date
                            .checked_sub_days(Days::new(days.unsigned_abs()))
                            .ok_or_else(checked_days_arith_err)?
                    };
                    write!(str, "{}", d.format_with_items(YMD_FORMAT.as_slice().iter()))?;
                }
                (DatagenStrategy::Zipf, Some(values)) => {
                    let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    let new_value = values[idx].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (m, _) => {
                    return Err(anyhow!(
                        "Invalid strategy `{m:?}` for field: `{:?}` with type {:?}",
                        field.name,
                        field.columntype.typ
                    ));
                }
            };

            Ok(())
        } else {
            unreachable!("Value is not a string");
        }
    }

    fn generate_timestamp(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        const MAX_DATETIME_VALUE: i64 = 4102444800000; // 2100-01-01 00:00:00.000

        if let Value::String(str) = obj {
            str.clear();
            let range = parse_range_for_datetime(&field.name, &settings.range)?;
            let (min, max) = range.unwrap_or((0, MAX_DATETIME_VALUE)); // 4102444800 => 2100-01-01
            if min >= max {
                return Err(anyhow!(
                    "Empty range, min >= max for field {:?}",
                    field.name
                ));
            }

            if let Some(nl) = Self::maybe_null(field, settings, rng) {
                *obj = nl;
                return Ok(());
            }
            let scale = settings.scale;

            // We're allocating new strings here with `to_rfc_3339` instead of
            // `write!(str, "{}", dt.format_with_items(self.rfc3339_format.as_slice().iter()))`
            //
            // You'd think this would be slower, but it turned out to be faster,
            // maybe it uses a more optimized parser that isn't taken when creating
            // a parser with `StrftimeItems::new()`?
            //
            // ```
            // StrftimeItems::new("%+")
            //    .parse_to_owned()
            //    .expect("%+ is a valid date format");
            // ```
            match (&settings.strategy, &settings.values) {
                (DatagenStrategy::Increment, None) => {
                    let val = (incr as i64 * scale) % (max - min);
                    let dt = DateTime::from_timestamp_millis(min).unwrap_or(DateTime::UNIX_EPOCH)
                        + Duration::milliseconds(val);
                    *obj = Value::String(dt.to_rfc3339());
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    let new_value = values[incr % values.len()].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Uniform, None) => {
                    let dist = Uniform::from(min..max);
                    let dt = DateTime::from_timestamp_millis(rng.sample(dist) * scale)
                        .unwrap_or(DateTime::UNIX_EPOCH);
                    *obj = Value::String(dt.to_rfc3339());
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    let new_value = values[rng.sample(Uniform::from(0..values.len()))].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Zipf, None) => {
                    let range = max - min;
                    let zipf = Zipf::new(range as u64, settings.e as f64).unwrap();
                    let val = rng.sample(zipf) as i64 - 1;
                    let dt = DateTime::from_timestamp_millis(min).unwrap_or(DateTime::UNIX_EPOCH)
                        + Duration::milliseconds(val);
                    *obj = Value::String(dt.to_rfc3339());
                }
                (DatagenStrategy::Zipf, Some(values)) => {
                    let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    let new_value = values[idx].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (m, _) => {
                    return Err(anyhow!(
                        "Invalid strategy `{m:?}` for field: `{:?}` with type {:?}",
                        field.name,
                        field.columntype.typ
                    ));
                }
            };

            Ok(())
        } else {
            unreachable!("Value is not a string");
        }
    }

    fn generate_string(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        use fake::faker::address::raw::*;
        use fake::faker::barcode::raw::*;
        use fake::faker::company::raw::*;
        use fake::faker::creditcard::raw::*;
        use fake::faker::currency::raw::*;
        use fake::faker::filesystem::raw::*;
        use fake::faker::http::raw::*;
        use fake::faker::internet::raw::*;
        use fake::faker::job::raw as job;
        use fake::faker::lorem::raw::*;
        use fake::faker::name::raw::*;
        use fake::faker::phone_number::raw::*;
        use fake::locales::*;
        use fake::Dummy;

        if let Value::String(str) = obj {
            str.clear();
            let range = range_as_i64(&field.name, &settings.range)?;

            let (min, max) = range
                .map(|(a, b)| (a.try_into().unwrap_or(0), b.try_into().unwrap_or(25)))
                .unwrap_or((0, 25));
            if min >= max {
                return Err(anyhow!(
                    "Empty range, min >= max for field {:?}",
                    field.name
                ));
            }

            if let Some(nl) = Self::maybe_null(field, settings, rng) {
                *obj = nl;
                return Ok(());
            }

            match (&settings.strategy, &settings.values) {
                (DatagenStrategy::Increment, None) => {
                    write!(str, "{}", incr as i64 * settings.scale)?;
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    let new_value = values[incr % values.len()].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Uniform, None) => {
                    let len = rng.sample(Uniform::from(min..max));
                    str.extend(rng.sample_iter(&Alphanumeric).map(char::from).take(len));
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    let new_value = values[rng.sample(Uniform::from(0..values.len()))].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Zipf, None) => {
                    let zipf = Zipf::new(max as u64, settings.e as f64).unwrap();
                    let len = rng.sample(zipf) as usize - 1;
                    str.extend(rng.sample_iter(&Alphanumeric).map(char::from).take(len));
                }
                (DatagenStrategy::Zipf, Some(values)) => {
                    let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    let new_value = values[idx].clone();
                    field_is_string(field, &new_value)?;
                    *obj = new_value;
                }
                (DatagenStrategy::Word, _) => *str = Dummy::dummy_with_rng(&Word(EN), rng),
                (DatagenStrategy::Words, _) => {
                    let v: Vec<String> = Dummy::dummy_with_rng(&Words(EN, min..max), rng);
                    *str = v.join(" ");
                }
                (DatagenStrategy::Sentence, _) => {
                    *str = Dummy::dummy_with_rng(&Sentence(EN, min..max), rng)
                }
                (DatagenStrategy::Sentences, _) => {
                    let v: Vec<String> = Dummy::dummy_with_rng(&Sentences(EN, min..max), rng);
                    *str = v.join(" ");
                }
                (DatagenStrategy::Paragraph, _) => {
                    *str = Dummy::dummy_with_rng(&Paragraph(EN, min..max), rng)
                }
                (DatagenStrategy::Paragraphs, _) => {
                    let v: Vec<String> = Dummy::dummy_with_rng(&Paragraphs(EN, min..max), rng);
                    *str = v.join(" ")
                }
                (DatagenStrategy::FirstName, _) => {
                    *str = Dummy::dummy_with_rng(&FirstName(EN), rng)
                }
                (DatagenStrategy::LastName, _) => *str = Dummy::dummy_with_rng(&LastName(EN), rng),
                (DatagenStrategy::Title, _) => *str = Dummy::dummy_with_rng(&Title(EN), rng),
                (DatagenStrategy::Suffix, _) => *str = Dummy::dummy_with_rng(&Suffix(EN), rng),
                (DatagenStrategy::Name, _) => *str = Dummy::dummy_with_rng(&Name(EN), rng),
                (DatagenStrategy::NameWithTitle, _) => {
                    *str = Dummy::dummy_with_rng(&NameWithTitle(EN), rng)
                }
                (DatagenStrategy::DomainSuffix, _) => {
                    *str = Dummy::dummy_with_rng(&DomainSuffix(EN), rng)
                }
                (DatagenStrategy::Email, _) => *str = Dummy::dummy_with_rng(&SafeEmail(EN), rng),
                (DatagenStrategy::Username, _) => *str = Dummy::dummy_with_rng(&Username(EN), rng),
                (DatagenStrategy::Password, _) => {
                    *str = Dummy::dummy_with_rng(&Password(EN, min..max), rng)
                }
                (DatagenStrategy::Field, _) => *str = Dummy::dummy_with_rng(&job::Field(EN), rng),
                (DatagenStrategy::Position, _) => {
                    *str = Dummy::dummy_with_rng(&job::Position(EN), rng)
                }
                (DatagenStrategy::Seniority, _) => {
                    *str = Dummy::dummy_with_rng(&job::Seniority(EN), rng)
                }
                (DatagenStrategy::JobTitle, _) => {
                    *str = Dummy::dummy_with_rng(&job::Title(EN), rng)
                }
                (DatagenStrategy::IPv4, _) => *str = Dummy::dummy_with_rng(&IPv4(EN), rng),
                (DatagenStrategy::IPv6, _) => *str = Dummy::dummy_with_rng(&IPv6(EN), rng),
                (DatagenStrategy::IP, _) => *str = Dummy::dummy_with_rng(&IP(EN), rng),
                (DatagenStrategy::MACAddress, _) => {
                    *str = Dummy::dummy_with_rng(&MACAddress(EN), rng)
                }
                (DatagenStrategy::UserAgent, _) => {
                    *str = Dummy::dummy_with_rng(&UserAgent(EN), rng)
                }
                (DatagenStrategy::RfcStatusCode, _) => {
                    *str = Dummy::dummy_with_rng(&RfcStatusCode(EN), rng)
                }
                (DatagenStrategy::ValidStatusCode, _) => {
                    *str = Dummy::dummy_with_rng(&ValidStatusCode(EN), rng)
                }
                (DatagenStrategy::CompanySuffix, _) => {
                    *str = Dummy::dummy_with_rng(&CompanySuffix(EN), rng)
                }
                (DatagenStrategy::CompanyName, _) => {
                    *str = Dummy::dummy_with_rng(&CompanyName(EN), rng)
                }
                (DatagenStrategy::Buzzword, _) => *str = Dummy::dummy_with_rng(&Buzzword(EN), rng),
                (DatagenStrategy::BuzzwordMiddle, _) => {
                    *str = Dummy::dummy_with_rng(&BuzzwordMiddle(EN), rng)
                }
                (DatagenStrategy::BuzzwordTail, _) => {
                    *str = Dummy::dummy_with_rng(&BuzzwordTail(EN), rng)
                }
                (DatagenStrategy::CatchPhrase, _) => {
                    *str = Dummy::dummy_with_rng(&CatchPhrase(EN), rng)
                }
                (DatagenStrategy::BsVerb, _) => *str = Dummy::dummy_with_rng(&BsVerb(EN), rng),
                (DatagenStrategy::BsAdj, _) => *str = Dummy::dummy_with_rng(&BsAdj(EN), rng),
                (DatagenStrategy::BsNoun, _) => *str = Dummy::dummy_with_rng(&BsNoun(EN), rng),
                (DatagenStrategy::Bs, _) => *str = Dummy::dummy_with_rng(&Bs(EN), rng),
                (DatagenStrategy::Profession, _) => {
                    *str = Dummy::dummy_with_rng(&Profession(EN), rng)
                }
                (DatagenStrategy::Industry, _) => *str = Dummy::dummy_with_rng(&Industry(EN), rng),
                (DatagenStrategy::CurrencyCode, _) => {
                    *str = Dummy::dummy_with_rng(&CurrencyCode(EN), rng)
                }
                (DatagenStrategy::CurrencyName, _) => {
                    *str = Dummy::dummy_with_rng(&CurrencyName(EN), rng)
                }
                (DatagenStrategy::CurrencySymbol, _) => {
                    *str = Dummy::dummy_with_rng(&CurrencySymbol(EN), rng)
                }
                (DatagenStrategy::CreditCardNumber, _) => {
                    *str = Dummy::dummy_with_rng(&CreditCardNumber(EN), rng)
                }
                (DatagenStrategy::CityPrefix, _) => {
                    *str = Dummy::dummy_with_rng(&CityPrefix(EN), rng)
                }
                (DatagenStrategy::CitySuffix, _) => {
                    *str = Dummy::dummy_with_rng(&CitySuffix(EN), rng)
                }
                (DatagenStrategy::CityName, _) => *str = Dummy::dummy_with_rng(&CityName(EN), rng),
                (DatagenStrategy::CountryName, _) => {
                    *str = Dummy::dummy_with_rng(&CountryName(EN), rng)
                }
                (DatagenStrategy::CountryCode, _) => {
                    *str = Dummy::dummy_with_rng(&CountryCode(EN), rng)
                }
                (DatagenStrategy::StreetSuffix, _) => {
                    *str = Dummy::dummy_with_rng(&StreetSuffix(EN), rng)
                }
                (DatagenStrategy::StreetName, _) => {
                    *str = Dummy::dummy_with_rng(&StreetName(EN), rng)
                }
                (DatagenStrategy::TimeZone, _) => *str = Dummy::dummy_with_rng(&TimeZone(EN), rng),
                (DatagenStrategy::StateName, _) => {
                    *str = Dummy::dummy_with_rng(&StateName(EN), rng)
                }
                (DatagenStrategy::StateAbbr, _) => {
                    *str = Dummy::dummy_with_rng(&StateAbbr(EN), rng)
                }
                (DatagenStrategy::SecondaryAddressType, _) => {
                    *str = Dummy::dummy_with_rng(&SecondaryAddressType(EN), rng)
                }
                (DatagenStrategy::SecondaryAddress, _) => {
                    *str = Dummy::dummy_with_rng(&SecondaryAddress(EN), rng)
                }
                (DatagenStrategy::ZipCode, _) => *str = Dummy::dummy_with_rng(&ZipCode(EN), rng),
                (DatagenStrategy::PostCode, _) => *str = Dummy::dummy_with_rng(&PostCode(EN), rng),
                (DatagenStrategy::BuildingNumber, _) => {
                    *str = Dummy::dummy_with_rng(&BuildingNumber(EN), rng)
                }
                (DatagenStrategy::Latitude, _) => *str = Dummy::dummy_with_rng(&Latitude(EN), rng),
                (DatagenStrategy::Longitude, _) => {
                    *str = Dummy::dummy_with_rng(&Longitude(EN), rng)
                }
                (DatagenStrategy::Isbn, _) => *str = Dummy::dummy_with_rng(&Isbn(EN), rng),
                (DatagenStrategy::Isbn13, _) => *str = Dummy::dummy_with_rng(&Isbn13(EN), rng),
                (DatagenStrategy::Isbn10, _) => *str = Dummy::dummy_with_rng(&Isbn10(EN), rng),
                (DatagenStrategy::PhoneNumber, _) => {
                    *str = Dummy::dummy_with_rng(&PhoneNumber(EN), rng)
                }
                (DatagenStrategy::CellNumber, _) => {
                    *str = Dummy::dummy_with_rng(&CellNumber(EN), rng)
                }
                (DatagenStrategy::FilePath, _) => *str = Dummy::dummy_with_rng(&FilePath(EN), rng),
                (DatagenStrategy::FileName, _) => *str = Dummy::dummy_with_rng(&FilePath(EN), rng),
                (DatagenStrategy::FileExtension, _) => {
                    *str = Dummy::dummy_with_rng(&FileExtension(EN), rng)
                }
                (DatagenStrategy::DirPath, _) => {
                    *str = Dummy::dummy_with_rng(&FileExtension(EN), rng)
                }
                (_, _) => {}
            };

            Ok(())
        } else {
            unreachable!("Value is not a string");
        }
    }

    fn generate_integer<N: Bounded + ToPrimitive>(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        let min = N::min_value().to_i64().unwrap_or(i64::MIN);
        let max = if field.columntype.scale.is_none() {
            N::max_value().to_i64().unwrap_or(i64::MAX)
        } else {
            // We don't have a SQL type for u8 but we need one for the value type of the binary array
            // so by setting scale on tinyint we currently indicate that we're dealing with a u8
            // rather than an i8
            u8::MAX as i64
        };
        let range = range_as_i64(&field.name, &settings.range)?;
        if let Some((a, b)) = range {
            if a > b {
                return Err(anyhow!(
                    "Invalid range, min > max for field {:?}",
                    field.name
                ));
            }
        }
        let scale = settings.scale;

        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            *obj = nl;
            return Ok(());
        }

        match (&settings.strategy, &settings.values, &range) {
            (DatagenStrategy::Increment, None, None) => {
                let val = (incr as i64 * scale) % max;
                *obj = Value::Number(serde_json::Number::from(val));
            }
            (DatagenStrategy::Increment, None, Some((a, b))) => {
                let range = b - a;
                let val_in_range = (incr as i64 * scale) % range;
                let val = a + val_in_range;
                debug_assert!(val >= *a && val < *b);
                *obj = Value::Number(serde_json::Number::from(val));
            }
            (DatagenStrategy::Increment, Some(values), _) => {
                let new_value = values[incr % values.len()].clone();
                field_is_number(field, &new_value)?;
                *obj = new_value;
            }
            (DatagenStrategy::Uniform, None, None) => {
                let dist = Uniform::from(min..max);
                *obj = Value::Number(serde_json::Number::from(rng.sample(dist) * scale));
            }
            (DatagenStrategy::Uniform, None, Some((a, b))) => {
                let dist = Uniform::from(*a..*b);
                *obj = Value::Number(serde_json::Number::from(rng.sample(dist) * scale));
            }
            (DatagenStrategy::Uniform, Some(values), _) => {
                let dist = Uniform::from(0..values.len());
                let new_value = values[rng.sample(dist)].clone();
                field_is_number(field, &new_value)?;
                *obj = new_value;
            }
            (DatagenStrategy::Zipf, None, None) => {
                let zipf = Zipf::new(max as u64, settings.e as f64).unwrap();
                let val_in_range = rng.sample(zipf) as i64 - 1;
                *obj = Value::Number(serde_json::Number::from(val_in_range));
            }
            (DatagenStrategy::Zipf, None, Some((a, b))) => {
                let range = b - a;
                let zipf = Zipf::new(range as u64, settings.e as f64).unwrap();
                let val_in_range = rng.sample(zipf) as i64 - 1;
                *obj = Value::Number(serde_json::Number::from(val_in_range + a));
            }
            (DatagenStrategy::Zipf, Some(values), _) => {
                let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                let new_value = values[idx].clone();
                field_is_number(field, &new_value)?;
                *obj = new_value;
            }
            (m, _, _) => {
                return Err(anyhow!(
                    "Invalid strategy `{m:?}` for field: `{:?}` with type {:?}",
                    field.name,
                    field.columntype.typ
                ));
            }
        };

        Ok(())
    }

    fn generate_real<N: Bounded + ToPrimitive>(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        let min = N::min_value().to_f64().unwrap_or(f64::MIN);
        let max = N::max_value().to_f64().unwrap_or(f64::MAX);
        let range = range_as_f64(&field.name, &settings.range)?;

        if let Some((a, b)) = range {
            if a > b {
                return Err(anyhow!(
                    "Invalid range, min > max for field {:?}",
                    field.name
                ));
            }
        }
        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            *obj = nl;
            return Ok(());
        }
        let scale = settings.scale as f64;

        match (&settings.strategy, &settings.values, &range) {
            (DatagenStrategy::Increment, None, None) => {
                let val = (incr as f64 * scale) % max;
                *obj = Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                );
            }
            (DatagenStrategy::Increment, None, Some((a, b))) => {
                let range = b - a;
                let val_in_range = (incr as f64 * scale) % range;
                let val = a + val_in_range;
                debug_assert!(val >= *a && val < *b);
                *obj = Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                );
            }
            (DatagenStrategy::Increment, Some(values), _) => {
                *obj = values[incr % values.len()].clone();
            }
            (DatagenStrategy::Uniform, None, None) => {
                let dist = Uniform::from(min..max);
                *obj = Value::Number(
                    serde_json::Number::from_f64(rng.sample(dist) * scale)
                        .unwrap_or(serde_json::Number::from(0)),
                );
            }
            (DatagenStrategy::Uniform, None, Some((a, b))) => {
                let dist = Uniform::from(*a..*b);
                *obj = Value::Number(
                    serde_json::Number::from_f64(rng.sample(dist) * scale)
                        .unwrap_or(serde_json::Number::from(0)),
                );
            }
            (DatagenStrategy::Uniform, Some(values), _) => {
                let dist = Uniform::from(0..values.len());
                *obj = values[rng.sample(dist)].clone();
            }
            (DatagenStrategy::Zipf, None, None) => {
                let zipf = Zipf::new(max as u64, settings.e as f64).unwrap();
                let val_in_range = rng.sample(zipf) as i64 - 1;
                *obj = Value::Number(serde_json::Number::from(val_in_range));
            }
            (DatagenStrategy::Zipf, None, Some((a, b))) => {
                let range = b - a;
                let zipf = Zipf::new(range as u64, settings.e as f64).unwrap();
                let val_in_range = rng.sample(zipf) - 1.0;
                *obj = Value::Number(
                    serde_json::Number::from_f64(*a + val_in_range)
                        .unwrap_or(serde_json::Number::from(0)),
                );
            }
            (DatagenStrategy::Zipf, Some(values), _) => {
                let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                *obj = values[idx].clone();
            }
            (m, _, _) => {
                return Err(anyhow!(
                    "Invalid strategy `{m:?}` for field: `{:?}` with type {:?}",
                    field.name,
                    field.columntype.typ
                ));
            }
        };

        Ok(())
    }

    fn generate_boolean(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            *obj = nl;
            return Ok(());
        }

        *obj = match (&settings.strategy, &settings.values) {
            (DatagenStrategy::Increment, None) => Value::Bool(incr % 2 == 1),
            (DatagenStrategy::Increment, Some(values)) => values[incr % values.len()].clone(),
            (DatagenStrategy::Uniform, None) => Value::Bool(rand::random::<bool>()),
            (DatagenStrategy::Uniform, Some(values)) => {
                values[rand::random::<usize>() % values.len()].clone()
            }
            (DatagenStrategy::Zipf, None) => {
                let zipf = Zipf::new(2, settings.e as f64).unwrap();
                let x = rng.sample(zipf) as usize;
                Value::Bool(x == 1)
            }
            (DatagenStrategy::Zipf, Some(values)) => {
                let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                values[idx].clone()
            }
            (m, _) => {
                return Err(anyhow!(
                    "Invalid strategy `{m:?}` for field: `{:?}` with type {:?}",
                    field.name,
                    field.columntype.typ
                ));
            }
        };

        Ok(())
    }

    fn generate_uuid(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        incr: usize,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        let range = parse_range_for_uuid(&field.name, &settings.range)?;
        if let Some((a, b)) = range {
            if a > b {
                return Err(anyhow!(
                    "Invalid range, min > max for field {:?}",
                    field.name
                ));
            }
        }
        let scale = settings.scale;

        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            *obj = nl;
            return Ok(());
        }

        match (&settings.strategy, &settings.values, &range) {
            (DatagenStrategy::Increment, None, None) => {
                let val = (incr as u128 * scale as u128) % u128::MAX;
                *obj = Value::String(uuid::Uuid::from_u128(val).to_string());
            }
            (DatagenStrategy::Increment, None, Some((a, b))) => {
                let range = b - a;
                let val_in_range = (incr as u128 * scale as u128) % range;
                let val = a + val_in_range;
                debug_assert!(val >= *a && val < *b);
                *obj = Value::String(uuid::Uuid::from_u128(val).to_string());
            }
            (DatagenStrategy::Increment, Some(values), _) => {
                let new_value = values[incr % values.len()].clone();
                field_is_uuid(field, &new_value)?;
                *obj = new_value;
            }
            (DatagenStrategy::Uniform, None, None) => {
                let dist = Uniform::from(0..u128::MAX);
                *obj = Value::String(
                    uuid::Uuid::from_u128(rng.sample(dist) * scale as u128).to_string(),
                );
            }
            (DatagenStrategy::Uniform, None, Some((a, b))) => {
                let dist = Uniform::from(*a..*b);
                *obj = Value::String(
                    uuid::Uuid::from_u128(rng.sample(dist) * scale as u128).to_string(),
                );
            }
            (DatagenStrategy::Uniform, Some(values), _) => {
                let dist = Uniform::from(0..values.len());
                let new_value = values[rng.sample(dist)].clone();
                field_is_uuid(field, &new_value)?;
                *obj = new_value;
            }
            (DatagenStrategy::Zipf, Some(values), _) => {
                let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                let new_value = values[idx].clone();
                field_is_uuid(field, &new_value)?;
                *obj = new_value;
            }
            (m, _, _) => {
                return Err(anyhow!(
                    "Invalid strategy `{m:?}` for field: `{:?}` with type {:?}",
                    field.name,
                    field.columntype.typ
                ));
            }
        };

        Ok(())
    }

    /// Generates a JSON record based on the schema and configuration.
    ///
    /// - `idx` is the index of the record to generate, it should be global
    ///   across all threads.
    fn make_json_record(&mut self, idx: usize) -> AnyResult<&Value> {
        let mut rng = SmallRng::seed_from_u64(self.seed.wrapping_add(idx as u64));
        let mut obj = self.json_obj.take().unwrap();
        self.current = idx;

        let r = self.generate_fields(
            &self.schema.fields,
            &self.config.fields,
            self.current,
            &mut rng,
            &mut obj,
        );
        self.json_obj = Some(obj);
        r?;

        Ok(self.json_obj.as_ref().unwrap())
    }
}
