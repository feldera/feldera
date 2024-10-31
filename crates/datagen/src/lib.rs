//! A datagen input adapter that generates random data based on a schema and config.

use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Write;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration as StdDuration;

use anyhow::{anyhow, Result as AnyResult};
use atomic::Atomic;
use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, Days, Duration, NaiveDate, NaiveTime, Timelike};
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Jitter, Quota, RateLimiter};
use num_traits::{clamp, Bounded, ToPrimitive};
use rand::distributions::{Alphanumeric, Uniform};
use rand::rngs::SmallRng;
use rand::{thread_rng, Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde_json::{to_writer, Map, Value};
use tokio::sync::Notify;
use tokio::time::Instant as TokioInstant;

use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::format::Parser;
use feldera_adapterlib::transport::{
    InputConsumer, InputEndpoint, InputQueue, InputReader, InputReaderCommand,
    NonFtInputReaderCommand, TransportInputEndpoint,
};
use feldera_adapterlib::PipelineState;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier, SqlType};
use feldera_types::transport::datagen::{
    DatagenInputConfig, DatagenStrategy, GenerationPlan, RngFieldSettings,
};

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
    fn is_fault_tolerant(&self) -> bool {
        false
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
    status: Arc<Atomic<PipelineState>>,
    config: DatagenInputConfig,
    /// Amount of records generated so far.
    generated: Arc<AtomicUsize>,
    queue: Arc<InputQueue>,
    notifier: Arc<Notify>,
}

impl InputGenerator {
    fn new(
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        mut config: DatagenInputConfig,
        schema: Relation,
    ) -> AnyResult<Self> {
        let notifier = Arc::new(Notify::new());

        let generated = Arc::new(AtomicUsize::new(0));
        let status = Arc::new(Atomic::new(PipelineState::Paused));

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

        let rate_limiters = config
            .plan
            .iter()
            .map(|p| {
                RateLimiter::direct(Quota::per_second(
                    p.rate.and_then(NonZeroU32::new).unwrap_or(NonZeroU32::MAX),
                ))
            })
            .collect::<Vec<RateLimiter<_, _, _>>>();
        let progress: Vec<AtomicUsize> = (0..config.plan.len())
            .map(|_| AtomicUsize::new(0))
            .collect();
        let shared_state: Arc<Vec<(AtomicUsize, RateLimiter<_, _, _>)>> =
            Arc::new(progress.into_iter().zip(rate_limiters).collect());

        // Long-running tasks with high CPU usage don't work well on cooperative runtimes,
        // so we use a separate blocking task if we think this will happen for our workload.
        let needs_blocking_tasks = config.plan.iter().any(|p| {
            u32::from(p.rate.and_then(NonZeroU32::new).unwrap_or(NonZeroU32::MAX))
                > (250_000 * config.workers as u32)
                && p.limit.unwrap_or(usize::MAX) > 10_000_000
        });

        let queue = Arc::new(InputQueue::new(consumer.clone()));
        for _ in 0..config.workers {
            let config = config.clone();
            let status = status.clone();
            let generated = generated.clone();
            let schema = schema.clone();
            let notifier = notifier.clone();
            let consumer = consumer.clone();
            let parser = parser.fork();
            let queue = queue.clone();
            let shared_state = shared_state.clone();

            if needs_blocking_tasks {
                std::thread::spawn(move || {
                    TOKIO.block_on(Self::worker_thread(
                        config,
                        shared_state,
                        schema,
                        consumer,
                        parser,
                        queue,
                        notifier,
                        status,
                        generated,
                    ));
                });
            } else {
                TOKIO.spawn(Self::worker_thread(
                    config,
                    shared_state,
                    schema,
                    consumer,
                    parser,
                    queue,
                    notifier,
                    status,
                    generated,
                ));
            }
        }

        Ok(Self {
            status,
            config,
            generated,
            queue,
            notifier,
        })
    }

    #[allow(unused)]
    fn completed(&self) -> bool {
        let limit: usize = self
            .config
            .plan
            .iter()
            .map(|p| p.limit.unwrap_or(usize::MAX))
            .fold(0, |acc, l| acc.checked_add(l).unwrap_or(usize::MAX));
        self.generated.load(Ordering::Relaxed) >= limit
    }

    fn unpark(&self) {
        self.notifier.notify_waiters()
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    async fn worker_thread(
        config: DatagenInputConfig,
        shared_state: Arc<
            Vec<(
                AtomicUsize,
                RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>,
            )>,
        >,
        schema: Relation,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        queue: Arc<InputQueue>,
        notifier: Arc<Notify>,
        status: Arc<Atomic<PipelineState>>,
        generated: Arc<AtomicUsize>,
    ) {
        let mut buffer = Vec::new();
        static START_ARR: &[u8; 1] = b"[";
        static END_ARR: &[u8; 1] = b"]";
        static REC_DELIM: &[u8; 1] = b",";

        let seed = config
            .seed
            .unwrap_or(if consumer.is_pipeline_fault_tolerant() {
                0
            } else {
                thread_rng().gen()
            });
        for (plan, (progress, rate_limiter)) in config.plan.into_iter().zip(shared_state.iter()) {
            // Inserting in batches improves performance by around ~80k records/s for
            // me, so we always do it rather than giving the user the option.
            // If we have a low rate, it's necessary to adjust the batch-size down otherwise no inserts might be visible
            // for a long time (until a batch is full).
            let batch_size: usize = min(plan.rate.unwrap_or(u32::MAX) as usize, 10_000);
            let per_thread_chunk: usize = plan.worker_chunk_size.unwrap_or(batch_size);

            let limit = plan.limit.unwrap_or(usize::MAX);
            let schema = schema.clone();
            let mut generator = RecordGenerator::new(seed, &plan, &schema);

            // Count how long we took to so far to create a batch
            // If we end up taking too long we send a batch earlier even if we don't reach `batch_size`
            const BATCH_CREATION_TIMEOUT: StdDuration = StdDuration::from_secs(1);
            let mut batch_creation_duration = TokioInstant::now();

            // Make sure we generate records from 0..limit:
            loop {
                // Where to start generating records for this iteration, this needs to be synchronized among thread
                // so each thread generates a unique set of records.
                let start = progress.fetch_add(per_thread_chunk, Ordering::Relaxed);
                if start >= limit {
                    break;
                }
                debug_assert!(start < limit);

                // The current record range this thread is working on within 0..limit
                let generate_range = start..min(start + per_thread_chunk, limit);
                // The current record within 0..batch_size
                let mut batch_idx = 0;

                for idx in generate_range.clone() {
                    loop {
                        match status.load(Ordering::Acquire) {
                            PipelineState::Paused => notifier.notified().await,
                            PipelineState::Running => break,
                            PipelineState::Terminated => return,
                        }
                    }
                    match generator.make_json_record(idx) {
                        Ok(record) => {
                            if batch_idx == 0 {
                                buffer.clear();
                                buffer.extend(START_ARR);
                            } else {
                                buffer.extend(REC_DELIM);
                            }
                            //eprintln!("Record: {}", record);
                            to_writer(&mut buffer, &record).unwrap();
                            batch_idx += 1;

                            if batch_idx % batch_size == 0
                                || batch_creation_duration.elapsed() > BATCH_CREATION_TIMEOUT
                            {
                                buffer.extend(END_ARR);
                                queue.push(parser.parse(&buffer), buffer.len());
                                buffer.clear();
                                buffer.extend(START_ARR);
                                batch_idx = 0;
                                batch_creation_duration = TokioInstant::now();
                            }
                        }
                        Err(e) => {
                            consumer.error(true, e);
                            consumer.eoi();
                            return;
                        }
                    }

                    rate_limiter
                        .until_ready_with_jitter(Jitter::up_to(StdDuration::from_millis(20)))
                        .await;
                }
                if !buffer.is_empty() {
                    buffer.extend(END_ARR);
                    queue.push(parser.parse(&buffer), buffer.len());
                }
                // Update global progress after we created all records for a batch
                //eprintln!("adding {} to generated", generate_range.len());
                generated.fetch_add(generate_range.len(), Ordering::Relaxed);
            }
        }
        consumer.eoi();
    }
}

impl InputReader for InputGenerator {
    fn request(&self, command: InputReaderCommand) {
        match command.as_nonft().unwrap() {
            NonFtInputReaderCommand::Queue => self.queue.queue(),
            NonFtInputReaderCommand::Transition(state) => {
                self.status.store(state, Ordering::Release);
                if state != PipelineState::Paused {
                    self.unpark();
                }
            }
        }
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
