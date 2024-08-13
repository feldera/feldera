//! A datagen input adapter that generates random data based on a schema and config.

use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Write;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use crate::transport::Step;
use crate::{InputConsumer, InputEndpoint, InputReader, PipelineState, TransportInputEndpoint};
use anyhow::{anyhow, Result as AnyResult};
use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, Days, Duration, NaiveDate, NaiveTime};
use dbsp::circuit::tokio::TOKIO;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Jitter, Quota, RateLimiter};
use num_traits::{clamp, Bounded, FromPrimitive, ToPrimitive};
use pipeline_types::program_schema::{Field, Relation, SqlType};
use pipeline_types::transport::datagen::{
    DatagenInputConfig, DatagenStrategy, GenerationPlan, RngFieldSettings,
};
use rand::distributions::{Alphanumeric, Uniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde_json::{to_writer, Map, Value};
use tokio::sync::Notify;

pub(crate) struct GeneratorEndpoint {
    config: DatagenInputConfig,
}

impl GeneratorEndpoint {
    pub(crate) fn new(config: DatagenInputConfig) -> Self {
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
        _start_step: Step,
        schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(InputGenerator::new(
            consumer,
            self.config.clone(),
            schema,
        )?))
    }
}

struct InputGenerator {
    status: Arc<AtomicU32>,
    config: DatagenInputConfig,
    /// Amount of records generated so far.
    generated: Arc<AtomicUsize>,
    notifier: Arc<Notify>,
}

impl InputGenerator {
    fn new(
        consumer: Box<dyn InputConsumer>,
        config: DatagenInputConfig,
        schema: Relation,
    ) -> AnyResult<Self> {
        let notifier = Arc::new(Notify::new());

        let generated = Arc::new(AtomicUsize::new(0));
        let status = Arc::new(AtomicU32::new(PipelineState::Paused as u32));

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

        //eprintln!("Starting {} workers", config.workers);
        for worker in 0..config.workers {
            let config = config.clone();
            let status = status.clone();
            let generated = generated.clone();
            let schema = schema.clone();
            let notifier = notifier.clone();
            let consumer = consumer.fork();
            let shared_state = shared_state.clone();

            if needs_blocking_tasks {
                std::thread::spawn(move || {
                    TOKIO.block_on(Self::worker_thread(
                        worker,
                        config,
                        shared_state,
                        schema,
                        consumer,
                        notifier,
                        status,
                        generated,
                    ));
                });
            } else {
                TOKIO.spawn(Self::worker_thread(
                    worker,
                    config,
                    shared_state,
                    schema,
                    consumer,
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
        worker_idx: usize,
        config: DatagenInputConfig,
        shared_state: Arc<
            Vec<(
                AtomicUsize,
                RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>,
            )>,
        >,
        schema: Relation,
        mut consumer: Box<dyn InputConsumer>,
        notifier: Arc<Notify>,
        status: Arc<AtomicU32>,
        generated: Arc<AtomicUsize>,
    ) {
        let mut buffer = Vec::new();
        static START_ARR: &[u8; 1] = b"[";
        static END_ARR: &[u8; 1] = b"]";
        static REC_DELIM: &[u8; 1] = b",";

        for (plan, (progress, rate_limiter)) in config.plan.into_iter().zip(shared_state.iter()) {
            // Inserting in batches improves performance by around ~80k records/s for
            // me, so we always do it rather than giving the user the option.
            // If we have a low rate, it's necessary to adjust the batch-size down otherwise no inserts might be visible
            // for a long time (until a batch is full).
            let batch_size: usize = min(plan.rate.unwrap_or(u32::MAX) as usize, 10_000);
            let per_thread_chunk: usize = 10 * batch_size;

            let limit = plan.limit.unwrap_or(usize::MAX);
            let schema = schema.clone();
            let mut generator = RecordGenerator::new(worker_idx, config.seed, plan, schema);

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
                    let record = generator.make_json_record(idx);
                    match PipelineState::from_u32(status.load(Ordering::Acquire)) {
                        Some(PipelineState::Paused) => notifier.notified().await,
                        Some(PipelineState::Running) => { /* continue */ }
                        Some(PipelineState::Terminated) => return,
                        _ => unreachable!(),
                    }
                    match record {
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

                            if batch_idx % batch_size == 0 {
                                buffer.extend(END_ARR);
                                consumer.input_chunk(&buffer);
                                buffer.clear();
                                buffer.extend(START_ARR);
                                batch_idx = 0;
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
                    consumer.input_chunk(&buffer);
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
    fn pause(&self) -> AnyResult<()> {
        // Notify worker thread via the status flag.  The worker may
        // send another buffer downstream before the flag takes effect.
        self.status
            .store(PipelineState::Paused as u32, Ordering::Release);
        Ok(())
    }

    fn start(&self, _step: Step) -> AnyResult<()> {
        self.status
            .store(PipelineState::Running as u32, Ordering::Release);

        // Wake up the worker if it's paused.
        self.unpark();
        Ok(())
    }

    fn disconnect(&self) {
        self.status
            .store(PipelineState::Terminated as u32, Ordering::Release);

        // Wake up the worker if it's paused.
        self.unpark();
    }
}

impl Drop for InputGenerator {
    fn drop(&mut self) {
        self.disconnect();
    }
}

struct RecordGenerator {
    config: GenerationPlan,
    schema: Relation,
    current: usize,
    rng: Option<SmallRng>,
    ymd_format: Vec<Item<'static>>,
    json_obj: Option<Value>,
}

impl RecordGenerator {
    fn new(worker_idx: usize, seed: Option<u64>, config: GenerationPlan, schema: Relation) -> Self {
        let rng = if let Some(seed) = seed {
            SmallRng::seed_from_u64(seed + worker_idx as u64)
        } else {
            SmallRng::from_entropy()
        };

        let ymd_format = StrftimeItems::new("%Y-%m-%d")
            .parse_to_owned()
            .expect("%Y-%m-%d is a valid date format");

        Self {
            config,
            schema,
            current: 0,
            ymd_format,
            rng: Some(rng),
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
            SqlType::Char
            | SqlType::Varchar
            | SqlType::Binary
            | SqlType::Varbinary
            | SqlType::Timestamp
            | SqlType::Date
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
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        let map = obj.as_object_mut().unwrap();
        for key in settings.keys() {
            if !fields.iter().any(|f| f.name.eq(key)) {
                return Err(anyhow!(
                    "Field `{}` specified in datagen does not exist in the table schema.",
                    key
                ));
            }
        }

        let default_settings = Box::<RngFieldSettings>::default();
        for field in fields {
            let field_settings = settings.get(&field.name).unwrap_or(&default_settings);
            let obj = map
                .entry(&field.name)
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
            self.generate_field(field, field_settings, rng, obj)?;
        }

        Ok(())
    }

    fn generate_field(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        match field.columntype.typ {
            SqlType::Boolean => self.generate_boolean(field, settings, rng, obj),
            SqlType::TinyInt => self.generate_integer::<i8>(field, settings, rng, obj),
            SqlType::SmallInt => self.generate_integer::<i16>(field, settings, rng, obj),
            SqlType::Int => self.generate_integer::<i32>(field, settings, rng, obj),
            SqlType::BigInt => self.generate_integer::<i64>(field, settings, rng, obj),
            SqlType::Real => self.generate_real::<f64>(field, settings, rng, obj),
            SqlType::Double => self.generate_real::<f64>(field, settings, rng, obj),
            SqlType::Decimal => self.generate_real::<f64>(field, settings, rng, obj),
            SqlType::Char | SqlType::Varchar | SqlType::Binary | SqlType::Varbinary => {
                self.generate_string(field, settings, rng, obj)
            }
            SqlType::Timestamp => self.generate_timestamp(field, settings, rng, obj),
            SqlType::Date => self.generate_date(field, settings, rng, obj),
            SqlType::Time => self.generate_time(field, settings, rng, obj),
            SqlType::Interval(_unit) => {
                // I don't think this can show up in a table schema
                *obj = Value::Null;
                Ok(())
            }
            SqlType::Array => self.generate_array(field, settings, rng, obj),
            SqlType::Map => self.generate_map(field, settings, rng, obj),
            SqlType::Struct => self.generate_fields(
                field.columntype.fields.as_ref().unwrap(),
                settings.fields.as_ref().unwrap_or(&HashMap::new()),
                rng,
                obj,
            ),
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
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        if let Value::Array(arr) = obj {
            if settings.range.iter().any(|(a, b)| *a < 0 || *b < 0) {
                return Err(anyhow!(
                    "Range for field `{:?}` must be positive.",
                    field.name
                ));
            }
            let (min, max) = settings
                .range
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
                name: "array_element".to_string(),
                case_sensitive: false,
                columntype,
            };

            if let Some(nl) = Self::maybe_null(field, settings, rng) {
                *obj = nl;
                return Ok(());
            }

            match (&settings.strategy, &settings.values) {
                (DatagenStrategy::Increment, None) => {
                    let val = self.current * scale;
                    let range = max - min;
                    let len = min + (val % range);
                    arr.resize_with(len, || Self::typ_to_default(arr_field.columntype.typ));
                    for e in arr.iter_mut() {
                        self.generate_field(&arr_field, &value_settings, rng, e)?;
                    }
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    *obj = values[self.current % values.len()].clone();
                }
                (DatagenStrategy::Uniform, None) => {
                    let len = rng.sample(Uniform::from(min..max)) * scale;
                    arr.resize_with(len, || Self::typ_to_default(arr_field.columntype.typ));
                    for e in arr.iter_mut() {
                        self.generate_field(&arr_field, &value_settings, rng, e)?;
                    }
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    *obj = values[rng.sample(Uniform::from(0..values.len()))].clone();
                }
                (DatagenStrategy::Zipf, None) => {
                    let range = max - min;
                    let zipf = Zipf::new(range as u64, settings.e as f64).unwrap();
                    let len = rng.sample(zipf) as usize - 1;
                    arr.resize_with(len, || Self::typ_to_default(arr_field.columntype.typ));
                    for e in arr.iter_mut() {
                        self.generate_field(&arr_field, &value_settings, rng, e)?;
                    }
                }
                (DatagenStrategy::Zipf, Some(values)) => {
                    let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    *obj = values[idx].clone()
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
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        const MAX_TIME_VALUE: u64 = 86400000; // 24h in milliseconds
        if let Value::String(str) = obj {
            str.clear();
            if settings.range.iter().any(|(a, b)| *a < 0 || *b < 0) {
                return Err(anyhow!(
                    "Range for field `{:?}` must be positive.",
                    field.name
                ));
            }
            let (min, max) = settings
                .range
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
                    let val = (self.current as u64 * scale) % max;
                    let t = start_time + Duration::milliseconds(val as i64);
                    write!(str, "{}", t)?;
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    *obj = values[self.current % values.len()].clone();
                }
                (DatagenStrategy::Uniform, None) => {
                    let dist = Uniform::from(min..max);
                    let val = rng.sample(dist) * scale;
                    let t = start_time + Duration::milliseconds(val as i64);
                    write!(str, "{}", t)?;
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    *obj = values[rng.sample(Uniform::from(0..values.len()))].clone();
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
                    *obj = values[idx].clone();
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
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        const MAX_DATE_VALUE: i64 = 54787; // 2100-01-01
        if let Value::String(str) = obj {
            str.clear();

            let (min, max) = settings.range.unwrap_or((0, MAX_DATE_VALUE));
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

            match (&settings.strategy, &settings.values) {
                (DatagenStrategy::Increment, None) => {
                    let val = (self.current as i64 * scale) % (max - min);
                    let d = unix_date + Days::new(val as u64);

                    write!(
                        str,
                        "{}",
                        d.format_with_items(self.ymd_format.as_slice().iter())
                    )?;
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    *obj = values[self.current % values.len()].clone();
                }
                (DatagenStrategy::Uniform, None) => {
                    let dist = Uniform::from(min..max);
                    let days = rng.sample(dist);
                    let d = if days > 0 {
                        unix_date + Days::new(days.unsigned_abs() * scale.unsigned_abs())
                    } else {
                        unix_date - Days::new(days.unsigned_abs() * scale.unsigned_abs())
                    };
                    write!(
                        str,
                        "{}",
                        d.format_with_items(self.ymd_format.as_slice().iter())
                    )?;
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    *obj = values[rng.sample(Uniform::from(0..values.len()))].clone();
                }
                (DatagenStrategy::Zipf, None) => {
                    let range = max - min;
                    let zipf = Zipf::new(range as u64, settings.e as f64).unwrap();
                    let val = rng.sample(zipf) as i64 - 1;
                    let days = clamp(min + val, min, max);
                    let d = if days > 0 {
                        unix_date + Days::new(days as u64)
                    } else {
                        unix_date - Days::new(days.unsigned_abs())
                    };
                    write!(
                        str,
                        "{}",
                        d.format_with_items(self.ymd_format.as_slice().iter())
                    )?;
                }
                (DatagenStrategy::Zipf, Some(values)) => {
                    let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    *obj = values[idx].clone();
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
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        const MAX_DATETIME_VALUE: i64 = 4102444800000; // 2100-01-01 00:00:00.000

        if let Value::String(str) = obj {
            str.clear();

            let (min, max) = settings.range.unwrap_or((0, MAX_DATETIME_VALUE)); // 4102444800 => 2100-01-01
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
                    let val = (self.current as i64 * scale) % (max - min);
                    let dt = DateTime::from_timestamp_millis(min).unwrap_or(DateTime::UNIX_EPOCH)
                        + Duration::milliseconds(val);
                    *obj = Value::String(dt.to_rfc3339());
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    *obj = values[self.current % values.len()].clone();
                }
                (DatagenStrategy::Uniform, None) => {
                    let dist = Uniform::from(min..max);
                    let dt = DateTime::from_timestamp_millis(min).unwrap_or(DateTime::UNIX_EPOCH)
                        + Duration::milliseconds(rng.sample(dist) * scale);
                    *obj = Value::String(dt.to_rfc3339());
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    *obj = values[rng.sample(Uniform::from(0..values.len()))].clone();
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
                    *obj = values[idx].clone();
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

            let (min, max) = settings
                .range
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
                    write!(str, "{}", self.current as i64 * settings.scale)?;
                }
                (DatagenStrategy::Increment, Some(values)) => {
                    *obj = values[self.current % values.len()].clone()
                }
                (DatagenStrategy::Uniform, None) => {
                    let len = rng.sample(Uniform::from(min..max));
                    str.extend(rng.sample_iter(&Alphanumeric).map(char::from).take(len));
                }
                (DatagenStrategy::Uniform, Some(values)) => {
                    *obj = values[rng.sample(Uniform::from(0..values.len()))].clone();
                }
                (DatagenStrategy::Zipf, None) => {
                    let zipf = Zipf::new(max as u64, settings.e as f64).unwrap();
                    let len = rng.sample(zipf) as usize - 1;
                    str.extend(rng.sample_iter(&Alphanumeric).map(char::from).take(len));
                }
                (DatagenStrategy::Zipf, Some(values)) => {
                    let zipf = Zipf::new(values.len() as u64, settings.e as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    *obj = values[idx].clone();
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
                    *str = Dummy::dummy_with_rng(&CatchPhase(EN), rng)
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
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        let min = N::min_value().to_i64().unwrap_or(i64::MIN);
        let max = N::max_value().to_i64().unwrap_or(i64::MAX);
        if let Some((a, b)) = settings.range {
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

        match (&settings.strategy, &settings.values, &settings.range) {
            (DatagenStrategy::Increment, None, None) => {
                let val = (self.current as i64 * scale) % max;
                *obj = Value::Number(serde_json::Number::from(val));
            }
            (DatagenStrategy::Increment, None, Some((a, b))) => {
                let range = b - a;
                let val_in_range = (self.current as i64 * scale) % range;
                let val = a + val_in_range;
                debug_assert!(val >= *a && val < *b);
                *obj = Value::Number(serde_json::Number::from(val));
            }
            (DatagenStrategy::Increment, Some(values), _) => {
                *obj = values[self.current % values.len()].clone();
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
                *obj = values[rng.sample(dist)].clone()
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
                *obj = values[idx].clone()
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
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        let min = N::min_value().to_f64().unwrap_or(f64::MIN);
        let max = N::max_value().to_f64().unwrap_or(f64::MAX);
        if let Some((a, b)) = settings.range {
            if a > b {
                return Err(anyhow!(
                    "Invalid range, min > max for field {:?}",
                    field.name
                ));
            }
        }
        let range = settings.range.map(|(a, b)| (a as f64, b as f64));
        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            *obj = nl;
            return Ok(());
        }
        let scale = settings.scale as f64;

        match (&settings.strategy, &settings.values, &range) {
            (DatagenStrategy::Increment, None, None) => {
                let val = (self.current as f64 * scale) % max;
                *obj = Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                );
            }
            (DatagenStrategy::Increment, None, Some((a, b))) => {
                let range = b - a;
                let val_in_range = (self.current as f64 * scale) % range;
                let val = a + val_in_range;
                debug_assert!(val >= *a && val < *b);
                *obj = Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                );
            }
            (DatagenStrategy::Increment, Some(values), _) => {
                *obj = values[self.current % values.len()].clone();
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
        rng: &mut SmallRng,
        obj: &mut Value,
    ) -> AnyResult<()> {
        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            *obj = nl;
            return Ok(());
        }

        *obj = match (&settings.strategy, &settings.values) {
            (DatagenStrategy::Increment, None) => Value::Bool(self.current % 2 == 1),
            (DatagenStrategy::Increment, Some(values)) => {
                values[self.current % values.len()].clone()
            }
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
    ///  across all threads.
    fn make_json_record(&mut self, idx: usize) -> AnyResult<&Value> {
        let mut rng = self.rng.take().unwrap();
        let mut obj = self.json_obj.take().unwrap();
        self.current = idx;

        let r = self.generate_fields(&self.schema.fields, &self.config.fields, &mut rng, &mut obj);
        self.rng = Some(rng);
        self.json_obj = Some(obj);
        r?;

        Ok(self.json_obj.as_ref().unwrap())
    }
}

#[cfg(test)]
mod test {
    use crate::test::{mock_input_pipeline, MockDeZSet, MockInputConsumer, TestStruct2};
    use crate::InputReader;
    use anyhow::Result as AnyResult;
    use pipeline_types::config::{InputEndpointConfig, TransportConfig};
    use pipeline_types::program_schema::{Field, Relation};
    use pipeline_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
    use pipeline_types::transport::datagen::GenerationPlan;
    use std::collections::BTreeMap;
    use std::time::Duration;
    use std::{env, thread};

    fn mk_pipeline<T, U>(
        config_str: &str,
        fields: Vec<Field>,
    ) -> AnyResult<(Box<dyn InputReader>, MockInputConsumer, MockDeZSet<T, U>)>
    where
        T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
        U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    {
        let relation = Relation::new("test_input", false, fields, true, BTreeMap::new());
        let (endpoint, consumer, zset) =
            mock_input_pipeline::<T, U>(serde_yaml::from_str(config_str).unwrap(), relation)?;
        endpoint.start(0)?;
        Ok((endpoint, consumer, zset))
    }

    #[test]
    fn test_limit_increment() {
        let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 10, fields: {} } ]
"#;
        let (_endpoint, consumer, zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }

        let zst = zset.state();
        let iter = zst.flushed.iter();
        let mut idx = 0;

        for upd in iter {
            let record = upd.unwrap_insert();
            assert_eq!(record.field, idx);
            assert_eq!(record.field_1, idx % 2 == 1);
            assert_eq!(record.field_5.field, idx % 2 == 1);
            idx += 1;
        }
        assert_eq!(idx, 10);
    }

    #[test]
    fn test_scaled_range_increment() {
        let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 10, fields: { "id": { "strategy": "increment", "range": [10, 20], scale: 3 } } } ]
"#;
        let (_endpoint, consumer, zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }

        let zst = zset.state();
        let iter = zst.flushed.iter();
        let mut idx = 10;
        for upd in iter {
            let record = upd.unwrap_insert();
            assert_eq!(record.field, 10 + (idx % 10));
            idx += 3;
        }
    }

    #[test]
    fn test_uniform_range() {
        let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 1, fields: { "id": { "strategy": "uniform", "range": [10, 20] } } } ]
"#;
        let (_endpoint, consumer, zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }

        let zst = zset.state();
        let iter = zst.flushed.iter();
        for upd in iter {
            let record = upd.unwrap_insert();
            assert!(record.field >= 10 && record.field < 20);
        }
    }

    #[test]
    fn test_values() {
        let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 4, fields: { "id": { values: [99, 100, 101] } } } ]
"#;
        let (_endpoint, consumer, zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }

        let mut next = 99;
        let zst = zset.state();
        let iter = zst.flushed.iter();
        for upd in iter {
            let record = upd.unwrap_insert();
            assert_eq!(record.field, next);
            next += 1;
            if next == 102 {
                next = 99;
            }
        }
    }

    #[test]
    fn missing_config_does_something_sane() {
        let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
      workers: 3
"#;
        let cfg: InputEndpointConfig = serde_yaml::from_str(config_str).unwrap();

        if let TransportConfig::Datagen(dtg) = cfg.connector_config.transport {
            assert_eq!(dtg.plan.len(), 1);
            assert_eq!(dtg.plan[0], GenerationPlan::default());
        }
    }

    #[test]
    fn test_null() {
        let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 10, fields: { "name": { null_percentage: 100 } } } ]
"#;
        let (_endpoint, consumer, zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }

        let zst = zset.state();
        let iter = zst.flushed.iter();
        for upd in iter {
            let record = upd.unwrap_insert();
            assert!(record.field_0.is_none());
        }
    }

    #[test]
    fn test_null_percentage() {
        let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 100, fields: { "name": { null_percentage: 50 } } } ]
"#;
        let (_endpoint, consumer, zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }

        let zst = zset.state();
        let iter = zst.flushed.iter();
        for upd in iter {
            let record = upd.unwrap_insert();
            // This assert is not asserting anything useful, but it's just running this test
            // checks that datagen doesn't panic when null_percentage is set
            // as we always need a proper Value::String type in the &mut Value field and
            // with null percentage it sometimes can get set to Value::Null
            assert!(record.field_0.is_none() || record.field_0.is_some());
        }
    }

    #[test]
    fn test_string_generators() {
        let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 2, fields: { "name": { "strategy": "word" } } } ]
"#;
        let (_endpoint, consumer, zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }

        let zst = zset.state();
        let iter = zst.flushed.iter();
        for upd in iter {
            let record = upd.unwrap_insert();
            assert!(record.field_0.is_some());
        }
    }

    #[test]
    #[ignore]
    fn test_tput() {
        static DEFAULT_SIZE: usize = 5_000_000;
        let size = env::var("RECORDS")
            .map(|r| r.parse().unwrap_or(DEFAULT_SIZE))
            .unwrap_or(DEFAULT_SIZE);

        static DEFAULT_WORKERS: usize = 1;
        let workers = env::var("WORKERS")
            .map(|r| r.parse().unwrap_or(DEFAULT_WORKERS))
            .unwrap_or(DEFAULT_WORKERS);

        let config_str = format!(
            "
stream: test_input
transport:
    name: datagen
    config:
        plan: [ {{ limit: {size}, fields: {{}} }} ]
        workers: {workers}
"
        );
        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config_str.as_str(), TestStruct2::schema())
                .unwrap();

        let start = std::time::Instant::now();
        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(25));
        }
        let elapsed = start.elapsed();
        let tput = size as f64 / elapsed.as_secs_f64();
        println!("Tput({workers}, {size}): {tput:.2} records/sec");
    }
}
