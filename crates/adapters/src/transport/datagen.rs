//! A datagen input adapter that generates random data based on a schema and config.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{anyhow, Result as AnyResult};
use chrono::{DateTime, Days, Duration, NaiveDate, NaiveTime};
use crossbeam::sync::{Parker, Unparker};
use dbsp::circuit::tokio::TOKIO;
use fake::Dummy;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Jitter, Quota, RateLimiter};
use log::warn;
use num_traits::{clamp, Bounded, FromPrimitive, ToPrimitive};
use pipeline_types::program_schema::{Field, Relation, SqlType};
use pipeline_types::transport::datagen::{
    DatagenInputConfig, DatagenStrategy, GenerationPlan, RngFieldSettings, StringMethod,
};
use rand::distributions::{Alphanumeric, Uniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde_json::{Map, Value};

use crate::transport::Step;
use crate::{InputConsumer, InputEndpoint, InputReader, PipelineState, TransportInputEndpoint};

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
    unparker: Option<Unparker>,
}

impl InputGenerator {
    fn new(
        consumer: Box<dyn InputConsumer>,
        config: DatagenInputConfig,
        schema: Relation,
    ) -> AnyResult<Self> {
        let parker = Parker::new();
        let unparker = Some(parker.unparker().clone());
        let generated = Arc::new(AtomicUsize::new(0));
        if config.workers > 1 {
            warn!("More than one `workers` is not yet supported.  Ignoring.");
        }

        let worker = 0;
        let config_clone = config.clone();
        let status = Arc::new(AtomicU32::new(PipelineState::Paused as u32));
        let status_clone = status.clone();
        let generated_clone = generated.clone();
        let schema = schema.clone();

        let rate_limiters = Arc::new(
            config
                .plan
                .iter()
                .map(|p| RateLimiter::direct(Quota::per_second(p.rate.unwrap_or(NonZeroU32::MAX))))
                .collect::<Vec<RateLimiter<_, _, _>>>(),
        );

        TOKIO.spawn(Self::worker_thread(
            worker,
            config_clone,
            rate_limiters,
            schema,
            consumer,
            parker,
            status_clone,
            generated_clone,
        ));

        Ok(Self {
            status,
            config,
            generated,
            unparker,
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
        if let Some(unparker) = &self.unparker {
            unparker.unpark();
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn worker_thread(
        worker_idx: usize,
        config: DatagenInputConfig,
        rate_limiters: Arc<Vec<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>>,
        schema: Relation,
        mut consumer: Box<dyn InputConsumer>,
        parker: Parker,
        status: Arc<AtomicU32>,
        generated: Arc<AtomicUsize>,
    ) {
        for (plan, rate_limiter) in config.plan.into_iter().zip(rate_limiters.iter()) {
            let schema = schema.clone();
            let generated = generated.clone();
            let mut generator =
                RecordGenerator::new(worker_idx, config.seed, plan, schema, generated);
            for record in &mut generator {
                match PipelineState::from_u32(status.load(Ordering::Acquire)) {
                    Some(PipelineState::Paused) => parker.park(),
                    Some(PipelineState::Running) => { /* continue */ }
                    Some(PipelineState::Terminated) => return,
                    _ => unreachable!(),
                }
                match record {
                    Ok(record) => {
                        let s = record.to_string();
                        consumer.input_chunk(s.as_bytes());
                    }
                    Err(e) => {
                        // We skip this plan and continue with the next one.
                        consumer.error(false, e);
                        break;
                    }
                }

                rate_limiter
                    .until_ready_with_jitter(Jitter::up_to(StdDuration::from_millis(20)))
                    .await;
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
    current: Arc<AtomicUsize>,
    rng: Option<SmallRng>,
}

impl RecordGenerator {
    fn new(
        worker_idx: usize,
        seed: Option<u64>,
        config: GenerationPlan,
        schema: Relation,
        current: Arc<AtomicUsize>,
    ) -> Self {
        let rng = if let Some(seed) = seed {
            SmallRng::seed_from_u64(seed + worker_idx as u64)
        } else {
            SmallRng::from_entropy()
        };

        Self {
            config,
            schema,
            current,
            rng: Some(rng),
        }
    }

    fn generate_fields(
        &self,
        fields: &[Field],
        settings: &HashMap<String, Box<RngFieldSettings>>,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
        let default_settings = Box::<RngFieldSettings>::default();
        let map = fields
            .iter()
            .try_fold(serde_json::Map::new(), |mut map, field| {
                let field_settings = settings.get(&field.name).unwrap_or(&default_settings);
                let r = map.insert(
                    field.name.clone(),
                    self.generate_field(field, field_settings, rng)?,
                );
                assert!(r.is_none());
                Ok::<Map<String, Value>, anyhow::Error>(map)
            })?;
        Ok(Value::Object(map))
    }

    fn generate_field(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
        match field.columntype.typ {
            SqlType::Boolean => self.generate_boolean(field, settings, rng),
            SqlType::TinyInt => self.generate_integer::<i8>(field, settings, rng),
            SqlType::SmallInt => self.generate_integer::<i16>(field, settings, rng),
            SqlType::Int => self.generate_integer::<i32>(field, settings, rng),
            SqlType::BigInt => self.generate_integer::<i64>(field, settings, rng),
            SqlType::Real => self.generate_real::<f64>(field, settings, rng),
            SqlType::Double => self.generate_real::<f64>(field, settings, rng),
            SqlType::Decimal => self.generate_real::<f64>(field, settings, rng),
            SqlType::Char | SqlType::Varchar | SqlType::Binary | SqlType::Varbinary => {
                self.generate_string(field, settings, rng)
            }
            SqlType::Timestamp => self.generate_timestamp(field, settings, rng),
            SqlType::Date => self.generate_date(field, settings, rng),
            SqlType::Time => self.generate_time(field, settings, rng),
            SqlType::Interval(_unit) => Ok(Value::Null), // I don't think this can show up in a table schema
            SqlType::Array => self.generate_array(field, settings, rng),
            SqlType::Map => self.generate_map(field, settings, rng),
            SqlType::Struct => self.generate_fields(
                field.columntype.fields.as_ref().unwrap(),
                settings.fields.as_ref().unwrap_or(&HashMap::new()),
                rng,
            ),
            SqlType::Null => Ok(Value::Null),
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
    ) -> AnyResult<Value> {
        Ok(Value::Object(Map::new()))
    }

    fn generate_array(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
        let (min, max) = settings
            .range
            .map(|(a, b)| (a.try_into().unwrap_or(0), b.try_into().unwrap_or(5)))
            .unwrap_or((0usize, 5usize));
        if min > max {
            return Err(anyhow!(
                "Invalid range, min > max for field {:?}",
                field.name
            ));
        }

        let value_settings = settings.value.clone().unwrap_or_default();
        let columntype = *field.columntype.component.as_ref().unwrap().clone();
        let arr_field = Field {
            name: "array_element".to_string(),
            case_sensitive: false,
            columntype,
        };

        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            return Ok(nl);
        }

        Ok(match (&settings.strategy, &settings.values) {
            (DatagenStrategy::Increment { scale }, None) => {
                let val =
                    self.current.load(Ordering::Relaxed) * (*scale).try_into().unwrap_or(1usize);
                let range = max - min;
                let len = min + (val % range);
                let values: Result<Vec<_>, _> = (0..len)
                    .map(|_| self.generate_field(&arr_field, &value_settings, rng))
                    .collect();
                Value::Array(values?)
            }
            (DatagenStrategy::Increment { scale }, Some(values)) => {
                values[(self.current.load(Ordering::Relaxed)
                    * (*scale).try_into().unwrap_or(1usize))
                    % values.len()]
                .clone()
            }
            (DatagenStrategy::Uniform, None) => {
                let len = rng.sample(Uniform::from(min..max));
                let values: Result<Vec<_>, _> = (0..len)
                    .map(|_| self.generate_field(&arr_field, settings, rng))
                    .collect();
                Value::Array(values?)
            }
            (DatagenStrategy::Uniform, Some(values)) => {
                values[rng.sample(Uniform::from(0..values.len()))].clone()
            }
            (DatagenStrategy::Zipf { s }, None) => {
                let range = max - min;
                let zipf = Zipf::new(range as u64, *s as f64).unwrap();
                let len = rng.sample(zipf) as usize - 1;
                let values: Result<Vec<_>, _> = (min..(min + len))
                    .map(|_| self.generate_field(field, settings, rng))
                    .collect();
                Value::Array(values?)
            }
            (DatagenStrategy::Zipf { s }, Some(values)) => {
                let zipf = Zipf::new(values.len() as u64, *s as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                values[idx].clone()
            }
            (DatagenStrategy::String { .. }, _) => {
                return Err(anyhow!(
                    "Invalid method `string` for non-string field: `{:?}`",
                    field.name
                ));
            }
        })
    }

    fn generate_time(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
        const MAX_TIME_VALUE: u64 = 86400000; // 24h in milliseconds
        let (min, max) = settings
            .range
            .map(|(a, b)| {
                (
                    a.try_into().unwrap_or(0u64),
                    b.try_into().unwrap_or(MAX_TIME_VALUE),
                )
            })
            .unwrap_or((0, MAX_TIME_VALUE));
        if min > max {
            return Err(anyhow!(
                "Invalid range, min > max for field {:?}",
                field.name
            ));
        }

        let start_time = NaiveTime::MIN;

        match (&settings.strategy, &settings.values) {
            (DatagenStrategy::Increment { scale }, None) => {
                let val = (self.current.load(Ordering::Relaxed) as u64
                    * (*scale).try_into().unwrap_or(1u64))
                    % max;
                let t = start_time + Duration::milliseconds(val as i64);
                Ok(Value::String(t.to_string()))
            }
            (DatagenStrategy::Increment { scale }, Some(values)) => {
                Ok(values[(self.current.load(Ordering::Relaxed)
                    * (*scale).try_into().unwrap_or(1usize))
                    % values.len()]
                .clone())
            }
            (DatagenStrategy::Uniform, None) => {
                let dist = Uniform::from(min..max);
                let val = rng.sample(dist);
                let t = start_time + Duration::milliseconds(val as i64);
                Ok(Value::String(t.to_string()))
            }
            (DatagenStrategy::Uniform, Some(values)) => {
                Ok(values[rng.sample(Uniform::from(0..values.len()))].clone())
            }
            (DatagenStrategy::Zipf { s }, None) => {
                let range = max - min;
                let zipf = Zipf::new(range, *s as f64).unwrap();
                let val = rng.sample(zipf) as u64 - 1;
                let t = start_time + Duration::milliseconds(val as i64);
                Ok(Value::String(t.to_string()))
            }
            (DatagenStrategy::Zipf { s }, Some(values)) => {
                let zipf = Zipf::new(values.len() as u64, *s as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                Ok(values[idx].clone())
            }
            (DatagenStrategy::String { .. }, _) => Err(anyhow!(
                "Invalid method `string` for non-string field: `{:?}`",
                field.name
            )),
        }
    }

    fn generate_date(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
        const MAX_DATE_VALUE: i64 = 54787; // 2100-01-01
        let (min, max) = settings.range.unwrap_or((0, MAX_DATE_VALUE));
        if min > max {
            return Err(anyhow!(
                "Invalid range, min > max for field {:?}",
                field.name
            ));
        }
        let unix_date: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            return Ok(nl);
        }

        Ok(match (&settings.strategy, &settings.values) {
            (DatagenStrategy::Increment { scale }, None) => {
                let val = (self.current.load(Ordering::Relaxed) as i64 * *scale) % (max - min);
                let d = unix_date + Days::new(val as u64);
                Value::String(d.format("%Y-%m-%d").to_string())
            }
            (DatagenStrategy::Increment { scale }, Some(values)) => {
                values[(self.current.load(Ordering::Relaxed)
                    * (*scale).try_into().unwrap_or(1usize))
                    % values.len()]
                .clone()
            }
            (DatagenStrategy::Uniform, None) => {
                let dist = Uniform::from(min..max);
                let days = rng.sample(dist);
                let d = if days > 0 {
                    unix_date + Days::new(days.unsigned_abs())
                } else {
                    unix_date - Days::new(days.unsigned_abs())
                };
                Value::String(d.format("%Y-%m-%d").to_string())
            }
            (DatagenStrategy::Uniform, Some(values)) => {
                values[rng.sample(Uniform::from(0..values.len()))].clone()
            }
            (DatagenStrategy::Zipf { s }, None) => {
                let range = max - min;
                let zipf = Zipf::new(range as u64, *s as f64).unwrap();
                let val = rng.sample(zipf) as i64 - 1;
                let days = clamp(min + val, min, max);
                let d = if days > 0 {
                    unix_date + Days::new(days as u64)
                } else {
                    unix_date - Days::new(days.unsigned_abs())
                };
                Value::String(d.format("%Y-%m-%d").to_string())
            }
            (DatagenStrategy::Zipf { s }, Some(values)) => {
                let zipf = Zipf::new(values.len() as u64, *s as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                values[idx].clone()
            }
            (DatagenStrategy::String { .. }, _) => {
                return Err(anyhow!(
                    "Invalid method `string` for non-string field: `{:?}`",
                    field.name
                ));
            }
        })
    }

    fn generate_timestamp(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
        const MAX_DATETIME_VALUE: i64 = 4102444800000; // 2100-01-01 00:00:00.000

        let (min, max) = settings.range.unwrap_or((0, MAX_DATETIME_VALUE)); // 4102444800 => 2100-01-01
        if min > max {
            return Err(anyhow!(
                "Invalid range, min > max for field {:?}",
                field.name
            ));
        }

        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            return Ok(nl);
        }

        Ok(match (&settings.strategy, &settings.values) {
            (DatagenStrategy::Increment { scale }, None) => {
                let val = (self.current.load(Ordering::Relaxed) as i64 * *scale) % (max - min);
                let dt = DateTime::from_timestamp_millis(min).unwrap_or(DateTime::UNIX_EPOCH)
                    + Duration::milliseconds(val);
                Value::String(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            }
            (DatagenStrategy::Increment { scale }, Some(values)) => {
                values[(self.current.load(Ordering::Relaxed)
                    * (*scale).try_into().unwrap_or(1usize))
                    % values.len()]
                .clone()
            }
            (DatagenStrategy::Uniform, None) => {
                let dist = Uniform::from(min..max);
                let dt = DateTime::from_timestamp_millis(min).unwrap_or(DateTime::UNIX_EPOCH)
                    + Duration::milliseconds(rng.sample(dist));
                Value::String(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            }
            (DatagenStrategy::Uniform, Some(values)) => {
                values[rng.sample(Uniform::from(0..values.len()))].clone()
            }
            (DatagenStrategy::Zipf { s }, None) => {
                let range = max - min;
                let zipf = Zipf::new(range as u64, *s as f64).unwrap();
                let val = rng.sample(zipf) as i64 - 1;
                let dt = DateTime::from_timestamp_millis(min).unwrap_or(DateTime::UNIX_EPOCH)
                    + Duration::milliseconds(val);
                Value::String(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            }
            (DatagenStrategy::Zipf { s }, Some(values)) => {
                let zipf = Zipf::new(values.len() as u64, *s as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                values[idx].clone()
            }
            (DatagenStrategy::String { .. }, _) => {
                return Err(anyhow!(
                    "Invalid method `string` for non-string field: `{:?}`",
                    field.name
                ));
            }
        })
    }

    fn generate_string(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
        let (min, max) = settings
            .range
            .map(|(a, b)| (a.try_into().unwrap_or(0), b.try_into().unwrap_or(25)))
            .unwrap_or((0, 25));
        if min > max {
            return Err(anyhow!(
                "Invalid range, min > max for field {:?}",
                field.name
            ));
        }

        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            return Ok(nl);
        }

        Ok(match (&settings.strategy, &settings.values) {
            (DatagenStrategy::Increment { scale }, None) => Value::String(format!(
                "{}",
                self.current.load(Ordering::Relaxed) as i64 * *scale
            )),
            (DatagenStrategy::Increment { scale }, Some(values)) => {
                values[(self.current.load(Ordering::Relaxed)
                    * (*scale).try_into().unwrap_or(1usize))
                    % values.len()]
                .clone()
            }
            (DatagenStrategy::Uniform, None) => {
                let len = rng.sample(Uniform::from(min..max));
                Value::String(
                    rng.sample_iter(&Alphanumeric)
                        .map(char::from)
                        .take(len)
                        .collect::<String>(),
                )
            }
            (DatagenStrategy::Uniform, Some(values)) => {
                values[rng.sample(Uniform::from(0..values.len()))].clone()
            }
            (DatagenStrategy::Zipf { s }, None) => {
                let zipf = Zipf::new(max as u64, *s as f64).unwrap();
                let len = rng.sample(zipf) as usize - 1;
                Value::String(
                    rng.sample_iter(&Alphanumeric)
                        .map(char::from)
                        .take(len)
                        .collect::<String>(),
                )
            }
            (DatagenStrategy::Zipf { s }, Some(values)) => {
                let zipf = Zipf::new(values.len() as u64, *s as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                values[idx].clone()
            }
            (DatagenStrategy::String { method }, _) => generate_fake_string(settings, method, rng),
        })
    }

    fn generate_integer<N: Bounded + ToPrimitive>(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
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

        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            return Ok(nl);
        }

        Ok(
            match (&settings.strategy, &settings.values, &settings.range) {
                (DatagenStrategy::Increment { scale }, None, None) => {
                    let val = (self.current.load(Ordering::Relaxed) as i64 * *scale) % max;
                    Value::Number(serde_json::Number::from(val))
                }
                (DatagenStrategy::Increment { scale }, None, Some((a, b))) => {
                    let range = b - a;
                    let val_in_range =
                        (self.current.load(Ordering::Relaxed) as i64 * *scale) % range;
                    let val = a + val_in_range;
                    debug_assert!(val >= *a && val < *b);
                    Value::Number(serde_json::Number::from(val))
                }
                (DatagenStrategy::Increment { scale }, Some(values), _) => {
                    values[(self.current.load(Ordering::Relaxed)
                        * (*scale).try_into().unwrap_or(1usize))
                        % values.len()]
                    .clone()
                }
                (DatagenStrategy::Uniform, None, None) => {
                    let dist = Uniform::from(min..max);
                    Value::Number(serde_json::Number::from(rng.sample(dist)))
                }
                (DatagenStrategy::Uniform, None, Some((a, b))) => {
                    let dist = Uniform::from(*a..*b);
                    Value::Number(serde_json::Number::from(rng.sample(dist)))
                }
                (DatagenStrategy::Uniform, Some(values), _) => {
                    let dist = Uniform::from(0..values.len());
                    values[rng.sample(dist)].clone()
                }
                (DatagenStrategy::Zipf { s }, None, None) => {
                    let zipf = Zipf::new(max as u64, *s as f64).unwrap();
                    let val_in_range = rng.sample(zipf) as i64 - 1;
                    Value::Number(serde_json::Number::from(val_in_range))
                }
                (DatagenStrategy::Zipf { s }, None, Some((a, b))) => {
                    let range = b - a;
                    let zipf = Zipf::new(range as u64, *s as f64).unwrap();
                    let val_in_range = rng.sample(zipf) as i64 - 1;
                    Value::Number(serde_json::Number::from(val_in_range + a))
                }
                (DatagenStrategy::Zipf { s }, Some(values), _) => {
                    let zipf = Zipf::new(values.len() as u64, *s as f64).unwrap();
                    let idx = rng.sample(zipf) as usize - 1;
                    values[idx].clone()
                }
                (DatagenStrategy::String { .. }, _, _) => {
                    return Err(anyhow!(
                        "Invalid method `string` for non-string field: `{:?}`",
                        field.name
                    ));
                }
            },
        )
    }

    fn generate_real<N: Bounded + ToPrimitive>(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
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
            return Ok(nl);
        }

        Ok(match (&settings.strategy, &settings.values, &range) {
            (DatagenStrategy::Increment { scale }, None, None) => {
                let val = (self.current.load(Ordering::Relaxed) as f64 * *scale as f64) % max;
                Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                )
            }
            (DatagenStrategy::Increment { scale }, None, Some((a, b))) => {
                let range = b - a;
                let val_in_range =
                    (self.current.load(Ordering::Relaxed) as f64 * *scale as f64) % range;
                let val = a + val_in_range;
                debug_assert!(val >= *a && val < *b);
                Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                )
            }
            (DatagenStrategy::Increment { scale }, Some(values), _) => {
                values[(self.current.load(Ordering::Relaxed)
                    * (*scale).try_into().unwrap_or(1usize))
                    % values.len()]
                .clone()
            }
            (DatagenStrategy::Uniform, None, None) => {
                let dist = Uniform::from(min..max);
                Value::Number(
                    serde_json::Number::from_f64(rng.sample(dist))
                        .unwrap_or(serde_json::Number::from(0)),
                )
            }
            (DatagenStrategy::Uniform, None, Some((a, b))) => {
                let dist = Uniform::from(*a..*b);
                Value::Number(
                    serde_json::Number::from_f64(rng.sample(dist))
                        .unwrap_or(serde_json::Number::from(0)),
                )
            }
            (DatagenStrategy::Uniform, Some(values), _) => {
                let dist = Uniform::from(0..values.len());
                values[rng.sample(dist)].clone()
            }
            (DatagenStrategy::Zipf { s }, None, None) => {
                let zipf = Zipf::new(max as u64, *s as f64).unwrap();
                let val_in_range = rng.sample(zipf) as i64 - 1;
                Value::Number(serde_json::Number::from(val_in_range))
            }
            (DatagenStrategy::Zipf { s }, None, Some((a, b))) => {
                let range = b - a;
                let zipf = Zipf::new(range as u64, *s as f64).unwrap();
                let val_in_range = rng.sample(zipf) - 1.0;
                Value::Number(
                    serde_json::Number::from_f64(*a + val_in_range)
                        .unwrap_or(serde_json::Number::from(0)),
                )
            }
            (DatagenStrategy::Zipf { s }, Some(values), _) => {
                let zipf = Zipf::new(values.len() as u64, *s as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                values[idx].clone()
            }
            (DatagenStrategy::String { .. }, _, _) => {
                return Err(anyhow!(
                    "Invalid method `string` for non-string field: `{:?}`",
                    field.name
                ));
            }
        })
    }

    fn generate_boolean(
        &self,
        field: &Field,
        settings: &RngFieldSettings,
        rng: &mut SmallRng,
    ) -> AnyResult<Value> {
        if let Some(nl) = Self::maybe_null(field, settings, rng) {
            return Ok(nl);
        }

        Ok(match (&settings.strategy, &settings.values) {
            (DatagenStrategy::Increment { scale }, None) => Value::Bool(
                (self.current.load(Ordering::Relaxed) * (*scale).try_into().unwrap_or(1usize)) % 2
                    == 1,
            ),
            (DatagenStrategy::Increment { scale }, Some(values)) => {
                values[(self.current.load(Ordering::Relaxed)
                    * (*scale).try_into().unwrap_or(1usize))
                    % values.len()]
                .clone()
            }
            (DatagenStrategy::Uniform, None) => Value::Bool(rand::random::<bool>()),
            (DatagenStrategy::Uniform, Some(values)) => {
                values[rand::random::<usize>() % values.len()].clone()
            }
            (DatagenStrategy::Zipf { s }, None) => {
                let zipf = Zipf::new(2, *s as f64).unwrap();
                let x = rng.sample(zipf) as usize;
                Value::Bool(x == 1)
            }
            (DatagenStrategy::Zipf { s }, Some(values)) => {
                let zipf = Zipf::new(values.len() as u64, *s as f64).unwrap();
                let idx = rng.sample(zipf) as usize - 1;
                values[idx].clone()
            }
            (DatagenStrategy::String { .. }, _) => {
                return Err(anyhow!(
                    "Invalid method `string` for non-string field: `{:?}`",
                    field.name
                ));
            }
        })
    }

    fn make_json_record(&mut self) -> AnyResult<Value> {
        let mut rng = self.rng.take().unwrap();
        let value = self.generate_fields(&self.schema.fields, &self.config.fields, &mut rng);
        self.current.fetch_add(1, Ordering::Relaxed);
        self.rng = Some(rng);
        value
    }
}

impl Iterator for RecordGenerator {
    type Item = AnyResult<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.load(Ordering::Relaxed) < self.config.limit.unwrap_or(usize::MAX) {
            Some(self.make_json_record())
        } else {
            None
        }
    }
}

fn generate_fake_string(
    settings: &RngFieldSettings,
    method: &StringMethod,
    rng: &mut SmallRng,
) -> Value {
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

    let (min, max) = settings
        .range
        .map(|(a, b)| (a.try_into().unwrap_or(0), b.try_into().unwrap_or(25)))
        .unwrap_or((0, 25));

    Value::String(match method {
        StringMethod::Word => Dummy::dummy_with_rng(&Word(EN), rng),
        StringMethod::Words => {
            let v: Vec<String> = Dummy::dummy_with_rng(&Words(EN, min..max), rng);
            v.join(" ")
        }
        StringMethod::Sentence => Dummy::dummy_with_rng(&Sentence(EN, min..max), rng),
        StringMethod::Sentences => {
            let v: Vec<String> = Dummy::dummy_with_rng(&Sentences(EN, min..max), rng);
            v.join(" ")
        }
        StringMethod::Paragraph => Dummy::dummy_with_rng(&Paragraph(EN, min..max), rng),
        StringMethod::Paragraphs => {
            let v: Vec<String> = Dummy::dummy_with_rng(&Paragraphs(EN, min..max), rng);
            v.join(" ")
        }
        StringMethod::FirstName => Dummy::dummy_with_rng(&FirstName(EN), rng),
        StringMethod::LastName => Dummy::dummy_with_rng(&LastName(EN), rng),
        StringMethod::Title => Dummy::dummy_with_rng(&Title(EN), rng),
        StringMethod::Suffix => Dummy::dummy_with_rng(&Suffix(EN), rng),
        StringMethod::Name => Dummy::dummy_with_rng(&Name(EN), rng),
        StringMethod::NameWithTitle => Dummy::dummy_with_rng(&NameWithTitle(EN), rng),
        StringMethod::DomainSuffix => Dummy::dummy_with_rng(&DomainSuffix(EN), rng),
        StringMethod::Email => Dummy::dummy_with_rng(&SafeEmail(EN), rng),
        StringMethod::Username => Dummy::dummy_with_rng(&Username(EN), rng),
        StringMethod::Password => Dummy::dummy_with_rng(&Password(EN, min..max), rng),
        StringMethod::Field => Dummy::dummy_with_rng(&job::Field(EN), rng),
        StringMethod::Position => Dummy::dummy_with_rng(&job::Position(EN), rng),
        StringMethod::Seniority => Dummy::dummy_with_rng(&job::Seniority(EN), rng),
        StringMethod::JobTitle => Dummy::dummy_with_rng(&job::Title(EN), rng),
        StringMethod::IPv4 => Dummy::dummy_with_rng(&IPv4(EN), rng),
        StringMethod::IPv6 => Dummy::dummy_with_rng(&IPv6(EN), rng),
        StringMethod::IP => Dummy::dummy_with_rng(&IP(EN), rng),
        StringMethod::MACAddress => Dummy::dummy_with_rng(&MACAddress(EN), rng),
        StringMethod::UserAgent => Dummy::dummy_with_rng(&UserAgent(EN), rng),
        StringMethod::RfcStatusCode => Dummy::dummy_with_rng(&RfcStatusCode(EN), rng),
        StringMethod::ValidStatusCode => Dummy::dummy_with_rng(&ValidStatusCode(EN), rng),
        StringMethod::CompanySuffix => Dummy::dummy_with_rng(&CompanySuffix(EN), rng),
        StringMethod::CompanyName => Dummy::dummy_with_rng(&CompanyName(EN), rng),
        StringMethod::Buzzword => Dummy::dummy_with_rng(&Buzzword(EN), rng),
        StringMethod::BuzzwordMiddle => Dummy::dummy_with_rng(&BuzzwordMiddle(EN), rng),
        StringMethod::BuzzwordTail => Dummy::dummy_with_rng(&BuzzwordTail(EN), rng),
        StringMethod::CatchPhrase => Dummy::dummy_with_rng(&CatchPhase(EN), rng),
        StringMethod::BsVerb => Dummy::dummy_with_rng(&BsVerb(EN), rng),
        StringMethod::BsAdj => Dummy::dummy_with_rng(&BsAdj(EN), rng),
        StringMethod::BsNoun => Dummy::dummy_with_rng(&BsNoun(EN), rng),
        StringMethod::Bs => Dummy::dummy_with_rng(&Bs(EN), rng),
        StringMethod::Profession => Dummy::dummy_with_rng(&Profession(EN), rng),
        StringMethod::Industry => Dummy::dummy_with_rng(&Industry(EN), rng),
        StringMethod::CurrencyCode => Dummy::dummy_with_rng(&CurrencyCode(EN), rng),
        StringMethod::CurrencyName => Dummy::dummy_with_rng(&CurrencyName(EN), rng),
        StringMethod::CurrencySymbol => Dummy::dummy_with_rng(&CurrencySymbol(EN), rng),
        StringMethod::CreditCardNumber => Dummy::dummy_with_rng(&CreditCardNumber(EN), rng),
        StringMethod::CityPrefix => Dummy::dummy_with_rng(&CityPrefix(EN), rng),
        StringMethod::CitySuffix => Dummy::dummy_with_rng(&CitySuffix(EN), rng),
        StringMethod::CityName => Dummy::dummy_with_rng(&CityName(EN), rng),
        StringMethod::CountryName => Dummy::dummy_with_rng(&CountryName(EN), rng),
        StringMethod::CountryCode => Dummy::dummy_with_rng(&CountryCode(EN), rng),
        StringMethod::StreetSuffix => Dummy::dummy_with_rng(&StreetSuffix(EN), rng),
        StringMethod::StreetName => Dummy::dummy_with_rng(&StreetName(EN), rng),
        StringMethod::TimeZone => Dummy::dummy_with_rng(&TimeZone(EN), rng),
        StringMethod::StateName => Dummy::dummy_with_rng(&StateName(EN), rng),
        StringMethod::StateAbbr => Dummy::dummy_with_rng(&StateAbbr(EN), rng),
        StringMethod::SecondaryAddressType => Dummy::dummy_with_rng(&SecondaryAddressType(EN), rng),
        StringMethod::SecondaryAddress => Dummy::dummy_with_rng(&SecondaryAddress(EN), rng),
        StringMethod::ZipCode => Dummy::dummy_with_rng(&ZipCode(EN), rng),
        StringMethod::PostCode => Dummy::dummy_with_rng(&PostCode(EN), rng),
        StringMethod::BuildingNumber => Dummy::dummy_with_rng(&BuildingNumber(EN), rng),
        StringMethod::Latitude => Dummy::dummy_with_rng(&Latitude(EN), rng),
        StringMethod::Longitude => Dummy::dummy_with_rng(&Longitude(EN), rng),
        StringMethod::Isbn => Dummy::dummy_with_rng(&Isbn(EN), rng),
        StringMethod::Isbn13 => Dummy::dummy_with_rng(&Isbn13(EN), rng),
        StringMethod::Isbn10 => Dummy::dummy_with_rng(&Isbn10(EN), rng),
        StringMethod::PhoneNumber => Dummy::dummy_with_rng(&PhoneNumber(EN), rng),
        StringMethod::CellNumber => Dummy::dummy_with_rng(&CellNumber(EN), rng),
        StringMethod::FilePath => Dummy::dummy_with_rng(&FilePath(EN), rng),
        StringMethod::FileName => Dummy::dummy_with_rng(&FilePath(EN), rng),
        StringMethod::FileExtension => Dummy::dummy_with_rng(&FileExtension(EN), rng),
        StringMethod::DirPath => Dummy::dummy_with_rng(&FileExtension(EN), rng),
    })
}

#[cfg(test)]
mod test {
    use crate::test::{mock_input_pipeline, MockDeZSet, MockInputConsumer, TestStruct2};
    use crate::InputReader;
    use anyhow::Result as AnyResult;
    use pipeline_types::program_schema::{Field, Relation};
    use pipeline_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
    use std::collections::BTreeMap;
    use std::thread;
    use std::time::Duration;

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
        plan: [ { limit: 10, fields: { "id": { "strategy": { "name": "increment", scale: 3 }, "range": [10, 20] } } } ]
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
        plan: [ { limit: 1, fields: { "id": { "strategy": { "name": "uniform" }, "range": [10, 20] } } } ]
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
}
