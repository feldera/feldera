//! Nexmark provides a configurable Nexmark data source.
//!
//! Based on the API defined by the [Java Flink NEXMmark implementation](https://github.com/nexmark/nexmark)
//! with some inspiration from the [Megaphone Nexmark tests](https://github.com/strymon-system/megaphone/tree/master/nexmark)
//! which are themselves based on the [Timely Nexmark benchmark implementation](https://github.com/Shinmera/bsc-thesis/tree/master/benchmarks).
//!
//! I'm writing this Nexmark data source generator with the intention of
//! moving it to its own repo and publishing as a re-usable crate, not only
//! so we can work with the wider community to improve it, but also because
//! it will allow us later to extend it to support multiple data sources in
//! parallel when DBSP can be scaled.

use self::generator::{config::Config, NexmarkGenerator, NextEvent};
use self::model::Event;
use crate::{
    algebra::{ZRingValue, ZSet},
    circuit::operator_traits::Data,
    OrdZSet,
};
use rand::thread_rng;
use std::{
    marker::PhantomData,
    ops::Range,
    sync::mpsc,
    thread::{self, sleep},
    time::Duration,
    time::SystemTime,
};

pub mod config;
pub mod generator;
pub mod model;
pub mod queries;

pub struct NexmarkSource<W, C> {
    // Channel on which the source receives next events.
    next_event_rx: mpsc::Receiver<Option<NextEvent>>,

    // next_event stores the next event during `next` when `next_event()` is called but returns an
    // event in the future, so that we can include it in the next call to next.
    next_event: Option<NextEvent>,

    /// An optional iterator that provides wallclock timestamps in tests.
    /// This is set to None by default.
    wallclock_iterator: Option<Range<u64>>,

    _t: PhantomData<(C, W)>,
}

impl<W, C> NexmarkSource<W, C> {
    pub fn from_next_events(next_event_rx: mpsc::Receiver<Option<NextEvent>>) -> Self {
        NexmarkSource {
            next_event: None,
            next_event_rx,
            wallclock_iterator: None,
            _t: PhantomData,
        }
    }
    pub fn new(config: Config) -> NexmarkSource<isize, OrdZSet<Event, isize>> {
        let (next_event_tx, next_event_rx) = mpsc::sync_channel(5);
        thread::spawn(move || {
            let mut gen = NexmarkGenerator::new(config, thread_rng(), 0);
            while gen.has_next() {
                next_event_tx.send(gen.next_event().unwrap()).unwrap();
            }
        });

        NexmarkSource::from_next_events(next_event_rx)
    }

    fn wallclock_time(&mut self) -> u64 {
        match &mut self.wallclock_iterator {
            Some(i) => i.next().unwrap(),
            None => SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}

impl<W, C> Iterator for NexmarkSource<W, C>
where
    W: ZRingValue + 'static,
    C: Data + ZSet<Key = Event, R = W>,
{
    type Item = Vec<(Event, W)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Grab a next event, either the last event from the previous call that
        // was saved because it couldn't yet be emitted, or the next generated
        // event.
        let next_event = self
            .next_event
            .clone()
            .or_else(|| self.next_event_rx.recv().unwrap_or(None))?;
        // Otherwise we want to emit at least one event, so if the next event
        // is still in the future, we sleep until we can emit it.
        let mut wallclock_time_now = self.wallclock_time();
        if next_event.wallclock_timestamp > wallclock_time_now {
            let millis_to_sleep = next_event.wallclock_timestamp - wallclock_time_now;
            sleep(Duration::from_millis(millis_to_sleep));
            wallclock_time_now += millis_to_sleep;
        }

        // Collect as many next events as are ready.
        let mut next_events = vec![next_event];
        let mut next_event = self.next_event_rx.recv().unwrap();
        while next_events.len() < 1000
            && next_event
                .is_some_and(|next_event| next_event.wallclock_timestamp <= wallclock_time_now)
        {
            next_events.push(next_event.unwrap());
            // recv can only error if the sending half of a channel is disconnected.
            next_event = self.next_event_rx.recv().unwrap_or(None);
        }

        // Ensure we remember the last event that was generated but not emitted for the
        // next call.
        self.next_event = next_event;

        Some(
            next_events
                .into_iter()
                .map(|next_event| (next_event.event, W::one()))
                .collect(),
        )
    }
}

#[cfg(test)]
pub mod tests {
    use self::generator::tests::generate_expected_next_events;
    use super::*;
    use crate::{trace::Batch, Circuit, OrdZSet};
    use core::ops::Range;
    use rand::rngs::mock::StepRng;

    /// Returns a source that generates the default events/s with the specified
    /// range of wallclock time ticks.
    pub fn make_source_with_wallclock_times(
        times: Range<u64>,
        max_events: u64,
    ) -> NexmarkSource<isize, OrdZSet<Event, isize>> {
        let (next_event_tx, next_event_rx) = mpsc::sync_channel(max_events as usize + 1);
        let mut generator =
            NexmarkGenerator::new(Config::default(), StepRng::new(0, 1), times.start);
        for _ in 0..max_events {
            next_event_tx.send(generator.next_event().unwrap()).unwrap();
        }
        next_event_tx.send(None).unwrap();

        // Create a source using the pre-generated next events.
        let mut source = NexmarkSource::from_next_events(next_event_rx);
        source.wallclock_iterator = Some(times);
        source
    }

    pub fn generate_expected_zset_tuples(
        wallclock_base_time: u64,
        num_events: usize,
    ) -> Vec<(Event, isize)> {
        let expected_events = generate_expected_next_events(wallclock_base_time, num_events);

        expected_events
            .into_iter()
            .filter(|event| event.is_some())
            .map(|event| (event.unwrap().event, 1))
            .collect()
    }

    // Generates a zset manually using the default test NexmarkGenerator
    fn generate_expected_zset(
        wallclock_base_time: u64,
        num_events: usize,
    ) -> OrdZSet<Event, isize> {
        OrdZSet::<Event, isize>::from_keys(
            (),
            generate_expected_zset_tuples(wallclock_base_time, num_events),
        )
    }

    #[test]
    fn test_nexmark_dbsp_source_full_batch() {
        let (circuit, mut input_handle) = Circuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset();

            let expected_zset = generate_expected_zset(0, 10);

            stream.inspect(move |data: &OrdZSet<Event, isize>| {
                assert_eq!(data, &expected_zset);
            });
            input_handle
        })
        .unwrap();

        let mut source = make_source_with_wallclock_times(0..9, 10);
        input_handle.append(&mut source.next().unwrap());

        circuit.step().unwrap();
    }

    // With the default rate of 10_000 events per second, or 10 per millisecond,
    // and then using canned milliseconds for the wallclock time, we can expect
    // batches of 10 events per call to next.
    #[test]
    fn test_next_batched() {
        let wallclock_time = 0;
        let mut source = make_source_with_wallclock_times(0..3, 60);
        let expected_zset_tuples = generate_expected_zset_tuples(wallclock_time, 60);

        assert_eq!(
            source.next().unwrap(),
            Vec::from(&expected_zset_tuples[0..10])
        );

        assert_eq!(
            source.next().unwrap(),
            Vec::from(&expected_zset_tuples[10..20])
        );

        assert_eq!(
            source.next().unwrap(),
            Vec::from(&expected_zset_tuples[20..30])
        );
    }
}
