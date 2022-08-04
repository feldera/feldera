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

use self::generator::{config::Config, EventGenerator, NexmarkGenerator, NextEvent};
use self::model::Event;
use crate::{
    algebra::{ZRingValue, ZSet},
    circuit::{
        operator_traits::{Data, Operator, SourceOperator},
        Scope,
    },
    OrdZSet,
};
use rand::prelude::ThreadRng;
use rand::{thread_rng, Rng};
use std::sync::mpsc::Sender;
use std::thread::sleep;
use std::time::Duration;
use std::{borrow::Cow, marker::PhantomData};

pub mod config;
pub mod generator;
pub mod model;
pub mod queries;

pub struct NexmarkSource<R: Rng, W, C> {
    /// The generator for events emitted by this source.
    // TODO(absoludity): Longer-term, it'd be great to use a client (such as a gRPC client) here to
    // completely separate the generator and allow the generator to be used with other languages
    // etc.
    generator: Box<dyn EventGenerator<R>>,
    // next_event stores the next event during `eval` when `next_event()` is called but returns an
    // event in the future, so that we can include it in the next call to eval.
    next_event: Option<NextEvent>,
    // fixedpoint_sync stores a channel transmitter so that when the source reaches its fixed
    // point, it can communicate this by sending the number of events that have been generated,
    // without sharing memory.
    fixedpoint_sync: Sender<u64>,

    _t: PhantomData<(C, W)>,
}

impl<R, W, C> NexmarkSource<R, W, C>
where
    R: Rng + 'static,
{
    pub fn from_generator<G: EventGenerator<R> + 'static>(
        generator: G,
        fixedpoint_sync: Sender<u64>,
    ) -> Self {
        NexmarkSource {
            generator: Box::new(generator),
            next_event: None,
            fixedpoint_sync,
            _t: PhantomData,
        }
    }
    pub fn new(
        config: Config,
        fixedpoint_sync: Sender<u64>,
    ) -> NexmarkSource<ThreadRng, isize, OrdZSet<Event, isize>> {
        NexmarkSource::from_generator(NexmarkGenerator::new(config, thread_rng()), fixedpoint_sync)
    }
}

impl<R, W, C> Operator for NexmarkSource<R, W, C>
where
    C: Data,
    R: Rng + 'static,
    W: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("NexmarkSource")
    }

    // For the ability to reset the circuit and run it from clean state without
    // rebuilding it, then clock_start resets the generator to the beginning of the
    // sequence.
    fn clock_start(&mut self, _scope: Scope) {
        self.generator.reset();
    }

    // Returns true if the generator has no more data (and so this source will
    // return empty zsets from now on).
    fn fixedpoint(&self, _scope: Scope) -> bool {
        !self.generator.has_next()
    }
}

impl<R, W, C> SourceOperator<C> for NexmarkSource<R, W, C>
where
    R: Rng + 'static,
    W: ZRingValue + 'static,
    C: Data + ZSet<Key = Event, R = W>,
{
    fn eval(&mut self) -> C {
        // Grab a next event, either the last event from the previous call that
        // was saved because it couldn't yet be emitted, or the next generated
        // event.
        let next_event = self
            .next_event
            .clone()
            .or_else(|| self.generator.next_event().unwrap());

        if self.fixedpoint(1) {
            self.fixedpoint_sync
                .send(self.generator.event_count())
                .unwrap();
        }

        // If there are no more events, we return an empty set.
        if next_event.is_none() {
            return C::empty(());
        }

        // Otherwise we want to emit at least one event, so if the next event
        // is still in the future, we sleep until we can emit it.
        let next_event = next_event.unwrap();
        let mut wallclock_time_now = self.generator.wallclock_time();
        if next_event.wallclock_timestamp > wallclock_time_now {
            let millis_to_sleep = next_event.wallclock_timestamp - wallclock_time_now;
            sleep(Duration::from_millis(millis_to_sleep));
            wallclock_time_now += millis_to_sleep;
        }

        // Collect as many next events as are ready.
        let mut next_events = vec![next_event];
        let mut next_event = self.generator.next_event().unwrap();
        while next_event
            .is_some_and(|next_event| next_event.wallclock_timestamp <= wallclock_time_now)
        {
            next_events.push(next_event.unwrap());
            next_event = self.generator.next_event().unwrap();
        }

        // Ensure we remember the last event that was generated but not emitted for the
        // next call.
        self.next_event = next_event;

        C::from_keys(
            (),
            next_events
                .into_iter()
                .map(|next_event| (next_event.event, W::one()))
                .collect(),
        )
    }
}

#[cfg(test)]
pub mod tests {
    use self::generator::tests::{generate_expected_next_events, RangedTimeGenerator};
    use super::*;
    use crate::{trace::Batch, Circuit, OrdZSet};
    use core::ops::Range;
    use rand::rngs::mock::StepRng;
    use std::sync::{mpsc, mpsc::Receiver};

    /// Returns a source that generates the default events/s with the specified
    /// range of wallclock time ticks.
    pub fn make_source_with_wallclock_times(
        times: Range<u64>,
        max_events: u64,
    ) -> (
        NexmarkSource<StepRng, isize, OrdZSet<Event, isize>>,
        Receiver<u64>,
    ) {
        let (tx, rx) = mpsc::channel();
        (
            NexmarkSource::from_generator(RangedTimeGenerator::new(times, max_events), tx),
            rx,
        )
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
    fn test_start_clock() {
        let expected_zset = generate_expected_zset(0, 2);

        let (mut source, _) = make_source_with_wallclock_times(0..2, 2);

        assert_eq!(source.eval(), expected_zset);

        // Calling start_clock begins the events again (manually setting the
        // wallclock base time again for the test).
        source.clock_start(1);

        assert_eq!(source.eval(), expected_zset);
    }

    // After exhausting events, the source indicates a fixed point.
    #[test]
    fn test_fixed_point() {
        let (mut source, _rx) = make_source_with_wallclock_times(0..1, 1);
        assert!(!source.fixedpoint(1));

        source.eval();

        assert!(source.fixedpoint(1));
    }

    // After exhausting events, the source sends a message on the fixedpoint_sync
    // channel.
    #[test]
    fn test_fixed_point_sync_channel() {
        let (mut source, rx) = make_source_with_wallclock_times(0..3, 3);
        assert!(rx.try_recv().is_err());

        source.eval();
        source.eval();
        source.eval();

        assert_eq!(rx.try_recv().unwrap(), 3);
    }

    // After exhausting events, the source returns empty ZSets.
    #[test]
    fn test_eval_empty_zset() {
        let (mut source, _rx) = make_source_with_wallclock_times(0..2, 1);

        source.eval();

        assert_eq!(source.eval(), OrdZSet::empty(()));
    }

    #[test]
    fn test_nexmark_dbsp_source_full_batch() {
        let root = Circuit::build(move |circuit| {
            let (source, _) = make_source_with_wallclock_times(0..9, 10);
            let expected_zset = generate_expected_zset(0, 10);

            circuit
                .add_source(source)
                .inspect(move |data: &OrdZSet<Event, isize>| {
                    assert_eq!(data, &expected_zset);
                });
        })
        .unwrap();

        root.step().unwrap();
    }

    // With the default rate of 10_000 events per second, or 10 per millisecond,
    // and then using canned milliseconds for the wallclock time, we can expect
    // batches of 10 events per call to eval.
    #[test]
    fn test_eval_batched() {
        let wallclock_time = 0;
        let (mut source, _) = make_source_with_wallclock_times(0..3, 60);
        let expected_zset_tuples = generate_expected_zset_tuples(wallclock_time, 60);

        assert_eq!(
            source.eval(),
            OrdZSet::from_tuples((), Vec::from(&expected_zset_tuples[0..10]))
        );

        assert_eq!(
            source.eval(),
            OrdZSet::from_tuples((), Vec::from(&expected_zset_tuples[10..20]))
        );

        assert_eq!(
            source.eval(),
            OrdZSet::from_tuples((), Vec::from(&expected_zset_tuples[20..30]))
        );
    }
}
