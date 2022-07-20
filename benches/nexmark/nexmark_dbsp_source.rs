//! DBSP Source operator that reads from a Nexmark Generator.

use crate::generator::{NexmarkGenerator, NextEvent};
use dbsp::{
    algebra::{ZRingValue, ZSet},
    circuit::{
        operator_traits::{Data, Operator, SourceOperator},
        Scope,
    },
};
use rand::Rng;
use std::{borrow::Cow, marker::PhantomData};

pub struct NexmarkDBSPSource<R: Rng, W, C> {
    /// The generator for events emitted by this source.
    // TODO(absoludity): Longer-term, it'd be great to use a client (such as a gRPC client) here to
    // completely separate the generator and allow the generator to be used with other languages
    // etc.
    generator: NexmarkGenerator<R>,
    batch_size: usize,
    _t: PhantomData<(C, W)>,
}

impl<R, W, C> NexmarkDBSPSource<R, W, C>
where
    R: Rng,
{
    pub fn from_generator(generator: NexmarkGenerator<R>, batch_size: usize) -> Self {
        NexmarkDBSPSource {
            generator,
            batch_size,
            _t: PhantomData,
        }
    }
}

impl<R, W, C> Operator for NexmarkDBSPSource<R, W, C>
where
    C: Data,
    R: Rng + 'static,
    W: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("NexmarkDBSPSource")
    }

    // Do clock_start and clock_end make sense for an infinite source? Or should
    // the Nexmark generator use the max_events and stop generating events after
    // that.
    /// That would also mean then that the fixed point for this data source
    /// would be at max_events?  Currently the generator will keep spitting out
    /// events, but maybe `next_event` could be updated to return a
    /// `Result<Option<NextEvent>>` and return Ok(None) after max events?
    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }
}

impl<R, W, C> SourceOperator<C> for NexmarkDBSPSource<R, W, C>
where
    R: Rng + 'static,
    W: ZRingValue + 'static,
    C: Data + ZSet<Key = NextEvent, R = W>,
{
    fn eval(&mut self) -> C {
        // Q: Why is the time argument expecting ()? Same for the key tuple's second el?
        C::from_tuples(
            (),
            (0..self.batch_size)
                .map(|_| ((self.generator.next_event().unwrap(), ()), W::one()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::generator::{config::Config, tests::generate_expected_next_events};
    use dbsp::{circuit::Root, trace::ord::OrdZSet, trace::Batch};
    use rand::rngs::mock::StepRng;

    // Generates a zset manually using the default test NexmarkGenerator
    fn generate_expected_zset(
        wallclock_base_time: u64,
        num_events: usize,
    ) -> OrdZSet<NextEvent, isize> {
        let expected_events = generate_expected_next_events(wallclock_base_time, num_events);
        let expected_zset_tuples = expected_events
            .into_iter()
            .map(|event| ((event, ()), 1))
            .collect();

        OrdZSet::<NextEvent, isize>::from_tuples((), expected_zset_tuples)
    }

    #[test]
    fn test_nexmark_dbsp_source_batch_10() {
        let root = Root::build(move |circuit| {
            let mut source = NexmarkDBSPSource::from_generator(
                NexmarkGenerator::new(Config::default(), StepRng::new(0, 1)),
                10,
            );
            source.generator.set_wallclock_base_time(1_000_000);

            let expected_zset = generate_expected_zset(1_000_000, 10);

            circuit
                .add_source(source)
                .inspect(move |data: &OrdZSet<NextEvent, isize>| {
                    assert_eq!(data, &expected_zset);
                });
        })
        .unwrap();

        root.step().unwrap();
    }

    // TODO: test multiple batches from a source.
}
