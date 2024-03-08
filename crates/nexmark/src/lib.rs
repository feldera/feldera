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

use self::{
    config::Config as NexmarkConfig,
    generator::{config::Config as GeneratorConfig, NexmarkGenerator, NextEvent},
    model::Event,
};
use rand::{rngs::ThreadRng, Rng};
use std::{
    collections::VecDeque,
    ops::Range,
    sync::mpsc,
    thread::{self, sleep},
    time::{Duration, SystemTime},
};

pub mod config;
pub mod generator;
pub mod model;
pub mod queries;

/// BatchedReceiver abstracts the Receiver interface for channels of VecDeque's.
pub struct BatchedReceiver<T> {
    rx: mpsc::Receiver<VecDeque<T>>,
    current_queue: VecDeque<T>,
}

impl<T> BatchedReceiver<T> {
    fn new(rx: mpsc::Receiver<VecDeque<T>>) -> Self {
        Self {
            rx,
            current_queue: VecDeque::new(),
        }
    }

    fn recv(&mut self) -> Result<T, mpsc::RecvError> {
        match self.current_queue.pop_front() {
            Some(t) => Ok(t),
            None => {
                self.current_queue = self.rx.recv()?;
                Ok(self.current_queue.pop_front().unwrap())
            }
        }
    }
}

/// BatchedSender abstracts the Sender interface for channels of VecDeque's.
pub struct BatchedSender<T> {
    batch_size: usize,
    current_queue: VecDeque<T>,
    tx: mpsc::SyncSender<VecDeque<T>>,
}

fn batched_channel<T>(batch_size: usize) -> (BatchedSender<T>, BatchedReceiver<T>) {
    let (tx, rx) = mpsc::sync_channel(3);
    (
        BatchedSender {
            batch_size,
            current_queue: VecDeque::<T>::with_capacity(batch_size),
            tx,
        },
        BatchedReceiver::new(rx),
    )
}

impl<T> BatchedSender<T> {
    pub fn send(&mut self, t: T) -> Result<(), mpsc::SendError<VecDeque<T>>> {
        // The order here ensures that we never end up sending an empty
        // vector.
        if self.current_queue.len() == self.batch_size {
            self.tx.send(std::mem::replace(
                &mut self.current_queue,
                VecDeque::with_capacity(self.batch_size),
            ))?;
        }
        self.current_queue.push_back(t);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), mpsc::SendError<VecDeque<T>>> {
        self.tx.send(std::mem::replace(
            &mut self.current_queue,
            VecDeque::with_capacity(self.batch_size),
        ))
    }
}

pub struct NexmarkSource {
    // TODO(absoludity): Longer-term, it'd be great to extract this to a separate gRPC service that
    // generates and streams the events, so that user benchmarks, such as DBSP, will only need the
    // gRPC client (and their process will only be receiving the stream, so no need to measure the
    // CPU usage of the source). This could additionally allow the NexmarkSource to be used by
    // other projects (in other languages).

    // Channel on which the source receives vectors of next events.
    next_events_rx: BatchedReceiver<NextEvent>,

    /// An optional iterator that provides wallclock timestamps in tests.
    /// This is set to None by default.
    wallclock_iterator: Option<Range<u64>>,
}

// Creates and spawns the generators according to the nexmark config, returning
// the receiver to listen on for next events.
fn create_generators_for_config<R: Rng + Default>(
    nexmark_config: NexmarkConfig,
    // TODO: I originally planned for this function to be generic for Rng, so I could test
    // it with a StepRng, but was unable to because although `R::default()` can be used to
    // instantiate a ThreadRng, `Default` is not supported for `StepRng`. Not sure if it's
    // worth writing a wrapper around `StepRng` with a `Default` implementation (or if there's
    // a better way).
) -> BatchedReceiver<NextEvent> {
    let wallclock_base_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let buffer_size = nexmark_config.source_buffer_size;
    let mut next_event_rxs: Vec<BatchedReceiver<NextEvent>> = (0..nexmark_config
        .num_event_generators)
        .map(|generator_num| {
            GeneratorConfig::new(
                nexmark_config.clone(),
                wallclock_base_time,
                0,
                generator_num,
            )
        })
        .map(|generator_config| {
            let (mut tx, rx) = batched_channel(buffer_size);
            thread::Builder::new()
                .name(format!("generator-{}", generator_config.first_event_number))
                .spawn(move || {
                    let mut generator =
                        NexmarkGenerator::new(generator_config, R::default(), wallclock_base_time);
                    while let Ok(Some(event)) = generator.next_event() {
                        tx.send(event).unwrap();
                    }
                    tx.flush().unwrap();
                })
                .unwrap();
            rx
        })
        .collect();

    // Finally, read from the generators round-robin, sending the ordered
    // events down a single channel buffered.
    let (mut next_events_tx, next_events_rx) = batched_channel(buffer_size);
    thread::Builder::new()
        .name("nexmark collector".into())
        .spawn(move || {
            let mut num_completed_receivers = 0;
            while num_completed_receivers < next_event_rxs.len() {
                for rx in &mut next_event_rxs {
                    match rx.recv() {
                        Ok(e) => next_events_tx.send(e).unwrap(),
                        _ => {
                            num_completed_receivers += 1;
                        }
                    }
                }
            }
            next_events_tx.flush().unwrap();
        })
        .unwrap();

    next_events_rx
}

impl NexmarkSource {
    pub fn from_next_events(next_events_rx: BatchedReceiver<NextEvent>) -> Self {
        NexmarkSource {
            next_events_rx,
            wallclock_iterator: None,
        }
    }

    pub fn new(nexmark_config: NexmarkConfig) -> NexmarkSource {
        NexmarkSource::from_next_events(create_generators_for_config::<ThreadRng>(nexmark_config))
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

impl Iterator for NexmarkSource {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        let next_event = self.next_events_rx.recv().ok()?;
        // If the next event is still in the future then we're getting ahead of
        // ourselves, so we sleep until we can emit it.
        let wallclock_time_now = self.wallclock_time();
        if next_event.wallclock_timestamp > wallclock_time_now {
            let millis_to_sleep = next_event.wallclock_timestamp - wallclock_time_now;
            sleep(Duration::from_millis(millis_to_sleep));
        }

        Some(next_event.event)
    }
}

#[cfg(test)]
pub mod tests {
    use self::{
        generator::{config::Config as GeneratorConfig, tests::generate_expected_next_events},
        model::Event,
    };
    use core::iter::zip;

    use super::*;
    use core::ops::Range;
    use dbsp::{utils::Tup2, OrdZSet, RootCircuit, ZWeight};
    use rand::rngs::mock::StepRng;
    use rstest::rstest;

    /// Returns a source that generates the default events/s with the specified
    /// range of wallclock time ticks.
    pub fn make_source_with_wallclock_times(times: Range<u64>, max_events: u64) -> NexmarkSource {
        let (next_event_tx, next_event_rx) = mpsc::sync_channel(max_events as usize + 1);
        let mut generator = NexmarkGenerator::new(
            GeneratorConfig {
                nexmark_config: NexmarkConfig {
                    num_event_generators: 1,
                    ..NexmarkConfig::default()
                },
                base_time: times.start,
                ..GeneratorConfig::default()
            },
            StepRng::new(0, 1),
            0,
        );
        let mut v = VecDeque::new();
        for _ in 0..max_events {
            v.push_back(generator.next_event().unwrap().unwrap());
        }
        next_event_tx.send(v).unwrap();

        // Create a source using the pre-generated next events.
        let mut source = NexmarkSource::from_next_events(BatchedReceiver::new(next_event_rx));
        source.wallclock_iterator = Some(times);
        source
    }

    pub fn generate_expected_zset_tuples(
        wallclock_base_time: u64,
        num_events: usize,
    ) -> Vec<Tup2<Event, ZWeight>> {
        let expected_events = generate_expected_next_events(wallclock_base_time, num_events);

        expected_events
            .into_iter()
            .filter(|event| event.is_some())
            .map(|event| Tup2(event.unwrap().event, 1))
            .collect()
    }

    // Generates a zset manually using the default test NexmarkGenerator
    fn generate_expected_zset(wallclock_base_time: u64, num_events: usize) -> OrdZSet<Event> {
        OrdZSet::<Event>::from_keys(
            (),
            generate_expected_zset_tuples(wallclock_base_time, num_events),
        )
    }

    #[test]
    fn test_nexmark_dbsp_source_full_batch() {
        let (circuit, input_handle) = RootCircuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset();

            let expected_zset = generate_expected_zset(0, 10);

            stream.inspect(move |data: &OrdZSet<Event>| {
                assert_eq!(data, &expected_zset);
            });
            Ok(input_handle)
        })
        .unwrap();

        let source = make_source_with_wallclock_times(0..10, 10);
        input_handle.append(&mut source.take(10).map(|e| Tup2(e, 1)).collect());

        circuit.step().unwrap();
    }

    #[test]
    fn test_source_with_multiple_generators() {
        let nexmark_config = NexmarkConfig {
            num_event_generators: 3,
            first_event_rate: 1_000_000,
            max_events: 10,
            ..NexmarkConfig::default()
        };
        let receiver = create_generators_for_config::<ThreadRng>(nexmark_config);
        let source = NexmarkSource::from_next_events(receiver);

        let expected_zset_tuple = generate_expected_zset_tuples(0, 10);

        // Until I can use the multi-threaded generators with the StepRng, just compare
        // the event types (effectively the same).
        for (got, want) in zip(
            source.take(10),
            expected_zset_tuple.into_iter().map(|Tup2(e, _)| e),
        ) {
            match want {
                Event::Person(_) => match got {
                    Event::Person(_) => (),
                    _ => panic!("expected person, got {got:?}"),
                },
                Event::Auction(_) => match got {
                    Event::Auction(_) => (),
                    _ => panic!("expected auction, got {got:?}"),
                },
                Event::Bid(_) => match got {
                    Event::Bid(_) => (),
                    _ => panic!("expected bid, got {got:?}"),
                },
            }
        }
    }

    #[rstest]
    #[case::two_batches_of_4(vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]])]
    #[case::four_batches_of_2(vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7]])]
    fn test_batched_receiver(#[case] input_vecs: Vec<Vec<usize>>) {
        let (tx, rx) = mpsc::channel();
        let expected_output = input_vecs.concat();
        for v in input_vecs {
            tx.send(VecDeque::from(v)).unwrap();
        }

        let mut batched_receiver = BatchedReceiver::new(rx);

        expected_output
            .into_iter()
            .for_each(|n| assert_eq!(n, batched_receiver.recv().unwrap()))
    }

    #[rstest]
    #[case::nine_batched_by_4(4, vec![0, 1, 2, 3, 4, 5, 6, 7, 8], vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8]])]
    #[case::nine_batched_by_3(3, vec![0, 1, 2, 3, 4, 5, 6, 7, 8], vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8]])]
    fn test_batched_sender(
        #[case] batch_size: usize,
        #[case] input_vec: Vec<usize>,
        #[case] expected_vecs: Vec<Vec<usize>>,
    ) {
        let (mut tx, rx) = batched_channel(batch_size);

        input_vec.into_iter().for_each(|x| tx.send(x).unwrap());
        tx.flush().unwrap();

        // Get the non-batched receiver to check the batching.
        let rx = rx.rx;
        expected_vecs
            .into_iter()
            .for_each(|v| assert_eq!(VecDeque::from(v), rx.recv().unwrap()));
    }
}
