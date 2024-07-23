use std::{thread::sleep, time::Duration};

use csv::WriterBuilder;
use rdkafka::{
    error::KafkaError,
    producer::{base_producer::ThreadedProducer, BaseRecord, DefaultProducerContext, Producer},
    types::RDKafkaErrorCode,
    util::Timeout,
};
use serde::Serialize;

/// Accumulates data until it crosses a threshold size and then outputs it to it
/// to a Kafka topic.
pub struct BufferedTopic<'a> {
    producer: &'a ThreadedProducer<DefaultProducerContext>,
    topic: String,
    buffer: Vec<u8>,
    record_size: usize,
}

impl<'a> BufferedTopic<'a> {
    /// Creates a new `BufferedTopic` to output to `topic` via `producer`.  The
    /// buffer is flushed whenever it reaches `record_size` bytes.
    pub fn new(
        producer: &'a ThreadedProducer<DefaultProducerContext>,
        topic: String,
        record_size: usize,
    ) -> Self {
        Self {
            producer,
            topic,
            buffer: Vec::new(),
            record_size,
        }
    }

    /// Writes `record`, flushing automatically if the threshold is reached.
    pub fn write<S>(&mut self, record: S)
    where
        S: Serialize,
    {
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        writer.serialize(record).unwrap();
        let s = writer.into_inner().unwrap();
        if self.buffer.len() + s.len() > self.record_size && !self.buffer.is_empty() {
            self.flush();
        }
        self.buffer.extend_from_slice(&s[..]);
        if self.buffer.len() >= self.record_size {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        if !self.buffer.is_empty() {
            loop {
                let record: BaseRecord<(), _> = BaseRecord::to(&self.topic).payload(&self.buffer);
                match self.producer.send(record) {
                    Ok(()) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                        // The `ThreadedProducer` can't necessarily keep up if
                        // we're using a small record size.  Give it a chance.
                        sleep(Duration::from_millis(1))
                    }
                    Err((e, _)) => panic!("{e}"),
                }
            }
            self.buffer.clear();
        }
    }
}

impl<'a> Drop for BufferedTopic<'a> {
    fn drop(&mut self) {
        self.flush();
        self.producer.flush(Timeout::Never).unwrap();
    }
}
