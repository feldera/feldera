use clap::Parser;
use csv::WriterBuilder;
use dbsp_nexmark::{config::GeneratorOptions, NexmarkSource};
use env_logger::Env;
use indicatif::ProgressBar;
use rdkafka::{
    config::FromClientConfig,
    producer::{base_producer::ThreadedProducer, BaseRecord, DefaultProducerContext, Producer},
    util::Timeout,
    ClientConfig,
};
use serde::Serialize;

/// Accumulates data until it crosses a threshold size and then outputs it to it
/// to a Kafka topic.
struct BufferedTopic<'a> {
    producer: &'a ThreadedProducer<DefaultProducerContext>,
    topic: String,
    buffer: Vec<u8>,
}

/// Maximum size of a record to send to the Kafka broker.  This shouldn't be
/// increased beyond 1_000_000, which is the default maximum in a couple of
/// places in the Kafka broker that has to be increased in multiple places (see
/// https://stackoverflow.com/questions/21020347/how-can-i-send-large-messages-with-kafka-over-15mb).
const MAX_LEN: usize = 512 * 1024;

impl<'a> BufferedTopic<'a> {
    /// Creates a new `BufferedTopic` to output to `topic` via `producer`.  The
    /// buffer is flushed whenever it reaches `MAX_LEN` bytes.
    pub fn new(producer: &'a ThreadedProducer<DefaultProducerContext>, topic: String) -> Self {
        Self {
            producer,
            topic,
            buffer: Vec::new(),
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
        if self.buffer.len() + s.len() > MAX_LEN {
            self.flush();
        }
        self.buffer.extend_from_slice(&s[..]);
    }

    pub fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let record: BaseRecord<(), _> = BaseRecord::to(&self.topic).payload(&self.buffer);
            self.producer.send(record).unwrap();
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

/// Feeds Nexmark input data into Kafka topics.
///
/// This program generates Nexmark events and feeds them into Kafka topics named
/// `person`, `auction`, and `bid`.  By default, it writes 100,000,000 events,
/// which can easily be too many, so consider specifying `--max-events`.
/// Specify `-O bootstrap.servers=<broker>` to use a broker other than
/// `localhost:9092`.
///
/// If the topics that this writes already exist, this will append to them.  To
/// ensure that the topics contain only the generated output, delete them first,
/// e.g. with a command like `rpk topic delete person auction bid`.
///
/// To verify that the expected number of events was produced, take the sum of
/// the number of lines in the produced topics, e.g.:
///
/// for topic in person bid auction; do rpk topic consume -f '%v' -o :end $topic; done | wc -l
#[derive(Parser)]
#[clap(name = "generate")]
struct Options {
    #[clap(flatten)]
    generator_options: GeneratorOptions,

    /// Set a Kafka client option, e.g. `-O key=value`.
    #[clap(short = 'O')]
    kafka_options: Vec<String>,

    /// Specify a prefix to add to each of the topic names.
    #[clap(long, default_value = "")]
    topic_prefix: String,

    /// Specify a suffix to add to each of the topic names.
    #[clap(long, default_value = "")]
    topic_suffix: String,

    /// Topic for producing person events (plus the prefix and suffix, if any).
    #[clap(long, default_value = "person")]
    person_topic: String,

    /// Topic for producing auction events (plus the prefix and suffix, if any).
    #[clap(long, default_value = "auction")]
    auction_topic: String,

    /// Topic for producing bid events (plus the prefix and suffix, if any).
    #[clap(long, default_value = "bid")]
    bid_topic: String,

    /// Disable progress bar.
    #[clap(long = "no-progress", default_value_t = true, action = clap::ArgAction::SetFalse)]
    pub progress: bool,
}

impl Options {
    fn topic(&self, infix: &str) -> String {
        format!("{}{}{}", &self.topic_prefix, infix, &self.topic_suffix)
    }
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let options = Options::parse();

    let progress_bar = if options.progress && options.generator_options.max_events > 0 {
        ProgressBar::new(options.generator_options.max_events)
    } else {
        ProgressBar::hidden()
    };

    let mut source = NexmarkSource::new(options.generator_options.clone());
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", "localhost:9092");
    for option in &options.kafka_options {
        let (key, value) = option
            .split_once('=')
            .expect(&format!("{option}: expected '=' in argument"));
        client_config.set(key, value);
    }
    let producer = ThreadedProducer::from_config(&client_config).unwrap();
    let mut persons = BufferedTopic::new(&producer, options.topic(&options.person_topic));
    let mut auctions = BufferedTopic::new(&producer, options.topic(&options.auction_topic));
    let mut bids = BufferedTopic::new(&producer, options.topic(&options.bid_topic));
    for event in &mut source {
        match event {
            dbsp_nexmark::model::Event::Person(person) => persons.write(person),
            dbsp_nexmark::model::Event::Auction(auction) => auctions.write(auction),
            dbsp_nexmark::model::Event::Bid(bid) => bids.write(bid),
        }
        progress_bar.inc(1);
    }
}
