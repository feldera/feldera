use std::error::Error;

use clap::Parser;
use rdkafka::{
    admin::{AdminClient, AdminOptions},
    config::FromClientConfig,
    producer::ThreadedProducer,
    ClientConfig,
};
use tiktok_gen::{buffered_topic::BufferedTopic, interactions::InteractionGenerator};

#[derive(Debug, Parser)]
struct GeneratorOptions {
    users: u32,
    videos: u32,
    interactions: u32,
    historical: bool,
}

#[derive(Parser)]
#[clap(name = "tiktok_gen")]
struct Options {
    /// Set the number of users to be generated.
    #[clap(short = 'U', long, default_value = "1000")]
    users: u32,

    /// Set the number of videos to be generated.
    #[clap(short = 'V', long, default_value = "1000")]
    videos: u32,

    /// Set the number of interactions to be generated.
    #[clap(short = 'I', long, default_value = "50000000")]
    interactions: u32,

    /// Set 'True` to generate historical data.
    #[clap(short = 'H', long, default_value = "false")]
    historical: bool,

    /// Set a Kafka client option, e.g. `-O key=value`.
    #[clap(short = 'O', long)]
    kafka_options: Vec<String>,

    /// Specify a prefix to add to each of the topic names.
    #[clap(long, default_value = "")]
    topic_prefix: String,

    /// Specify a suffix to add to each of the topic names.
    #[clap(long, default_value = "")]
    topic_suffix: String,

    /// Topic for producing interaction events (plus the prefix and suffix, if any).
    #[clap(long, default_value = "interactions")]
    interactions_topic: String,

    /// Set `True` to delete the topic if it already exists.
    #[clap(long, default_value = "false")]
    delete_topic_if_exists: bool,

    /// The Kafka bootstrap.servers.
    #[clap(short = 'B', long, default_value = "localhost:9092")]
    bootstrap_servers: String,

    /// Kafka broker message size.  Messages will ordinarily be `SIZE` bytes or
    /// less, but if `SIZE` is less than the length of an individual record,
    /// then each record will be sent in its own message.
    ///
    /// `SIZE` shouldn't be increased beyond 1_000_000, which is the default
    /// maximum in a couple of places in the Kafka broker that has to be
    /// increased in multiple places (see
    /// https://stackoverflow.com/questions/21020347/how-can-i-send-large-messages-with-kafka-over-15mb).
    #[clap(long, default_value_t = 512 * 1024, value_name = "SIZE")]
    record_size: usize,
}

impl Options {
    fn topic(&self, infix: &str) -> String {
        format!("{}{}{}", &self.topic_prefix, infix, &self.topic_suffix)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let options = Options::parse();

    let start = std::time::Instant::now();

    let interactions = InteractionGenerator::default()
        .max_users(options.users)
        .max_videos(options.videos)
        .max_interactions(options.interactions)
        .start_date(if options.historical {
            chrono::Utc::now() - chrono::TimeDelta::days(630)
        } else {
            chrono::Utc::now()
        })
        .generate();

    let took = start.elapsed();

    println!("Sample interactions:\n{:#?}", interactions.get(0..10));

    println!("generating events took: {}s", took.as_secs());

    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", &options.bootstrap_servers);
    for option in &options.kafka_options {
        let (key, value) = option
            .split_once('=')
            .unwrap_or_else(|| panic!("{option}: expected '=' in argument"));
        client_config.set(key, value);
    }

    let topic = options.topic(&options.interactions_topic);

    let admin_client = AdminClient::from_config(&client_config)?;

    if options.delete_topic_if_exists {
        _ = admin_client.delete_topics(&[&topic], &AdminOptions::new());
    }

    let producer = ThreadedProducer::from_config(&client_config).unwrap();
    let mut interactions_topic = BufferedTopic::new(&producer, topic, options.record_size);

    for event in interactions {
        interactions_topic.write(event);
    }

    Ok(())
}
