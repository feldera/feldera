use circular_queue::CircularQueue;
use futures::executor::block_on;
use log::{debug, info, warn};
use mockd::datetime::date_range;
use rand::{random, thread_rng, Rng};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::{FromClientConfig, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer},
    producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer},
    util::Timeout,
    ClientConfig, Offset, TopicPartitionList,
};
use serde::Serialize;
use serde_json::json;
use std::{
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

static TOPIC_VULNERABILITY: &str = "secops_vulnerability";
static TOPIC_PIPELINE: &str = "secops_pipeline";
static TOPIC_PIPELINE_SOURCES: &str = "secops_pipeline_sources";
static TOPIC_ARTIFACT: &str = "secops_artifact";
static TOPIC_CLUSTER: &str = "secops_cluster";
static TOPIC_K8SOBJECT: &str = "secops_k8sobject";

static NUM_REPOSITORIES: u64 = 1000u64;
static NUM_COMMITS_PER_REPO: u64 = 100u64;
static NUM_VULNERABILITIES: u64 = 100u64;
static NUM_SOURCES_PER_PIPELINE: u64 = 4;
static NUM_CLUSTERS: u64 = 10;

// The generator will try to stay ahead of the consumer
// by at least this many messages.
static GENERATOR_THRESHOLD: i64 = 1000;

// Frequency of polling for consumer offset.
static CONSUMER_LAG_POLL_PERIOD: Duration = Duration::from_millis(3000);

pub struct KafkaResources {}

/// An object that creates Kafka topics on startup and deletes them
/// on drop.  Helps make sure that test runs don't leave garbage behind.
impl KafkaResources {
    pub fn create_topics(topics: &[(&str, i32, &str)]) -> Self {
        let mut client_config = ClientConfig::new();
        client_config
            .set(
                "bootstrap.servers",
                std::env::var("REDPANDA_BROKERS").unwrap_or("localhost".to_string()),
            )
            .set_log_level(RDKafkaLogLevel::Debug);
        let admin_client = AdminClient::from_config(&client_config).unwrap();

        let new_topics = topics
            .iter()
            .map(|(topic_name, partitions, retention_bytes)| {
                NewTopic::new(topic_name, *partitions, TopicReplication::Fixed(1))
                    .set("retention.bytes", retention_bytes)
            })
            .collect::<Vec<_>>();
        let topic_names = topics
            .iter()
            .map(|(topic_name, _partitions, _retention_bytes)| &**topic_name)
            .collect::<Vec<_>>();

        // Delete topics if they exist from previous runs.
        let _ = block_on(admin_client.delete_topics(&topic_names, &AdminOptions::new()));

        block_on(admin_client.create_topics(&new_topics, &AdminOptions::new())).unwrap();

        Self {}
    }
}

#[derive(Clone)]
pub struct KafkaProducer {
    producer: Arc<ThreadedProducer<DefaultProducerContext>>,
}

impl Default for KafkaProducer {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaProducer {
    pub fn new() -> Self {
        let mut producer_config = ClientConfig::new();
        producer_config
            .set(
                "bootstrap.servers",
                std::env::var("REDPANDA_BROKERS").unwrap_or("localhost".to_string()),
            )
            .set("message.timeout.ms", "0") // infinite timeout
            .set_log_level(RDKafkaLogLevel::Debug);
        let producer = ThreadedProducer::from_config(&producer_config).unwrap();

        Self {
            producer: Arc::new(producer),
        }
    }

    pub fn insert_into_topic<T, I>(&self, topic: &str, data: I, updates_per_message: usize)
    where
        T: Serialize + Clone,
        I: IntoIterator<Item = T>,
    {
        self.send_to_topic(
            topic,
            data.into_iter().map(|x| (x, true)),
            updates_per_message,
        )
    }

    pub fn send_to_topic<T, I>(&self, topic: &str, data: I, updates_per_message: usize)
    where
        T: Serialize + Clone,
        I: IntoIterator<Item = (T, bool)>,
    {
        let mut bytes = Vec::new();
        let mut updates = 0;
        for (val, insert) in data.into_iter() {
            let json = if insert {
                json!({"insert": val.clone()})
            } else {
                json!({"delete": val.clone()})
            };
            serde_json::to_writer(&mut bytes, &json).unwrap();
            updates += 1;
            if updates >= updates_per_message {
                let record = <BaseRecord<(), [u8], ()>>::to(topic).payload(&bytes);
                self.producer.send(record).unwrap();
                bytes.clear();
                updates = 0;
            } else {
                bytes.push(b'\n');
            }
        }

        if updates > 0 {
            let record = <BaseRecord<(), [u8], ()>>::to(topic).payload(&bytes);
            self.producer.send(record).unwrap();
        }
        self.producer.flush(Timeout::Never).unwrap();
    }

    pub fn send_string(&self, string: &str, topic: &str) {
        let record = <BaseRecord<(), str, ()>>::to(topic).payload(string);
        self.producer.send(record).unwrap();
        self.producer.flush(Timeout::Never).unwrap();
    }
}

pub struct CircularProducer<T> {
    topic: String,
    chunk_size: usize,
    producer: KafkaProducer,
    buffer: CircularQueue<T>,
}

impl<T> CircularProducer<T>
where
    T: Clone + Serialize,
{
    pub fn new(topic: &str, chunk_size: usize, capacity: usize, producer: KafkaProducer) -> Self {
        Self {
            topic: topic.to_string(),
            chunk_size,
            producer,
            buffer: CircularQueue::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, data: &[T]) {
        let mut updates = Vec::with_capacity(data.len() * 2);

        for val in data.iter() {
            if let Some(popped) = self.buffer.push(val.clone()) {
                updates.push((popped, false));
            }
            updates.push((val.clone(), true));
        }

        self.producer
            .send_to_topic(&self.topic, updates, self.chunk_size);
    }
}

fn random_date() -> String {
    date_range(
        "2022-04-23T19:30:12Z".to_string(),
        "2023-03-12T19:30:12Z".to_string(),
    )
    .naive_utc()
    .to_string()
}

fn random_checksum() -> (String, String) {
    if random::<bool>() {
        ("SHA256".to_string(), format!("{:x}", random::<u128>()))
    } else {
        (
            "SHA512".to_string(),
            format!("{:x}{:x}", random::<u128>(), random::<u128>()),
        )
    }
}

fn random_repo() -> u64 {
    thread_rng().gen_range(0..NUM_REPOSITORIES)
}

fn random_commit() -> u64 {
    let repo = random_repo();
    (repo << 32) + thread_rng().gen_range(0..NUM_COMMITS_PER_REPO)
}

fn random_userid() -> u64 {
    random::<u16>() as u64
}

fn random_cve() -> String {
    let mut rng = thread_rng();

    format!(
        "CVE-{}-{}",
        rng.gen_range(2010..2022),
        rng.gen_range(1000..100000)
    )
}

fn random_severity() -> u16 {
    let mut rng = thread_rng();

    rng.gen_range(0..5)
}

fn random_priority() -> String {
    let mut rng = thread_rng();

    match rng.gen_range(0..2) {
        0 => "LOW".to_string(),
        1 => "MEDIUM".to_string(),
        _ => "HIGH".to_string(),
    }
}

fn random_clusterid() -> u64 {
    thread_rng().gen_range(0..NUM_CLUSTERS)
}

fn random_namespace() -> String {
    let namespaces = ["ns-gateway", "ns-frontend", "ns-backend"];

    namespaces[thread_rng().gen_range(0..namespaces.len())].to_string()
}

// create table repository (
//    repository_id bigint not null,
//    type varchar not null,
//    url varchar not null,
//    name varchar not null
//);
#[derive(Clone, Debug, Serialize)]
struct Repository {
    repository_id: u64,
    #[serde(rename = "type")]
    _type: String,
    url: String,
    name: String,
}

// create table git_commit (
//    git_commit_id bigint not null,
//    repository_id bigint not null foreign key references repository(repository_id),
//    commit_id varchar not null,
//    commit_date timestamp not null,
//    commit_owner varchar not null
//);
#[derive(Clone, Debug, Serialize)]
struct GitCommit {
    git_commit_id: u64,
    repository_id: u64,
    commit_id: String,
    commit_date: String,
    commit_owner: String,
}

// create table vulnerability (
//    vulnerability_id bigint not null,
//     discovery_date timestamp not null,
//     discovered_by varchar not null,
//     discovered_in bigint not null foreign key references git_commit(git_commit_id),
//     update_date timestamp,
//     updatedby_user_id bigint,
//     checksum varchar not null,
//     checksum_type varchar not null,
//     vulnerability_reference_id varchar not null,
//     severity int,
//     priority varchar
// );
#[derive(Clone, Debug, Serialize)]
struct Vulnerability {
    vulnerability_id: u64,
    discovery_date: String,
    discovered_by_user_id: u64,
    discovered_in: u64,
    update_date: String,
    updated_by_user_id: u64,
    checksum: String,
    checksum_type: String,
    vulnerability_reference_id: String,
    severity: u16,
    priority: String,
}

fn generate_vulnerabilities(num_vulnerabilities: u64) -> Vec<Vulnerability> {
    (0..num_vulnerabilities)
        .map(|vulnerability_id| {
            let (checksum_type, checksum) = random_checksum();
            Vulnerability {
                vulnerability_id,
                discovery_date: random_date(),
                discovered_by_user_id: random_userid(),
                discovered_in: random_commit(),
                update_date: random_date(),
                updated_by_user_id: random_userid(),
                checksum,
                checksum_type,
                vulnerability_reference_id: random_cve(),
                severity: random_severity(),
                priority: random_priority(),
            }
        })
        .collect()
}

// create table k8scluster (
//     k8scluster_id bigint not null,
//     k8s_uri varchar not null,
//     name varchar not null,
//     k8s_service_provider varchar not null
// );
#[derive(Clone, Debug, Serialize)]
struct K8sCluster {
    k8scluster_id: u64,
    k8s_uri: String,
    name: String,
    k8s_service_provider: String,
}

fn generate_clusters(num_clusters: u64) -> Vec<K8sCluster> {
    (0..num_clusters)
        .map(|k8scluster_id| {
            K8sCluster {
                k8scluster_id,
                k8s_uri: format!("company.com/cluster{k8scluster_id}"),
                name: format!("cluster{k8scluster_id}"),
                k8s_service_provider: "".to_string(), // TODO
            }
        })
        .collect()
}

// create table pipeline (
//    pipeline_id bigint not null,
//    create_date timestamp not null,
//    createdby_user_id bigint not null,
//    update_date timestamp,
//    updatedby_user_id bigint
// );
#[derive(Clone, Debug, Serialize)]
struct Pipeline {
    pipeline_id: u64,
    create_date: String,
    createdby_user_id: u64,
    update_date: String,
    updatedby_user_id: u64,
}

// create table pipeline_sources (
//     git_commit_id bigint not null /* foreign key references git_commit(git_commit_id) */,
//     pipeline_id bigint not null /* foreign key references pipeline(pipeline_id) */
// );
#[derive(Clone, Debug, Serialize)]
struct PipelineSource {
    git_commit_id: u64,
    pipeline_id: u64,
}

// create table artifact (
//     artifact_id bigint not null,
//     artifact_uri varchar not null,
//     create_date timestamp not null,
//     createdby_user_id bigint not null,
//     checksum varchar not null,
//     checksum_type varchar not null,
//     artifact_size_in_bytes bigint not null,
//     artifact_type varchar not null,
//     builtby_pipeline_id bigint not null /* foreign key references pipeline(pipeline_id) */,
//     parent_artifact_id bigint /* foreign key references artifact(artifact_id) */
// );
#[derive(Clone, Debug, Serialize)]
struct Artifact {
    artifact_id: u64,
    artifact_uri: String,
    create_date: String,
    createdby_user_id: u64,
    checksum: String,
    checksum_type: String,
    artifact_size_in_bytes: u64,
    artifact_type: String,
    builtby_pipeline_id: u64,
    parent_artifact_id: Option<u64>,
}

// create table k8sobject (
//    k8sobject_id bigint not null,
//    artifact_id bigint not null /* foreign key references artifact(artifact_id) */,
//    create_date timestamp not null,
//    createdby_user_id bigint not null,
//    update_date timestamp,
//    updatedby_user_id bigint,
//    checksum varchar not null,
//    checksum_type varchar not null,
//    deployed_id bigint not null /* foreign key references k8scluster(k8scluster_id) */,
//    deployment_type varchar not null,
//    k8snamespace varchar not null
// );
#[derive(Clone, Debug, Serialize)]
struct K8sObject {
    k8sobject_id: u64,
    artifact_id: u64,
    create_date: String,
    createdby_user_id: u64,
    update_date: Option<String>,
    updatedby_user_id: Option<u64>,
    checksum: String,
    checksum_type: String,
    deployed_id: u64,
    deployment_type: String,
    k8snamespace: String,
}

fn generate_pipelines(
    id_from: u64,
    id_to: u64,
) -> (
    Vec<Pipeline>,
    Vec<PipelineSource>,
    Vec<Artifact>,
    Vec<K8sObject>,
) {
    let mut pipelines = Vec::with_capacity((id_to - id_from) as usize);
    let mut sources = Vec::with_capacity(((id_to - id_from) * NUM_SOURCES_PER_PIPELINE) as usize);
    let mut artifacts = Vec::with_capacity(((id_to - id_from) * NUM_SOURCES_PER_PIPELINE) as usize);
    let mut k8sobjects = Vec::with_capacity(2 * (id_to - id_from) as usize);

    for pipeline_id in id_from..id_to {
        let userid = random_userid();
        let date = random_date();

        pipelines.push(Pipeline {
            pipeline_id,
            create_date: date.clone(),
            createdby_user_id: userid,
            update_date: random_date(),
            updatedby_user_id: random_userid(),
        });

        let (checksum_type, checksum) = random_checksum();
        let container_id = pipeline_id << 32;

        artifacts.push(Artifact {
            artifact_id: container_id,
            artifact_uri: format!("artifactory.com/container{pipeline_id}"),
            create_date: date.clone(),
            createdby_user_id: userid,
            checksum,
            checksum_type,
            artifact_size_in_bytes: thread_rng().gen_range(0..1_000_000_000),
            artifact_type: "CONTAINER".to_string(),
            builtby_pipeline_id: pipeline_id,
            parent_artifact_id: None,
        });

        for i in 0..2 {
            let (checksum_type, checksum) = random_checksum();

            k8sobjects.push(K8sObject {
                k8sobject_id: container_id + i,
                artifact_id: container_id,
                create_date: random_date(),
                createdby_user_id: random_userid(),
                update_date: None,
                updatedby_user_id: None,
                checksum,
                checksum_type,
                deployed_id: random_clusterid(),
                deployment_type: "".to_string(),
                k8snamespace: random_namespace(),
            });
        }

        for i in 0..NUM_SOURCES_PER_PIPELINE {
            sources.push(PipelineSource {
                git_commit_id: random_commit(),
                pipeline_id,
            });

            let artifact_id = (pipeline_id << 32) + i;
            let (checksum_type, checksum) = random_checksum();

            artifacts.push(Artifact {
                artifact_id,
                artifact_uri: format!("artifactory.com/binary{artifact_id}"),
                create_date: date.clone(),
                createdby_user_id: userid,
                checksum,
                checksum_type,
                artifact_size_in_bytes: thread_rng().gen_range(0..1_000_000_000),
                artifact_type: "BINARY".to_string(),
                builtby_pipeline_id: pipeline_id,
                parent_artifact_id: Some(container_id),
            });
        }
    }

    (pipelines, sources, artifacts, k8sobjects)
}

// Returns `true` if the consumer, if any, is catching up with the producer.
// Uses consumer offset in TOPIC_PIPELINE_SOURCES as indicator.
// Assumes ther consumer group id is also TOPIC_PIPELINE_SOURCES
// (the demo script must ensure this is the case).
fn consumer_catching_up() -> bool {
    let mut topics = TopicPartitionList::new();
    topics.add_partition(TOPIC_PIPELINE_SOURCES, 0);

    let mut client_config = ClientConfig::new();
    client_config
        .set(
            "bootstrap.servers",
            std::env::var("REDPANDA_BROKERS").unwrap_or("localhost".to_string()),
        )
        .set("group.id", TOPIC_PIPELINE_SOURCES)
        .set_log_level(RDKafkaLogLevel::Debug);
    let consumer = BaseConsumer::from_config(&client_config).unwrap();
    let offset = match consumer.committed_offsets(topics.clone(), Duration::from_millis(3000)) {
        Ok(offsets) => offsets.elements()[0].offset(),
        Err(e) => {
            warn!("Error retrieving offsets: {e}");
            return false;
        }
    };

    debug!("consumer offset: {offset:?}");

    let offset = match offset {
        Offset::Offset(offset) => offset,
        Offset::Beginning => 0,
        Offset::Invalid => 0,
        _ => panic!("Unexpected offset {offset:?}"),
    };

    let (_low, high) = consumer
        .fetch_watermarks(TOPIC_PIPELINE_SOURCES, 0, Duration::from_millis(3000))
        .unwrap();

    debug!("high watermark: {high}");

    high - offset < GENERATOR_THRESHOLD
}

fn main() {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();

    if args.len() > 2 {
        println!("Usage: secops_simulator [<num_pipelines>]");
        std::process::exit(1);
    }

    let num_pipelines = if args.len() == 2 {
        let num_pipelines: isize = args
            .get(1)
            .unwrap()
            .parse()
            .expect("Num pipelines should be an integer");
        if num_pipelines > 0 {
            Some(num_pipelines as u64)
        } else {
            None
        }
    } else {
        None
    };

    info!("Creating topics.");
    let _kafka_resources = KafkaResources::create_topics(&[
        (TOPIC_VULNERABILITY, 1, "-1"),
        (TOPIC_CLUSTER, 1, "-1"),
        (TOPIC_PIPELINE, 1, "536870912"),
        (TOPIC_PIPELINE_SOURCES, 1, "536870912"),
        (TOPIC_ARTIFACT, 1, "536870912"),
        (TOPIC_K8SOBJECT, 1, "536870912"),
    ]);

    let producer = KafkaProducer::new();

    info!("Generating repositories");

    info!("Generating vulnerabilities");
    let vulnerabilities = generate_vulnerabilities(NUM_VULNERABILITIES);
    producer.insert_into_topic(TOPIC_VULNERABILITY, vulnerabilities, 100);

    info!("Generating k8s clusters");
    let clusters = generate_clusters(NUM_CLUSTERS);
    producer.insert_into_topic(TOPIC_CLUSTER, clusters, 100);

    info!("Generating pipelines");
    let mut generated_pipelines = 0;

    let mut pipeline_producer =
        CircularProducer::new(TOPIC_PIPELINE, 100, 100_000, producer.clone());
    let mut pipeline_sources_producer = CircularProducer::new(
        TOPIC_PIPELINE_SOURCES,
        100,
        100_000 * NUM_SOURCES_PER_PIPELINE as usize,
        producer.clone(),
    );
    let mut artifacts_producer = CircularProducer::new(
        TOPIC_ARTIFACT,
        100,
        100_000 * (NUM_SOURCES_PER_PIPELINE + 1) as usize,
        producer.clone(),
    );
    let mut k8sobjects_producer =
        CircularProducer::new(TOPIC_K8SOBJECT, 100, 2 * 100_000, producer.clone());

    let mut last_poll = Instant::now();

    let mut pause = false;
    loop {
        let (pipelines, pipeline_sources, artifacts, k8sobjects) =
            generate_pipelines(generated_pipelines, generated_pipelines + 300);

        pipeline_producer.push(&pipelines);
        pipeline_sources_producer.push(&pipeline_sources);
        artifacts_producer.push(&artifacts);
        k8sobjects_producer.push(&k8sobjects);

        generated_pipelines += 300;

        if let Some(num_pipelines) = num_pipelines {
            if generated_pipelines >= num_pipelines {
                break;
            }
        }

        // Stop producing if the consumer is behind.  This makes sure that the
        // consumer doesn't write to Kafka continuously when the demo is not
        // running or paused.
        if last_poll.elapsed() >= CONSUMER_LAG_POLL_PERIOD && num_pipelines.is_none() {
            while !consumer_catching_up() {
                if !pause {
                    info!("Pausing the SecOps test data generator");
                    pause = true;
                }
                sleep(Duration::from_millis(1_000));
            }
            if pause {
                info!("Unpausing the SecOps test data generator");
                pause = false;
            }
            last_poll = Instant::now();
        }
    }
}
