use csv::WriterBuilder as CsvWriterBuilder;
use futures::executor::block_on;
use mockd::datetime::date_range;
use rand::{random, thread_rng, Rng};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::{FromClientConfig, RDKafkaLogLevel},
    producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer},
    util::Timeout,
    ClientConfig,
};
use serde::Serialize;

static TOPIC_REPOSITORY: &str = "secops_repository";
static TOPIC_GIT_COMMIT: &str = "secops_git_commit";
static TOPIC_VULNERABILITY: &str = "secops_vulnerability";
static TOPIC_PIPELINE: &str = "secops_pipeline";
static TOPIC_PIPELINE_SOURCES: &str = "secops_pipeline_sources";
static TOPIC_ARTIFACT: &str = "secops_artifact";
static TOPIC_CLUSTER: &str = "secops_cluster";
static TOPIC_K8SOBJECT: &str = "secops_k8sobject";

static NUM_REPOSITORIES: u64 = 1000u64;
static NUM_COMMITS_PER_REPO: u64 = 1000u64;
static NUM_VULNERABILITIES: u64 = 100u64;
static NUM_PIPELINES: u64 = 2_000_000;
static NUM_SOURCES_PER_PIPELINE: u64 = 10;
static NUM_CLUSTERS: u64 = 10;

pub struct KafkaResources {
    admin_client: AdminClient<DefaultClientContext>,
    topics: Vec<String>,
}

/// An object that creates Kafka topics on startup and deletes them
/// on drop.  Helps make sure that test runs don't leave garbage behind.
impl KafkaResources {
    pub fn create_topics(topics: &[(&str, i32)]) -> Self {
        let mut admin_config = ClientConfig::new();
        admin_config
            .set(
                "bootstrap.servers",
                std::env::var("REDPANDA_BROKERS").unwrap_or("localhost".to_string()),
            )
            .set_log_level(RDKafkaLogLevel::Debug);
        let admin_client = AdminClient::from_config(&admin_config).unwrap();

        let new_topics = topics
            .iter()
            .map(|(topic_name, partitions)| {
                NewTopic::new(topic_name, *partitions, TopicReplication::Fixed(1))
            })
            .collect::<Vec<_>>();
        let topic_names = topics
            .iter()
            .map(|(topic_name, _partitions)| &**topic_name)
            .collect::<Vec<_>>();

        // Delete topics if they exist from previous runs.
        let _ = block_on(admin_client.delete_topics(&topic_names, &AdminOptions::new()));

        block_on(admin_client.create_topics(&new_topics, &AdminOptions::new())).unwrap();

        Self {
            admin_client,
            topics: topics
                .iter()
                .map(|(topic_name, _)| topic_name.to_string())
                .collect::<Vec<_>>(),
        }
    }
}

pub struct KafkaProducer {
    producer: ThreadedProducer<DefaultProducerContext>,
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

        Self { producer }
    }

    pub fn send_to_topic<T>(&self, topic: &str, data: &[Vec<T>])
    where
        T: Serialize + Clone,
    {
        for batch in data {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::with_capacity(batch.len() * 32));

            for val in batch.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();
            let bytes = writer.into_inner().unwrap();

            let record = <BaseRecord<(), [u8], ()>>::to(topic).payload(&bytes);
            self.producer.send(record).unwrap();
        }
        self.producer.flush(Timeout::Never).unwrap();
        // println!("Data written to '{topic}'");
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

fn generate_repositories(num_repos: u64) -> Vec<Repository> {
    (0..num_repos)
        .map(|repository_id| Repository {
            repository_id: repository_id,
            _type: if random::<bool>() {
                "public".to_string()
            } else {
                "private".to_string()
            },
            url: format!("https://github.com/myorg/repo{repository_id}"),
            name: format!("repo{repository_id}"),
        })
        .collect()
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

fn generate_commits(repository_id: u64, num_commits: u64) -> Vec<GitCommit> {
    (0..num_commits)
        .map(|commit| GitCommit {
            git_commit_id: (repository_id << 32) + commit,
            repository_id,
            commit_id: format!("{:x}", random::<u128>()),
            commit_date: random_date(),
            commit_owner: format!("user{}", random::<u16>()),
        })
        .collect()
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

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() > 2 {
        println!("Usage: secops_simulator <num_pipelines>");
        std::process::exit(1);
    }
    let num_pipelines = if args.len() == 2 {
        args.get(1)
            .unwrap()
            .parse()
            .expect("Num pipelines should be an integer")
    } else {
        NUM_PIPELINES
    };

    println!("Creating topics. Num piplines: {}", num_pipelines);
    let _kafka_resources = KafkaResources::create_topics(&[
        (TOPIC_REPOSITORY, 1),
        (TOPIC_GIT_COMMIT, 1),
        (TOPIC_VULNERABILITY, 1),
        (TOPIC_CLUSTER, 1),
        (TOPIC_PIPELINE, 1),
        (TOPIC_PIPELINE_SOURCES, 1),
        (TOPIC_ARTIFACT, 1),
        (TOPIC_K8SOBJECT, 1),
    ]);

    let producer = KafkaProducer::new();

    println!("Generating repositories");
    let repositories = generate_repositories(NUM_REPOSITORIES);
    println!("repos: {repositories:#?}");
    producer.send_to_topic(TOPIC_REPOSITORY, &[repositories]);

    println!("Generating commits");
    for repo_id in 0..NUM_REPOSITORIES {
        let commits = generate_commits(repo_id, NUM_COMMITS_PER_REPO);
        // println!("commits: {commits:#?}");
        producer.send_to_topic(TOPIC_GIT_COMMIT, &[commits]);
    }

    println!("Generating vulnerabilities");
    let vulnerabilities = generate_vulnerabilities(NUM_VULNERABILITIES);
    // println!("vulnerabilities: {vulnerabilities:#?}");
    producer.send_to_topic(TOPIC_VULNERABILITY, &[vulnerabilities]);

    println!("Generating k8s clusters");
    let clusters = generate_clusters(NUM_CLUSTERS);
    // println!("clusters: {clusters:#?}");
    producer.send_to_topic(TOPIC_CLUSTER, &[clusters]);

    println!("Generating pipelines");
    let mut generated_pipelines = 0;
    while generated_pipelines < num_pipelines {
        let (pipelines, pipeline_sources, artifacts, k8sobjects) =
            generate_pipelines(generated_pipelines, generated_pipelines + 300);

        // println!("pipelines: {pipelines:#?}");
        // println!("pipeline_sources: {pipeline_sources:#?}");
        producer.send_to_topic(TOPIC_PIPELINE, &[pipelines]);
        producer.send_to_topic(TOPIC_PIPELINE_SOURCES, &[pipeline_sources]);
        producer.send_to_topic(TOPIC_ARTIFACT, &[artifacts]);
        producer.send_to_topic(TOPIC_K8SOBJECT, &[k8sobjects]);

        generated_pipelines += 300;
    }
}
