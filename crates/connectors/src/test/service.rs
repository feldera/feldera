//! The services outside the test binary that suite cases use, and a probe that tells whether
//! a service answers.

use std::ffi::OsString;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::OnceLock;
use std::time::Duration;

use feldera_types::transport::kafka::default_redpanda_server;
use url::Url;

/// How long a probe waits for a service to take a connection.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// The port of a Kafka broker whose address names none.
const KAFKA_PORT: u16 = 9092;

/// A service outside the test binary that a suite case uses.
///
/// A case names its services with `#[engine_test(services(...))]`. On an engine that asks for
/// it ([`ConnectorTestEngine::skip_absent_services`]), the harness ignores a case whose service
/// does not answer. The probe looks for each service where its cases look: at the address in
/// an environment variable, or at the cases' default address.
///
/// [`ConnectorTestEngine::skip_absent_services`]: feldera_adapterlib::test::ConnectorTestEngine::skip_absent_services
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Service {
    /// A Kafka broker: one of the brokers in `REDPANDA_BROKERS`, by default `localhost:9092`.
    Kafka,
    /// A Postgres server at `POSTGRES_URL`, by default
    /// `postgres://postgres:password@localhost:5432`.
    Postgres,
    /// A Postgres server that takes TLS connections at `POSTGRES_SSL_URL`, by default
    /// `postgres://postgres:secret@localhost:5432/postgres`, whose CA certificate
    /// `POSTGRES_SSL_CA_PEM` or `POSTGRES_SSL_CA_LOCATION` holds. Without the certificate the
    /// service counts as absent.
    PostgresTls,
    /// DynamoDB Local at `DYNAMODB_ENDPOINT`, by default `http://127.0.0.1:8000`.
    DynamoDbLocal,
    /// The `nats-server` binary on `PATH`. The cases start their own servers.
    Nats,
    /// A Redis server at `REDIS_URL`, by default `redis://localhost:6379/0`.
    Redis,
    /// An S3 server, such as MinIO, at `FELDERA_ICEBERG_S3_ENDPOINT`, by default
    /// `http://localhost:9000`.
    S3,
    /// The Pub/Sub emulator at `PUBSUB_EMULATOR_HOST`, by default `127.0.0.1:8685`.
    PubSubEmulator,
}

impl Service {
    /// Every service, in the order of declaration.
    pub const ALL: [Service; 8] = [
        Service::Kafka,
        Service::Postgres,
        Service::PostgresTls,
        Service::DynamoDbLocal,
        Service::Nats,
        Service::Redis,
        Service::S3,
        Service::PubSubEmulator,
    ];

    /// Returns where the cases find the service: its address, or for [`Service::Nats`], the
    /// name of the binary.
    pub fn address(self) -> String {
        match self {
            Service::Kafka => default_redpanda_server(),
            Service::Postgres => env_or(
                "POSTGRES_URL",
                "postgres://postgres:password@localhost:5432",
            ),
            Service::PostgresTls => env_or(
                "POSTGRES_SSL_URL",
                "postgres://postgres:secret@localhost:5432/postgres",
            ),
            Service::DynamoDbLocal => std::env::var("DYNAMODB_ENDPOINT")
                .ok()
                .filter(|endpoint| !endpoint.trim().is_empty())
                .unwrap_or_else(|| "http://127.0.0.1:8000".to_string()),
            Service::Nats => "nats-server".to_string(),
            Service::Redis => env_or("REDIS_URL", "redis://localhost:6379/0"),
            Service::S3 => env_or("FELDERA_ICEBERG_S3_ENDPOINT", "http://localhost:9000"),
            Service::PubSubEmulator => env_or("PUBSUB_EMULATOR_HOST", "127.0.0.1:8685"),
        }
    }

    /// Returns `true` if the service answers. A process probes each service once.
    pub fn is_present(self) -> bool {
        static PRESENT: [OnceLock<bool>; Service::ALL.len()] =
            [const { OnceLock::new() }; Service::ALL.len()];
        *PRESENT[self as usize].get_or_init(|| self.probe())
    }

    /// Probes the service at its [`Service::address`].
    fn probe(self) -> bool {
        let address = self.address();
        match self {
            Service::Kafka => address
                .split(',')
                .any(|broker| answers(&with_port(broker.trim(), KAFKA_PORT))),
            Service::Nats => on_path(&address, std::env::var_os("PATH")),
            Service::PubSubEmulator => answers(&address),
            Service::PostgresTls => {
                names_a_ca(|name| std::env::var_os(name))
                    && url_endpoint(&address).is_some_and(|endpoint| answers(&endpoint))
            }
            Service::Postgres | Service::DynamoDbLocal | Service::Redis | Service::S3 => {
                url_endpoint(&address).is_some_and(|endpoint| answers(&endpoint))
            }
        }
    }
}

/// Returns `true` if `var`, a lookup of environment variables, names the CA certificate of a
/// TLS-enabled Postgres server, which the cases that connect over TLS need.
fn names_a_ca(var: impl Fn(&str) -> Option<OsString>) -> bool {
    ["POSTGRES_SSL_CA_PEM", "POSTGRES_SSL_CA_LOCATION"]
        .into_iter()
        .any(|name| var(name).is_some_and(|value| !value.is_empty()))
}

/// Returns the value of the environment variable `name`, or `default` if it is not set.
fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Returns `address` with `port` if it names no port.
fn with_port(address: &str, port: u16) -> String {
    match address.rsplit_once(':') {
        Some((_, named)) if named.parse::<u16>().is_ok() => address.to_string(),
        _ => format!("{address}:{port}"),
    }
}

/// Returns the `host:port` that `url` names, with the usual port of its scheme if it names
/// none.
fn url_endpoint(url: &str) -> Option<String> {
    let url = Url::parse(url).ok()?;
    let host = url.host_str()?;
    let port = url.port_or_known_default().or(match url.scheme() {
        "postgres" | "postgresql" => Some(5432),
        "redis" | "rediss" => Some(6379),
        _ => None,
    })?;
    Some(format!("{host}:{port}"))
}

/// Returns `true` if a server takes a TCP connection at `endpoint`, a `host:port`.
fn answers(endpoint: &str) -> bool {
    endpoint.to_socket_addrs().is_ok_and(|mut addresses| {
        addresses.any(|address| TcpStream::connect_timeout(&address, PROBE_TIMEOUT).is_ok())
    })
}

/// Returns `true` if a directory in `path`, a value of `PATH`, holds a file named `binary`.
fn on_path(binary: &str, path: Option<OsString>) -> bool {
    path.is_some_and(|path| {
        std::env::split_paths(&path).any(|directory| directory.join(binary).is_file())
    })
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::net::TcpListener;
    use std::path::Path;

    use super::{Service, answers, names_a_ca, on_path, url_endpoint, with_port};

    /// A probe finds a server that listens, and no server where none listens.
    #[test]
    fn a_probe_finds_a_listening_server() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        assert!(answers(&endpoint));
        drop(listener);
        assert!(!answers(&endpoint));
        // An address without a port names no server.
        assert!(!answers("localhost"));
    }

    /// A broker without a port gets the Kafka port, and a broker with one keeps it.
    #[test]
    fn a_broker_without_a_port_gets_the_kafka_port() {
        assert_eq!(with_port("localhost", 9092), "localhost:9092");
        assert_eq!(with_port("localhost:19092", 9092), "localhost:19092");
        assert_eq!(with_port("kafka.example", 9092), "kafka.example:9092");
    }

    /// A URL gives its host and port, or the usual port of its scheme.
    #[test]
    fn a_url_gives_its_endpoint() {
        assert_eq!(
            url_endpoint("postgres://postgres:password@localhost:5432").as_deref(),
            Some("localhost:5432")
        );
        assert_eq!(
            url_endpoint("postgres://db.example/postgres").as_deref(),
            Some("db.example:5432")
        );
        assert_eq!(
            url_endpoint("redis://localhost/0").as_deref(),
            Some("localhost:6379")
        );
        assert_eq!(
            url_endpoint("http://127.0.0.1:8000").as_deref(),
            Some("127.0.0.1:8000")
        );
        assert_eq!(url_endpoint("http://minio").as_deref(), Some("minio:80"));
        assert_eq!(url_endpoint("unknown://host"), None);
        assert_eq!(url_endpoint("not a url"), None);
    }

    /// A binary is on `PATH` when one of its directories holds it.
    #[test]
    fn a_binary_on_the_path_is_found() {
        let with = tempfile::tempdir().unwrap();
        let without = tempfile::tempdir().unwrap();
        File::create(with.path().join("nats-server")).unwrap();
        let path = |directories: &[&Path]| Some(std::env::join_paths(directories).unwrap());
        assert!(on_path("nats-server", path(&[without.path(), with.path()])));
        assert!(!on_path("nats-server", path(&[without.path()])));
        assert!(!on_path("nats-server", None));
    }

    /// A TLS-enabled Postgres server needs a CA certificate, in either variable.
    #[test]
    fn a_tls_postgres_server_needs_a_ca() {
        let only = |set: &'static str, value: &'static str| {
            move |name: &str| (name == set).then(|| value.into())
        };
        assert!(names_a_ca(only("POSTGRES_SSL_CA_PEM", "-----BEGIN")));
        assert!(names_a_ca(only("POSTGRES_SSL_CA_LOCATION", "/ca.pem")));
        assert!(!names_a_ca(only("POSTGRES_SSL_CA_LOCATION", "")));
        assert!(!names_a_ca(only("POSTGRES_SSL_URL", "postgres://host")));
    }

    /// `Service::ALL` lists each service once, in the order of declaration, which the cache
    /// of `Service::is_present` relies on.
    #[test]
    fn all_lists_each_service_in_order() {
        for (index, service) in Service::ALL.into_iter().enumerate() {
            assert_eq!(service as usize, index, "{service:?}");
        }
    }
}
