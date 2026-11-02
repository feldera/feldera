//! Calls from one host of a multihost pipeline to another.

use std::time::Duration;

use anyhow::{Context, Result as AnyResult, anyhow, bail};
use feldera_mtls::PipelinePeerTls;
use feldera_types::coordination::OutputPartReport;
use reqwest::{Client, StatusCode, Url};
use tracing::warn;

/// How long [PeerClient::report_output] waits before its first retry.  Each later retry waits
/// twice as long as the one before, up to [MAX_REPORT_RETRY_DELAY].
const FIRST_RETRY_DELAY: Duration = Duration::from_millis(200);

/// The longest delay between two tries of [PeerClient::report_output].
const MAX_REPORT_RETRY_DELAY: Duration = Duration::from_secs(5);

/// How long a connection to another host may take to open.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A client for the `/coordination` endpoints of the other hosts of a multihost pipeline.
#[derive(Clone)]
pub struct PeerClient {
    client: Client,

    /// The base URL of each host, in host order.
    host_urls: Vec<Url>,

    /// How long [Self::report_output] waits before its first retry.
    first_retry_delay: Duration,
}

impl PeerClient {
    /// Returns a client for the hosts at `host_urls`, in host order.  With
    /// `tls`, the client uses the pipeline's peer TLS configuration, which
    /// presents this host's pipeline certificate and admits only hosts of the
    /// same pipeline.  Otherwise, with `root_certificate`, the client trusts
    /// HTTPS certificates that chain to it, as the hosts' certificates do.
    pub fn new(
        host_urls: &[String],
        tls: Option<&PipelinePeerTls>,
        root_certificate: Option<reqwest::Certificate>,
    ) -> AnyResult<Self> {
        let host_urls = host_urls
            .iter()
            .map(|url| Url::parse(url).with_context(|| format!("invalid host URL {url:?}")))
            .collect::<AnyResult<Vec<_>>>()?;
        let mut builder = Client::builder().connect_timeout(CONNECT_TIMEOUT);
        if let Some(tls) = tls {
            builder = builder.use_preconfigured_tls((*tls.client).clone());
        } else if let Some(root_certificate) = root_certificate {
            builder = builder.add_root_certificate(root_certificate);
        }
        Ok(Self {
            client: builder.build()?,
            host_urls,
            first_retry_delay: FIRST_RETRY_DELAY,
        })
    }

    /// Reports a part of a distributed output connector to its leader on `host`, and returns once
    /// the leader has committed the report's step.
    ///
    /// The report is sent again, after a delay that grows to [MAX_REPORT_RETRY_DELAY], until the
    /// leader answers, because the leader might not be ready yet and a repeated report does no
    /// harm.  If the leader's commit failed, this returns that error.  The caller stops it at
    /// shutdown.
    pub async fn report_output(&self, host: usize, report: &OutputPartReport) -> AnyResult<()> {
        let url = self.host_urls[host].join("coordination/output/commit")?;
        let mut delay = self.first_retry_delay;
        let mut attempts = 0u64;
        loop {
            let error = match self
                .client
                .post(url.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(report)?)
                .send()
                .await
            {
                Ok(response) if response.status() == StatusCode::OK => return Ok(()),
                Ok(response) if response.status() == StatusCode::CONFLICT => {
                    let body = response.text().await.unwrap_or_default();
                    bail!(
                        "the leader on host {host} failed to commit step {}: {body}",
                        report.step
                    );
                }
                // The leader would refuse the same report again, for example as too large.
                Ok(response)
                    if response.status().is_client_error()
                        && !matches!(
                            response.status(),
                            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
                        ) =>
                {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    bail!(
                        "the leader on host {host} refused the report of step {}: {status}: {body}",
                        report.step
                    );
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    anyhow!("{status}: {body}")
                }
                Err(error) => anyhow!(error),
            };
            attempts += 1;
            if attempts.is_power_of_two() {
                warn!(
                    "reporting step {} of output connector {:?} to its leader on host {host} failed {attempts} times, most recently with: {error:#}",
                    report.step, report.endpoint
                );
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_REPORT_RETRY_DELAY);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    /// Starts a server that answers its `n`th request with `statuses[n]`, or
    /// 200 OK after them, and returns its URL and the number of requests it
    /// got.
    async fn status_server(statuses: Vec<u16>) -> (String, Arc<Mutex<usize>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(0));
        tokio::spawn({
            let requests = requests.clone();
            async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0; 4096];
                    let (header_end, length) = loop {
                        let n = stream.read(&mut buffer).await.unwrap();
                        request.extend_from_slice(&buffer[..n]);
                        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&request[..end]).to_lowercase();
                            let length = headers
                                .lines()
                                .find_map(|line| line.strip_prefix("content-length: "))
                                .map_or(0, |n| n.trim().parse().unwrap());
                            break (end + 4, length);
                        }
                    };
                    while request.len() < header_end + length {
                        let n = stream.read(&mut buffer).await.unwrap();
                        request.extend_from_slice(&buffer[..n]);
                    }
                    let status = {
                        let mut requests = requests.lock().unwrap();
                        *requests += 1;
                        statuses.get(*requests - 1).copied().unwrap_or(200)
                    };
                    let header = format!(
                        "HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    );
                    stream.write_all(header.as_bytes()).await.unwrap();
                    stream.shutdown().await.unwrap();
                }
            }
        });
        (url, requests)
    }

    /// A part's report of step 4.
    fn step_4() -> feldera_types::coordination::OutputPartReport {
        feldera_types::coordination::OutputPartReport {
            endpoint: "spread".into(),
            host: 1,
            step: 4,
            payload: serde_json::json!([]),
        }
    }

    /// Reports step 4 to the leader at `url`, as a part does.
    async fn report(url: &str) -> anyhow::Result<()> {
        let mut client = super::PeerClient::new(&[url.to_string()], None, None).unwrap();
        client.first_retry_delay = Duration::from_millis(1);
        tokio::time::timeout(Duration::from_secs(10), client.report_output(0, &step_4()))
            .await
            .expect("the report should finish")
    }

    /// A part retries its report while the leader is not ready, and stops at
    /// once when the leader says that its commit failed.
    #[tokio::test]
    async fn report_retries_until_the_leader_answers() {
        let (url, requests) = status_server(vec![503, 503]).await;
        report(&url).await.unwrap();
        assert_eq!(*requests.lock().unwrap(), 3);

        let (url, requests) = status_server(vec![409]).await;
        assert!(report(&url).await.is_err());
        assert_eq!(*requests.lock().unwrap(), 1);
    }

    /// A part does not retry a report that the leader refuses as malformed or too large, because
    /// the same report would be refused again.  It still retries a leader that is busy.
    #[tokio::test]
    async fn report_refused_by_the_leader_is_not_retried() {
        for status in [400, 404, 413] {
            let (url, requests) = status_server(vec![status]).await;
            assert!(report(&url).await.is_err(), "{status}");
            assert_eq!(*requests.lock().unwrap(), 1, "{status}");
        }
        let (url, requests) = status_server(vec![429, 408]).await;
        report(&url).await.unwrap();
        assert_eq!(*requests.lock().unwrap(), 3);
    }

    /// Without client certificates, a host reaches another host's HTTPS server
    /// by trusting the root CA of the pipeline certificate chain, which is not
    /// one of the public roots.
    #[actix_web::test]
    async fn https_report_trusts_the_pipeline_root() {
        use actix_web::{App, HttpResponse, HttpServer, web};
        use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut ca_params = rcgen::CertificateParams::default();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_pem = ca_params.clone().self_signed(&ca_key).unwrap().pem();
        let mut leaf_params = rcgen::CertificateParams::default();
        leaf_params.subject_alt_names =
            vec![rcgen::SanType::DnsName("localhost".try_into().unwrap())];
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf = leaf_params
            .signed_by(&leaf_key, &rcgen::Issuer::new(ca_params, ca_key))
            .unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
            )
            .unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "https://localhost:{}/",
            listener.local_addr().unwrap().port()
        );
        let server = HttpServer::new(|| {
            App::new().route(
                "/coordination/output/commit",
                web::post().to(|| async { HttpResponse::Ok().finish() }),
            )
        })
        .workers(1)
        .listen_rustls_0_23(listener, server_config)
        .unwrap()
        .run();
        let handle = server.handle();
        actix_web::rt::spawn(server);

        // With the public roots alone, the host's certificate is refused, so the report is retried
        // until the timeout.
        let client = super::PeerClient::new(std::slice::from_ref(&url), None, None).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), client.report_output(0, &step_4()))
                .await
                .is_err()
        );

        let root = reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap();
        let client = super::PeerClient::new(std::slice::from_ref(&url), None, Some(root)).unwrap();
        tokio::time::timeout(Duration::from_secs(10), client.report_output(0, &step_4()))
            .await
            .expect("the report should finish")
            .unwrap();

        handle.stop(false).await;
    }
}
