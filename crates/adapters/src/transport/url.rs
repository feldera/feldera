use super::{InputConsumer, InputEndpoint, InputTransport};
use crate::PipelineState;
use actix::System;
use anyhow::{anyhow, Result as AnyResult};
use awc::{Client, Connector};
use futures::StreamExt;
use lazy_static::lazy_static;
use rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, sync::Arc, thread::spawn};
use tokio::{
    select,
    sync::watch::{channel, Receiver, Sender},
};
use utoipa::ToSchema;
use webpki_roots::TLS_SERVER_ROOTS;

/// [`InputTransport`] implementation that reads data from an HTTP or HTTPS URL.
///
/// The input transport factory gives this transport the name `url`.
pub struct UrlInputTransport;

impl InputTransport for UrlInputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("url")
    }

    /// Creates a new [`InputEndpoint`] for reading from an HTTP or HTTPS URL,
    /// interpreting `config` as a [`UrlInputConfig`].
    ///
    /// See [`InputTransport::new_endpoint()`] for more information.
    fn new_endpoint(&self, _name: &str, config: &YamlValue) -> AnyResult<Box<dyn InputEndpoint>> {
        let config = UrlInputConfig::deserialize(config)?;
        let ep = UrlInputEndpoint::new(config);
        Ok(Box::new(ep))
    }
}

/// Configuration for reading data from an HTTP or HTTPS URL with
/// [`UrlInputTransport`].
#[derive(Clone, Deserialize, ToSchema)]
pub struct UrlInputConfig {
    /// URL.
    pub path: String,
}

struct UrlInputEndpoint {
    config: UrlInputConfig,
    sender: Sender<PipelineState>,
    receiver: Receiver<PipelineState>,
}

impl UrlInputEndpoint {
    fn new(config: UrlInputConfig) -> Self {
        let (sender, receiver) = channel(PipelineState::Paused);
        Self {
            config,
            sender,
            receiver,
        }
    }

    async fn worker_thread(
        config: UrlInputConfig,
        consumer: &mut Box<dyn InputConsumer>,
        mut receiver: Receiver<PipelineState>,
    ) -> AnyResult<()> {
        let client = Client::builder()
            .connector(Connector::new().rustls(rustls_config()))
            .finish();
        let request = client.get(config.path);
        let mut response = request.send().await.map_err(
            // `awc` intentionally uses errors that aren't `Sync`, but
            // `anyhow::Error` requires `Sync`.  Transform the error so we can
            // return it.
            |error| anyhow!("{error}"),
        )?;

        while *receiver.borrow() != PipelineState::Terminated {
            select! {
                _ = receiver.changed() => (),
                result = response.next(), if *receiver.borrow() == PipelineState::Running => {
                    match result {
                        None => return Ok(()),
                        Some(Ok(data)) => consumer.input_fragment(&data)?,
                        Some(Err(error)) => Err(error)?,
                    }
                }
            }
        }

        Ok(())
    }
}

impl InputEndpoint for UrlInputEndpoint {
    fn connect(&mut self, mut consumer: Box<dyn InputConsumer>) -> AnyResult<()> {
        let config = self.config.clone();
        let receiver = self.receiver.clone();
        let _worker = spawn(move || {
            System::new().block_on(async move {
                let result = Self::worker_thread(config, &mut consumer, receiver).await;
                if let Some(error) = result.err().or_else(|| consumer.eoi().err()) {
                    consumer.error(true, error);
                }
            });
        });
        Ok(())
    }

    fn pause(&self) -> AnyResult<()> {
        Ok(self.sender.send(PipelineState::Paused)?)
    }

    fn start(&self) -> AnyResult<()> {
        Ok(self.sender.send(PipelineState::Running)?)
    }

    fn disconnect(&self) {
        let _ = self.sender.send(PipelineState::Terminated);
    }
}

impl Drop for UrlInputEndpoint {
    fn drop(&mut self) {
        self.disconnect();
    }
}

fn rustls_config() -> Arc<ClientConfig> {
    lazy_static! {
        static ref ROOT_STORE: Arc<ClientConfig> = {
            let mut root_store = RootCertStore::empty();
            root_store.add_server_trust_anchors(TLS_SERVER_ROOTS.iter().map(|ta| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            }));

            Arc::new(
                ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_store)
                    .with_no_client_auth(),
            )
        };
    }

    ROOT_STORE.clone()
}

#[cfg(test)]
mod test {
    use crate::test::{mock_input_pipeline, wait};
    use actix::System;
    use actix_web::{middleware, web, App, HttpServer, Result};
    use csv::Writer;
    use serde::{Deserialize, Serialize};
    use std::{
        sync::mpsc::channel,
        thread::{sleep, spawn},
        time::Duration,
    };

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
    struct TestStruct {
        s: String,
        b: bool,
        i: i64,
    }

    impl TestStruct {
        fn new(s: String, b: bool, i: i64) -> Self {
            Self { s, b, i }
        }
    }

    #[actix_web::test]
    async fn test_csv_url() -> Result<()> {
        let test_data = vec![
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];

        let mut csv_writer = Writer::from_writer(Vec::new());
        test_data
            .iter()
            .for_each(|record| csv_writer.serialize(record).unwrap());

        // Start HTTP server listening on an arbitrary local port, and obtain
        // its socket address as `addr`.
        let (sender, receiver) = channel();
        spawn(move || {
            System::new().block_on(async {
                let server = HttpServer::new(|| {
                    App::new()
                        // enable logger
                        .wrap(middleware::Logger::default())
                        .service(web::resource("/test.csv").to(|| async {
                            "\
foo,true,10
bar,false,-10
"
                        }))
                })
                .bind(("127.0.0.1", 0))
                .unwrap();
                sender.send(server.addrs()[0]).unwrap();
                server.run().await.unwrap();
            });
        });
        let addr = receiver.recv().unwrap();

        // Create a transport endpoint attached to the file.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: url
    config:
        path: http://{addr}/test.csv
format:
    name: csv
"#
        );

        let (endpoint, consumer, zset) =
            mock_input_pipeline::<TestStruct>(serde_yaml::from_str(&config_str).unwrap()).unwrap();

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(consumer.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        endpoint.start().unwrap();
        wait(|| zset.state().flushed.len() == test_data.len(), None);
        for (i, (val, polarity)) in zset.state().flushed.iter().enumerate() {
            assert!(polarity);
            assert_eq!(val, &test_data[i]);
        }
        Ok(())
    }
}
