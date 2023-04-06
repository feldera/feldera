use crate::{
    db::ConfigDescr, AttachedConnector, Direction, ErrorResponse, ManagerConfig,
    NewPipelineRequest, NewPipelineResponse, PipelineId, ProjectDB, ProjectId, ProjectStatus,
    Version,
};
use actix_web::{http::Method, HttpResponse};
use anyhow::{Error as AnyError, Result as AnyResult};
use awc::Client;
use log::error;
use regex::Regex;
use serde::Serialize;
use std::{
    error::Error as StdError, fmt, fmt::Display, path::Path, pin::Pin, process::Stdio, sync::Arc,
};
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all, remove_file, File},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeek, BufReader, SeekFrom},
    process::{Child, Command},
    sync::Mutex,
    time::{sleep, Duration, Instant},
};

const STARTUP_TIMEOUT: Duration = Duration::from_millis(10_000);
const LOG_SUFFIX_LEN: i64 = 10_000;

#[derive(Debug)]
pub(crate) enum RunnerError {
    PipelineShutdown(PipelineId),
}

impl Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunnerError::PipelineShutdown(pipeline_id) => {
                write!(f, "Pipeline '{pipeline_id}' has been shut down")
            }
        }
    }
}

impl StdError for RunnerError {}

/// The Runner component responsible for running and interacting with
/// pipelines at runtime.
///
/// # Starting a pipeline
///
/// Starting a pipeline amounts to running the compiled executable with
/// selected config, and monitoring the pipeline log file for either
/// "Started HTTP server on port XXXXX" or "Failed to create server
/// [detailed error message]".  In the former case, the port number is
/// recorded in the database.  In the latter case, the error message is
/// returned to the client.
///
/// # Killing a pipeline
///
/// To stop the pipeline, the runner sends a `/kill` HTTP request to the
/// pipeline.  This request is asynchronous: the pipeline may continue running
/// for a few seconds after the request succeeds.
///
/// # Prometheus
///
/// The runner registers pipelines with Prometheus using the
/// `file_sd_config` mechanism.  When creating a pipeline, the runner
/// creates a Prometheus target config file in a directory, which Prometheus
/// monitors.
pub struct Runner {
    db: Arc<Mutex<ProjectDB>>,
    config: ManagerConfig,
    // TODO: The Prometheus server should be isntantiated and managed by k8s.
    prometheus_server: Option<Child>,
}

/// Pipeline metadata.
///
/// A pipeline is initialized with a static metadata string (supplied via
/// file read on startup).  This metadata is accessed via pipeline's
/// `/metadata` endpoint.  The pipeline doesn't enforce any particular
/// format, schema, or semantics of the metadata string, but various tools
/// and UIs may.
#[derive(Serialize)]
struct PipelineMetadata {
    /// Project id.
    project_id: ProjectId,
    /// Project version.
    version: Version,
    /// Project code.
    code: String,
}

impl Drop for Runner {
    fn drop(&mut self) {
        if let Some(mut prometheus) = self.prometheus_server.take() {
            let _ = prometheus.start_kill();
        }
    }
}

impl Runner {
    pub(crate) async fn new(db: Arc<Mutex<ProjectDB>>, config: &ManagerConfig) -> AnyResult<Self> {
        // Initialize Prometheus.
        let prometheus_server = Self::start_prometheus(config).await?;
        Ok(Self {
            db,
            config: config.clone(),
            prometheus_server,
        })
    }

    async fn start_prometheus(config: &ManagerConfig) -> AnyResult<Option<Child>> {
        // Create `prometheus` dir before starting any pipelines so that the
        // Prometheus server can locate the directory to scan.
        let prometheus_dir = config.prometheus_dir();
        create_dir_all(&prometheus_dir).await.map_err(|e| {
            AnyError::msg(format!(
                "error creating Prometheus configs directory '{}': {e}",
                prometheus_dir.display()
            ))
        })?;

        // FIXME: This should be handled by the deployment infrastructure.
        if config.with_prometheus {
            // Prometheus server configuration.
            let prometheus_config = format!(
                r#"
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: dbsp
    file_sd_configs:
    - files:
      - '{}/pipeline*.yaml'
"#,
                prometheus_dir.display()
            );
            let prometheus_config_file = config.prometheus_server_config_file();
            fs::write(&prometheus_config_file, prometheus_config)
                .await
                .map_err(|e| {
                    AnyError::msg(format!(
                        "error writing Prometheus config file '{}': {e}",
                        prometheus_config_file.display()
                    ))
                })?;

            // Start the Prometheus server, which will
            // inherit stdout, stderr from us.
            let prometheus_process = Command::new("prometheus")
                .arg("--config.file")
                .arg(&prometheus_config_file)
                .stdin(Stdio::null())
                .spawn()
                .map_err(|e| AnyError::msg(format!("failed to start Prometheus server, {e}")))?;

            Ok(Some(prometheus_process))
        } else {
            Ok(None)
        }
    }

    /// Start a new pipeline.
    ///
    /// Starts the pipeline executable and waits for the pipeline to initialize,
    /// returning pipeline id and port number.
    pub(crate) async fn run_pipeline(
        &self,
        request: &NewPipelineRequest,
    ) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;

        // Read and validate project config.
        let config_descr = db.get_config(request.config_id).await?;
        if config_descr.project_id.is_none() {
            return Ok(HttpResponse::BadRequest().body(format!(
                "Config '{}' does not have a project set",
                request.config_id
            )));
        }
        let project_version = config_descr.project_id.unwrap();

        // Check: project exists, version = current version, compilation completed.
        let project_descr = db.get_project(project_version).await?;
        if project_descr.status != ProjectStatus::Success {
            return Ok(HttpResponse::Conflict().body("Project hasn't been compiled yet"));
        };

        let pipeline_id = db
            .new_pipeline(request.config_id, request.config_version)
            .await?;
        db.add_pipeline_to_config(config_descr.config_id, pipeline_id)
            .await?;

        // Run the pipeline executable.
        let mut pipeline_process = self.start(&db, request, &config_descr, pipeline_id).await?;

        // Unlock db -- the next part can be slow.
        drop(db);

        match Self::wait_for_startup(&self.config.log_file_path(pipeline_id)).await {
            Ok(port) => {
                // Store pipeline in the database.
                if let Err(e) = self
                    .db
                    .lock()
                    .await
                    .pipeline_set_port(pipeline_id, port)
                    .await
                {
                    let _ = pipeline_process.kill().await;
                    return Err(e);
                };
                let json_string =
                    serde_json::to_string(&NewPipelineResponse { pipeline_id, port }).unwrap();

                // Create Prometheus config file for the pipeline.
                // The Prometheus server should pick up this file automatically.
                self.create_prometheus_config(
                    &project_descr.name,
                    config_descr.project_id.unwrap(),
                    pipeline_id,
                    port,
                )
                .await
                .unwrap_or_else(|e| {
                    // Don't abandon pipeline, just log the error.
                    error!(
                        "Failed to create Prometheus config file for pipeline '{pipeline_id}': {e}"
                    );
                });

                Ok(HttpResponse::Ok()
                    .content_type(mime::APPLICATION_JSON)
                    .body(json_string))
            }
            Err(e) => {
                let _ = pipeline_process.kill().await;
                self.db.lock().await.delete_pipeline(pipeline_id).await?;
                Err(e)
            }
        }
    }

    async fn start(
        &self,
        db: &ProjectDB,
        request: &NewPipelineRequest,
        config_descr: &ConfigDescr,
        pipeline_id: PipelineId,
    ) -> AnyResult<Child> {
        assert!(
            config_descr.project_id.is_some(),
            "pre-condition for start(): config.project_id is set"
        );
        let project_id = config_descr.project_id.unwrap();

        // Assemble the final config by including all attached connectors.
        async fn generate_attached_connector_config(
            db: &ProjectDB,
            config: &mut String,
            ac: &AttachedConnector,
        ) -> AnyResult<()> {
            let ident = 4;
            config.push_str(format!("{:ident$}connector-{}:\n", "", ac.uuid.as_str()).as_str());
            let ident = 8;
            config.push_str(format!("{:ident$}stream: {}\n", "", ac.config.as_str()).as_str());
            let connector = db.get_connector(ac.connector_id).await?;
            for config_line in connector.config.lines() {
                config.push_str(format!("{:ident$}{config_line}\n", "").as_str());
            }
            Ok(())
        }
        async fn add_debug_websocket(config: &mut String, ac: &AttachedConnector) -> AnyResult<()> {
            let ident = 4;
            config.push_str(format!("{:ident$}debug-{}:\n", "", ac.uuid.as_str()).as_str());
            let ident = 8;
            config.push_str(format!("{:ident$}stream: {}\n", "", ac.config.as_str()).as_str());
            for config_line in ["transport:", "     name: http", "format:", "    name: csv"].iter()
            {
                config.push_str(format!("{:ident$}{config_line}\n", "").as_str());
            }
            Ok(())
        }

        let mut config = config_descr.config.clone();
        config.push_str("inputs:\n");
        for ac in config_descr
            .attached_connectors
            .iter()
            .filter(|ac| ac.direction == Direction::Input)
        {
            generate_attached_connector_config(db, &mut config, ac).await?;
        }
        config.push_str("outputs:\n");
        for ac in config_descr
            .attached_connectors
            .iter()
            .filter(|ac| ac.direction == Direction::Output)
        {
            generate_attached_connector_config(db, &mut config, ac).await?;
            add_debug_websocket(&mut config, ac).await?;
        }
        log::debug!("Pipeline config is '{}'", config);

        // Create pipeline directory (delete old directory if exists); write metadata
        // and config files to it.
        let pipeline_dir = self.config.pipeline_dir(pipeline_id);
        create_dir_all(&pipeline_dir).await?;
        let config_file_path = self.config.config_file_path(pipeline_id);
        fs::write(&config_file_path, config.as_str()).await?;

        let (_version, code) = db.project_code(project_id).await?;

        let metadata = PipelineMetadata {
            project_id,
            version: request.config_version,
            code,
        };
        let metadata_file_path = self.config.metadata_file_path(pipeline_id);
        fs::write(
            &metadata_file_path,
            serde_json::to_string(&metadata).unwrap(),
        )
        .await?;

        let log_file_path = self.config.log_file_path(pipeline_id);
        let log_file = File::create(&log_file_path).await?;
        let out_file_path = self.config.out_file_path(pipeline_id);
        let out_file = File::create(&out_file_path).await?;

        // Locate project executable.
        let executable = self.config.project_executable(project_id);

        // Run executable, set current directory to pipeline directory, pass metadata
        // file and config as arguments.
        let pipeline_process = Command::new(&executable)
            .current_dir(self.config.pipeline_dir(pipeline_id))
            .arg("--config-file")
            .arg(&config_file_path)
            .arg("--metadata-file")
            .arg(&metadata_file_path)
            .stdin(Stdio::null())
            .stdout(out_file.into_std().await)
            .stderr(log_file.into_std().await)
            .spawn()
            .map_err(|e| AnyError::msg(format!("failed to run '{}': {e}", executable.display())))?;

        Ok(pipeline_process)
    }

    /// Monitor pipeline log until either port number or error shows up or
    /// the child process exits.
    async fn wait_for_startup(log_file_path: &Path) -> AnyResult<u16> {
        let mut log_file_lines = BufReader::new(File::open(log_file_path).await?).lines();

        let start = Instant::now();

        let portnum_regex = Regex::new(r"Started HTTP server on port (\w+)\b").unwrap();
        let error_regex = Regex::new(r"Failed to create pipeline.*").unwrap();

        loop {
            if let Some(line) = log_file_lines.next_line().await? {
                if let Some(captures) = portnum_regex.captures(&line) {
                    if let Some(portnum_match) = captures.get(1) {
                        if let Ok(port) = portnum_match.as_str().parse::<u16>() {
                            return Ok(port);
                        } else {
                            return Err(AnyError::msg("invalid port number in log: '{line}'"));
                        }
                    } else {
                        return Err(AnyError::msg(
                            "couldn't parse server port number from log: '{line}'",
                        ));
                    }
                };
                if let Some(mtch) = error_regex.find(&line) {
                    return Err(AnyError::msg(mtch.as_str().to_string()));
                };
            }

            if start.elapsed() > STARTUP_TIMEOUT {
                let log = Self::log_suffix(log_file_path).await;
                return Err(AnyError::msg(format!("waiting for pipeline initialization status timed out after {STARTUP_TIMEOUT:?}\n{log}")));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Create Prometheus config file for a pipeline.
    async fn create_prometheus_config(
        &self,
        project_name: &str,
        project_id: ProjectId,
        pipeline_id: PipelineId,
        port: u16,
    ) -> AnyResult<()> {
        let config = format!(
            r#"- targets: [ "localhost:{port}" ]
  labels:
    project_name: "{project_name}"
    pipeline_id: {pipeline_id}
    project_id: {project_id}"#
        );
        fs::write(
            self.config.prometheus_pipeline_config_file(pipeline_id),
            config,
        )
        .await?;

        Ok(())
    }

    /*
    async fn create_topics(&self, config_yaml: &str) -> AnyResult<(KafkaResources, String)> {
        let mut config: ControllerConfig = serde_yaml::from_str(config_yaml)
            .map_err(|e| AnyError::msg(format!("error parsing pipeline configuration: {e}")))?;

        for (_, input_config) in config.inputs.iter_mut() {
            if input_config.transport.name == "kafka" {
                if let Some(topics) = input_config.transport.config.get("topics") {
                    let topics_string = serde_yaml::to_string(topics);
                    let topics: Vec<String> = serde_yaml::from_value(topics)
                        .map_err(|e| AnyError::msg(format!("error parsing Kafka topic list '{topics_string}': {e}")))?;
                    for topic in topics.iter_mut() {
                        if topic == "?" {
                            *topic = self.generate_topic_name("input_");
                        }
                    }
                    input_config.transport.config.set("topics", serde_yaml::to_value(topics))
                }
            }
        }

        for (_, output_config) in config.outputs.iter_mut() {
            if input_config.transport.name == "kafka" {
                if let Some(YamlValue::String(topic) = input_config.transport.config.get_mut("topic") {
                    if topic == "?" {
                        topic = Self::generate_topic_name("output_");
                    }
                }
            }
        }

        let new_config = serde_yaml::to_string(config);

        Ok((kafka_resources, new_config))

    }
    */

    async fn log_suffix_inner(log_file_path: &Path) -> AnyResult<String> {
        let mut buf = Vec::with_capacity(LOG_SUFFIX_LEN as usize);

        let mut file = File::open(log_file_path).await?;

        Pin::new(&mut file).start_seek(SeekFrom::End(-LOG_SUFFIX_LEN))?;
        file.read_to_end(&mut buf).await?;

        let suffix = String::from_utf8_lossy(&buf);
        Ok(format!("log file tail:\n{suffix}"))
    }

    /// Read up to `LOG_SUFFIX_LEN` bytes from the end of the pipeline log in
    /// order to include the suffix of the log in a diagnostic message.
    async fn log_suffix(log_file_path: &Path) -> String {
        Self::log_suffix_inner(log_file_path)
            .await
            .unwrap_or_else(|e| format!("[unable to read log file: {e}]"))
    }

    /// Send a `/kill` request to the pipeline process, but keep the pipeline
    /// state in the database and file system.
    ///
    /// After calling this method, the user can still do post-mortem analysis
    /// of the pipeline, e.g., access its logs or study its performance
    /// metrics in Prometheus.
    ///
    /// Use the [`delete_pipeline`](`Self::delete_pipeline`) method to remove
    /// all traces of the pipeline from the manager.
    pub(crate) async fn shutdown_pipeline(
        &self,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;

        self.do_shutdown_pipeline(&db, pipeline_id).await
    }

    async fn do_shutdown_pipeline(
        &self,
        db: &ProjectDB,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        let pipeline_descr = db.get_pipeline(pipeline_id).await?;

        if pipeline_descr.killed {
            return Ok(HttpResponse::Ok().json("Pipeline already shut down."));
        };

        let url = format!("http://localhost:{}/kill", pipeline_descr.port);
        let response = match reqwest::get(&url).await {
            Ok(response) => response,
            Err(_) => {
                db.remove_pipeline_from_config(pipeline_descr.config_id)
                    .await?;
                db.set_pipeline_killed(pipeline_id).await?;
                // We failed to reach the pipeline, which likely means
                // that it crashed or was killed manually by the user.
                return Ok(
                    HttpResponse::Ok().json(&format!("Pipeline at '{url}' already shut down."))
                );
            }
        };

        if response.status().is_success() {
            db.remove_pipeline_from_config(pipeline_descr.config_id)
                .await?;
            db.set_pipeline_killed(pipeline_id).await?;
            Ok(HttpResponse::Ok().json("Pipeline successfully terminated."))
        } else {
            Ok(HttpResponse::InternalServerError().json(
                &ErrorResponse::new(&format!(
                    "Failed to shut down the pipeline; response from pipeline controller: {response:?}"
                )),
            ))
        }
    }

    /// Kill the pipeline if it is still running, delete its file system state
    /// and remove the pipeline from the database.
    // Takes a reference to an already locked DB instance, since this function
    // is invoked in contexts where the client already holds the lock.
    pub(crate) async fn delete_pipeline(
        &self,
        db: &ProjectDB,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        // Kill pipeline.
        let response = self.do_shutdown_pipeline(db, pipeline_id).await?;
        if !response.status().is_success() {
            return Ok(response);
        }

        // TODO: Delete temporary topics.

        // Delete Prometheus config.
        let _ = remove_file(self.config.prometheus_pipeline_config_file(pipeline_id)).await;

        // Delete pipeline directory.
        remove_dir_all(self.config.pipeline_dir(pipeline_id)).await?;
        db.delete_pipeline(pipeline_id).await?;

        Ok(HttpResponse::Ok().json("Pipeline successfully deleted."))
    }

    pub(crate) async fn forward_to_pipeline(
        &self,
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
    ) -> AnyResult<HttpResponse> {
        let pipeline_descr = self.db.lock().await.get_pipeline(pipeline_id).await?;

        if pipeline_descr.killed {
            return Err(AnyError::from(RunnerError::PipelineShutdown(pipeline_id)));
        }

        let client = Client::default();
        let request = client.request(
            method,
            &format!(
                "http://localhost:{port}/{endpoint}",
                port = pipeline_descr.port
            ),
        );

        let mut response = request
            .send()
            .await
            .map_err(|e| AnyError::msg(format!("Failed to connect to pipeline: {e}")))?;

        let response_body = response.body().await?;

        let mut response_builder = HttpResponse::build(response.status());
        // Remove `Connection` as per
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Connection#Directives
        for (header_name, header_value) in response
            .headers()
            .iter()
            .filter(|(h, _)| *h != "connection")
        {
            response_builder.insert_header((header_name.clone(), header_value.clone()));
        }

        Ok(response_builder.body(response_body))
    }
}
