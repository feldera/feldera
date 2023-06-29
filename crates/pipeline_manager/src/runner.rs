use crate::{
    auth::TenantId, db::storage::Storage, db::AttachedConnector, db::PipelineDescr,
    db::PipelineStatus, ManagerConfig, PipelineId, ProgramId, ProgramStatus, ProjectDB, Version,
};
use actix_web::{http::Method, web::Payload, HttpRequest, HttpResponse, HttpResponseBuilder};
use anyhow::{anyhow, bail, Result as AnyResult};
use awc::Client;
use dbsp_adapters::DetailedError;
use serde::Serialize;
use std::{
    borrow::Cow, error::Error as StdError, fmt, fmt::Display, path::Path, process::Stdio, sync::Arc,
};
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all},
    process::{Child, Command},
    sync::Mutex,
    time::{sleep, Duration, Instant},
};

pub(crate) const STARTUP_TIMEOUT: Duration = Duration::from_millis(10_000);
const PORT_FILE_LOG_QUIET_PERIOD: Duration = Duration::from_millis(2_000);

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum RunnerError {
    PipelineShutdown {
        pipeline_id: PipelineId,
    },
    HttpForwardError {
        pipeline_id: PipelineId,
        error: String,
    },
    PortFileParseError {
        pipeline_id: PipelineId,
        error: String,
    },
    PipelineInitializationTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
    ProgramNotSet {
        pipeline_id: PipelineId,
    },
    ProgramNotCompiled {
        pipeline_id: PipelineId,
    },
    PipelineStartupError {
        pipeline_id: PipelineId,
        // TODO: This should be IOError, so we can serialize the error code
        // similar to `DBSPError::IO`.
        error: String,
    },
}

impl DetailedError for RunnerError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::PipelineShutdown { .. } => Cow::from("PipelineShutdown"),
            Self::HttpForwardError { .. } => Cow::from("HttpForwardError"),
            Self::PortFileParseError { .. } => Cow::from("PortFileParseError"),
            Self::PipelineInitializationTimeout { .. } => {
                Cow::from("PipelineInitializationTimeout")
            }
            Self::ProgramNotSet { .. } => Cow::from("ProgramNotSet"),
            Self::ProgramNotCompiled { .. } => Cow::from("ProgramNotCompiled"),
            Self::PipelineStartupError { .. } => Cow::from("PipelineStartupError"),
        }
    }
}

impl Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PipelineShutdown { pipeline_id } => {
                write!(f, "Pipeline '{pipeline_id}' is not currently running.")
            }
            Self::HttpForwardError { pipeline_id, error } => {
                write!(
                    f,
                    "Error forwarding HTTP request to pipeline '{pipeline_id}': '{error}'"
                )
            }
            Self::PipelineInitializationTimeout {
                pipeline_id,
                timeout,
            } => {
                write!(f, "Waiting for pipeline '{pipeline_id}' initialization status timed out after {timeout:?}")
            }
            Self::PortFileParseError { pipeline_id, error } => {
                write!(
                    f,
                    "Could not parse port for pipeline '{pipeline_id}' from port file: '{error}'"
                )
            }
            Self::ProgramNotSet { pipeline_id } => {
                write!(f, "Pipeline '{pipeline_id}' does not have a program set")
            }
            Self::ProgramNotCompiled { pipeline_id } => {
                write!(
                    f,
                    "Program hasn't been compiled for pipeline '{pipeline_id}' yet"
                )
            }
            Self::PipelineStartupError { pipeline_id, error } => {
                write!(f, "Failed to start pipeline '{pipeline_id}': '{error}'")
            }
        }
    }
}

impl StdError for RunnerError {}

/// A runner component responsible for running and interacting with
/// pipelines at runtime.
pub(crate) enum Runner {
    Local(LocalRunner),
}

/// A runner that executes pipelines locally
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
/// # Shutting down a pipeline
///
/// To shutdown the pipeline, the runner sends a `/shutdown` HTTP request to the
/// pipeline.  This request is asynchronous: the pipeline may continue running
/// for a few seconds after the request succeeds.
pub struct LocalRunner {
    db: Arc<Mutex<ProjectDB>>,
    config: ManagerConfig,
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
    /// Program id.
    program_id: ProgramId,
    /// Program version.
    version: Version,
    /// Program code.
    code: String,
}

impl Runner {
    /// Start a new pipeline.
    ///
    /// Starts the pipeline executable and waits for the pipeline to initialize,
    /// returning pipeline id and port number.
    pub(crate) async fn deploy_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.deploy_pipeline(tenant_id, pipeline_id).await,
        }
    }

    /// Send a `/shutdown` request to the pipeline process, but keep the
    /// pipeline state in the database and file system.
    ///
    /// After calling this method, the user can still do post-mortem analysis
    /// of the pipeline, e.g., access its logs.
    ///
    /// Use the [`delete_pipeline`](`Self::delete_pipeline`) method to remove
    /// all traces of the pipeline from the manager.
    pub(crate) async fn shutdown_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.shutdown_pipeline(tenant_id, pipeline_id).await,
        }
    }

    /// Delete the pipeline from the database. Shuts down the pipeline first if
    /// it is already running.
    /// Takes a reference to an already locked DB instance, since this function
    /// is invoked in contexts where the client already holds the lock.
    pub(crate) async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        db: &ProjectDB,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.delete_pipeline(tenant_id, db, pipeline_id).await,
        }
    }

    pub(crate) async fn pause_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.pause_pipeline(tenant_id, pipeline_id).await?,
        };
        self.forward_to_pipeline(tenant_id, pipeline_id, Method::GET, "pause")
            .await
    }

    pub(crate) async fn start_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.start_pipeline(tenant_id, pipeline_id).await?,
        };
        self.forward_to_pipeline(tenant_id, pipeline_id, Method::GET, "start")
            .await
    }

    pub(crate) async fn forward_to_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => {
                local
                    .forward_to_pipeline(tenant_id, pipeline_id, method, endpoint)
                    .await
            }
        }
    }

    pub(crate) async fn forward_to_pipeline_as_stream(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        endpoint: &str,
        req: HttpRequest,
        body: Payload,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(r) => {
                r.forward_to_pipeline_as_stream(tenant_id, pipeline_id, endpoint, req, body)
                    .await
            }
        }
    }
}

impl LocalRunner {
    pub(crate) fn new(db: Arc<Mutex<ProjectDB>>, config: &ManagerConfig) -> AnyResult<Self> {
        Ok(Self {
            db,
            config: config.clone(),
        })
    }

    pub(crate) async fn deploy_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;

        // Read and validate config.
        let pipeline_descr = db.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        if pipeline_descr.program_id.is_none() {
            bail!(RunnerError::ProgramNotSet { pipeline_id });
        }
        let program_version = pipeline_descr.program_id.unwrap();

        // Check: program exists, version = current version, compilation completed.
        let program_descr = db.get_program_by_id(tenant_id, program_version).await?;
        if program_descr.status != ProgramStatus::Success {
            bail!(RunnerError::ProgramNotCompiled { pipeline_id });
        };

        // Run the pipeline executable.
        let mut pipeline_process = self.start(tenant_id, &db, &pipeline_descr).await?;

        // Unlock db -- the next part can be slow.
        drop(db);

        match Self::wait_for_startup(pipeline_id, &self.config.port_file_path(pipeline_id)).await {
            Ok(port) => {
                // Store pipeline in the database.
                if let Err(e) = self
                    .db
                    .lock()
                    .await
                    .set_pipeline_deployed(tenant_id, pipeline_id, port)
                    .await
                {
                    let _ = pipeline_process.kill().await;
                    bail!(e);
                };
                Ok(HttpResponse::Ok().json("Pipeline successfully deployed."))
            }
            Err(e) => {
                let _ = pipeline_process.kill().await;
                self.db
                    .lock()
                    .await
                    .set_pipeline_status(tenant_id, pipeline_id, PipelineStatus::Shutdown)
                    .await?;
                Err(e)
            }
        }
    }

    pub(crate) async fn shutdown_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;
        self.do_shutdown_pipeline(tenant_id, &db, pipeline_id).await
    }

    pub(crate) async fn pause_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> AnyResult<bool> {
        let db = self.db.lock().await;
        Ok(db
            .set_pipeline_status(tenant_id, pipeline_id, PipelineStatus::Paused)
            .await?)
    }

    pub(crate) async fn start_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> AnyResult<bool> {
        let db = self.db.lock().await;
        Ok(db
            .set_pipeline_status(tenant_id, pipeline_id, PipelineStatus::Running)
            .await?)
    }

    pub(crate) async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        db: &ProjectDB,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        // Kill pipeline.
        let response = self
            .do_shutdown_pipeline(tenant_id, db, pipeline_id)
            .await?;
        if !response.status().is_success() {
            return Ok(response);
        }

        // Delete pipeline directory.
        match remove_dir_all(self.config.pipeline_dir(pipeline_id)).await {
            Ok(_) => (),
            Err(e) => {
                log::warn!(
                    "Failed to delete pipeline directory for pipeline {}: {}",
                    pipeline_id,
                    e
                );
            }
        }
        db.delete_pipeline(tenant_id, pipeline_id).await?;

        Ok(HttpResponse::Ok().json("Pipeline successfully deleted."))
    }

    pub(crate) async fn forward_to_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
    ) -> AnyResult<HttpResponse> {
        let pipeline_descr = self
            .db
            .lock()
            .await
            .get_pipeline_by_id(tenant_id, pipeline_id)
            .await?;

        if pipeline_descr.status == PipelineStatus::Shutdown {
            bail!(RunnerError::PipelineShutdown { pipeline_id });
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
            .map_err(|e| RunnerError::HttpForwardError {
                pipeline_id,
                error: e.to_string(),
            })?;

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

    pub(crate) async fn forward_to_pipeline_as_stream(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        endpoint: &str,
        req: HttpRequest,
        body: Payload,
    ) -> AnyResult<HttpResponse> {
        let pipeline_descr = self
            .db
            .lock()
            .await
            .get_pipeline_by_id(tenant_id, pipeline_id)
            .await?;
        if pipeline_descr.status == PipelineStatus::Shutdown {
            bail!(RunnerError::PipelineShutdown { pipeline_id });
        }
        let port = pipeline_descr.port;

        // TODO: it might be better to have ?name={}, otherwise we have to
        // restrict name format
        let url = format!("http://localhost:{port}/{endpoint}?{}", req.query_string());

        let client = awc::Client::new();

        let mut request = client.request(req.method().clone(), url);

        for header in req
            .headers()
            .into_iter()
            .filter(|(h, _)| *h != "connection")
        {
            request = request.append_header(header);
        }

        let response =
            request
                .send_stream(body)
                .await
                .map_err(|e| RunnerError::HttpForwardError {
                    pipeline_id,
                    error: e.to_string(),
                })?;

        let mut builder = HttpResponseBuilder::new(response.status());
        for header in response.headers().into_iter() {
            builder.append_header(header);
        }
        Ok(builder.streaming(response))
    }

    async fn start(
        &self,
        tenant_id: TenantId,
        db: &ProjectDB,
        pipeline_descr: &PipelineDescr,
    ) -> AnyResult<Child> {
        assert!(
            pipeline_descr.program_id.is_some(),
            "pre-condition for start(): config.program_id is set"
        );
        let pipeline_id = pipeline_descr.pipeline_id;
        let program_id = pipeline_descr.program_id.unwrap();

        // Assemble the final config by including all attached connectors.
        async fn generate_attached_connector_config(
            tenant_id: TenantId,
            db: &ProjectDB,
            config: &mut String,
            ac: &AttachedConnector,
        ) -> AnyResult<()> {
            let ident = 4;
            config.push_str(format!("{:ident$}{}:\n", "", ac.name.as_str()).as_str());
            let ident = 8;
            config.push_str(format!("{:ident$}stream: {}\n", "", ac.config.as_str()).as_str());
            let connector = db.get_connector_by_id(tenant_id, ac.connector_id).await?;
            for config_line in connector.config.lines() {
                config.push_str(format!("{:ident$}{config_line}\n", "").as_str());
            }
            Ok(())
        }

        let mut config = pipeline_descr.config.clone();
        config.push_str(format!("name: pipeline-{pipeline_id}\n").as_str());
        config.push_str("inputs:\n");
        for ac in pipeline_descr
            .attached_connectors
            .iter()
            .filter(|ac| ac.is_input)
        {
            generate_attached_connector_config(tenant_id, db, &mut config, ac).await?;
        }
        config.push_str("outputs:\n");
        for ac in pipeline_descr
            .attached_connectors
            .iter()
            .filter(|ac| !ac.is_input)
        {
            generate_attached_connector_config(tenant_id, db, &mut config, ac).await?;
        }
        log::debug!("Pipeline config is '{}'", config);

        // Create pipeline directory (delete old directory if exists); write metadata
        // and config files to it.
        let pipeline_dir = self.config.pipeline_dir(pipeline_id);
        create_dir_all(&pipeline_dir).await?;
        let config_file_path = self.config.config_file_path(pipeline_id);
        fs::write(&config_file_path, config.as_str()).await?;

        let (_version, code) = db.program_code(tenant_id, program_id).await?;

        let metadata = PipelineMetadata {
            program_id,
            version: pipeline_descr.version,
            code,
        };
        let metadata_file_path = self.config.metadata_file_path(pipeline_id);
        fs::write(
            &metadata_file_path,
            serde_json::to_string(&metadata).unwrap(),
        )
        .await?;

        // Locate project executable.
        let executable = self.config.project_executable(program_id);

        // Run executable, set current directory to pipeline directory, pass metadata
        // file and config as arguments.
        let pipeline_process = Command::new(executable)
            .current_dir(self.config.pipeline_dir(pipeline_id))
            .arg("--config-file")
            .arg(&config_file_path)
            .arg("--metadata-file")
            .arg(&metadata_file_path)
            .stdin(Stdio::null())
            .spawn()
            .map_err(|e| {
                anyhow!(RunnerError::PipelineStartupError {
                    pipeline_id,
                    error: e.to_string()
                })
            })?;

        Ok(pipeline_process)
    }

    /// Monitor pipeline log until either port number or error shows up or
    /// the child process exits.
    async fn wait_for_startup(pipeline_id: PipelineId, port_file_path: &Path) -> AnyResult<u16> {
        let start = Instant::now();
        let mut count = 0;
        loop {
            let res: Result<String, std::io::Error> = fs::read_to_string(port_file_path).await;
            match res {
                Ok(port) => {
                    let parse = port.trim().parse::<u16>();
                    return match parse {
                        Ok(port) => Ok(port),
                        Err(e) => Err(anyhow!(RunnerError::PortFileParseError {
                            pipeline_id,
                            error: e.to_string()
                        })),
                    };
                }
                Err(e) => {
                    if start.elapsed() > STARTUP_TIMEOUT {
                        return Err(anyhow!(RunnerError::PipelineInitializationTimeout {
                            pipeline_id,
                            timeout: STARTUP_TIMEOUT
                        }));
                    }
                    if start.elapsed() > PORT_FILE_LOG_QUIET_PERIOD && (count % 10) == 0 {
                        log::info!("Could not read runner port file yet. Retrying\n{}", e);
                    }
                    count += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn do_shutdown_pipeline(
        &self,
        tenant_id: TenantId,
        db: &ProjectDB,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        let pipeline_descr = db.get_pipeline_by_id(tenant_id, pipeline_id).await?;

        if pipeline_descr.status == PipelineStatus::Shutdown {
            return Ok(HttpResponse::Ok().json("Pipeline already shut down."));
        };

        let url = format!("http://localhost:{}/shutdown", pipeline_descr.port);

        let client = awc::Client::new();

        let mut response = match client.get(&url).send().await {
            Ok(response) => response,
            Err(_) => {
                db.set_pipeline_status(tenant_id, pipeline_id, PipelineStatus::Shutdown)
                    .await?;
                // We failed to reach the pipeline, which likely means
                // that it crashed or was killed manually by the user.
                return Ok(
                    HttpResponse::Ok().json(&format!("Pipeline at '{url}' already shut down."))
                );
            }
        };

        if response.status().is_success() {
            db.set_pipeline_status(tenant_id, pipeline_id, PipelineStatus::Shutdown)
                .await?;
            Ok(HttpResponse::Ok().json("Pipeline successfully terminated."))
        } else {
            let mut builder = HttpResponseBuilder::new(response.status());
            for header in response.headers().into_iter() {
                builder.append_header(header);
            }
            Ok(builder.body(response.body().await?))
        }
    }
}
