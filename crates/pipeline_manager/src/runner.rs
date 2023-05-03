use crate::{
    db::storage::Storage, db::AttachedConnector, db::ConfigDescr, Direction, ErrorResponse,
    ManagerConfig, NewPipelineRequest, NewPipelineResponse, PipelineId, ProjectDB, ProjectId,
    ProjectStatus, Version,
};
use actix_web::{
    http::{Error, Method},
    web::BytesMut,
    HttpRequest, HttpResponse,
};
use actix_web_actors::ws::handshake;
use anyhow::{Error as AnyError, Result as AnyResult};
use awc::Client;
use futures_util::StreamExt;
use regex::Regex;
use serde::Serialize;
use std::{
    error::Error as StdError, fmt, fmt::Display, path::Path, pin::Pin, process::Stdio, sync::Arc,
};
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all, File},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeek, AsyncWriteExt, BufReader, SeekFrom},
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
/// # Killing a pipeline
///
/// To stop the pipeline, the runner sends a `/kill` HTTP request to the
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
    /// Project id.
    project_id: ProjectId,
    /// Project version.
    version: Version,
    /// Project code.
    code: String,
}

impl Runner {
    /// Start a new pipeline.
    ///
    /// Starts the pipeline executable and waits for the pipeline to initialize,
    /// returning pipeline id and port number.
    pub(crate) async fn run_pipeline(
        &self,
        request: &NewPipelineRequest,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.run_pipeline(request).await,
        }
    }

    /// Send a `/kill` request to the pipeline process, but keep the pipeline
    /// state in the database and file system.
    ///
    /// After calling this method, the user can still do post-mortem analysis
    /// of the pipeline, e.g., access its logs.
    ///
    /// Use the [`delete_pipeline`](`Self::delete_pipeline`) method to remove
    /// all traces of the pipeline from the manager.
    pub(crate) async fn shutdown_pipeline(
        &self,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.shutdown_pipeline(pipeline_id).await,
        }
    }

    /// Delete the pipeline from the database. Shuts down the pipeline first if
    /// it is already running.
    /// Takes a reference to an already locked DB instance, since this function
    /// is invoked in contexts where the client already holds the lock.
    pub(crate) async fn delete_pipeline(
        &self,
        db: &ProjectDB,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.delete_pipeline(db, pipeline_id).await,
        }
    }

    pub(crate) async fn forward_to_pipeline(
        &self,
        pipeline_id: PipelineId,
        method: Method,
        endpoint: &str,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => {
                local
                    .forward_to_pipeline(pipeline_id, method, endpoint)
                    .await
            }
        }
    }

    pub(crate) async fn forward_to_pipeline_as_stream(
        &self,
        pipeline_id: PipelineId,
        uuid: &str,
        req: HttpRequest,
        body: actix_web::web::Payload,
    ) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(r) => {
                r.forward_to_pipeline_as_stream(pipeline_id, uuid, req, body)
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

    pub(crate) async fn shutdown_pipeline(
        &self,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;

        self.do_shutdown_pipeline(&db, pipeline_id).await
    }

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

    pub(crate) async fn forward_to_pipeline_as_stream(
        &self,
        pipeline_id: PipelineId,
        uuid: &str,
        req: HttpRequest,
        mut body: actix_web::web::Payload,
    ) -> AnyResult<HttpResponse> {
        let pipeline_descr = self.db.lock().await.get_pipeline(pipeline_id).await?;
        if pipeline_descr.killed {
            return Err(AnyError::from(RunnerError::PipelineShutdown(pipeline_id)));
        }
        let port = pipeline_descr.port;
        let direction = self
            .db
            .lock()
            .await
            .get_attached_connector_direction(uuid)
            .await?;
        let url = if direction == Direction::Input {
            format!("ws://localhost:{port}/input_endpoint/{uuid}")
        } else {
            format!("ws://localhost:{port}/output_endpoint/{uuid}")
        };

        let (_, socket) = awc::Client::new().ws(url).connect().await.unwrap();
        let mut io = socket.into_parts().io;
        let (tx, rx) = futures::channel::mpsc::unbounded();
        let mut buf = BytesMut::new();
        actix_web::rt::spawn(async move {
            loop {
                tokio::select! {
                    res = body.next() => {
                        match res {
                            None => return,
                            Some(body) => {
                                let bytes = body.unwrap();
                                io.write_all(&bytes).await.unwrap();
                            }
                        }
                    }
                    res = io.read_buf(&mut buf) => {
                        let size = res.unwrap();
                        let bytes = buf.split_to(size).freeze();
                        tx.unbounded_send(Ok::<_, Error>(bytes)).unwrap();
                    }
                }
            }
        });
        let mut resp = handshake(&req).unwrap();
        Ok(resp.streaming(rx))
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
            config.push_str(format!("{:ident$}{}:\n", "", ac.uuid.as_str()).as_str());
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
                if let Some(config_id) = pipeline_descr.config_id {
                    db.remove_pipeline_from_config(config_id).await?;
                }
                db.set_pipeline_killed(pipeline_id).await?;
                // We failed to reach the pipeline, which likely means
                // that it crashed or was killed manually by the user.
                return Ok(
                    HttpResponse::Ok().json(&format!("Pipeline at '{url}' already shut down."))
                );
            }
        };

        if response.status().is_success() {
            if let Some(config_id) = pipeline_descr.config_id {
                db.remove_pipeline_from_config(config_id).await?;
            }
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
}
