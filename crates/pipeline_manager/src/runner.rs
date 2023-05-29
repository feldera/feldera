use crate::{
    db::storage::Storage, db::AttachedConnector, db::PipelineDescr, ErrorResponse, ManagerConfig,
    PipelineId, ProgramId, ProgramStatus, ProjectDB, Version,
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
use serde::Serialize;
use std::{error::Error as StdError, fmt, fmt::Display, path::Path, process::Stdio, sync::Arc};
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all},
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::Mutex,
    time::{sleep, Duration, Instant},
};

const STARTUP_TIMEOUT: Duration = Duration::from_millis(10_000);
const PORT_FILE_LOG_QUIET_PERIOD: Duration = Duration::from_millis(2_000);

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
    pub(crate) async fn run_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<HttpResponse> {
        match self {
            Self::Local(local) => local.run_pipeline(pipeline_id).await,
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

    pub(crate) async fn run_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;

        // Read and validate config.
        let pipeline_descr = db.get_pipeline_by_id(pipeline_id).await?;
        if pipeline_descr.program_id.is_none() {
            return Ok(HttpResponse::BadRequest().body(format!(
                "Pipeline '{}' does not have a program set",
                pipeline_id
            )));
        }
        let program_version = pipeline_descr.program_id.unwrap();

        // Check: program exists, version = current version, compilation completed.
        let program_descr = db.get_program_by_id(program_version).await?;
        if program_descr.status != ProgramStatus::Success {
            return Ok(HttpResponse::Conflict().body("Program hasn't been compiled yet"));
        };

        // Run the pipeline executable.
        let mut pipeline_process = self.start(&db, &pipeline_descr).await?;

        // Unlock db -- the next part can be slow.
        drop(db);

        match Self::wait_for_startup(&self.config.port_file_path(pipeline_id)).await {
            Ok(port) => {
                // Store pipeline in the database.
                if let Err(e) = self
                    .db
                    .lock()
                    .await
                    .set_pipeline_deploy(pipeline_id, port)
                    .await
                {
                    let _ = pipeline_process.kill().await;
                    return Err(e);
                };
                Ok(HttpResponse::Ok().finish())
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
        let pipeline_descr = self.db.lock().await.get_pipeline_by_id(pipeline_id).await?;

        if pipeline_descr.shutdown {
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
        name: &str,
        req: HttpRequest,
        mut body: actix_web::web::Payload,
    ) -> AnyResult<HttpResponse> {
        let pipeline_descr = self.db.lock().await.get_pipeline_by_id(pipeline_id).await?;
        if pipeline_descr.shutdown {
            return Err(AnyError::from(RunnerError::PipelineShutdown(pipeline_id)));
        }
        let port = pipeline_descr.port;
        let is_input = self
            .db
            .lock()
            .await
            .attached_connector_is_input(name)
            .await?;

        // TODO: it might be better to have ?name={}, otherwise we have to
        // restrict name format
        let url = if is_input {
            format!("ws://localhost:{port}/input_endpoint/{name}")
        } else {
            format!("ws://localhost:{port}/output_endpoint/{name}")
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

    async fn start(&self, db: &ProjectDB, pipeline_descr: &PipelineDescr) -> AnyResult<Child> {
        assert!(
            pipeline_descr.program_id.is_some(),
            "pre-condition for start(): config.program_id is set"
        );
        let pipeline_id = pipeline_descr.pipeline_id;
        let program_id = pipeline_descr.program_id.unwrap();

        // Assemble the final config by including all attached connectors.
        async fn generate_attached_connector_config(
            db: &ProjectDB,
            config: &mut String,
            ac: &AttachedConnector,
        ) -> AnyResult<()> {
            let ident = 4;
            config.push_str(format!("{:ident$}{}:\n", "", ac.name.as_str()).as_str());
            let ident = 8;
            config.push_str(format!("{:ident$}stream: {}\n", "", ac.config.as_str()).as_str());
            let connector = db.get_connector_by_id(ac.connector_id).await?;
            for config_line in connector.config.lines() {
                config.push_str(format!("{:ident$}{config_line}\n", "").as_str());
            }
            Ok(())
        }
        async fn add_debug_websocket(config: &mut String, ac: &AttachedConnector) -> AnyResult<()> {
            let ident = 4;
            config.push_str(format!("{:ident$}debug-{}:\n", "", ac.name.as_str()).as_str());
            let ident = 8;
            config.push_str(format!("{:ident$}stream: {}\n", "", ac.config.as_str()).as_str());
            for config_line in ["transport:", "     name: http", "format:", "    name: csv"].iter()
            {
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
            generate_attached_connector_config(db, &mut config, ac).await?;
        }
        config.push_str("outputs:\n");
        for ac in pipeline_descr
            .attached_connectors
            .iter()
            .filter(|ac| !ac.is_input)
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

        let (_version, code) = db.program_code(program_id).await?;

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
        let pipeline_process = Command::new(&executable)
            .current_dir(self.config.pipeline_dir(pipeline_id))
            .arg("--config-file")
            .arg(&config_file_path)
            .arg("--metadata-file")
            .arg(&metadata_file_path)
            .stdin(Stdio::null())
            .spawn()
            .map_err(|e| AnyError::msg(format!("failed to run '{}': {e}", executable.display())))?;

        Ok(pipeline_process)
    }

    /// Monitor pipeline log until either port number or error shows up or
    /// the child process exits.
    async fn wait_for_startup(port_file_path: &Path) -> AnyResult<u16> {
        let start = Instant::now();
        let mut count = 0;
        loop {
            let res: Result<String, std::io::Error> = fs::read_to_string(port_file_path).await;
            match res {
                Ok(port) => {
                    let parse = port.trim().parse::<u16>();
                    return match parse {
                        Ok(port) => Ok(port),
                        Err(e) => Err(AnyError::msg(format!(
                            "Could not parse port from port file: {e:?}\n"
                        ))),
                    };
                }
                Err(e) => {
                    if start.elapsed() > STARTUP_TIMEOUT {
                        return Err(AnyError::msg(format!("Waiting for pipeline initialization status timed out after {STARTUP_TIMEOUT:?}\n")));
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
        db: &ProjectDB,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        let pipeline_descr = db.get_pipeline_by_id(pipeline_id).await?;

        if pipeline_descr.shutdown {
            return Ok(HttpResponse::Ok().json("Pipeline already shut down."));
        };

        let url = format!("http://localhost:{}/shutdown", pipeline_descr.port);
        let response = match reqwest::get(&url).await {
            Ok(response) => response,
            Err(_) => {
                db.set_pipeline_shutdown(pipeline_id).await?;
                // We failed to reach the pipeline, which likely means
                // that it crashed or was killed manually by the user.
                return Ok(
                    HttpResponse::Ok().json(&format!("Pipeline at '{url}' already shut down."))
                );
            }
        };

        if response.status().is_success() {
            db.set_pipeline_shutdown(pipeline_id).await?;
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
