//! A CLI App for the Feldera REST API.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fs::File;
use std::io::{stdout, ErrorKind, Read, Write};
use std::path::PathBuf;

use clap::{CommandFactory, Parser};
use clap_complete::CompleteEnv;
use env_logger::Env;
use feldera_rest_api::types::*;
use feldera_rest_api::*;
use feldera_types::config::{FtModel, RuntimeConfig, StorageOptions};
use feldera_types::error::ErrorResponse;
use futures_util::StreamExt;
use json_to_table::json_to_table;
use log::{debug, error, info, trace, warn};
use reqwest::header::{HeaderMap, HeaderValue, InvalidHeaderValue};
use reqwest::StatusCode;
use serde_json::json;
use tabled::builder::Builder;
use tabled::settings::Style;
use tempfile::tempfile;
use tokio::process::Command;
use tokio::runtime::Handle;
use tokio::time::{sleep, timeout, Duration, Instant};

mod adhoc;
mod bench;
mod cli;
mod debug;
mod shell;

pub(crate) const UPGRADE_NOTICE: &str =
    "Try upgrading to the latest CLI version to resolve this issue. Also make sure the pipeline is recompiled with the latest version of feldera. Report it on github.com/feldera/feldera if the issue persists.";

use crate::adhoc::handle_adhoc_query;
use crate::cli::*;
use crate::shell::shell;

/// Creates a unique filename by appending a number to the base name if it already exists.
fn unique_file(base: &str, extension: &str) -> Result<(PathBuf, File), std::io::Error> {
    let mut path = PathBuf::from(format!("{}.{}", base, extension));
    let mut count = 1;
    loop {
        let file = File::create_new(&path);
        match file {
            Ok(file) => {
                return Ok((path, file));
            }
            Err(e) => {
                if e.kind() == ErrorKind::AlreadyExists {
                    path = PathBuf::from(format!("{}_{}.{}", base, count, extension));
                    count += 1;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

/// Adds the API key to the headers if it was supplied
fn make_auth_headers(auth: &Option<String>) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    if let Some(key) = auth {
        let mut header_value = HeaderValue::from_str(format!("Bearer {}", key).as_str())?;
        header_value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, header_value);
    }
    Ok(headers)
}

/// Create a client with the given host, auth, and timeout.  If `insecure` is
/// true, then the client won't verify TLS certificates.
pub(crate) fn make_client(
    host: String,
    insecure: bool,
    auth: Option<String>,
    timeout: Option<u64>,
) -> Result<Client, Box<dyn std::error::Error>> {
    let mut client_builder = reqwest::ClientBuilder::new().danger_accept_invalid_certs(insecure);

    if let Some(timeout) = timeout {
        client_builder = client_builder.timeout(Duration::from_secs(timeout));
    }

    if host.starts_with("https://") {
        client_builder = client_builder.default_headers(make_auth_headers(&auth)?);
    } else if host.starts_with("http://") && auth.is_some() {
        warn!(
            "The provided API key is not added to the request because {host} does not use `https`."
        );
    }

    let client = client_builder.build()?;
    Ok(Client::new_with_client(host.as_str(), client))
}

/// A helper struct that disables the cache for a pipeline.
struct CacheDisabler {
    name: String,
    client: Client,
    original_pc: ProgramConfig,
}

impl CacheDisabler {
    async fn new(name: String, client: &Client, original_pc: ProgramConfig) -> Self {
        Self {
            name,
            client: client.clone(),
            original_pc,
        }
    }

    async fn restore(&self) {
        if self.original_pc.cache {
            debug!("Restoring original cache flag for pipeline {}", self.name);
            self.set_cache_flag(true).await;
        } else {
            debug!(
                "Cache for pipeline stays disabled as it was previously disabled {}",
                self.name
            );
        }
    }

    async fn disable(&self) {
        self.set_cache_flag(false).await;
    }

    async fn set_cache_flag(&self, flag: bool) {
        let mut pc = self.original_pc.clone();
        pc.cache = flag;

        self.client
            .patch_pipeline()
            .pipeline_name(self.name.clone())
            .body(PatchPipeline {
                description: None,
                name: None,
                program_code: None,
                udf_rust: None,
                udf_toml: None,
                program_config: Some(pc),
                runtime_config: None,
            })
            .send()
            .await
            .map_err(handle_errors_fatal(
                self.client.baseurl().clone(),
                format!("Failed to set compilation cache to {flag}").leak(),
                1,
            ))
            .unwrap();
    }
}

fn handle_errors_fatal(
    server: String,
    msg: &'static str,
    exit_code: i32,
) -> Box<dyn Fn(Error<ErrorResponse>) -> Infallible + Send> {
    assert_ne!(exit_code, 0, "Exit code must not be 0");
    Box::new(move |err: Error<ErrorResponse>| -> Infallible {
        match err {
            Error::ErrorResponse(e) => {
                eprintln!("{}", e.message);
                debug!("Details: {:#?}", e.details);
            }
            Error::InvalidRequest(s) => {
                eprintln!("{}: ", msg);
                error!("Invalid request ({})", s);
                error!("{}", UPGRADE_NOTICE);
            }
            Error::CommunicationError(e) => {
                eprint!("{}: ", msg);
                eprintln!("{}.", e);
                eprintln!("Check your network connection.");
            }
            Error::InvalidUpgrade(e) => {
                eprint!("{}: ", msg);
                eprintln!(
                    "Failed to upgrade connection ({}). Check your network connection.",
                    e
                );
            }
            Error::ResponseBodyError(e) => {
                eprintln!("{}: ", msg);
                error!(
                    "Unable to read the detailed error returned from {server} ({})",
                    e
                );
                error!("{}", UPGRADE_NOTICE);
            }
            Error::InvalidResponsePayload(b, e) => {
                eprintln!("{}", msg);
                if !b.is_empty() {
                    error!("Unable to parse the detailed response returned from `{server}`");
                    debug!("Parse Error: {:?}", e.to_string());
                    debug!("Response payload: {:?}", String::from_utf8_lossy(&b));
                } else {
                    error!("No detailed response (empty payload) returned from `{server}`");
                }
                error!("{}", UPGRADE_NOTICE);
            }
            Error::UnexpectedResponse(r) => {
                if r.status() == StatusCode::UNAUTHORIZED {
                    // The unauthorized error is often missing in the spec, and we can't currently have multiple
                    // return types until https://github.com/oxidecomputer/progenitor/pull/857 lands.
                    eprint!("{}: ", msg);
                    eprintln!("Unauthorized. Check your API key for {server}.");
                    if server.starts_with("http://") {
                        eprintln!("Did you mean to use https?");
                    }
                } else {
                    warn!(
                        "Unexpected error response from {server} -- this can happen if you're running different fda and feldera versions."
                    );
                    warn!("{}", UPGRADE_NOTICE);
                    debug!(
                        "Received HTTP status `{}` which is not declared as an expected response in OpenAPI.",
                        r.status()
                    );
                    std::io::stdout().flush().unwrap();
                    std::io::stderr().flush().unwrap();

                    eprint!("{}", msg);
                    let h = Handle::current();
                    // This spawns a separate thread because it's very hard to make this function async, I tried.
                    let st = std::thread::spawn(move || {
                        if let Ok(body) = h.block_on(r.text()) {
                            if let Ok(error) = serde_json::from_str::<ErrorResponse>(&body) {
                                eprintln!(": {}", error.message);
                            } else {
                                eprintln!(": {body}");
                            }
                        } else {
                            eprintln!();
                        }
                    });
                    st.join().unwrap();
                }
            }
            Error::PreHookError(e) => {
                eprintln!("{}: ", msg);
                error!("Unable to execute authentication pre-hook ({})", e);
                error!("{}", UPGRADE_NOTICE);
            }
            Error::PostHookError(e) => {
                eprintln!("{}: ", msg);
                eprint!("ERROR: Unable to execute authentication post-hook ({})", e);
                eprintln!("{}", UPGRADE_NOTICE);
            }
        };
        std::process::exit(exit_code);
    })
}

async fn api_key_commands(format: OutputFormat, action: ApiKeyActions, client: Client) {
    match action {
        ApiKeyActions::Create { name } => {
            debug!("Creating API key: {}", name);
            let response = client
                .post_api_key()
                .body(NewApiKeyRequest { name })
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to create API key",
                    1,
                ))
                .unwrap();
            match format {
                OutputFormat::Text => {
                    println!("API key '{}' created: {}", response.name, response.api_key);
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize API key response")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        ApiKeyActions::Delete { name } => {
            debug!("Deleting API key: {}", name);
            client
                .delete_api_key()
                .api_key_name(name.as_str())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to delete API key",
                    1,
                ))
                .unwrap();
            println!("API key '{}' deleted", name);
        }
        ApiKeyActions::List => {
            debug!("Listing API keys");
            let response = client
                .list_api_keys()
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to list API keys",
                    1,
                ))
                .unwrap();
            match format {
                OutputFormat::Text => {
                    let mut rows = vec![];
                    rows.push(["name".to_string(), "id".to_string()]);
                    for key in response.iter() {
                        rows.push([key.name.to_string(), key.id.0.to_string()]);
                    }
                    println!(
                        "{}",
                        Builder::from_iter(rows).build().with(Style::rounded())
                    );
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize API key list")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
    }
}

async fn pipelines(format: OutputFormat, client: Client) {
    debug!("Listing pipelines");
    let response = client
        .list_pipelines()
        .send()
        .await
        .map_err(handle_errors_fatal(
            client.baseurl().clone(),
            "Failed to list pipelines",
            1,
        ))
        .unwrap();
    let mut rows = vec![];
    rows.push(["name".to_string(), "status".to_string()]);
    for pipeline in response.iter() {
        rows.push([
            pipeline.name.to_string(),
            pipeline.deployment_status.to_string(),
        ]);
    }
    match format {
        OutputFormat::Text => {
            println!(
                "{}",
                Builder::from_iter(rows).build().with(Style::rounded())
            );
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response.into_inner())
                    .expect("Failed to serialize pipeline list")
            );
        }
        _ => {
            eprintln!("Unsupported output format: {}", format);
            std::process::exit(1);
        }
    }
}

fn patch_runtime_config(
    rc: &mut RuntimeConfig,
    key: RuntimeConfigKey,
    value: &str,
) -> Result<(), ()> {
    match key {
        RuntimeConfigKey::Workers => {
            rc.workers = value.parse().map_err(|_| ())?;
        }
        RuntimeConfigKey::Storage => {
            let enable: bool = value.parse().map_err(|_| ())?;
            if enable != rc.storage.is_some() {
                rc.storage = enable.then(StorageOptions::default);
            }
        }
        RuntimeConfigKey::FaultTolerance => {
            rc.fault_tolerance.model = match value {
                "false" | "none" => None,
                "true" => Some(FtModel::default()),
                _ => Some(value.parse().map_err(|_| ())?),
            }
        }
        RuntimeConfigKey::CheckpointInterval => {
            rc.fault_tolerance.checkpoint_interval_secs = match value.parse().map_err(|_| ())? {
                0 => None,
                interval => Some(interval),
            };
        }
        RuntimeConfigKey::CpuProfiler => {
            rc.cpu_profiler = value.parse().map_err(|_| ())?;
        }
        RuntimeConfigKey::Tracing => {
            rc.tracing = value.parse().map_err(|_| ())?;
        }
        RuntimeConfigKey::TracingEndpointJaeger => {
            rc.tracing_endpoint_jaeger = value.parse().map_err(|_| ())?;
        }
        RuntimeConfigKey::MinBatchSizeRecords => {
            rc.min_batch_size_records = value.parse().map_err(|_| ())?;
        }
        RuntimeConfigKey::MaxBufferingDelayUsecs => {
            rc.max_buffering_delay_usecs = value.parse().map_err(|_| ())?;
        }
        RuntimeConfigKey::CpuCoresMin => {
            rc.resources.cpu_cores_min = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::CpuCoresMax => {
            rc.resources.cpu_cores_max = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::MemoryMbMin => {
            rc.resources.memory_mb_min = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::MemoryMbMax => {
            rc.resources.memory_mb_max = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::StorageMbMax => {
            rc.resources.storage_mb_max = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::StorageClass => {
            rc.resources.storage_class = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::MinStorageBytes => {
            if let Some(storage) = rc.storage.as_mut() {
                storage.min_storage_bytes = Some(value.parse().map_err(|_| ())?);
            }
        }
        RuntimeConfigKey::ClockResolutionUsecs => {
            rc.clock_resolution_usecs = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::Logging => {
            rc.logging = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::HttpWorkers => {
            rc.http_workers = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::IoWorkers => {
            rc.io_workers = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::DevTweaks => {
            rc.dev_tweaks = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(value)
                .map_err(|_| ())?;
        }
    };

    Ok(())
}

/// Reads a file, returning the content as a string.
///
/// If the file path is `None`, returns `None`.
/// If the file cannot be read, returns `Err(())`.
async fn read_file(file_path: Option<String>) -> Result<Option<String>, ()> {
    if let Some(path) = file_path {
        match tokio::fs::read_to_string(path.as_str()).await {
            Ok(udf_code) => {
                debug!("Read from file: {}", path);
                Ok(Some(udf_code))
            }
            Err(e) => {
                eprintln!("Failed to read '{}': {}", path, e);
                Err(())
            }
        }
    } else {
        Ok(None)
    }
}

async fn read_program_code(
    program_path: Option<String>,
    stdin: bool,
) -> Result<Option<String>, ()> {
    if stdin {
        let mut program_code = String::new();
        let mut stdin = std::io::stdin();
        if stdin.read_to_string(&mut program_code).is_ok() {
            debug!("Read program code from stdin");
            Ok(Some(program_code))
        } else {
            eprintln!("Failed to read program code from stdin");
            Err(())
        }
    } else {
        read_file(program_path).await
    }
}

async fn wait_for_status(
    client: &Client,
    name: String,
    wait_for: CombinedStatus,
    waiting_text: &str,
) {
    wait_for_status_one_of(client, name, &[wait_for], waiting_text).await;
}

async fn wait_for_status_one_of(
    client: &Client,
    name: String,
    wait_for: &[CombinedStatus],
    waiting_text: &str,
) -> CombinedStatus {
    let mut print_every_30_seconds = Instant::now();
    loop {
        let pc = client
            .get_pipeline()
            .pipeline_name(name.clone())
            .send()
            .await
            .map_err(handle_errors_fatal(
                client.baseurl().clone(),
                "Failed to get program config",
                1,
            ))
            .unwrap();
        if wait_for.contains(&pc.deployment_status) {
            return pc.deployment_status;
        }
        if print_every_30_seconds.elapsed().as_secs() > 30 {
            info!("{}", waiting_text);
            print_every_30_seconds = Instant::now();
        }

        if let Some(deployment_error) = &pc.deployment_error {
            if deployment_error.error_code == "StartFailedDueToFailedCompilation" {
                eprintln!("Pipeline failed to start due to the following compilation error:");
                eprintln!();
                eprintln!(
                    "{}",
                    deployment_error
                        .details
                        .as_object()
                        .unwrap_or(&serde_json::Map::new())
                        .get("compiler_error")
                        .and_then(|e| e.as_str())
                        .unwrap_or_default()
                );
            } else {
                eprintln!("{}", deployment_error.message);
            }
            std::process::exit(1);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_storage_status(
    client: &Client,
    name: String,
    wait_for: StorageStatus,
    waiting_text: &str,
) {
    let mut print_every_30_seconds = Instant::now();
    let mut is_transitioning = true;
    while is_transitioning {
        let pc = client
            .get_pipeline()
            .pipeline_name(name.clone())
            .send()
            .await
            .map_err(handle_errors_fatal(
                client.baseurl().clone(),
                "Failed to get program config",
                1,
            ))
            .unwrap();
        is_transitioning = pc.storage_status != wait_for;
        if print_every_30_seconds.elapsed().as_secs() > 30 {
            info!("{}", waiting_text);
            print_every_30_seconds = Instant::now();
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_checkpoint(client: &Client, name: String, seq_number: u64, waiting_text: &str) {
    let mut print_every_30_seconds = Instant::now();
    let mut is_waiting_for_checkpoint = true;
    while is_waiting_for_checkpoint {
        let cs = client
            .get_checkpoint_status()
            .pipeline_name(name.clone())
            .send()
            .await
            .map_err(handle_errors_fatal(
                client.baseurl().clone(),
                "Failed to get pipeline checkpoint status",
                1,
            ))
            .unwrap();
        debug!("Checkpoint status {:?}", cs);

        is_waiting_for_checkpoint = cs.success.map(|c| c < seq_number).unwrap_or(true);
        if !is_waiting_for_checkpoint {
            // We have a checkpoint at least as recent as the requested sequence number
            break;
        }

        if let Some(failure) = &cs.failure {
            if failure.sequence_number < seq_number {
                continue;
            }
            eprintln!(
                "Pipeline failed to take the checkpoint#{} due to the following error:",
                failure.sequence_number
            );
            eprintln!();
            eprintln!("{}", failure.error);
            std::process::exit(1);
        }

        if print_every_30_seconds.elapsed().as_secs() > 30 {
            info!("{}", waiting_text);
            print_every_30_seconds = Instant::now();
        }

        sleep(Duration::from_millis(500)).await;
    }
}

async fn pipeline(format: OutputFormat, action: PipelineAction, client: Client) {
    match action {
        PipelineAction::Create {
            name,
            program_path,
            runtime_version,
            profile,
            udf_rs,
            udf_toml,
            stdin,
        } => {
            if let (Ok(program_code), Ok(udf_rust), Ok(udf_toml)) = (
                read_program_code(program_path, stdin).await,
                read_file(udf_rs).await,
                read_file(udf_toml).await,
            ) {
                let response = client
                    .post_pipeline()
                    .body(PostPutPipeline {
                        description: None,
                        name: name.to_string(),
                        program_code: program_code.unwrap_or_default(),
                        udf_rust,
                        udf_toml,
                        program_config: Some(ProgramConfig {
                            cache: true,
                            profile: Some(profile),
                            runtime_version,
                        }),
                        runtime_config: None,
                    })
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl().clone(),
                        "Failed to create pipeline",
                        1,
                    ))
                    .unwrap();
                match format {
                    OutputFormat::Text => {
                        println!("Pipeline created successfully.");
                        debug!("{:#?}", response);
                    }
                    OutputFormat::Json => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&response.into_inner())
                                .expect("Failed to serialize pipeline response")
                        );
                    }
                    _ => {
                        eprintln!("Unsupported output format: {}", format);
                        std::process::exit(1);
                    }
                }
            } else {
                // Already reported error in read_program_code or read_file.
                std::process::exit(1);
            }
        }
        PipelineAction::Start {
            name,
            recompile,
            no_wait,
            initial,
            bootstrap_policy,
        } => {
            if initial != "standby" && initial != "paused" && initial != "running" {
                eprintln!("Unsupported `--initial`: {}", initial);
                std::process::exit(1);
            }

            if bootstrap_policy != "allow"
                && bootstrap_policy != "reject"
                && bootstrap_policy != "await_approval"
            {
                // TODO: strong typing

                eprintln!("Unsupported `--bootstrap-policy`: {}", bootstrap_policy);
                std::process::exit(1);
            }

            // Force recompilation by adding/removing a space at the end of the program code
            // and disabling the compilation cache
            let pc = client
                .get_pipeline()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get program config",
                    1,
                ))
                .unwrap();

            if recompile {
                let cd =
                    CacheDisabler::new(name.clone(), &client, pc.program_config.clone().unwrap())
                        .await;
                cd.disable().await;

                let new_program = if pc
                    .program_code
                    .as_ref()
                    .unwrap()
                    .ends_with(|c: char| c.is_whitespace())
                {
                    pc.program_code.as_ref().unwrap().trim().to_string()
                } else {
                    pc.program_code.clone().unwrap() + " "
                };

                client
                    .patch_pipeline()
                    .pipeline_name(name.clone())
                    .body(PatchPipeline {
                        description: None,
                        name: None,
                        program_code: Some(new_program),
                        udf_rust: None,
                        udf_toml: None,
                        program_config: None,
                        runtime_config: None,
                    })
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl().clone(),
                        "Failed to recompile pipeline",
                        1,
                    ))
                    .unwrap();

                info!("Forcing recompilation this may take a few seconds...");
                cd.restore().await;
            };

            let mut print_every_30_seconds = Instant::now();
            let mut compiling = true;
            while compiling {
                let pc = client
                    .get_pipeline()
                    .pipeline_name(name.clone())
                    .selector(PipelineFieldSelector::Status)
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl().clone(),
                        "Failed to get program config",
                        1,
                    ))
                    .unwrap();
                compiling = matches!(pc.program_status, ProgramStatus::Pending)
                    || matches!(pc.program_status, ProgramStatus::CompilingRust)
                    || matches!(pc.program_status, ProgramStatus::CompilingSql)
                    || matches!(pc.program_status, ProgramStatus::SqlCompiled);

                if print_every_30_seconds.elapsed().as_secs() > 30 {
                    info!("Compiling pipeline...");
                    print_every_30_seconds = Instant::now();
                }
                sleep(Duration::from_millis(500)).await;
            }

            let response = client
                .post_pipeline_start()
                .pipeline_name(name.clone())
                .initial(&initial)
                .bootstrap_policy(&bootstrap_policy)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to start pipeline",
                    1,
                ))
                .unwrap();

            if !no_wait {
                let status = wait_for_status_one_of(
                    &client,
                    name.clone(),
                    if initial == "running" {
                        &[CombinedStatus::Running, CombinedStatus::AwaitingApproval]
                    } else if initial == "paused" {
                        &[CombinedStatus::Paused, CombinedStatus::AwaitingApproval]
                    } else {
                        &[CombinedStatus::Standby, CombinedStatus::AwaitingApproval]
                    },
                    "Starting the pipeline...",
                )
                .await;
                if status == CombinedStatus::AwaitingApproval {
                    let diff = client
                        .get_pipeline()
                        .pipeline_name(name.clone())
                        .selector(PipelineFieldSelector::Status)
                        .send()
                        .await
                        .map_err(handle_errors_fatal(
                            client.baseurl().clone(),
                            "Failed to get pipeline status",
                            1,
                        ))
                        .unwrap()
                        .deployment_runtime_status_details
                        .clone();

                    println!("Pipeline definition has changed since the last checkpoint. The pipeline is awaiting approval to proceed with bootstrapping the modified components. Run 'fda approve' to approve the changes or 'fda stop' to terminate the pipeline. Summary of changes:");

                    let diff_str = match diff {
                        // Normally shouldn't happen.
                        None => "<not available>".to_string(),
                        Some(diff) => match serde_json::from_str::<serde_json::Value>(diff) {
                            Ok(diff_json) => match format {
                                OutputFormat::Text => json_to_table(diff_json)
                                    .collapse()
                                    .into_pool_table()
                                    .to_string(),
                                OutputFormat::Json => {
                                    format!(
                                        "{}",
                                        serde_json::to_string_pretty(response.as_ref())
                                            .expect("Failed to serialize pipeline diff")
                                    )
                                }
                                _ => {
                                    format!("<unsupported output format: {}>", format)
                                }
                            },
                            // Also shouldn't happen.
                            Err(_) => diff,
                        },
                    };
                    println!("{}", diff_str);
                } else {
                    println!("Pipeline started successfully.");
                }
            }

            trace!("{:#?}", response);
        }
        PipelineAction::Checkpoint { name, no_wait } => {
            let response = client
                .checkpoint_pipeline()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to initiate pipeline checkpoint",
                    1,
                ))
                .unwrap();
            trace!("{:#?}", response);
            let checkpoint_sequence_number = response.into_inner().checkpoint_sequence_number;
            if !no_wait {
                wait_for_checkpoint(
                    &client,
                    name.clone(),
                    checkpoint_sequence_number,
                    "Taking a checkpoint...",
                )
                .await;
                println!(
                    "Pipeline checkpoint (#{}) taken successfully.",
                    checkpoint_sequence_number
                );
            }
        }
        PipelineAction::Approve { name } => {
            let response = client
                .post_pipeline_approve()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to approve pipeline changes",
                    1,
                ))
                .unwrap();
            println!("Pipeline changes approved successfully.");
            trace!("{:#?}", response);
        }
        PipelineAction::Pause { name, no_wait } => {
            let response = client
                .post_pipeline_pause()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to pause pipeline",
                    1,
                ))
                .unwrap();

            if !no_wait {
                wait_for_status(
                    &client,
                    name.clone(),
                    CombinedStatus::Paused,
                    "Pausing the pipeline...",
                )
                .await;
                println!("Pipeline paused successfully.");
            }
            trace!("{:#?}", response);
        }
        PipelineAction::Resume { name, no_wait } => {
            let response = client
                .post_pipeline_resume()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to resume pipeline",
                    1,
                ))
                .unwrap();

            if !no_wait {
                wait_for_status(
                    &client,
                    name.clone(),
                    CombinedStatus::Running,
                    "Resuming the pipeline...",
                )
                .await;
                println!("Pipeline resumed successfully.");
            }
            trace!("{:#?}", response);
        }
        PipelineAction::Restart {
            name,
            recompile,
            checkpoint,
            no_wait,
            initial,
            bootstrap_policy,
        } => {
            let current_status = client
                .get_pipeline()
                .pipeline_name(name.clone())
                .selector(PipelineFieldSelector::Status)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get pipeline status",
                    1,
                ))
                .unwrap();

            let _r = client
                .post_pipeline_stop()
                .pipeline_name(name.clone())
                .force(!checkpoint)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to stop pipeline",
                    1,
                ))
                .unwrap();
            wait_for_status(
                &client,
                name.clone(),
                CombinedStatus::Stopped,
                "Stopping the pipeline...",
            )
            .await;

            if current_status.deployment_status != CombinedStatus::Stopped {
                println!("Pipeline stop successful.");
            }

            let _ = Box::pin(pipeline(
                format,
                PipelineAction::Start {
                    name,
                    recompile,
                    no_wait,
                    initial,
                    bootstrap_policy,
                },
                client,
            ))
            .await;
        }
        PipelineAction::Stop {
            name,
            no_wait,
            checkpoint,
        } => {
            let response = client
                .post_pipeline_stop()
                .pipeline_name(name.clone())
                .force(!checkpoint)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to stop pipeline",
                    1,
                ))
                .unwrap();

            if !no_wait {
                wait_for_status(
                    &client,
                    name.clone(),
                    CombinedStatus::Stopped,
                    "Shutting down the pipeline...",
                )
                .await;
                println!("Pipeline stopped successfully.");
            }

            trace!("{:#?}", response);
        }
        PipelineAction::Clear { name, no_wait } => {
            let response = client
                .post_pipeline_clear()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to clear pipeline",
                    1,
                ))
                .unwrap();

            trace!("{:#?}", response);

            if !no_wait {
                wait_for_storage_status(
                    &client,
                    name.clone(),
                    StorageStatus::Cleared,
                    "Clearing pipeline...",
                )
                .await;
                println!("Pipeline cleared successfully.");
            }
        }
        PipelineAction::Stats { name } => {
            let response = client
                .get_pipeline_stats()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get pipeline stats",
                    1,
                ))
                .unwrap();

            match format {
                OutputFormat::Text => {
                    let table = json_to_table(&serde_json::Value::Object(response.into_inner()))
                        .collapse()
                        .into_pool_table()
                        .to_string();
                    println!("{}", table);
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(response.as_ref())
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::Metrics { name } => {
            let format = match format {
                OutputFormat::Json => MetricsFormat::Json,
                OutputFormat::Prometheus => MetricsFormat::Prometheus,
                _ => {
                    eprintln!("`{format}` is not supported as a metrics format (use `--format json` or `--format prometheus`)");
                    std::process::exit(1);
                }
            };
            let response = client
                .get_pipeline_metrics()
                .format(format)
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get pipeline metrics",
                    1,
                ))
                .unwrap();

            let mut byte_stream = response.into_inner();
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(chunk) => match stdout().write_all(&chunk) {
                        Ok(_) => (),
                        Err(error) => {
                            eprintln!("write to stdout failed ({error})");
                            std::process::exit(1);
                        }
                    },
                    Err(e) => {
                        eprintln!("ERROR: Unable to read server response: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
        PipelineAction::Logs { name, watch } => {
            // The log endpoint is a stream that never ends. However, we want to be able for
            // fda to exit after a certain amount of time without any new logs to emulate
            // a log snapshot functionality.
            let mut first_line = true;
            let next_line_timeout = if !watch {
                Duration::from_millis(250)
            } else {
                Duration::MAX
            };

            let response = client.get_pipeline_logs().pipeline_name(name).send().await;
            match response {
                Ok(response) => {
                    let mut byte_stream = response.into_inner();
                    while let Ok(Some(chunk)) = timeout(
                        if first_line {
                            Duration::from_secs(5)
                        } else {
                            next_line_timeout
                        },
                        byte_stream.next(),
                    )
                    .await
                    {
                        match chunk {
                            Ok(chunk) => {
                                let text = String::from_utf8_lossy(&chunk);
                                print!("{}", text);
                                first_line = false;
                            }
                            Err(e) => {
                                eprintln!("ERROR: Unable to read server response: {}", e);
                                std::process::exit(1);
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("ERROR: Failed to get pipeline logs: {}", e);
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::Delete { name, force } => {
            if force {
                let _ = Box::pin(pipeline(
                    format,
                    PipelineAction::Clear {
                        name: name.clone(),
                        no_wait: false,
                    },
                    client.clone(),
                ))
                .await;
            }

            let response = client
                .delete_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to delete the pipeline",
                    1,
                ))
                .unwrap();
            println!("Pipeline deleted successfully.");
            trace!("{:#?}", response);
        }
        PipelineAction::Status { name } => {
            let response = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get pipeline status",
                    1,
                ))
                .unwrap();

            match format {
                OutputFormat::Text => {
                    let value = serde_json::to_value(json! {
                        {
                            "deployment_status": response.deployment_status,
                            "deployment_status_since": response.deployment_status_since,
                            "deployment_desired_status": response.deployment_desired_status.to_string(),
                            "deployment_error": response.deployment_error,
                            "program_status": response.program_status,
                            "program_status_since": response.program_status_since,
                        }
                    })
                        .unwrap();
                    let table = json_to_table(&value).collapse().to_string();
                    println!("{}", table);
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::Config { name } => {
            let response = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get pipeline config",
                    1,
                ))
                .unwrap();
            match format {
                OutputFormat::Text => {
                    let value = serde_json::to_value(&response.runtime_config).unwrap();
                    let table = json_to_table(&value).collapse().to_string();
                    println!("{}", table);
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.runtime_config)
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::SetConfig { name, key, value } => {
            let mut rc = client
                .get_pipeline()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get pipeline config",
                    1,
                ))
                .map(|response| response.runtime_config.clone())
                .unwrap()
                .unwrap();

            if patch_runtime_config(&mut rc, key, value.as_str()).is_err() {
                eprintln!(
                    "Failed to parse value '{}' for updating key {:?}",
                    value, key
                );
                std::process::exit(1);
            }

            let response = client
                .patch_pipeline()
                .pipeline_name(name)
                .body(PatchPipeline {
                    description: None,
                    name: None,
                    program_code: None,
                    udf_rust: None,
                    udf_toml: None,
                    program_config: None,
                    runtime_config: Some(rc),
                })
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to set runtime config",
                    1,
                ))
                .unwrap();
            match format {
                OutputFormat::Text => {
                    println!("Runtime config updated successfully.");
                    let value = serde_json::to_value(&response.runtime_config).unwrap();
                    let table = json_to_table(&value).collapse().to_string();
                    println!("{}", table);
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.runtime_config)
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::Program { action } => program(format, action, client).await,
        PipelineAction::Connector {
            name,
            relation_name,
            connector_name,
            action,
        } => {
            connector(
                format,
                name,
                relation_name.as_str(),
                connector_name.as_str(),
                action,
                client,
            )
            .await
        }
        PipelineAction::HeapProfile {
            name,
            pprof,
            output,
        } => {
            let response = client
                .get_pipeline_heap_profile()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to obtain heap profile",
                    1,
                ))
                .unwrap();
            let mut byte_stream = response.into_inner();
            let mut buffer = Vec::new();
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(chunk) => buffer.extend_from_slice(&chunk),
                    Err(e) => {
                        eprintln!("ERROR: Unable to read server response: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            match output {
                None => {
                    let mut tempfile = tempfile().unwrap_or_else(|e| {
                        eprintln!("ERROR: Failed to create temporary file: {e}");
                        std::process::exit(1);
                    });
                    tempfile.write_all(&buffer).unwrap();

                    let mut args = pprof.split_whitespace();
                    let command_name = args.next().expect("`pprof` command cannot be empty");
                    let mut command = Command::new(command_name);
                    command.args(args);
                    command.arg("/dev/stdin");
                    command.stdin(tempfile);
                    match command.status().await {
                        Err(e) => {
                            eprintln!("ERROR: Failed to execute `{command_name}` ({e}). `pprof` is probably not installed. Install it from <https://github.com/google/pprof>.");
                            std::process::exit(1);
                        }
                        Ok(exit_status) if !exit_status.success() => {
                            eprintln!("Child process exited with {exit_status}");
                            std::process::exit(1);
                        }
                        Ok(_success) => (),
                    }
                }
                Some(filename) => {
                    tokio::fs::write(&filename, buffer)
                        .await
                        .unwrap_or_else(|e| {
                            eprintln!("ERROR: Failed to write {}: {e}", filename.display());
                            std::process::exit(1);
                        });
                }
            }
        }
        PipelineAction::CircuitProfile { name, output } => {
            let response = client
                .get_pipeline_circuit_profile()
                .pipeline_name(&name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to obtain circuit profile",
                    1,
                ))
                .unwrap();
            let mut byte_stream = response.into_inner();
            let mut buffer = Vec::new();
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(chunk) => buffer.extend_from_slice(&chunk),
                    Err(e) => {
                        eprintln!("ERROR: Unable to read server response: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            match output {
                None => {
                    let (path, mut file) =
                        unique_file(format!("{name}-circuit-profile").as_str(), "zip")
                            .unwrap_or_else(|e| {
                                eprintln!("ERROR: Failed to create circuit profile file: {e}");
                                std::process::exit(1);
                            });

                    file.write_all(&buffer).unwrap_or_else(|e| {
                        eprintln!("ERROR: Failed to create temporary file: {e}");
                        std::process::exit(1);
                    });
                    println!("Circuit profile written to {}", path.display());
                }
                Some(filename) => {
                    tokio::fs::write(&filename, buffer)
                        .await
                        .unwrap_or_else(|e| {
                            eprintln!("ERROR: Failed to write {}: {e}", filename.display());
                            std::process::exit(1);
                        });
                    println!("Circuit profile written to {}", filename.display());
                }
            }
        }
        PipelineAction::SupportBundle {
            name,
            output,
            no_circuit_profile,
            no_heap_profile,
            no_metrics,
            no_logs,
            no_stats,
            no_pipeline_config,
            no_system_config,
        } => {
            let mut request = client.get_pipeline_support_bundle().pipeline_name(&name);

            // Add query parameters for disabled collections
            if no_circuit_profile {
                request = request.circuit_profile(false);
            }
            if no_heap_profile {
                request = request.heap_profile(false);
            }
            if no_metrics {
                request = request.metrics(false);
            }
            if no_logs {
                request = request.logs(false);
            }
            if no_stats {
                request = request.stats(false);
            }
            if no_pipeline_config {
                request = request.pipeline_config(false);
            }
            if no_system_config {
                request = request.system_config(false);
            }

            let response = request
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to obtain support bundle",
                    1,
                ))
                .unwrap();
            let mut byte_stream = response.into_inner();
            let mut buffer = Vec::new();
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(chunk) => buffer.extend_from_slice(&chunk),
                    Err(e) => {
                        eprintln!("ERROR: Unable to read server response: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            match output {
                None => {
                    let (path, mut file) =
                        unique_file(format!("{name}-support-bundle").as_str(), "zip")
                            .unwrap_or_else(|e| {
                                eprintln!("ERROR: Failed to create circuit profile file: {e}");
                                std::process::exit(1);
                            });

                    file.write_all(&buffer).unwrap_or_else(|e| {
                        eprintln!("ERROR: Failed to create temporary file: {e}");
                        std::process::exit(1);
                    });
                    println!("Circuit profile written to {}", path.display());
                }
                Some(filename) => {
                    tokio::fs::write(&filename, buffer)
                        .await
                        .unwrap_or_else(|e| {
                            eprintln!("ERROR: Failed to write {}: {e}", filename.display());
                            std::process::exit(1);
                        });
                    println!("Support bundle written to {}", filename.display());
                }
            }
        }
        PipelineAction::Shell {
            name,
            start,
            initial,
            bootstrap_policy,
        } => {
            let client2 = client.clone();
            if start {
                let _ = Box::pin(pipeline(
                    format,
                    PipelineAction::Start {
                        name: name.clone(),
                        recompile: false,
                        no_wait: false,
                        initial,
                        bootstrap_policy,
                    },
                    client,
                ))
                .await;
            }
            shell(format, name, client2).await
        }
        PipelineAction::Query { name, sql, stdin } => {
            handle_adhoc_query(client, format, name, sql, stdin).await
        }
        PipelineAction::CompletionToken {
            name,
            table,
            connector,
        } => {
            let response = client
                .completion_token()
                .pipeline_name(name)
                .table_name(table)
                .connector_name(connector)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get completion status",
                    1,
                ))
                .unwrap();

            match format {
                OutputFormat::Text => {
                    println!("{}", &response.into_inner().token.to_string());
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!(
                        "Unsupported output format: {}, falling back to text",
                        format
                    );
                    println!("{}", &response.into_inner().token.to_string());
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::CompletionStatus { name, token } => {
            let response = client
                .completion_status()
                .pipeline_name(name)
                .token(token)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get completion status",
                    1,
                ))
                .unwrap();

            match format {
                OutputFormat::Text => {
                    println!("{}", &response.into_inner().status.to_string());
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!(
                        "Unsupported output format, falling back to text: {}",
                        format
                    );
                    println!("{}", &response.into_inner().status.to_string());
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::StartTransaction { name } => {
            let response = client
                .start_transaction()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to start transaction",
                    1,
                ))
                .unwrap();

            match format {
                OutputFormat::Text => {
                    println!(
                        "Transaction started successfully with ID: {}",
                        response.into_inner().transaction_id
                    );
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize transaction response")
                    );
                }
                _ => {
                    eprintln!(
                        "Unsupported output format, falling back to text: {}",
                        format
                    );
                    println!(
                        "Transaction started successfully with ID: {}",
                        response.into_inner().transaction_id
                    );
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::CommitTransaction {
            name,
            transaction_id,
            no_wait,
        } => {
            // First, get the current transaction ID from stats
            let stats = client
                .get_pipeline_stats()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get pipeline stats",
                    1,
                ))
                .unwrap();
            let current_transaction_id = stats
                .into_inner()
                .get("global_metrics")
                .and_then(|v| v.get("transaction_id"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as u64;

            // Check if there's no transaction in progress
            if current_transaction_id == 0 {
                eprintln!(
                    "Attempting to commit a transaction, but there is no transaction in progress"
                );
                std::process::exit(1);
            }

            // Verify transaction ID if provided
            if let Some(expected_transaction_id) = transaction_id {
                if current_transaction_id != expected_transaction_id {
                    eprintln!(
                        "Specified transaction {} doesn't match current active transaction {}",
                        expected_transaction_id, current_transaction_id
                    );
                    std::process::exit(1);
                }
            }

            let actual_transaction_id = current_transaction_id;

            let response = client
                .commit_transaction()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to commit transaction",
                    1,
                ))
                .unwrap();

            if !no_wait {
                // Wait for the transaction to commit by polling the stats
                loop {
                    let stats = client
                        .get_pipeline_stats()
                        .pipeline_name(name.clone())
                        .send()
                        .await
                        .map_err(handle_errors_fatal(
                            client.baseurl().clone(),
                            "Failed to get pipeline stats",
                            1,
                        ))
                        .unwrap();

                    let current_transaction_id = stats
                        .into_inner()
                        .get("global_metrics")
                        .and_then(|v| v.get("transaction_id"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0) as u64;

                    // If transaction_id is 0, it means the transaction has been committed
                    if current_transaction_id != actual_transaction_id {
                        break;
                    }

                    sleep(Duration::from_secs(1)).await;
                }
            }

            match format {
                OutputFormat::Text => {
                    println!("Transaction committed successfully.");
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize commit response")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        PipelineAction::Bench { args } => bench::bench(client, format, args).await,
    }
}

async fn connector(
    format: OutputFormat,
    pipeline_name: String,
    relation: &str,
    connector: &str,
    action: ConnectorAction,
    client: Client,
) {
    let rc = client
        .get_pipeline()
        .pipeline_name(pipeline_name.clone())
        .send()
        .await
        .map_err(handle_errors_fatal(
            client.baseurl().clone(),
            "Failed to get pipeline config",
            1,
        ))
        .map(|response| response.program_info.clone())
        .unwrap()
        .unwrap();

    let full_connector_name = format!("{relation}.{connector}");
    let relation_is_table = rc
        .input_connectors
        .iter()
        .any(|(name, _c)| *name == full_connector_name);
    let relation_is_view = rc
        .output_connectors
        .iter()
        .any(|(name, _c)| *name == full_connector_name);
    if !relation_is_table && !relation_is_view {
        eprintln!("No connector named {connector} found in pipeline {pipeline_name}");
        std::process::exit(1);
    }

    match action {
        ConnectorAction::Start => {
            if !relation_is_table {
                eprintln!("Can not start the output connector '{connector}'. Only input connectors (connectors attached to a table) can be started.");
                std::process::exit(1);
            }
            client
                .post_pipeline_input_connector_action()
                .pipeline_name(pipeline_name)
                .table_name(relation)
                .connector_name(connector)
                .action("start")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to start table connector",
                    1,
                ))
                .unwrap();
            println!("Table {relation} connector {connector} started successfully.");
        }
        ConnectorAction::Pause => {
            if !relation_is_table {
                eprintln!("Can not pause the output connector '{connector}'. Only input connectors (connectors attached to a table) can be paused.");
                std::process::exit(1);
            }
            client
                .post_pipeline_input_connector_action()
                .pipeline_name(pipeline_name)
                .table_name(relation)
                .connector_name(connector)
                .action("pause")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to pause table connector",
                    1,
                ))
                .unwrap();
            println!("Table {relation} connector {connector} paused successfully.");
        }
        ConnectorAction::Stats => {
            let response = if relation_is_table {
                client
                    .get_pipeline_input_connector_status()
                    .pipeline_name(pipeline_name)
                    .table_name(relation)
                    .connector_name(connector)
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl().clone(),
                        "Failed to get table connector stats",
                        1,
                    ))
                    .unwrap()
            } else {
                client
                    .get_pipeline_output_connector_status()
                    .pipeline_name(pipeline_name)
                    .view_name(relation)
                    .connector_name(connector)
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl().clone(),
                        "Failed to get view connector stats",
                        1,
                    ))
                    .unwrap()
            };

            match format {
                OutputFormat::Text => {
                    let table = json_to_table(&serde_json::Value::Object(response.into_inner()))
                        .collapse()
                        .into_pool_table()
                        .to_string();
                    println!("{}", table);
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
    };
}

async fn program(format: OutputFormat, action: ProgramAction, client: Client) {
    match action {
        ProgramAction::Get {
            name,
            udf_rs,
            udf_toml,
        } => {
            let response = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get program",
                    1,
                ))
                .unwrap();

            match format {
                OutputFormat::Text => {
                    if !udf_rs && !udf_toml {
                        println!("{}", response.program_code.clone().unwrap());
                    }
                    if udf_rs {
                        println!("{}", response.udf_rust.clone().unwrap());
                    }
                    if udf_toml {
                        println!("{}", response.udf_toml.clone().unwrap());
                    }
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        ProgramAction::Config { name } => {
            let response = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get program config",
                    1,
                ))
                .unwrap();
            match format {
                OutputFormat::Text => {
                    let value = serde_json::to_value(&response.program_config).unwrap();
                    let table = json_to_table(&value).collapse().to_string();
                    println!("{}", table);
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.program_config)
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        ProgramAction::SetConfig {
            name,
            profile,
            runtime_version,
        } => {
            let pp = PatchPipeline {
                description: None,
                name: None,
                program_code: None,
                udf_rust: None,
                udf_toml: None,
                program_config: Some(ProgramConfig {
                    profile,
                    cache: true,
                    runtime_version,
                }),
                runtime_config: None,
            };
            let response = client
                .patch_pipeline()
                .pipeline_name(name)
                .body(pp)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to set program config",
                    1,
                ))
                .unwrap();
            match format {
                OutputFormat::Text => {
                    println!("Program config updated successfully.");
                    let value = serde_json::to_value(&response.program_config).unwrap();
                    let table = json_to_table(&value).collapse().to_string();
                    println!("{}", table);
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.program_config)
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        ProgramAction::Status { name } => {
            let response = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl().clone(),
                    "Failed to get program status",
                    1,
                ))
                .unwrap();

            match format {
                OutputFormat::Text => {
                    println!(
                        "{:#?} (version {}, last updated {})",
                        response.program_status,
                        response.program_version.0,
                        response.program_status_since
                    );
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&response.into_inner())
                            .expect("Failed to serialize pipeline stats")
                    );
                }
                _ => {
                    eprintln!("Unsupported output format: {}", format);
                    std::process::exit(1);
                }
            }
        }
        ProgramAction::Set {
            name,
            program_path,
            udf_rs,
            udf_toml,
            stdin,
        } => {
            if let (Ok(program_code), Ok(udf_rust), Ok(udf_toml)) = (
                read_program_code(program_path, stdin).await,
                read_file(udf_rs).await,
                read_file(udf_toml).await,
            ) {
                let pp = PatchPipeline {
                    description: None,
                    name: None,
                    program_code,
                    udf_rust,
                    udf_toml,
                    program_config: None,
                    runtime_config: None,
                };
                let response = client
                    .patch_pipeline()
                    .pipeline_name(name)
                    .body(pp)
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl().clone(),
                        "Failed to set program code",
                        1,
                    ))
                    .unwrap();

                match format {
                    OutputFormat::Text => {
                        println!("Program updated successfully.");
                    }
                    OutputFormat::Json => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&response.into_inner())
                                .expect("Failed to serialize pipeline stats")
                        );
                    }
                    _ => {
                        eprintln!("Unsupported output format: {}", format);
                        std::process::exit(1);
                    }
                }
            } else {
                // Already reported error in read_program_code or read_file.
                std::process::exit(1);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    CompleteEnv::with_factory(Cli::command).complete();

    let mut cli = Cli::parse();
    let env = Env::default().default_filter_or("warn");
    let _r = env_logger::Builder::from_env(env)
        .format_target(false)
        .format_timestamp(None)
        .try_init();
    // If a `/` is at the end of the host, the manager returns HTML,
    // instead of an API response.
    //
    // Needs to be fixed in the manager routing but for compat reasons
    // we remove it here.
    if cli.host.ends_with("/") {
        cli.host = cli.host.trim_end_matches('/').to_string();
    }

    let client = || {
        make_client(cli.host, cli.insecure, cli.auth, cli.timeout)
            .map_err(|e| {
                eprintln!("Failed to create HTTP client: {}", e);
                std::process::exit(1);
            })
            .unwrap()
    };

    match cli.command {
        Commands::Apikey { action } => api_key_commands(cli.format, action, client()).await,
        Commands::Pipelines => pipelines(cli.format, client()).await,
        Commands::Pipeline(action) => pipeline(cli.format, action, client()).await,
        Commands::Debug { action } => debug::debug(action),
    }
}
