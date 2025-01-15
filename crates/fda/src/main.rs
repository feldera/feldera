//! A CLI App for the Feldera REST API.

use std::convert::Infallible;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};

use clap::{CommandFactory, Parser};
use clap_complete::CompleteEnv;
use feldera_types::config::{FtConfig, RuntimeConfig};
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
use tokio::time::{sleep, timeout, Duration};

mod cli;
mod shell;

#[allow(clippy::all, unused)]
mod cd {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

pub(crate) const UGPRADE_NOTICE: &str =
    "Try upgrading to the latest CLI version to resolve this issue or report it on github.com/feldera/feldera if you're already on the latest version.";

use crate::cd::types::*;
use crate::cd::*;
use crate::cli::*;
use crate::shell::shell;

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

/// Create a client with the given host, auth, and timeout.
pub(crate) fn make_client(
    host: String,
    auth: Option<String>,
    timeout: Option<u64>,
) -> Result<Client, Box<dyn std::error::Error>> {
    let mut client_builder = reqwest::ClientBuilder::new();

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

/// A helper struct that temporarily disables the cache for a pipeline
/// as long as it is in scope.
struct TemporaryCacheDisable {
    name: String,
    client: Client,
    original_pc: ProgramConfig,
}

impl TemporaryCacheDisable {
    async fn new(name: String, client: Client, original_pc: ProgramConfig) -> Self {
        TemporaryCacheDisable::set_cache_flag_disabled(
            client.clone(),
            name.clone(),
            original_pc.clone(),
            true,
        )
        .await;

        Self {
            name,
            client,
            original_pc,
        }
    }

    async fn set_cache_flag_disabled(
        client: Client,
        name: String,
        original_pc: ProgramConfig,
        disable: bool,
    ) {
        let pc = if disable {
            let mut disabled_cache_pc = original_pc.clone();
            disabled_cache_pc.cache = Some(false);
            disabled_cache_pc
        } else {
            original_pc.clone()
        };

        client
            .patch_pipeline()
            .pipeline_name(name.clone())
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
                client.baseurl.clone(),
                "Failed to enable/disable compilation cache",
                1,
            ))
            .unwrap();
    }
}

impl Drop for TemporaryCacheDisable {
    fn drop(&mut self) {
        if let Ok(handle) = Handle::try_current() {
            debug!("TemporaryCacheDisable::drop reset cache value to original setting");
            let client = self.client.clone();
            let name = self.name.clone();
            let original_pc = self.original_pc.clone();
            handle.spawn(TemporaryCacheDisable::set_cache_flag_disabled(
                client,
                name,
                original_pc,
                false,
            ));
        } else {
            // This shouldn't happen the way we currently run things
            unreachable!("No Tokio runtime available for re-enabling the cache.");
        }
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
                error!("{}", UGPRADE_NOTICE);
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
                error!("{}", UGPRADE_NOTICE);
            }
            Error::InvalidResponsePayload(b, e) => {
                eprintln!("{}", msg);
                error!("Unable to parse the detailed response returned from {server}");
                if !b.is_empty() {
                    debug!("Parse Error: {:?}", e.to_string());
                    debug!("Response payload: {:?}", String::from_utf8_lossy(&b));
                }
                error!("{}", UGPRADE_NOTICE);
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
                    warn!("{}", UGPRADE_NOTICE);

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
                error!("{}", UGPRADE_NOTICE);
            }
            Error::PostHookError(e) => {
                eprintln!("{}: ", msg);
                eprint!("ERROR: Unable to execute authentication post-hook ({})", e);
                eprintln!("{}", UGPRADE_NOTICE);
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
                    client.baseurl,
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
                    client.baseurl,
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
                    client.baseurl,
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
            client.baseurl,
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
            rc.storage = value.parse().map_err(|_| ())?;
        }
        RuntimeConfigKey::FaultTolerance => {
            let enable: bool = value.parse().map_err(|_| ())?;
            rc.fault_tolerance = match enable {
                false => None,
                true => Some(FtConfig::default()),
            }
        }
        RuntimeConfigKey::CheckpointInterval => {
            let ft = rc.fault_tolerance.get_or_insert_with(FtConfig::default);
            ft.checkpoint_interval_secs = value.parse().map_err(|_| ())?;
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
            rc.min_storage_bytes = Some(value.parse().map_err(|_| ())?);
        }
        RuntimeConfigKey::ClockResolutionUsecs => {
            rc.clock_resolution_usecs = Some(value.parse().map_err(|_| ())?);
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
    wait_for: PipelineStatus,
    waiting_text: &str,
) {
    let mut print_every_30_seconds = tokio::time::Instant::now();
    let mut is_transitioning = true;
    while is_transitioning {
        let pc = client
            .get_pipeline()
            .pipeline_name(name.clone())
            .send()
            .await
            .map_err(handle_errors_fatal(
                client.baseurl.clone(),
                "Failed to get program config",
                1,
            ))
            .unwrap();
        is_transitioning = pc.deployment_status != wait_for;
        if print_every_30_seconds.elapsed().as_secs() > 30 {
            info!("{}", waiting_text);
            print_every_30_seconds = tokio::time::Instant::now();
        }

        if wait_for != PipelineStatus::Shutdown && pc.deployment_status == PipelineStatus::Failed {
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
            } else {
                eprintln!("Pipeline failed to reach status {:?}", wait_for);
            }
            std::process::exit(1);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn pipeline(format: OutputFormat, action: PipelineAction, client: Client) {
    match action {
        PipelineAction::Create {
            name,
            program_path,
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
                        program_config: Some(profile.into()),
                        runtime_config: None,
                    })
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl,
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
        } => {
            // Force recompilation by adding/removing a space at the end of the program code
            // and disabling the compilation cache
            let pc = client
                .get_pipeline()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl.clone(),
                    "Failed to get program config",
                    1,
                ))
                .unwrap();

            let _cd = if recompile {
                let cd = TemporaryCacheDisable::new(
                    name.clone(),
                    client.clone(),
                    pc.program_config.clone().unwrap(),
                )
                .await;
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
                        client.baseurl.clone(),
                        "Failed to recompile pipeline",
                        1,
                    ))
                    .unwrap();
                info!("Forcing recompilation this may take a few seconds...");
                Some(cd)
            } else {
                None
            };

            let mut print_every_30_seconds = tokio::time::Instant::now();
            let mut compiling = true;
            while compiling {
                let pc = client
                    .get_pipeline()
                    .pipeline_name(name.clone())
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl.clone(),
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
                    print_every_30_seconds = tokio::time::Instant::now();
                }
                sleep(Duration::from_millis(500)).await;
            }

            let response = client
                .post_pipeline_action()
                .pipeline_name(name.clone())
                .action("start")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl.clone(),
                    "Failed to start pipeline",
                    1,
                ))
                .unwrap();

            if !no_wait {
                wait_for_status(
                    &client,
                    name.clone(),
                    PipelineStatus::Running,
                    "Starting the pipeline...",
                )
                .await;
                println!("Pipeline started successfully.");
            }

            trace!("{:#?}", response);
        }
        PipelineAction::Checkpoint { name } => {
            let response = client
                .checkpoint_pipeline()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl.clone(),
                    "Failed to checkpoint pipeline",
                    1,
                ))
                .unwrap();
            trace!("{:#?}", response);
        }
        PipelineAction::Pause { name, no_wait } => {
            let response = client
                .post_pipeline_action()
                .pipeline_name(name.clone())
                .action("pause")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl.clone(),
                    "Failed to pause pipeline",
                    1,
                ))
                .unwrap();

            if !no_wait {
                wait_for_status(
                    &client,
                    name.clone(),
                    PipelineStatus::Paused,
                    "Pausing the pipeline...",
                )
                .await;
                println!("Pipeline paused successfully.");
            }
            trace!("{:#?}", response);
        }
        PipelineAction::Restart {
            name,
            recompile,
            no_wait,
        } => {
            let current_status = client
                .get_pipeline()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl.clone(),
                    "Failed to get pipeline status",
                    1,
                ))
                .unwrap();

            let _r = client
                .post_pipeline_action()
                .pipeline_name(name.clone())
                .action("shutdown")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl.clone(),
                    "Failed to stop pipeline",
                    1,
                ))
                .unwrap();
            wait_for_status(
                &client,
                name.clone(),
                PipelineStatus::Shutdown,
                "Shutting down the pipeline...",
            )
            .await;

            if current_status.deployment_status != PipelineStatus::Shutdown {
                println!("Pipeline shutdown successful.");
            }

            let _ = Box::pin(pipeline(
                format,
                PipelineAction::Start {
                    name,
                    recompile,
                    no_wait,
                },
                client,
            ))
            .await;
        }
        PipelineAction::Shutdown { name, no_wait } => {
            let response = client
                .post_pipeline_action()
                .pipeline_name(name.clone())
                .action("shutdown")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl.clone(),
                    "Failed to stop pipeline",
                    1,
                ))
                .unwrap();

            if !no_wait {
                wait_for_status(
                    &client,
                    name.clone(),
                    PipelineStatus::Shutdown,
                    "Shutting down the pipeline...",
                )
                .await;
                println!("Pipeline shutdown successful.");
            }

            trace!("{:#?}", response);
        }
        PipelineAction::Stats { name } => {
            let response = client
                .get_pipeline_stats()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
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
        PipelineAction::Delete { name } => {
            let response = client
                .delete_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
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
                    client.baseurl,
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
            }
        }
        PipelineAction::Config { name } => {
            let response = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
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
            }
        }
        PipelineAction::SetConfig { name, key, value } => {
            let mut rc = client
                .get_pipeline()
                .pipeline_name(name.clone())
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl.clone(),
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
                return;
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
                    client.baseurl.clone(),
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
            }
        }
        PipelineAction::Program { action } => program(format, action, client).await,
        PipelineAction::TableConnector {
            name,
            table_name,
            connector_name,
            action,
        } => {
            table_connector(
                name,
                table_name.as_str(),
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
                    client.baseurl.clone(),
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
        PipelineAction::Shell { name, start } => {
            let client2 = client.clone();
            if start {
                let _ = Box::pin(pipeline(
                    format,
                    PipelineAction::Start {
                        name: name.clone(),
                        recompile: false,
                        no_wait: false,
                    },
                    client,
                ))
                .await;
            }
            shell(format, name, client2).await
        }
        PipelineAction::Query { name, sql, stdin } => {
            let format = match format {
                OutputFormat::Text => "text",
                OutputFormat::Json => "json",
            };

            let response = client
                .pipeline_adhoc_sql()
                .pipeline_name(name)
                .format(format)
                .sql(sql.unwrap_or_else(|| {
                    if stdin {
                        let mut program_code = String::new();
                        let mut stdin_stream = std::io::stdin();
                        if stdin_stream.read_to_string(&mut program_code).is_ok() {
                            debug!("Read SQL from stdin");
                            program_code
                        } else {
                            eprintln!("Failed to read SQL from stdin");
                            std::process::exit(1);
                        }
                    } else {
                        eprintln!("`query` command expects a SQL query or a pipe from stdin. For example, `fda query p1 'select * from foo'` or `echo 'select * from foo' | fda query p1`");
                        std::process::exit(1);
                    }
                }))
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
                    "Failed to execute SQL query",
                    1,
                ))
                .unwrap();

            let mut byte_stream = response.into_inner();
            while let Some(chunk) = byte_stream.next().await {
                let mut buffer = Vec::new();
                match chunk {
                    Ok(chunk) => buffer.extend_from_slice(&chunk),
                    Err(e) => {
                        eprintln!("ERROR: Unable to read server response: {}", e);
                        std::process::exit(1);
                    }
                }
                let text = String::from_utf8_lossy(&buffer);
                print!("{}", text);
            }
            println!()
        }
    }
}

async fn table_connector(
    pipeline_name: String,
    table_name: &str,
    connector_name: &str,
    action: ConnectorAction,
    client: Client,
) {
    match action {
        ConnectorAction::Start => {
            client
                .post_pipeline_input_connector_action()
                .pipeline_name(pipeline_name)
                .table_name(table_name)
                .connector_name(connector_name)
                .action("start")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
                    "Failed to start table connector",
                    1,
                ))
                .unwrap();
            println!("Table {table_name} connector {connector_name} started successfully.");
        }
        ConnectorAction::Pause => {
            client
                .post_pipeline_input_connector_action()
                .pipeline_name(pipeline_name)
                .table_name(table_name)
                .connector_name(connector_name)
                .action("pause")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
                    "Failed to pause table connector",
                    1,
                ))
                .unwrap();
            println!("Table {table_name} connector {connector_name} paused successfully.");
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
                    client.baseurl,
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
            }
        }
        ProgramAction::Config { name } => {
            let response = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
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
            }
        }
        ProgramAction::SetConfig { name, profile } => {
            let pp = PatchPipeline {
                description: None,
                name: None,
                program_code: None,
                udf_rust: None,
                udf_toml: None,
                program_config: Some(ProgramConfig {
                    profile: Some(profile.into()),
                    cache: None,
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
                    client.baseurl,
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
            }
        }
        ProgramAction::Status { name } => {
            let response = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
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
                        client.baseurl,
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
                }
            } else {
                // Already reported error in read_program_code or read_file.
                std::process::exit(1);
            }
        }
    }
}

fn debug(_format: OutputFormat, action: DebugActions) {
    match action {
        DebugActions::MsgpCat { path } => {
            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(error) => {
                    eprintln!("{}: open failed ({error})", path.display());
                    std::process::exit(1);
                }
            };
            loop {
                let value = match rmpv::decode::value::read_value(&mut file) {
                    Ok(value) => value,
                    Err(rmpv::decode::Error::InvalidMarkerRead(error))
                        if error.kind() == ErrorKind::UnexpectedEof =>
                    {
                        break
                    }
                    Err(error) => {
                        eprintln!("{}: read failed ({error})", path.display());
                        std::process::exit(1);
                    }
                };
                println!("{value}");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    CompleteEnv::with_factory(Cli::command).complete();

    let mut cli = Cli::parse();

    let _r = env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
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
        make_client(cli.host, cli.auth, cli.timeout)
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
        Commands::Debug { action } => debug(cli.format, action),
    }
}
