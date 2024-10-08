//! A CLI App for the Feldera REST API.

use std::convert::Infallible;
use std::io::{Read, Write};

use clap::{CommandFactory, Parser};
use clap_complete::CompleteEnv;
use feldera_types::config::RuntimeConfig;
use feldera_types::error::ErrorResponse;
use futures_util::StreamExt;
use log::{debug, error, info, trace, warn};
use reqwest::header::{HeaderMap, HeaderValue, InvalidHeaderValue};
use reqwest::StatusCode;
use tabled::builder::Builder;
use tabled::settings::Style;
use tempfile::tempfile;
use tokio::process::Command;
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

fn handle_errors_fatal(
    server: String,
    msg: &'static str,
    exit_code: i32,
) -> Box<dyn Fn(Error<ErrorResponse>) -> Infallible + Send> {
    assert_ne!(exit_code, 0, "Exit code must not be 0");
    Box::new(move |err: Error<ErrorResponse>| -> Infallible {
        match &err {
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
                    debug!("Response payload: {:?}", String::from_utf8_lossy(b));
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
                    eprintln!("{}: ", msg);
                    error!("Unexpected response from {server}: {:?}", r);
                    error!("{}", UGPRADE_NOTICE);
                }
            }
            Error::PreHookError(e) => {
                eprintln!("{}: ", msg);
                error!("Unable to execute authentication pre-hook ({})", e);
                error!("{}", UGPRADE_NOTICE);
            }
        };
        std::process::exit(exit_code);
    })
}

async fn api_key_commands(action: ApiKeyActions, client: Client) {
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
            println!("API key '{}' created: {}", response.name, response.api_key);
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
    }
}

async fn pipelines(client: Client) {
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
    println!(
        "{}",
        Builder::from_iter(rows).build().with(Style::rounded())
    );
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

async fn read_program_code(program_path: Option<String>, stdin: bool) -> Result<String, ()> {
    if program_path.is_none() && !stdin {
        eprintln!("No program code provided. Use `--stdin` to read from stdin.");
        return Err(());
    }
    let program_path = program_path.unwrap_or_else(|| "".to_string());
    if stdin {
        let mut program_code = String::new();
        let mut stdin = std::io::stdin();
        if stdin.read_to_string(&mut program_code).is_ok() {
            debug!("Read program code from stdin");
            Ok(program_code)
        } else {
            eprintln!("Failed to read program code from stdin");
            Err(())
        }
    } else if let Ok(program_code) = tokio::fs::read_to_string(program_path.as_str()).await {
        debug!("Read program code from file: {}", program_path);
        Ok(program_code)
    } else {
        eprintln!("Failed to read program code from file: {}", program_path);
        Err(())
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
                eprintln!("{}", deployment_error.message);
            } else {
                eprintln!("Pipeline failed to reach status {:?}", wait_for);
            }
            std::process::exit(1);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn pipeline(action: PipelineAction, client: Client) {
    match action {
        PipelineAction::Create {
            name,
            program_path,
            profile,
            stdin,
        } => {
            if let Ok(program_code) = read_program_code(program_path, stdin).await {
                let response = client
                    .post_pipeline()
                    .body(PipelineDescr {
                        description: "".to_string(),
                        name: name.to_string(),
                        program_code,
                        udf_rust: None,
                        udf_toml: None,
                        program_config: profile.into(),
                        runtime_config: RuntimeConfig::default(),
                    })
                    .send()
                    .await
                    .map_err(handle_errors_fatal(
                        client.baseurl,
                        "Failed to create pipeline",
                        1,
                    ))
                    .unwrap();
                println!("Pipeline created successfully.");
                debug!("{:#?}", response);
            } else {
                // Already reported error in read_program_code.
                std::process::exit(1);
            }
        }
        PipelineAction::Start {
            name,
            recompile,
            no_wait,
        } => {
            if recompile {
                // Force recompilation by adding/removing a space at the end of the program code.
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

                let new_program = if pc.program_code.ends_with(|c: char| c.is_whitespace()) {
                    pc.program_code.trim().to_string()
                } else {
                    pc.program_code.clone() + " "
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
            }

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
                    || matches!(pc.program_status, ProgramStatus::CompilingSql);

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
            println!(
                "{}",
                serde_json::to_string_pretty(response.as_ref())
                    .expect("Failed to serialize pipeline stats")
            );
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

            println!(
                "{}",
                serde_json::to_string_pretty(&response.into_inner())
                    .expect("Failed to serialize pipeline stats")
            );
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
            println!(
                "{}",
                serde_json::to_string_pretty(&response.runtime_config)
                    .expect("Failed to serialize pipeline stats")
            );
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
            println!("Runtime config updated successfully.");
            println!(
                "{}",
                serde_json::to_string_pretty(&response.runtime_config)
                    .expect("Failed to serialize pipeline stats")
            );
        }
        PipelineAction::Program { action } => program(action, client).await,
        PipelineAction::Endpoint {
            name,
            endpoint_name,
            action,
        } => endpoint(name, endpoint_name.as_str(), action, client).await,
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
                    PipelineAction::Start {
                        name: name.clone(),
                        recompile: false,
                        no_wait: false,
                    },
                    client,
                ))
                .await;
            }
            shell(name, client2).await
        }
        PipelineAction::Query { name, sql, stdin } => {
            let response = client
                .pipeline_adhoc_sql()
                .pipeline_name(name)
                .format("text")
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

async fn endpoint(
    pipeline_name: String,
    endpoint_name: &str,
    action: EndpointAction,
    client: Client,
) {
    match action {
        EndpointAction::Start => {
            client
                .input_endpoint_action()
                .pipeline_name(pipeline_name)
                .endpoint_name(endpoint_name)
                .action("start")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
                    "Failed to start endpoint",
                    1,
                ))
                .unwrap();
            println!("Endpoint {} started successfully.", endpoint_name);
        }
        EndpointAction::Pause => {
            client
                .input_endpoint_action()
                .pipeline_name(pipeline_name)
                .endpoint_name(endpoint_name)
                .action("pause")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
                    "Failed to pause endpoint",
                    1,
                ))
                .unwrap();
            println!("Endpoint {} paused successfully.", endpoint_name);
        }
    };
}

async fn program(action: ProgramAction, client: Client) {
    match action {
        ProgramAction::Get { name } => {
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
            println!("{}", response.program_code);
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
            println!(
                "{}",
                serde_json::to_string_pretty(&response.program_config)
                    .expect("Failed to serialize pipeline stats")
            );
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
                }),
                runtime_config: None,
            };
            client
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
            println!("Program config updated successfully.");
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
            println!(
                "{:#?} (version {}, last updated {})",
                response.program_status, response.program_version.0, response.program_status_since
            );
        }
        ProgramAction::Set {
            name,
            program_path,
            stdin,
        } => {
            if let Ok(program_code) = read_program_code(program_path, stdin).await {
                let pp = PatchPipeline {
                    description: None,
                    name: None,
                    program_code: Some(program_code),
                    udf_rust: None,
                    udf_toml: None,
                    program_config: None,
                    runtime_config: None,
                };
                client
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
                println!("Program updated successfully.");
            } else {
                // Already reported error in read_program_code.
                std::process::exit(1);
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

    let client = make_client(cli.host, cli.auth, cli.timeout)
        .map_err(|e| {
            eprintln!("Failed to create HTTP client: {}", e);
            std::process::exit(1);
        })
        .unwrap();

    match cli.command {
        Commands::Apikey { action } => api_key_commands(action, client).await,
        Commands::Pipelines => pipelines(client).await,
        Commands::Pipeline(action) => pipeline(action, client).await,
    }
}
