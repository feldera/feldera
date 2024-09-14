//! A CLI App for the Feldera REST API.

use std::convert::Infallible;
use std::io::Read;

use clap::{CommandFactory, Parser};
use clap_complete::CompleteEnv;
use feldera_types::config::RuntimeConfig;
use feldera_types::error::ErrorResponse;
use log::{debug, error, info, trace};
use reqwest::header::{HeaderMap, HeaderValue, InvalidHeaderValue};
use reqwest::StatusCode;
use tabled::builder::Builder;
use tabled::settings::Style;
use tokio::time::{sleep, Duration};

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
    timeout: u64,
) -> Result<Client, Box<dyn std::error::Error>> {
    let client_builder = reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(timeout))
        .default_headers(make_auth_headers(&auth)?);
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

async fn read_program_code(program_path: String, stdin: bool) -> Result<String, ()> {
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
        PipelineAction::Start { name, recompile } => {
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
                .pipeline_name(name)
                .action("start")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
                    "Failed to start pipeline",
                    1,
                ))
                .unwrap();
            println!("Pipeline started successfully.");
            trace!("{:#?}", response);
        }
        PipelineAction::Pause { name } => {
            let response = client
                .post_pipeline_action()
                .pipeline_name(name)
                .action("pause")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
                    "Failed to pause pipeline",
                    1,
                ))
                .unwrap();
            println!("Pipeline paused successfully.");
            trace!("{:#?}", response);
        }
        PipelineAction::Restart { name, recompile } => {
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

            let mut print_every_30_seconds = tokio::time::Instant::now();
            let mut shutting_down = true;
            while shutting_down {
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
                shutting_down = !matches!(pc.deployment_status, PipelineStatus::Shutdown);
                if print_every_30_seconds.elapsed().as_secs() > 30 {
                    info!("Shutting down the pipeline...");
                    print_every_30_seconds = tokio::time::Instant::now();
                }
                sleep(Duration::from_millis(500)).await;
            }
            println!("Pipeline shutdown successful.");

            let _ = Box::pin(pipeline(PipelineAction::Start { name, recompile }, client)).await;
        }
        PipelineAction::Shutdown { name } => {
            let response = client
                .post_pipeline_action()
                .pipeline_name(name)
                .action("shutdown")
                .send()
                .await
                .map_err(handle_errors_fatal(
                    client.baseurl,
                    "Failed to stop pipeline",
                    1,
                ))
                .unwrap();
            println!("Pipeline shutdown successful.");
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
        PipelineAction::Program { name, action } => program(name, action, client).await,
        PipelineAction::Endpoint {
            name,
            endpoint_name,
            action,
        } => endpoint(name, endpoint_name.as_str(), action, client).await,
        PipelineAction::Shell { name, start } => {
            let client2 = client.clone();
            if start {
                let _ = Box::pin(pipeline(
                    PipelineAction::Start {
                        name: name.clone(),
                        recompile: false,
                    },
                    client,
                ))
                .await;
            }
            shell(name, client2).await
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

async fn program(name: String, action: Option<ProgramAction>, client: Client) {
    match action {
        None => {
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
        Some(ProgramAction::Config) => {
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
        Some(ProgramAction::SetConfig { profile }) => {
            let pp = PatchPipeline {
                description: None,
                name: None,
                program_code: None,
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
        Some(ProgramAction::Status) => {
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
        Some(ProgramAction::Set {
            program_path,
            stdin,
        }) => {
            if let Ok(program_code) = read_program_code(program_path, stdin).await {
                let pp = PatchPipeline {
                    description: None,
                    name: None,
                    program_code: Some(program_code),
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

    let cli = Cli::parse();
    let _r = env_logger::try_init();

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
