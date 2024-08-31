//! A CLI App for the Feldera REST API.

use std::convert::Infallible;
use std::io::Read;

use clap::{CommandFactory, Parser};
use feldera_types::config::RuntimeConfig;
use feldera_types::error::ErrorResponse;
use log::{debug, error, info, trace};
use reqwest::header::HeaderValue;
use reqwest::StatusCode;
use tabled::builder::Builder;
use tabled::settings::Style;
use tokio::time::{sleep, Duration};

mod cli;

#[allow(clippy::all, unused)]
mod cd {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}

use crate::cd::types::*;
use crate::cd::*;
use crate::cli::*;

async fn add_auth_headers(
    key: &Option<String>,
    req: &mut reqwest::Request,
) -> Result<(), reqwest::header::InvalidHeaderValue> {
    // You can perform asynchronous, fallible work in a request hook, then
    // modify the request right before it is transmitted to the server; e.g.,
    // for generating an authenticaiton signature based on the complete set of
    // request header values:
    if let Some(key) = key {
        req.headers_mut().insert(
            reqwest::header::AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", key).as_str())?,
        );
    }
    Ok(())
}

fn handle_errors_fatal(
    msg: &'static str,
    exit_code: i32,
) -> Box<dyn Fn(Error<ErrorResponse>) -> Infallible + Send> {
    assert_ne!(exit_code, 0, "Exit code must not be 0");
    Box::new(move |err: Error<ErrorResponse>| -> Infallible {
        const UGPRADE_NOTICE: &str = "Try upgrading to the latest CLI version or report the issue on github.com/feldera/feldera.";

        match &err {
            Error::ErrorResponse(e) => {
                eprintln!("{}", e.message);
                debug!("Details: {:#?}", e.details);
            }
            Error::InvalidRequest(s) => {
                eprint!("{}: ", msg);
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
                eprint!("{}: ", msg);
                error!("Unable to read the error returned from the server ({})", e);
                error!("{}", UGPRADE_NOTICE);
            }
            Error::InvalidResponsePayload(b, e) => {
                eprintln!("{}", msg);
                if !b.is_empty() {
                    error!(
                        "Unable to parse the error returned from the server ({})",
                        e.to_string()
                    );
                    error!("{}", UGPRADE_NOTICE);
                }
            }
            Error::UnexpectedResponse(r) => {
                if r.status() == StatusCode::UNAUTHORIZED {
                    // The unauthorized error is often missing in the spec, and we can't currently have multiple
                    // return types until https://github.com/oxidecomputer/progenitor/pull/857 lands.
                    eprint!("{}: ", msg);
                    eprintln!("Unauthorized. Check your API key.");
                    std::process::exit(1);
                } else {
                    eprint!("{}: ", msg);
                    error!("Unexpected response from the server: {:?}", r);
                    error!("{}", UGPRADE_NOTICE);
                }
            }
            Error::PreHookError(e) => {
                eprint!("{}: ", msg);
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
            client
                .create_api_key()
                .body(NewApiKeyRequest { name })
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to create API key", 1),
                    |response| {
                        println!("API key '{}' created: {}", response.name, response.api_key);
                        std::process::exit(0);
                    },
                );
        }
        ApiKeyActions::Delete { name } => {
            debug!("Deleting API key: {}", name);
            client
                .delete_api_key()
                .api_key_name(name.as_str())
                .send()
                .await
                .map_or_else(handle_errors_fatal("Failed to delete API key", 1), |_| {
                    println!("API key '{}' deleted", name);
                    std::process::exit(0);
                });
        }
        ApiKeyActions::List => {
            debug!("Listing API keys");
            client.list_api_keys().send().await.map_or_else(
                handle_errors_fatal("Failed to list API keys", 1),
                |response| {
                    let mut rows = vec![];
                    rows.push(["name".to_string(), "id".to_string()]);
                    for key in response.iter() {
                        rows.push([key.name.to_string(), key.id.0.to_string()]);
                    }
                    println!(
                        "{}",
                        Builder::from_iter(rows).build().with(Style::rounded())
                    );
                    std::process::exit(0);
                },
            );
        }
    }
}

async fn pipelines(client: Client) {
    debug!("Listing pipelines");
    client.list_pipelines().send().await.map_or_else(
        handle_errors_fatal("Failed to list pipelines", 1),
        |response| {
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
            std::process::exit(0);
        },
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

async fn pipeline(name: &str, action: Option<PipelineAction>, client: Client) {
    debug!("Listing pipelines");
    match action {
        Some(PipelineAction::Create {
            program_path,
            profile,
            stdin,
        }) => {
            if let Ok(program_code) = read_program_code(program_path, stdin).await {
                client
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
                    .map_or_else(
                        handle_errors_fatal("Failed to create pipeline", 1),
                        |response| {
                            println!("Pipeline created successfully.");
                            debug!("{:#?}", response);
                            std::process::exit(0);
                        },
                    );
            } else {
                // Already reported error in read_program_code.
                std::process::exit(1);
            }
        }
        Some(PipelineAction::Start { recompile }) => {
            if recompile {
                // Force recompilation by adding/removing a space at the end of the program code.
                let pc = client
                    .get_pipeline()
                    .pipeline_name(name)
                    .send()
                    .await
                    .map_err(handle_errors_fatal("Failed to get program config", 1))
                    .unwrap();

                let new_program = if pc.program_code.ends_with(|c: char| c.is_whitespace()) {
                    pc.program_code.trim().to_string()
                } else {
                    pc.program_code.clone() + " "
                };

                client
                    .patch_pipeline()
                    .pipeline_name(name)
                    .body(PatchPipeline {
                        description: None,
                        name: None,
                        program_code: Some(new_program),
                        program_config: None,
                        runtime_config: None,
                    })
                    .send()
                    .await
                    .map_err(handle_errors_fatal("Failed to recompile pipeline", 1))
                    .unwrap();
                info!("Forcing recompilation this may take a few seconds...");
            }

            let mut print_every_30_seconds = tokio::time::Instant::now();
            let mut compiling = true;
            while compiling {
                let pc = client
                    .get_pipeline()
                    .pipeline_name(name)
                    .send()
                    .await
                    .map_err(handle_errors_fatal("Failed to get program config", 1))
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

            client
                .post_pipeline_action()
                .pipeline_name(name)
                .action("start")
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to start pipeline", 1),
                    |response| {
                        println!("Pipeline started successfully.");
                        trace!("{:#?}", response);
                        std::process::exit(0);
                    },
                );
        }
        Some(PipelineAction::Pause) => {
            client
                .post_pipeline_action()
                .pipeline_name(name)
                .action("pause")
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to pause pipeline", 1),
                    |response| {
                        println!("Pipeline paused successfully.");
                        trace!("{:#?}", response);
                        std::process::exit(0);
                    },
                );
        }
        Some(PipelineAction::Restart { recompile }) => {
            let _r = client
                .post_pipeline_action()
                .pipeline_name(name)
                .action("shutdown")
                .send()
                .await
                .map_err(handle_errors_fatal("Failed to stop pipeline", 1))
                .unwrap();

            let mut print_every_30_seconds = tokio::time::Instant::now();
            let mut shutting_down = true;
            while shutting_down {
                let pc = client
                    .get_pipeline()
                    .pipeline_name(name)
                    .send()
                    .await
                    .map_err(handle_errors_fatal("Failed to get program config", 1))
                    .unwrap();
                shutting_down = !matches!(pc.deployment_status, PipelineStatus::Shutdown);
                if print_every_30_seconds.elapsed().as_secs() > 30 {
                    info!("Shutting down the pipeline...");
                    print_every_30_seconds = tokio::time::Instant::now();
                }
                sleep(Duration::from_millis(500)).await;
            }
            println!("Pipeline shutdown successful.");

            let _ = Box::pin(pipeline(
                name,
                Some(PipelineAction::Start { recompile }),
                client,
            ))
            .await;
        }
        Some(PipelineAction::Shutdown) => {
            client
                .post_pipeline_action()
                .pipeline_name(name)
                .action("shutdown")
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to stop pipeline", 1),
                    |response| {
                        println!("Pipeline shutdown successful.");
                        trace!("{:#?}", response);
                        std::process::exit(0);
                    },
                );
        }
        Some(PipelineAction::Stats) => {
            client
                .get_pipeline_stats()
                .pipeline_name(name)
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to get pipeline stats", 1),
                    |response| {
                        println!(
                            "{}",
                            serde_json::to_string(response.as_ref())
                                .expect("Failed to serialize pipeline stats")
                        );
                        std::process::exit(0);
                    },
                );
        }
        Some(PipelineAction::Delete) => {
            client
                .delete_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to delete the pipeline", 1),
                    |response| {
                        println!("Pipeline deleted successfully.");
                        trace!("{:#?}", response);
                        std::process::exit(0);
                    },
                );
        }
        Some(PipelineAction::Status) => {
            client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to get pipeline status", 1),
                    |response| {
                        let mut rows = vec![];
                        rows.push([
                            "status".to_string(),
                            "desired_status".to_string(),
                            "program_status".to_string(),
                            "error".to_string(),
                            "location".to_string(),
                            "status_since".to_string(),
                        ]);
                        rows.push([
                            response.deployment_status.to_string(),
                            response.deployment_desired_status.to_string(),
                            format!("{:?}", response.program_status).to_string(),
                            response
                                .deployment_error
                                .as_ref()
                                .map(|e| e.message.to_string())
                                .unwrap_or(String::from("n/a")),
                            response
                                .deployment_location
                                .clone()
                                .unwrap_or(String::from("n/a")),
                            response.deployment_status_since.to_string(),
                        ]);
                        println!(
                            "{}",
                            Builder::from_iter(rows).build().with(Style::rounded())
                        );
                        std::process::exit(0);
                    },
                );
        }
        Some(PipelineAction::Config) => {
            client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to get pipeline config", 1),
                    |response| {
                        println!("{:#?}", response.runtime_config);
                        std::process::exit(0);
                    },
                );
        }
        Some(PipelineAction::SetConfig { key, value }) => {
            let mut rc = client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_err(handle_errors_fatal("Failed to get pipeline config", 1))
                .map(|response| response.runtime_config.clone())
                .unwrap();

            if patch_runtime_config(&mut rc, key, value.as_str()).is_err() {
                eprintln!(
                    "Failed to parse value '{}' for updating key {:?}",
                    value, key
                );
                return;
            }

            client
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
                .map_or_else(
                    handle_errors_fatal("Failed to set runtime config", 1),
                    |response| {
                        println!("Runtime config updated successfully.");
                        println!("{:#?}", response.runtime_config);
                        std::process::exit(0);
                    },
                );
        }
        Some(PipelineAction::Program { action }) => program(name, action, client).await,
        Some(PipelineAction::Endpoint {
            endpoint_name,
            action,
        }) => endpoint(name, endpoint_name.as_str(), action, client).await,
        Some(PipelineAction::Shell) => unimplemented!(),
        None => {
            client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to get pipeline", 1),
                    |response| {
                        println!("{:#?}", response);
                        std::process::exit(0);
                    },
                );
        }
    }
}

async fn endpoint(
    pipeline_name: &str,
    endpoint_name: &str,
    action: EndpointAction,
    client: Client,
) {
    match action {
        EndpointAction::Start => client
            .input_endpoint_action()
            .pipeline_name(pipeline_name)
            .endpoint_name(endpoint_name)
            .action("start")
            .send()
            .await
            .map_or_else(
                handle_errors_fatal("Failed to start endpoint", 1),
                |_response| {
                    println!("Endpoint {} started successfully.", endpoint_name);
                    std::process::exit(0);
                },
            ),
        EndpointAction::Pause => client
            .input_endpoint_action()
            .pipeline_name(pipeline_name)
            .endpoint_name(endpoint_name)
            .action("pause")
            .send()
            .await
            .map_or_else(
                handle_errors_fatal("Failed to pause endpoint", 1),
                |_response| {
                    println!("Endpoint {} paused successfully.", endpoint_name);
                    std::process::exit(0);
                },
            ),
    };
}

async fn program(name: &str, action: Option<ProgramAction>, client: Client) {
    match action {
        None => {
            client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to get program", 1),
                    |response| {
                        println!("{}", response.program_code);
                        std::process::exit(0);
                    },
                );
        }
        Some(ProgramAction::Config) => {
            client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to get program config", 1),
                    |response| {
                        println!("{:#?}", response.program_config);
                        std::process::exit(0);
                    },
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
                .map_or_else(
                    handle_errors_fatal("Failed to set program config", 1),
                    |_response| {
                        println!("Program config updated successfully.");
                        std::process::exit(0);
                    },
                );
        }
        Some(ProgramAction::Status) => {
            client
                .get_pipeline()
                .pipeline_name(name)
                .send()
                .await
                .map_or_else(
                    handle_errors_fatal("Failed to get program status", 1),
                    |response| {
                        println!(
                            "{:#?} (version {}, last updated {})",
                            response.program_status,
                            response.program_version.0,
                            response.program_status_since
                        );
                        std::process::exit(0);
                    },
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
                    .map_or_else(
                        handle_errors_fatal("Failed to set program code", 1),
                        |_response| {
                            println!("Program updated successfully.");
                            std::process::exit(0);
                        },
                    );
            } else {
                // Already reported error in read_program_code.
                std::process::exit(1);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let _r = env_logger::try_init();
    let cli = Cli::parse();
    let client = Client::new(cli.host.as_str(), cli.auth.clone());

    match cli.command {
        Commands::Apikey { action } => api_key_commands(action, client).await,
        Commands::Pipelines => pipelines(client).await,
        Commands::Pipeline { name, action } => pipeline(name.as_str(), action, client).await,
        Commands::ShellCompletion => {
            use clap_complete::{generate, shells::Shell};
            generate(
                Shell::from_env().unwrap_or(Shell::Bash),
                &mut Cli::command(),
                "fda",
                &mut std::io::stdout(),
            );
        }
    }
}
