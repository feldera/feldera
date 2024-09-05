use crate::cd::Client;
use crate::UGPRADE_NOTICE;
use directories::ProjectDirs;
use feldera_types::error::ErrorResponse;
use futures_util::StreamExt;
use progenitor_client::Error;
use reqwest::StatusCode;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, ExternalPrinter};
use tokio::signal;
use tokio_util::sync::CancellationToken;

const NEWLINE: &str = if cfg!(windows) { "\r\n" } else { "\n" };

const HELP_TEXT: &str = r#"You are using fda, the command-line interface to Feldera.
Type:  \h for help with SQL commands
       \? for help with fda shell commands
"#;

const SQL_HELP_TEXT: &str = r#"Send SQL commands to the pipeline.

Tables and views are only accessible if they are declared as materialized.
You can not create or alter tables and views using ad-hoc SQL.

Note that the all ad-hoc SQL commands are not evaluated incrementally but instead
executed using a batch engine. You can however, very cheaply query the state of a
materialized view.
"#;

/// Start an interactive shell for a pipeline.
pub async fn shell(name: &str, client: Client) {
    println!(
        "fda shell ({}). Type \"help\" for help. Use Ctrl-D (i.e. EOF) to exit.",
        env!("CARGO_PKG_VERSION")
    );
    println!();

    let mut rl = DefaultEditor::new().expect("Failed to create shell editor");
    const HISTORY_FILE: &str = "history.txt";
    let project_dirs = ProjectDirs::from("com", "Feldera", "fda");
    let config_dir = project_dirs
        .as_ref()
        .map(|proj_dirs| proj_dirs.config_dir());

    if let Some(config_dir) = config_dir {
        let _r = std::fs::create_dir_all(config_dir);
    };
    if let Some(config_dir) = config_dir {
        let _r = rl.load_history(&config_dir.join(HISTORY_FILE));
    }

    let prompt = format!("{}> ", name);
    loop {
        let readline = rl.readline(prompt.as_str());
        match readline {
            Ok(line) => {
                let trimmed_line = line.trim();
                let _r = rl.add_history_entry(trimmed_line);

                match trimmed_line {
                    "\\h" | "help" => {
                        println!("{}", HELP_TEXT);
                        continue;
                    }
                    "exit" => {
                        println!("Use Ctrl-D (i.e. EOF) to exit");
                        continue;
                    }
                    "\\?" => {
                        println!("{}", SQL_HELP_TEXT);
                        continue;
                    }
                    _ => {
                        if trimmed_line.is_empty() {
                            continue;
                        }

                        let client = client.clone();
                        let name = name.to_string();
                        let trimmed_line = trimmed_line.to_string();
                        let cancel_token = CancellationToken::new();
                        let cancel_token_child = cancel_token.clone();
                        let mut printer = rl
                            .create_external_printer()
                            .expect("Failed to create external printer");

                        // Print the SQL response, aborting if Ctrl+C is pressed
                        let req_handle = tokio::spawn(async move {
                            match client
                                .pipeline_adhoc_sql()
                                .pipeline_name(name)
                                .format("text")
                                .sql(trimmed_line)
                                .send()
                                .await
                            {
                                Ok(response) => {
                                    printer.print(NEWLINE.to_string()).unwrap();
                                    let mut byte_stream = response.into_inner();
                                    while let Some(chunk) = byte_stream.next().await {
                                        if cancel_token_child.is_cancelled() {
                                            return;
                                        }
                                        let mut buffer = Vec::new();
                                        buffer.extend_from_slice(
                                            &chunk.expect("Reading Chunk should succeed"),
                                        );
                                        let text = String::from_utf8_lossy(&buffer);
                                        printer.print(text.to_string()).unwrap();
                                    }
                                    printer.print(NEWLINE.to_string()).unwrap();
                                }
                                Err(err) => {
                                    println!();
                                    handle_sql_response_error(err);
                                    println!();
                                }
                            }
                        });

                        // Listen for Ctrl+C
                        let abort_task = tokio::spawn(async move {
                            signal::ctrl_c().await.unwrap();
                            cancel_token.cancel();
                            println!();
                            println!("ERROR: canceling statement due to user request.");
                            println!();
                        });

                        // Wait for either the request to finish or Ctrl+C
                        tokio::select! {
                            biased;
                            _ = abort_task => {}
                            _ = req_handle => {}
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                continue;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("ERROR: {:?}", err);
                break;
            }
        }
    }

    if let Some(config_dir) = config_dir {
        let _r = rl.save_history(&config_dir.join(HISTORY_FILE));
    }
}

fn handle_sql_response_error(err: Error<ErrorResponse>) {
    match &err {
        Error::ErrorResponse(e) => {
            eprintln!("ERROR: {}", e.message);
        }
        Error::InvalidRequest(s) => {
            eprintln!("ERROR: Invalid request ({})", s);
            eprintln!("{}", UGPRADE_NOTICE);
        }
        Error::CommunicationError(e) => {
            if e.is_timeout() {
                eprintln!("ERROR: Request timed out.");
                eprintln!("Try increasing the limit with the `--timeout` argument.");
            } else {
                eprintln!("ERROR: {}: ", e);
                eprintln!("Check your network connection.");
            }
        }
        Error::InvalidUpgrade(e) => {
            eprintln!("ERROR: {}: ", e);
            eprintln!("Check your network connection.");
        }
        Error::ResponseBodyError(e) => {
            eprintln!(
                "ERROR: Unable to read the error returned from the server ({})",
                e
            );
            eprintln!("{}", UGPRADE_NOTICE);
        }
        Error::InvalidResponsePayload(_b, e) => {
            eprintln!(
                "ERROR: Unable to parse the error returned from the server ({})",
                e
            );
            eprintln!("{}", UGPRADE_NOTICE);
        }
        Error::UnexpectedResponse(r) => {
            if r.status() == StatusCode::UNAUTHORIZED {
                // The unauthorized error is often missing in the spec, and we can't currently have multiple
                // return types until https://github.com/oxidecomputer/progenitor/pull/857 lands.
                eprintln!("ERROR: Unauthorized. Check your API key.");
            } else {
                eprintln!("ERROR: Unexpected response from the server: {:?}", r);
                eprintln!("{}", UGPRADE_NOTICE);
            }
        }
        Error::PreHookError(e) => {
            eprint!("ERROR: Unable to execute authentication pre-hook ({})", e);
            eprintln!("{}", UGPRADE_NOTICE);
        }
    };
}
