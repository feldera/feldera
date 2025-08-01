// API to read from tables/views and write into tables using HTTP
use actix_web::rt::time::timeout;
use futures_util::StreamExt;
use std::io::Write;
use std::time::Duration;
use zip::{CompressionMethod, ZipWriter};

use crate::api::endpoints::config::Configuration;
use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
use crate::db::{storage::Storage, types::tenant::TenantId};
use crate::error::ManagerError;
use actix_web::{
    get,
    http::Method,
    web::{self, Data as WebData, ReqData},
    HttpResponse,
};
use feldera_types::program_schema::SqlIdentifier;

/// Generate a support bundle containing diagnostic information from a pipeline.
///
/// This endpoint collects various diagnostic data from the pipeline including
/// circuit profile, heap profile, metrics, logs, stats, and connector statistics,
/// and packages them into a single ZIP file for support purposes.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Support bundle containing diagnostic information"
            , content_type = "application/zip"
            , body = Vec<u8>),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = SERVICE_UNAVAILABLE
            , body = ErrorResponse
            , examples(
                ("Pipeline is not deployed" = (value = json!(examples::error_pipeline_interaction_not_deployed()))),
                ("Pipeline is currently unavailable" = (value = json!(examples::error_pipeline_interaction_currently_unavailable()))),
                ("Disconnected during response" = (value = json!(examples::error_pipeline_interaction_disconnected()))),
                ("Response timeout" = (value = json!(examples::error_pipeline_interaction_timeout())))
            )
        ),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    tag = "Pipeline interaction"
)]
#[get("/pipelines/{pipeline_name}/support_bundle")]
pub(crate) async fn get_pipeline_support_bundle(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    let mut zip_buffer = Vec::new();
    let mut zip = ZipWriter::new(std::io::Cursor::new(&mut zip_buffer));

    // Helper function to fetch data from pipeline
    let fetch_data = |endpoint: &str, timeout: Option<Duration>| {
        let state = state.clone();
        let client = client.clone();
        let tenant_id = *tenant_id;
        let pipeline_name = pipeline_name.clone();
        let endpoint = endpoint.to_string();

        async move {
            state
                .runner
                .forward_http_request_to_pipeline_by_name(
                    client.as_ref(),
                    tenant_id,
                    &pipeline_name,
                    Method::GET,
                    &endpoint,
                    "",
                    timeout,
                )
                .await
        }
    };

    // Helper function to fetch data from pipeline
    let stream_logs = || {
        let state = state.clone();
        let client = client.clone();
        let tenant_id = *tenant_id;
        let pipeline_name = pipeline_name.clone();

        // The log endpoint is a stream that never ends. However, we want to be able for
        // fda to exit after a certain amount of time without any new logs to emulate
        // a log snapshot functionality.
        let mut first_line = true;
        let next_line_timeout = Duration::from_millis(250);
        let mut logs = String::with_capacity(4096);
        async move {
            let response = state
                .runner
                .get_logs_from_pipeline(&client, tenant_id, &pipeline_name)
                .await;
            match response {
                Ok(mut response) => {
                    while let Ok(Some(chunk)) = timeout(
                        if first_line {
                            Duration::from_secs(5)
                        } else {
                            next_line_timeout
                        },
                        response.next(),
                    )
                    .await
                    {
                        match chunk {
                            Ok(chunk) => {
                                let text = String::from_utf8_lossy(&chunk);
                                logs.push_str(&text);
                                first_line = false;
                            }
                            Err(e) => {
                                logs.push_str(&format!(
                                    "ERROR: Unable to read server response: {}",
                                    e
                                ));
                            }
                        }
                    }
                    return Ok(logs);
                }
                Err(e) => return Err(e),
            }
        }
    };

    // Helper function to get local manager data
    let get_pipeline_config = async {
        state
            .db
            .lock()
            .await
            .get_pipeline(*tenant_id, &pipeline_name)
            .await
            .map(|pipeline| serde_json::to_vec_pretty(&pipeline))
    };

    let get_system_config = async {
        let config = Configuration::gather(&state).await;
        Ok::<_, serde_json::Error>(serde_json::to_vec_pretty(&config)?)
    };

    // Launch all data collection tasks in parallel (including local ones)
    let (
        circuit_profile_result,
        heap_profile_result,
        metrics_result,
        logs_result,
        stats_result,
        pipeline_config_result,
        system_config_result,
    ) = tokio::join!(
        fetch_data("dump_profile", Some(Duration::from_secs(120))),
        fetch_data("heap_profile", None),
        fetch_data("metrics", None),
        stream_logs(),
        fetch_data("stats", None),
        get_pipeline_config,
        get_system_config,
    );

    // Helper function to add content to zip
    let mut add_to_zip =
        |filename: &str, content: Vec<u8>| -> Result<(), Box<dyn std::error::Error>> {
            let options =
                zip::write::FileOptions::default().compression_method(CompressionMethod::Deflated);
            zip.start_file(filename, options)?;
            zip.write_all(&content)?;
            Ok(())
        };

    // Process circuit profile result
    match circuit_profile_result {
        Ok(response) if response.status().is_success() => {
            if let Ok(body) = actix_web::body::to_bytes(response.into_body()).await {
                let _ = add_to_zip("circuit_profile.zip", body.to_vec());
            }
        }
        _ => {
            let _ = add_to_zip(
                "circuit_profile_error.txt",
                b"Failed to retrieve circuit profile".to_vec(),
            );
        }
    }

    // Process heap profile result
    match heap_profile_result {
        Ok(response) if response.status().is_success() => {
            if let Ok(body) = actix_web::body::to_bytes(response.into_body()).await {
                let _ = add_to_zip("heap_profile.pb.gz", body.to_vec());
            }
        }
        _ => {
            let _ = add_to_zip(
                "heap_profile_error.txt",
                b"Failed to retrieve heap profile".to_vec(),
            );
        }
    }

    // Process metrics result
    match metrics_result {
        Ok(response) if response.status().is_success() => {
            if let Ok(body) = actix_web::body::to_bytes(response.into_body()).await {
                let _ = add_to_zip("metrics.txt", body.to_vec());
            }
        }
        _ => {
            let _ = add_to_zip("metrics_error.txt", b"Failed to retrieve metrics".to_vec());
        }
    }

    // Process logs result
    match logs_result {
        Ok(body) => {
            let _ = add_to_zip("logs.txt", body.as_bytes().to_vec());
        }
        _ => {
            let _ = add_to_zip("logs_error.txt", b"Failed to retrieve logs".to_vec());
        }
    }

    // Process pipeline config result
    match pipeline_config_result {
        Ok(Ok(config_json)) => {
            let _ = add_to_zip("pipeline_config.json", config_json);
        }
        _ => {
            let _ = add_to_zip(
                "pipeline_config_error.txt",
                b"Failed to retrieve pipeline configuration".to_vec(),
            );
        }
    }

    // Process system config result
    match system_config_result {
        Ok(config_json) => {
            let _ = add_to_zip("system_config.json", config_json);
        }
        Err(_) => {
            let _ = add_to_zip(
                "system_config_error.txt",
                b"Failed to retrieve system configuration".to_vec(),
            );
        }
    }

    // Process stats result and use it to discover connectors
    let stats_body = match stats_result {
        Ok(response) if response.status().is_success() => {
            match actix_web::body::to_bytes(response.into_body()).await {
                Ok(body) => {
                    let body_vec = body.to_vec();
                    let _ = add_to_zip("stats.json", body_vec.clone());
                    Some(body_vec)
                }
                Err(_) => {
                    let _ = add_to_zip("stats_error.txt", b"Failed to retrieve stats".to_vec());
                    None
                }
            }
        }
        _ => {
            let _ = add_to_zip("stats_error.txt", b"Failed to retrieve stats".to_vec());
            None
        }
    };

    // Collect connector stats in parallel if we have stats data
    if let Some(stats_body) = stats_body {
        if let Ok(stats_str) = std::str::from_utf8(&stats_body) {
            if let Ok(stats_json) = serde_json::from_str::<serde_json::Value>(stats_str) {
                let mut connector_tasks = Vec::new();

                // Collect input connector endpoints
                if let Some(inputs) = stats_json.get("inputs").and_then(|v| v.as_object()) {
                    for (table_connector, _) in inputs {
                        if let Some((table_name, connector_name)) = table_connector.split_once('.')
                        {
                            let actual_table_name = SqlIdentifier::from(table_name).name();
                            let endpoint_name = format!("{}.{}", actual_table_name, connector_name);
                            let encoded_endpoint_name =
                                urlencoding::encode(&endpoint_name).to_string();
                            let endpoint =
                                format!("input_endpoints/{}/stats", encoded_endpoint_name);
                            let filename = format!(
                                "connectors/input_{}_{}_stats.json",
                                table_name, connector_name
                            );
                            let error_filename = format!(
                                "connectors/input_{}_{}_stats_error.txt",
                                table_name, connector_name
                            );

                            connector_tasks.push((
                                fetch_data(&endpoint, None),
                                filename,
                                error_filename,
                            ));
                        }
                    }
                }

                // Collect output connector endpoints
                if let Some(outputs) = stats_json.get("outputs").and_then(|v| v.as_object()) {
                    for (view_connector, _) in outputs {
                        if let Some((view_name, connector_name)) = view_connector.split_once('.') {
                            let actual_view_name = SqlIdentifier::from(view_name).name();
                            let endpoint_name = format!("{}.{}", actual_view_name, connector_name);
                            let encoded_endpoint_name =
                                urlencoding::encode(&endpoint_name).to_string();
                            let endpoint =
                                format!("output_endpoints/{}/stats", encoded_endpoint_name);
                            let filename = format!(
                                "connectors/output_{}_{}_stats.json",
                                view_name, connector_name
                            );
                            let error_filename = format!(
                                "connectors/output_{}_{}_stats_error.txt",
                                view_name, connector_name
                            );

                            connector_tasks.push((
                                fetch_data(&endpoint, None),
                                filename,
                                error_filename,
                            ));
                        }
                    }
                }

                // Execute all connector requests in parallel
                let connector_results =
                    futures_util::future::join_all(connector_tasks.into_iter().map(
                        |(task, filename, error_filename)| async move {
                            let result = task.await;
                            (result, filename, error_filename)
                        },
                    ))
                    .await;

                // Process connector results
                for (result, filename, error_filename) in connector_results {
                    match result {
                        Ok(response) if response.status().is_success() => {
                            if let Ok(body) = actix_web::body::to_bytes(response.into_body()).await
                            {
                                let _ = add_to_zip(&filename, body.to_vec());
                            } else {
                                let _ = add_to_zip(
                                    &error_filename,
                                    b"Failed to read connector stats response".to_vec(),
                                );
                            }
                        }
                        _ => {
                            let _ = add_to_zip(
                                &error_filename,
                                b"Failed to retrieve connector stats".to_vec(),
                            );
                        }
                    }
                }
            }
        }
    }

    // Add a manifest file with collection timestamp
    let manifest = format!(
        "Support Bundle for Pipeline: {}\nGenerated at: {}\nContents:\n- pipeline_config.json (SQL program and pipeline configuration)\n- system_config.json (Platform configuration and build info)\n- circuit_profile.zip\n- heap_profile.pb.gz\n- metrics.txt\n- logs.txt\n- stats.json\n- connectors/*/stats.json\n",
        pipeline_name,
        chrono::Utc::now().to_rfc3339()
    );
    let _ = add_to_zip("manifest.txt", manifest.into_bytes());

    // Finalize the zip
    if let Err(e) = zip.finish() {
        return Err(ManagerError::from(ApiError::UnableToCreateSupportBundle {
            reason: format!("Failed to create support bundle: {}", e),
        }));
    }

    drop(zip);

    Ok(HttpResponse::Ok()
        .content_type("application/zip")
        .insert_header((
            "Content-Disposition",
            format!(
                "attachment; filename=\"{}_support_bundle.zip\"",
                pipeline_name
            ),
        ))
        .body(zip_buffer))
}
