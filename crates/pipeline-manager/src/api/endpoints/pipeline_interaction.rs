// API to read from tables/views and write into tables using HTTP
use crate::api::error::ApiError;
use crate::api::examples;
use crate::api::main::ServerState;
use crate::api::util::parse_url_parameter;
#[cfg(not(feature = "feldera-enterprise"))]
use crate::common_error::CommonError;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;
use actix_http::StatusCode;
use actix_web::{
    get,
    http::{header, Method},
    post,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use feldera_types::program_schema::SqlIdentifier;
use feldera_types::query_params::MetricsParameters;
use log::{debug, info};
use std::time::Duration;

/// Push data to a SQL table.
///
/// The client sends data encoded using the format specified in the `?format=`
/// parameter as a body of the request.  The contents of the data must match
/// the SQL table schema specified in `table_name`
///
/// The pipeline ingests data as it arrives without waiting for the end of
/// the request.  Successful HTTP response indicates that all data has been
/// ingested successfully.
///
/// On success, returns a completion token that can be passed to the
/// '/completion_status' endpoint to check whether the pipeline has fully
/// processed the data.
// TODO: implement chunked and batch modes.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("table_name" = String, Path,
            description = "SQL table name. Unquoted SQL names have to be capitalized. Quoted SQL names have to exactly match the case from the SQL program."),
        ("force" = bool, Query, description = "When `true`, push data to the pipeline even if the pipeline is paused. The default value is `false`"),
        ("format" = String, Query, description = "Input data format, e.g., 'csv' or 'json'."),
        ("array" = Option<bool>, Query, description = "Set to `true` if updates in this stream are packaged into JSON arrays (used in conjunction with `format=json`). The default values is `false`."),
        ("update_format" = Option<JsonUpdateFormat>, Query, description = "JSON data change event format (used in conjunction with `format=json`).  The default value is 'insert_delete'."),
    ),
    request_body(
        content = String,
        description = "Input data in the specified format",
        content_type = "text/plain",
    ),
    responses(
        (status = OK
            , description = "Data successfully delivered to the pipeline. The body of the response contains a completion token that can be passed to the '/completion_status' endpoint to check whether the pipeline has fully processed the data."
            , content_type = "application/json"
            , body = CompletionTokenResponse),
        (status = NOT_FOUND
            , body = ErrorResponse
            , description = "Pipeline and/or table with that name does not exist"
            , examples(
                ("Pipeline with that name does not exist" = (value = json!(examples::error_unknown_pipeline_name()))),
                // ("Table with that name does not exist" = (value = json!(examples::error_unknown_input_table()))),
            )
        ),
        (status = BAD_REQUEST
            , body = ErrorResponse
            // , examples(
            //     ("Unknown input data format" = (value = json!(examples::error_unknown_input_format()))),
            //     ("Input parse error" = (value = json!(examples::error_parse_errors()))),
            // )
        ),
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
    tag = "Pipeline interaction",
)]
#[post("/pipelines/{pipeline_name}/ingress/{table_name}")]
pub(crate) async fn http_input(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_url_parameter(&req, "pipeline_name")?;
    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(ManagerError::from(ApiError::MissingUrlEncodedParam {
                param: "table_name",
            }));
        }
        Some(table_name) => table_name,
    };
    debug!("Table name {table_name:?}");

    let endpoint = format!("ingress/{table_name}");

    state
        .runner
        .forward_streaming_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            &endpoint,
            req,
            body,
            Some(Duration::from_secs(300)),
        )
        .await
}

/// Subscribe to a stream of updates from a SQL view or table.
///
/// The pipeline responds with a continuous stream of changes to the specified
/// table or view, encoded using the format specified in the `?format=`
/// parameter. Updates are split into `Chunk`s.
///
/// The pipeline continues sending updates until the client closes the
/// connection or the pipeline is stopped.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("table_name" = String, Path,
            description = "SQL table name. Unquoted SQL names have to be capitalized. Quoted SQL names have to exactly match the case from the SQL program."),
        ("format" = String, Query, description = "Output data format, e.g., 'csv' or 'json'."),
        ("array" = Option<bool>, Query, description = "Set to `true` to group updates in this stream into JSON arrays (used in conjunction with `format=json`). The default value is `false`"),
        ("backpressure" = Option<bool>, Query, description = r#"Apply backpressure on the pipeline when the HTTP client cannot receive data fast enough.
        When this flag is set to false (the default), the HTTP connector drops data chunks if the client is not keeping up with its output.  This prevents a slow HTTP client from slowing down the entire pipeline.
        When the flag is set to true, the connector waits for the client to receive each chunk and blocks the pipeline if the client cannot keep up."#)
    ),
    responses(
        (status = OK
            , description = "Connection to the endpoint successfully established. The body of the response contains a stream of data chunks."
            , content_type = "application/json"
            , body = Chunk),
        (status = NOT_FOUND
            , body = ErrorResponse
            , description = "Pipeline and/or table/view with that name does not exist"
            , examples(
                ("Pipeline with that name does not exist" = (value = json!(examples::error_unknown_pipeline_name()))),
                // ("Table or view with that name does not exist" = ...),
            )
        ),
        (status = BAD_REQUEST
            , body = ErrorResponse
            // , examples(
            //    ("Unknown output data format" = ...),
            // )
        ),
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
#[post("/pipelines/{pipeline_name}/egress/{table_name}")]
pub(crate) async fn http_output(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = parse_url_parameter(&req, "pipeline_name")?;
    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(ManagerError::from(ApiError::MissingUrlEncodedParam {
                param: "table_name",
            }));
        }
        Some(table_name) => table_name,
    };
    let endpoint = format!("egress/{table_name}");
    state
        .runner
        .forward_streaming_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            &endpoint,
            req,
            body,
            None,
        )
        .await
}

/// Start (resume) or pause the input connector.
///
/// The following values of the `action` argument are accepted: `start` and `pause`.
///
/// Input connectors can be in either the `Running` or `Paused` state. By default,
/// connectors are initialized in the `Running` state when a pipeline is deployed.
/// In this state, the connector actively fetches data from its configured data
/// source and forwards it to the pipeline. If needed, a connector can be created
/// in the `Paused` state by setting its
/// [`paused`](https://docs.feldera.com/connectors/#generic-attributes) property
/// to `true`. When paused, the connector remains idle until reactivated using the
/// `start` command. Conversely, a connector in the `Running` state can be paused
/// at any time by issuing the `pause` command.
///
/// The current connector state can be retrieved via the
/// `GET /v0/pipelines/{pipeline_name}/stats` endpoint.
///
/// Note that only if both the pipeline *and* the connector state is `Running`,
/// is the input connector active.
/// ```text
/// Pipeline state    Connector state    Connector is active?
/// --------------    ---------------    --------------------
/// Paused            Paused             No
/// Paused            Running            No
/// Running           Paused             No
/// Running           Running            Yes
/// ```
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("table_name" = String, Path, description = "Unique table name"),
        ("connector_name" = String, Path, description = "Unique input connector name"),
        ("action" = String, Path, description = "Input connector action (one of: start, pause)")
    ),
    responses(
        (status = OK
            , description = "Action has been processed"),
        (status = NOT_FOUND
            , body = ErrorResponse
            , description = "Pipeline, table and/or input connector with that name does not exist"
            , examples(
                ("Pipeline with that name does not exist" = (value = json!(examples::error_unknown_pipeline_name()))),
                // ("Table with that name does not exist" = ...),
                // ("Input connector with that name does not exist" = ...),
            )
        ),
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
#[post("/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/{action}")]
pub(crate) async fn post_pipeline_input_connector_action(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<(String, String, String, String)>,
) -> Result<HttpResponse, ManagerError> {
    // Parse the URL path parameters
    let (pipeline_name, table_name, connector_name, action) = path.into_inner();

    // Validate action
    let verb = match action.as_str() {
        "start" => "starting",
        "pause" => "pausing",
        _ => {
            return Err(ApiError::InvalidConnectorAction { action }.into());
        }
    };

    // The table name provided by the user is interpreted as
    // a SQL identifier to account for case (in-)sensitivity
    let actual_table_name = SqlIdentifier::from(&table_name).name();
    let endpoint_name = format!("{actual_table_name}.{connector_name}");

    // URL encode endpoint name to account for special characters
    let encoded_endpoint_name = urlencoding::encode(&endpoint_name).to_string();

    // Forward the action request to the pipeline
    let response = state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            &format!("input_endpoints/{encoded_endpoint_name}/{action}"),
            "",
            None,
        )
        .await?;

    // Log only if the response indicates success
    if response.status() == StatusCode::OK {
        info!(
            "Connector action: {verb} pipeline '{pipeline_name}' on table '{table_name}' on connector '{connector_name}' (tenant: {})",
            *tenant_id
        );
    }
    Ok(response)
}

/// Retrieve the status of an input connector.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("table_name" = String, Path, description = "Unique table name"),
        ("connector_name" = String, Path, description = "Unique input connector name")
    ),
    responses(
        (status = OK
            , description = "Input connector status retrieved successfully"
            , content_type = "application/json"
            , body = Object),
        (status = NOT_FOUND
            , body = ErrorResponse
            , description = "Pipeline, table and/or input connector with that name does not exist"
            , examples(
                ("Pipeline with that name does not exist" = (value = json!(examples::error_unknown_pipeline_name()))),
                // ("Table with that name does not exist" = ...),
                // ("Input connector with that name does not exist" = ...),
            )
        ),
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
#[get("/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/stats")]
pub(crate) async fn get_pipeline_input_connector_status(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<(String, String, String)>,
) -> Result<HttpResponse, ManagerError> {
    // Parse the URL path parameters
    let (pipeline_name, table_name, connector_name) = path.into_inner();

    // The table name provided by the user is interpreted as
    // a SQL identifier to account for case (in-)sensitivity
    let actual_table_name = SqlIdentifier::from(&table_name).name();
    let endpoint_name = format!("{actual_table_name}.{connector_name}");

    // URL encode endpoint name to account for special characters
    let encoded_endpoint_name = urlencoding::encode(&endpoint_name).to_string();

    // Forward the action request to the pipeline
    let response = state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            &format!("input_endpoints/{encoded_endpoint_name}/stats"),
            "",
            None,
        )
        .await?;

    Ok(response)
}

/// Retrieve the status of an output connector.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("view_name" = String, Path, description = "Unique SQL view name"),
        ("connector_name" = String, Path, description = "Unique output connector name")
    ),
    responses(
        (status = OK
            , description = "Output connector status retrieved successfully"
            , content_type = "application/json"
            , body = Object),
        (status = NOT_FOUND
            , body = ErrorResponse
            , description = "Pipeline, view and/or output connector with that name does not exist"
            , examples(
                ("Pipeline with that name does not exist" = (value = json!(examples::error_unknown_pipeline_name()))),
                // ("View with that name does not exist" = ...),
                // ("Output connector with that name does not exist" = ...),
            )
        ),
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
#[get("/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/stats")]
pub(crate) async fn get_pipeline_output_connector_status(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<(String, String, String)>,
) -> Result<HttpResponse, ManagerError> {
    // Parse the URL path parameters
    let (pipeline_name, view_name, connector_name) = path.into_inner();

    // The view name provided by the user is interpreted as
    // a SQL identifier to account for case (in-)sensitivity
    let actual_view_name = SqlIdentifier::from(&view_name).name();
    let endpoint_name = format!("{actual_view_name}.{connector_name}");

    // URL encode endpoint name to account for special characters
    let encoded_endpoint_name = urlencoding::encode(&endpoint_name).to_string();

    // Forward the action request to the pipeline
    let response = state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            &format!("output_endpoints/{encoded_endpoint_name}/stats"),
            "",
            None,
        )
        .await?;

    Ok(response)
}

/// Retrieve statistics (e.g., performance counters) of a running or paused pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        // TODO: implement `ToSchema` for `ControllerStatus`, which is the
        //       actual type returned by this endpoint and move it to feldera-types.
        (status = OK
            , description = "Pipeline statistics retrieved successfully"
            , body = Object),
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
#[get("/pipelines/{pipeline_name}/stats")]
pub(crate) async fn get_pipeline_stats(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "stats",
            request.query_string(),
            None,
        )
        .await
}

/// Retrieve circuit metrics of a running or paused pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        MetricsParameters
    ),
    responses(
        (status = OK
            , description = "Pipeline circuit metrics retrieved successfully"
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
#[get("/pipelines/{pipeline_name}/metrics")]
pub(crate) async fn get_pipeline_metrics(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    _query: web::Query<MetricsParameters>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "metrics",
            request.query_string(),
            None,
        )
        .await
}

/// Retrieve the circuit performance profile of a running or paused pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Circuit performance profile"
            , content_type = "application/zip"
            , body = Object),
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
#[get("/pipelines/{pipeline_name}/circuit_profile")]
pub(crate) async fn get_pipeline_circuit_profile(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "dump_profile",
            request.query_string(),
            Some(Duration::from_secs(120)),
        )
        .await
}

/// Syncs latest checkpoints to the object store configured in pipeline config.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Checkpoint synced to object store"
            , body = CheckpointResponse),
        (status = NOT_FOUND
            , description = "No checkpoints found"
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
#[post("/pipelines/{pipeline_name}/checkpoint/sync")]
pub(crate) async fn sync_checkpoint(
    state: WebData<ServerState>,
    _client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    #[cfg(not(feature = "feldera-enterprise"))]
    {
        let _ = (state, tenant_id, path.into_inner(), request);
        Err(CommonError::EnterpriseFeature {
            feature: "checkpoint".to_string(),
        }
        .into())
    }

    #[cfg(feature = "feldera-enterprise")]
    {
        let pipeline_name = path.into_inner();
        state
            .runner
            .forward_http_request_to_pipeline_by_name(
                _client.as_ref(),
                *tenant_id,
                &pipeline_name,
                Method::POST,
                "checkpoint/sync",
                request.query_string(),
                Some(Duration::from_secs(120)),
            )
            .await
    }
}

/// Initiates checkpoint for a running or paused pipeline.
///
/// Returns a checkpoint sequence number that can be used with `/checkpoint_status` to
/// determine when the checkpoint has completed.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Checkpoint initiated"
            , body = CheckpointResponse),
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
#[post("/pipelines/{pipeline_name}/checkpoint")]
pub(crate) async fn checkpoint_pipeline(
    state: WebData<ServerState>,
    _client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    #[cfg(not(feature = "feldera-enterprise"))]
    {
        let _ = (state, tenant_id, path.into_inner(), request);
        Err(CommonError::EnterpriseFeature {
            feature: "checkpoint".to_string(),
        }
        .into())
    }

    #[cfg(feature = "feldera-enterprise")]
    {
        let pipeline_name = path.into_inner();
        state
            .runner
            .forward_http_request_to_pipeline_by_name(
                _client.as_ref(),
                *tenant_id,
                &pipeline_name,
                Method::POST,
                "checkpoint",
                request.query_string(),
                Some(Duration::from_secs(120)),
            )
            .await
    }
}

/// Retrieve status of checkpoint activity in a pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
         , description = "Checkpoint status retrieved successfully"
         , content_type = "application/json"
         , body = CheckpointStatus),
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
#[get("/pipelines/{pipeline_name}/checkpoint_status")]
pub(crate) async fn get_checkpoint_status(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "checkpoint_status",
            request.query_string(),
            None,
        )
        .await
}

/// Retrieve status of checkpoint sync activity in a pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
         , description = "Checkpoint sync status retrieved successfully"
         , content_type = "application/json"
         , body = CheckpointStatus),
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
#[get("/pipelines/{pipeline_name}/checkpoint/sync_status")]
pub(crate) async fn get_checkpoint_sync_status(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "checkpoint/sync_status",
            request.query_string(),
            None,
        )
        .await
}

/// Retrieve the heap profile of a running or paused pipeline.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
    ),
    responses(
        (status = OK
            , description = "Heap usage profile as a gzipped protobuf that can be inspected by the pprof tool"
            , content_type = "application/protobuf"
            , body = Vec<u8>),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = BAD_REQUEST
            , description = "Getting a heap profile is not supported on this platform"
            , body = ErrorResponse),
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
#[get("/pipelines/{pipeline_name}/heap_profile")]
pub(crate) async fn get_pipeline_heap_profile(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "heap_profile",
            request.query_string(),
            None,
        )
        .await
}

/// Check if the request is a WebSocket upgrade request.
fn request_is_websocket(request: &HttpRequest) -> bool {
    request
        .headers()
        .get(header::CONNECTION)
        .and_then(|val| val.to_str().ok())
        .is_some_and(|conn| conn.to_ascii_lowercase().contains("upgrade"))
        && request
            .headers()
            .get(header::UPGRADE)
            .and_then(|val| val.to_str().ok())
            .is_some_and(|upgrade| upgrade.eq_ignore_ascii_case("websocket"))
}

/// Execute an ad-hoc SQL query in a running or paused pipeline.
///
/// The evaluation is not incremental.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("sql" = String, Query, description = "SQL query to execute"),
        ("format" = AdHocResultFormat, Query, description = "Input data format, e.g., 'text', 'json' or 'parquet'"),
    ),
    responses(
        (status = OK
            , description = "Ad-hoc SQL query result"
            , content_type = "text/plain"
            , body = Vec<u8>),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = BAD_REQUEST
            , description = "Invalid SQL query"
            , body = ErrorResponse),
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
#[get("/pipelines/{pipeline_name}/query")]
pub(crate) async fn pipeline_adhoc_sql(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();
    if request_is_websocket(&request) {
        state
            .runner
            .forward_websocket_request_to_pipeline_by_name(
                client.as_ref(),
                *tenant_id,
                &pipeline_name,
                "query",
                request,
                body,
            )
            .await
    } else {
        state
            .runner
            .forward_streaming_http_request_to_pipeline_by_name(
                client.as_ref(),
                *tenant_id,
                &pipeline_name,
                "query",
                request,
                body,
                Some(Duration::MAX),
            )
            .await
    }
}

/// Generate a completion token for an input connector.
///
/// Returns a token that can be passed to the `/completion_status` endpoint
/// to check whether the pipeline has finished processing all inputs received from the
/// connector before the token was generated.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("table_name" = String, Path,
            description = "SQL table name. Unquoted SQL names have to be capitalized. Quoted SQL names have to exactly match the case from the SQL program."),
        ("connector_name" = String, Path, description = "Unique input connector name")
    ),
    responses(
        (status = OK
            , description = "Completion token that can be passed to the '/completion_status' endpoint."
            , content_type = "application/json"
            , body = CompletionTokenResponse),
        (status = NOT_FOUND
            , description = "Specified pipeline, table, or connector does not exist"
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
#[get(
    "/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/completion_token"
)]
pub(crate) async fn completion_token(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<(String, String, String)>,
) -> Result<HttpResponse, ManagerError> {
    // Parse the URL path parameters
    let (pipeline_name, table_name, connector_name) = path.into_inner();

    let actual_table_name = SqlIdentifier::from(&table_name).name();
    let endpoint_name = format!("{actual_table_name}.{connector_name}");
    let encoded_endpoint_name: String = urlencoding::encode(&endpoint_name).to_string();

    // Forward the action request to the pipeline
    let response = state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            &format!("input_endpoints/{encoded_endpoint_name}/completion_token"),
            "",
            None,
        )
        .await?;

    Ok(response)
}

/// Check the status of a completion token returned by the `/ingress` or `/completion_token`
/// endpoint.
#[utoipa::path(
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    params(
        ("pipeline_name" = String, Path, description = "Unique pipeline name"),
        ("token" = String, Query, description = "Completion token returned by the '/ingress' or '/completion_status' endpoint."),
    ),
    responses(
        (status = OK
            , description = "The pipeline has finished processing inputs associated with the provided completion token."
            , content_type = "application/json"
            , body = CompletionStatusResponse),
        (status = ACCEPTED
            , description = "The pipeline is still processing inputs associated with the provided completion token."
            , content_type = "application/json"
            , body = CompletionStatusResponse),
        (status = GONE
            , description = "Completion token was created by a previous incarnation of the pipeline and is not valid for the current incarnation. This indicates that the pipeline was suspended and resumed from a checkpoint or restarted after a failure."
            , body = ErrorResponse,
        ),
        (status = NOT_FOUND
            , description = "Pipeline with that name does not exist"
            , body = ErrorResponse
            , example = json!(examples::error_unknown_pipeline_name())),
        (status = BAD_REQUEST
            , description = "An invalid completion token was provided"
            , body = ErrorResponse,
        ),
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
#[get("/pipelines/{pipeline_name}/completion_status")]
pub(crate) async fn completion_status(
    state: WebData<ServerState>,
    client: WebData<awc::Client>,
    tenant_id: ReqData<TenantId>,
    path: web::Path<String>,
    request: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let pipeline_name = path.into_inner();

    // Forward the action request to the pipeline
    let response = state
        .runner
        .forward_http_request_to_pipeline_by_name(
            client.as_ref(),
            *tenant_id,
            &pipeline_name,
            Method::GET,
            "completion_status",
            request.query_string(),
            None,
        )
        .await?;

    Ok(response)
}
