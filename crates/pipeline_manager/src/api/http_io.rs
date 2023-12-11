/// API to read from tables/views and write into tables using HTTP
use actix_web::{
    post,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::debug;

use crate::{
    api::{examples, parse_uuid_param},
    auth::TenantId,
    db::PipelineId,
};

use super::{ManagerError, ServerState};

/// Push data to a SQL table.
///
/// The client sends data encoded using the format specified in the `?format=`
/// parameter as a body of the request.  The contents of the data must match
/// the SQL table schema specified in `table_name`
///
/// The pipeline ingests data as it arrives without waiting for the end of
/// the request.  Successful HTTP response indicates that all data has been
/// ingested successfully.
// TODO: implement chunked and batch modes.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Data successfully delivered to the pipeline."
            , content_type = "application/json"),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(examples::invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
        (status = NOT_FOUND
            , description = "Specified table does not exist."
            , body = ErrorResponse
            // , example = json!(examples::unknown_input_table("MyTable"))
            ),
        (status = NOT_FOUND
            , description = "Pipeline is not currently running because it has been shutdown or not yet started."
            , body = ErrorResponse
            , example = json!(examples::pipeline_shutdown())),
        (status = BAD_REQUEST
            , description = "Unknown data format specified in the '?format=' argument."
            , body = ErrorResponse
            // , example = json!(examples::unknown_input_format())
            ),
        (status = BAD_REQUEST
            , description = "Error parsing input data."
            , body = ErrorResponse
            // , example = json!(examples::parse_errors())
            ),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier."),
        ("table_name" = String, Path,
            description = "SQL table name. Unquoted SQL names have to be capitalized. Quoted SQL names have to exactly match the case from the SQL program."),
        ("force" = bool, Query, description = "When `true`, push data to the pipeline even if the pipeline is paused. The default value is `false`"),
        ("format" = String, Query, description = "Input data format, e.g., 'csv' or 'json'."),
        ("array" = Option<bool>, Query, description = "Set to `true` if updates in this stream are packaged into JSON arrays (used in conjunction with `format=json`). The default values is `false`."),
        ("update_format" = Option<JsonUpdateFormat>, Query, description = "JSON data change event format (used in conjunction with `format=json`).  The default value is 'insert_delete'."),
    ),
    security(("JWT token or API key" = [])),
    tag = "HTTP input/output",
    request_body(
        content = String,
        description = "Contains the new input data in CSV.",
        content_type = "text/csv",
    ),
)]
#[post("/pipelines/{pipeline_id}/ingress/{table_name}")]
async fn http_input(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    debug!("Received {req:?}");

    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);
    debug!("Pipeline_id {:?}", pipeline_id);

    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(ManagerError::MissingUrlEncodedParam {
                param: "table_name",
            });
        }
        Some(table_name) => table_name,
    };
    debug!("Table name {table_name:?}");

    let endpoint = format!("ingress/{table_name}");

    state
        .runner
        .forward_to_pipeline_as_stream(*tenant_id, pipeline_id, &endpoint, req, body)
        .await
}

/// Subscribe to a stream of updates from a SQL view or table.
///
/// The pipeline responds with a continuous stream of changes to the specified
/// table or view, encoded using the format specified in the `?format=`
/// parameter. Updates are split into `Chunk`s.
///
/// The pipeline continues sending updates until the client closes the
/// connection or the pipeline is shut down.
///
/// This API is a POST instead of a GET, because when performing neighborhood
/// queries (query='neighborhood'), the call expects a request body which
/// contains, among other things, a full row to execute a neighborhood search
/// around. A row can be quite large and is not appropriate as a query
/// parameter.
#[utoipa::path(
    responses(
        (status = OK
            , description = "Connection to the endpoint successfully established. The body of the response contains a stream of data chunks."
            , content_type = "application/json"
            , body = Chunk),
        (status = BAD_REQUEST
            , description = "Specified pipeline id is not a valid uuid."
            , body = ErrorResponse
            , example = json!(examples::invalid_uuid_param())),
        (status = NOT_FOUND
            , description = "Specified pipeline id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_pipeline())),
        (status = NOT_FOUND
            , description = "Specified table or view does not exist."
            , body = ErrorResponse
            // , example = json!(examples::unknown_output_table("MyTable"))
            ),
        (status = GONE
            , description = "Pipeline is not currently running because it has been shutdown or not yet started."
            , body = ErrorResponse
            , example = json!(examples::pipeline_shutdown())),
        (status = BAD_REQUEST
            , description = "Unknown data format specified in the '?format=' argument."
            , body = ErrorResponse
            // , example = json!(examples::unknown_output_format())
            ),
        (status = INTERNAL_SERVER_ERROR
            , description = "Request failed."
            , body = ErrorResponse),
    ),
    params(
        ("pipeline_id" = Uuid, Path, description = "Unique pipeline identifier."),
        ("table_name" = String, Path,
            description = "SQL table name. Unquoted SQL names have to be capitalized. Quoted SQL names have to exactly match the case from the SQL program."),
        ("format" = String, Query, description = "Output data format, e.g., 'csv' or 'json'."),
        ("query" = Option<OutputQuery>, Query, description = "Query to execute on the table. Must be one of 'table', 'neighborhood', or 'quantiles'. The default value is 'table'"),
        ("mode" = Option<EgressMode>, Query, description = "Output mode. Must be one of 'watch' or 'snapshot'. The default value is 'watch'"),
        ("quantiles" = Option<u32>, Query, description = "For 'quantiles' queries: the number of quantiles to output. The default value is 100."),
        ("array" = Option<bool>, Query, description = "Set to `true` to group updates in this stream into JSON arrays (used in conjunction with `format=json`). The default value is `false`"),
    ),
    request_body(
        content = Option<NeighborhoodQuery>,
        description = "When the `query` parameter is set to 'neighborhood', the body of the request must contain a neighborhood specification.",
        content_type = "application/json",
    ),
    security(("JWT token or API key" = [])),
    tag = "HTTP input/output"
)]
#[post("/pipelines/{pipeline_id}/egress/{table_name}")]
async fn http_output(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: web::Payload,
) -> Result<HttpResponse, ManagerError> {
    debug!("Received {req:?}");

    let pipeline_id = PipelineId(parse_uuid_param(&req, "pipeline_id")?);
    debug!("Pipeline_id {:?}", pipeline_id);

    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(ManagerError::MissingUrlEncodedParam {
                param: "table_name",
            });
        }
        Some(table_name) => table_name,
    };
    debug!("Table name {table_name:?}");

    let endpoint = format!("egress/{table_name}");

    state
        .runner
        .forward_to_pipeline_as_stream(*tenant_id, pipeline_id, &endpoint, req, body)
        .await
}
