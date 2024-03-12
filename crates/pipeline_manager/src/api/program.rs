/// API to create, modify, delete and compile SQL programs
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post, put,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::info;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    api::{examples, parse_string_param},
    auth::TenantId,
    compiler::ProgramConfig,
    db::{storage::Storage, DBError, ProgramId, Version},
};

use super::{ManagerError, ServerState};
use uuid::Uuid;

#[derive(Debug, Deserialize, IntoParams)]
pub struct ProgramIdOrNameQuery {
    /// Unique program identifier.
    id: Option<Uuid>,
    /// Unique program name.
    name: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct WithCodeQuery {
    /// Option to include the SQL program code or not
    /// in the Program objects returned by the query.
    /// If false (default), the returned program object
    /// will not include the code.
    with_code: Option<bool>,
}

/// Request to queue a program for compilation.
#[derive(Deserialize, ToSchema)]
#[allow(dead_code)] // Reason: this request type is deprecated
pub(crate) struct CompileProgramRequest {
    /// Latest program version known to the client.
    version: Version,
}

/// Request to create a new program.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct NewProgramRequest {
    /// Program name.
    #[schema(example = "example-program")]
    name: String,
    /// Program description.
    #[schema(example = "Example description")]
    description: String,
    /// SQL code of the program.
    #[schema(example = "CREATE TABLE example(name VARCHAR);")]
    code: String,
    /// Program configuration.
    config: ProgramConfig,
}

/// Response to a new program request.
#[derive(Serialize, ToSchema)]
pub(crate) struct NewProgramResponse {
    /// Unique id assigned to the new program.
    program_id: ProgramId,
    /// Initial program version (this field is always set to 1 when
    /// a program is first created).
    #[schema(example = 1)]
    version: Version,
}

/// Request to update an existing program.
#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateProgramRequest {
    /// New program name. If absent, existing name will be kept unmodified.
    name: Option<String>,
    /// New program description. If absent, existing description will be kept
    /// unmodified.
    description: Option<String>,
    /// New SQL code for the program. If absent, existing program code will be
    /// kept unmodified.
    code: Option<String>,
    /// A version guard: update the program only if the program version
    /// matches the supplied value.
    guard: Option<Version>,
    /// Program configuration.
    config: ProgramConfig,
}

/// Response to a program update request.
#[derive(Serialize, ToSchema)]
pub(crate) struct UpdateProgramResponse {
    /// New program version. Equals the previous version if program code
    /// doesn't change or previous version +1 if it does.
    version: Version,
}

/// Request to create or replace a program.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateOrReplaceProgramRequest {
    /// Program description.
    #[schema(example = "Example description")]
    description: String,
    /// SQL code of the program.
    #[schema(example = "CREATE TABLE example(name VARCHAR);")]
    code: String,
    /// Program configuration.
    config: ProgramConfig,
}

/// Response to a create or replace program request.
#[derive(Serialize, ToSchema)]
pub(crate) struct CreateOrReplaceProgramResponse {
    /// Unique id assigned to the program.
    program_id: ProgramId,
    /// Program version. This field is always set to 1 when
    /// a program is first created. After an update, it equals
    /// the previous version if program code doesn't change
    /// or previous version +1 if it does.
    #[schema(example = 1)]
    version: Version,
}

/// Fetch programs, optionally filtered by name or ID.
#[utoipa::path(
    responses(
        (status = OK, description = "List of programs retrieved successfully", body = [ProgramDescr]),
        (status = NOT_FOUND
            , description = "Specified program name or ID does not exist"
            , body = ErrorResponse
            , examples(
                ("Unknown program name" = (value = json!(examples::unknown_name()))),
                ("Unknown program ID" = (value = json!(examples::unknown_program())))
            ),
        )
    ),
    params(ProgramIdOrNameQuery, WithCodeQuery),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[get("/programs")]
pub(crate) async fn get_programs(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: web::Query<ProgramIdOrNameQuery>,
    with_code: web::Query<WithCodeQuery>,
) -> Result<HttpResponse, ManagerError> {
    let with_code = with_code.with_code.unwrap_or(false);
    let programs = if let Some(id) = req.id {
        vec![
            state
                .db
                .lock()
                .await
                .get_program_by_id(*tenant_id, ProgramId(id), with_code)
                .await?,
        ]
    } else if let Some(name) = req.name.clone() {
        vec![
            state
                .db
                .lock()
                .await
                .get_program_by_name(*tenant_id, &name, with_code, None)
                .await?,
        ]
    } else {
        state
            .db
            .lock()
            .await
            .list_programs(*tenant_id, with_code)
            .await?
    };
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&programs))
}

/// Fetch a program by name.
#[utoipa::path(
    responses(
        (status = OK, description = "Program retrieved successfully", body = ProgramDescr),
        (status = NOT_FOUND
            , description = "Specified program name does not exist"
            , body = ErrorResponse
            , example = json!(examples::unknown_name())),
    ),
    params(
        ("program_name" = String, Path, description = "Unique program name"),
        WithCodeQuery
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[get("/programs/{program_name}")]
async fn get_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    query: web::Query<WithCodeQuery>,
) -> Result<HttpResponse, ManagerError> {
    let program_name = parse_string_param(&req, "program_name")?;
    let with_code = query.with_code.unwrap_or(false);
    let program = state
        .db
        .lock()
        .await
        .get_program_by_name(*tenant_id, &program_name, with_code, None)
        .await?;

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&program))
}

/// Create a new program.
#[utoipa::path(
    request_body = NewProgramRequest,
    responses(
        (status = CREATED, description = "Program created successfully", body = NewProgramResponse),
        (status = CONFLICT
            , description = "A program with this name already exists in the database"
            , body = ErrorResponse
            , example = json!(examples::duplicate_name())),
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[post("/programs")]
async fn new_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: web::Json<NewProgramRequest>,
) -> Result<HttpResponse, DBError> {
    let (program_id, version) = state
        .db
        .lock()
        .await
        .new_program(
            *tenant_id,
            Uuid::now_v7(),
            &request.name,
            &request.description,
            &request.code,
            &request.config,
            None,
        )
        .await?;

    info!(
        "Created program with name {} and id {} and version {} (tenant: {})",
        &request.name, program_id, version, *tenant_id
    );
    Ok(HttpResponse::Created()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&NewProgramResponse {
            program_id,
            version,
        }))
}

/// Change one or more of a program's code, description or name.
///
/// If a program's code changes, any ongoing compilation gets cancelled,
/// the program status is reset to `None`, and the program version
/// is incremented by 1.
///
/// Changing only the program's name or description does not affect its
/// version or the compilation process.
#[utoipa::path(
    request_body = UpdateProgramRequest,
    responses(
        (status = OK, description = "Program updated successfully", body = UpdateProgramResponse),
        (status = NOT_FOUND
            , description = "Specified program name does not exist"
            , body = ErrorResponse
            , example = json!(examples::unknown_name())),
        (status = CONFLICT
            , description = "A program with this name already exists in the database"
            , body = ErrorResponse
            , example = json!(examples::duplicate_name())),
    ),
    params(
        ("program_name" = String, Path, description = "Unique program name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[patch("/programs/{program_name}")]
async fn update_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<UpdateProgramRequest>,
) -> Result<HttpResponse, ManagerError> {
    let program_name = parse_string_param(&request, "program_name")?;
    let db = state.db.lock().await;
    let version = db
        .update_program_by_name(
            *tenant_id,
            &program_name,
            &body.name,
            &body.description,
            &body.code,
            &Some(body.config.clone()),
            body.guard,
        )
        .await?;

    info!(
        "Updated program {program_name} to version {version} (tenant: {})",
        *tenant_id
    );
    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdateProgramResponse { version }))
}

/// Create or replace a program.
#[utoipa::path(
    request_body = CreateOrReplaceProgramRequest,
    responses(
        (status = CREATED, description = "Program created successfully", body = CreateOrReplaceProgramResponse),
        (status = OK, description = "Program updated successfully", body = CreateOrReplaceProgramResponse),
        (status = CONFLICT
            , description = "A program with this name already exists in the database"
            , body = ErrorResponse
            , example = json!(examples::duplicate_name())),
    ),
    params(
        ("program_name" = String, Path, description = "Unique program name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[put("/programs/{program_name}")]
async fn create_or_replace_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<CreateOrReplaceProgramRequest>,
) -> Result<HttpResponse, ManagerError> {
    let program_name = parse_string_param(&request, "program_name")?;
    let (created, program_id, version) = state
        .db
        .lock()
        .await
        .create_or_replace_program(
            *tenant_id,
            &program_name,
            &body.description,
            &body.code,
            &body.config,
        )
        .await?;
    if created {
        info!(
            "Created program with name {} and id {} and version {} (tenant: {})",
            program_name, program_id, version, *tenant_id
        );
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplaceProgramResponse {
                program_id,
                version,
            }))
    } else {
        info!(
            "Updated program {program_name} to version {version} (tenant: {})",
            *tenant_id
        );
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplaceProgramResponse {
                program_id,
                version,
            }))
    }
}

/// Deprecated. Mark a program for compilation.
///
/// The client can track a program's compilation status by polling the
/// `/program/{program_name}` or `/programs` endpoints, and
/// then checking the `status` field of the program object.
#[utoipa::path(
    request_body = CompileProgramRequest,
    responses(
        (status = ACCEPTED, description = "Compilation request submitted"),
        (status = NOT_FOUND
            , description = "Specified program name does not exist"
            , body = ErrorResponse
            , example = json!(examples::unknown_name())),
        (status = CONFLICT
            , description = "Program version specified in the request doesn't match the latest program version in the database"
            , body = ErrorResponse
            , example = json!(examples::outdated_program_version())),
    ),
    params(
        ("program_name" = String, Path, description = "Unique program name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[post("/programs/{program_name}/compile")]
async fn compile_program(
    _state: WebData<ServerState>,
    _tenant_id: ReqData<TenantId>,
    _request: HttpRequest,
    _body: web::Json<CompileProgramRequest>,
) -> Result<HttpResponse, ManagerError> {
    Ok(HttpResponse::Accepted().finish())
}

/// Delete a program.
///
/// Deletion fails if there is at least one pipeline associated with the
/// program.
#[utoipa::path(
    responses(
        (status = OK, description = "Program successfully deleted"),
        (status = BAD_REQUEST
            , description = "Specified program is referenced by a pipeline"
            , body = ErrorResponse
            , example = json!(examples::program_in_use_by_pipeline()),
        ),
        (status = NOT_FOUND
            , description = "Specified program name does not exist"
            , body = ErrorResponse
            , example = json!(examples::unknown_name())),
    ),
    params(
        ("program_name" = String, Path, description = "Unique program name")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[delete("/programs/{program_name}")]
async fn delete_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let program_name = parse_string_param(&req, "program_name")?;
    let db = state.db.lock().await;
    let resp = db
        .delete_program(*tenant_id, &program_name)
        .await
        .map(|_| HttpResponse::Ok().finish())?;

    info!("Deleted program {program_name} (tenant: {})", *tenant_id);
    Ok(resp)
}
