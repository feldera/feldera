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
    api::{examples, parse_string_param, ProgramStatus},
    auth::TenantId,
    db::{storage::Storage, DBError, ProgramDescr, ProgramId, Version},
};

use super::{ManagerError, ServerState};
use uuid::Uuid;

/// Response to a program code request.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProgramCodeResponse {
    /// Current program meta-data.
    program: ProgramDescr,
    /// Program code.
    code: String,
}

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
pub(crate) struct CompileProgramRequest {
    /// Latest program version known to the client.
    version: Version,
}

/// Request to create a new Feldera program.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct NewProgramRequest {
    /// Program name.
    #[schema(example = "Example program")]
    name: String,
    /// Program description.
    #[schema(example = "Example description")]
    description: String,
    /// SQL code of the program.
    #[schema(example = "CREATE TABLE example(name VARCHAR);")]
    code: String,
}

/// Response to a new program request.
#[derive(Serialize, ToSchema)]
pub(crate) struct NewProgramResponse {
    /// Id of the newly created program.
    #[schema(example = 42)]
    program_id: ProgramId,
    /// Initial program version (this field is always set to 1 when
    /// a program is first created).
    #[schema(example = 1)]
    version: Version,
}

/// Update program request.
#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateProgramRequest {
    /// New name for the program.
    name: Option<String>,
    /// New description for the program.
    #[serde(default)]
    description: Option<String>,
    /// New SQL code for the program or `None` to keep existing program
    /// code unmodified.
    code: Option<String>,
    /// A version guard: update the program only if the program version
    /// matches the supplied value.
    guard: Option<Version>,
}

/// Response to a program update request.
#[derive(Serialize, ToSchema)]
pub(crate) struct UpdateProgramResponse {
    /// New program version.  Equals the previous version if program code
    /// doesn't change or previous version +1 if it does.
    version: Version,
}

/// Request to create or replace a Feldera program.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateOrReplaceProgramRequest {
    /// Program description.
    #[schema(example = "Example description")]
    description: String,
    /// SQL code of the program.
    #[schema(example = "CREATE TABLE Example(name varchar);")]
    code: String,
}

/// Response to a new program request.
#[derive(Serialize, ToSchema)]
pub(crate) struct CreateOrReplaceProgramResponse {
    /// Id of the newly created program.
    #[schema(example = 42)]
    program_id: ProgramId,
    /// Initial program version (this field is always set to 1 when
    /// a program is first created).
    #[schema(example = 1)]
    version: Version,
}

/// Fetch programs, optionally filtered by name or ID.
#[utoipa::path(
    responses(
        (status = OK, description = "Programs retrieved successfully.", body = [ProgramDescr]),
        (status = NOT_FOUND
            , description = "Specified program name or ID does not exist."
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
    if let Some(id) = req.id {
        let program = state
            .db
            .lock()
            .await
            .get_program_by_id(*tenant_id, ProgramId(id), with_code)
            .await?;

        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&vec![program]))
    } else if let Some(name) = req.name.clone() {
        let program = state
            .db
            .lock()
            .await
            .get_program_by_name(*tenant_id, &name, with_code, None)
            .await?;
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&vec![program]))
    } else {
        let programs = state
            .db
            .lock()
            .await
            .list_programs(*tenant_id, with_code)
            .await?;
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&programs))
    }
}

/// Fetch a program by ID.
#[utoipa::path(
    responses(
        (status = OK, description = "Program retrieved successfully.", body = ProgramDescr),
        (status = NOT_FOUND
            , description = "Specified program id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_program())),
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
            , description = "A program with this name already exists in the database."
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
    state
        .db
        .lock()
        .await
        .new_program(
            *tenant_id,
            Uuid::now_v7(),
            &request.name,
            &request.description,
            &request.code,
            None,
        )
        .await
        .map(|(program_id, version)| {
            info!(
                "Created program {program_id} with version {version} (tenant:{})",
                *tenant_id
            );
            HttpResponse::Created()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .json(&NewProgramResponse {
                    program_id,
                    version,
                })
        })
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
        (status = OK, description = "Program updated successfully.", body = UpdateProgramResponse),
        (status = NOT_FOUND
            , description = "Specified program id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_program())),
        (status = CONFLICT
            , description = "A program with this name already exists in the database."
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
    let program = db
        .get_program_by_name(*tenant_id, &program_name, false, None)
        .await?;
    let version = db
        .update_program(
            *tenant_id,
            program.program_id,
            &body.name,
            &body.description,
            &body.code,
            &None,
            &None,
            body.guard,
            None,
        )
        .await?;
    info!(
        "Updated program {program_name} to version {version} (tenant:{})",
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
            , description = "A program with this name already exists in the database."
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
        .create_or_replace_program(*tenant_id, &program_name, &body.description, &body.code)
        .await?;
    if created {
        Ok(HttpResponse::Created()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplaceProgramResponse {
                program_id,
                version,
            }))
    } else {
        Ok(HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .json(&CreateOrReplaceProgramResponse {
                program_id,
                version,
            }))
    }
}

/// Mark a program for compilation.
///
/// The client can track a program's compilation status by pollling the
/// `/program/{program_id}` or `/programs` endpoints, and
/// then checking the `status` field of the program object
#[utoipa::path(
    request_body = CompileProgramRequest,
    responses(
        (status = ACCEPTED, description = "Compilation request submitted."),
        (status = NOT_FOUND
            , description = "Specified program id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_program())),
        (status = CONFLICT
            , description = "Program version specified in the request doesn't match the latest program version in the database."
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
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<CompileProgramRequest>,
) -> Result<HttpResponse, ManagerError> {
    let program_name = parse_string_param(&request, "program_name")?;
    let descr = state
        .db
        .lock()
        .await
        .get_program_by_name(*tenant_id, &program_name, false, None)
        .await?;
    if descr.version != body.version {
        return Err(DBError::OutdatedProgramVersion {
            latest_version: descr.version,
        }
        .into());
    }
    // Do nothing if the program:
    // * is already Pending (we don't want to requeue it for compilation),
    // * if compilation is already in progress,
    // * or if the program has already been compiled
    if descr.status == ProgramStatus::Pending
        || descr.status.is_compiling()
        || descr.status == ProgramStatus::Success
    {
        return Ok(HttpResponse::Accepted().finish());
    }
    state
        .db
        .lock()
        .await
        .set_program_status_guarded(
            *tenant_id,
            descr.program_id,
            body.version,
            ProgramStatus::Pending,
        )
        .await?;
    Ok(HttpResponse::Accepted().finish())
}

/// Delete a program.
///
/// Deletion fails if there is at least one pipeline associated with the
/// program.
#[utoipa::path(
    responses(
        (status = OK, description = "Program successfully deleted."),
        (status = BAD_REQUEST
            , description = "Specified program id is referenced by a pipeline or is not a valid uuid."
            , body = ErrorResponse
            , examples (
                ("Program in use" =
                    (description = "Specified program id is referenced by a pipeline",
                      value = json!(examples::program_in_use_by_pipeline()))),
                ("Invalid uuid" =
                    (description = "Specified program id is not a valid uuid.",
                     value = json!(examples::invalid_uuid_param()))),
            )
        ),
        (status = NOT_FOUND
            , description = "Specified program id does not exist."
            , body = ErrorResponse
            , example = json!(examples::unknown_program())),
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
    info!("Deleted program {program_name} (tenant:{})", *tenant_id);
    Ok(resp)
}
