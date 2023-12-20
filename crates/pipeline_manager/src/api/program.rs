/// API to create, modify, delete and compile SQL programs
use actix_web::{
    delete, get,
    http::header::{CacheControl, CacheDirective},
    patch, post,
    web::{self, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use log::info;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    api::{examples, parse_uuid_param},
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
    #[schema(example = "CREATE TABLE Example(name varchar);")]
    code: String,
}

/// Response to a new program request.
#[derive(Serialize, ToSchema)]
pub(crate) struct NewProgramResponse {
    /// Id of the newly created program.
    #[schema(example = 42)]
    program_id: ProgramId,
    /// Initial program version (this field is always set to 1).
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
            .get_program_by_name(*tenant_id, &name, with_code)
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
        ("program_id" = Uuid, Path, description = "Unique program identifier"),
        WithCodeQuery
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[get("/programs/{program_id}")]
async fn get_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    query: web::Query<WithCodeQuery>,
) -> Result<HttpResponse, ManagerError> {
    let program_id = ProgramId(parse_uuid_param(&req, "program_id")?);
    let with_code = query.with_code.unwrap_or(false);
    let program = state
        .db
        .lock()
        .await
        .get_program_by_id(*tenant_id, program_id, with_code)
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
        ("program_id" = Uuid, Path, description = "Unique program identifier")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[patch("/programs/{program_id}")]
async fn update_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<UpdateProgramRequest>,
) -> Result<HttpResponse, ManagerError> {
    let program_id = ProgramId(parse_uuid_param(&request, "program_id")?);
    let version = state
        .db
        .lock()
        .await
        .update_program(
            *tenant_id,
            program_id,
            &body.name,
            &body.description,
            &body.code,
            &None,
            &None,
            body.guard,
        )
        .await?;
    info!(
        "Updated program {program_id} to version {version} (tenant:{})",
        *tenant_id
    );

    Ok(HttpResponse::Ok()
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(&UpdateProgramResponse { version }))
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
        ("program_id" = Uuid, Path, description = "Unique program identifier")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[post("/programs/{program_id}/compile")]
async fn compile_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    request: HttpRequest,
    body: web::Json<CompileProgramRequest>,
) -> Result<HttpResponse, ManagerError> {
    let program_id = ProgramId(parse_uuid_param(&request, "program_id")?);
    state
        .db
        .lock()
        .await
        .prepare_program_for_compilation(*tenant_id, program_id, body.version)
        .await?;
    info!(
        "Compilation request accepted for program {program_id} version {} (tenant:{})",
        body.version, *tenant_id
    );
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
        ("program_id" = Uuid, Path, description = "Unique program identifier")
    ),
    context_path = "/v0",
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Programs"
)]
#[delete("/programs/{program_id}")]
async fn delete_program(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let program_id = ProgramId(parse_uuid_param(&req, "program_id")?);
    let db = state.db.lock().await;
    let resp = db
        .delete_program(*tenant_id, program_id)
        .await
        .map(|_| HttpResponse::Ok().finish())?;
    info!("Deleted program {program_id} (tenant:{})", *tenant_id);
    Ok(resp)
}
