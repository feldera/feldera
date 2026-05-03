//! `connectors.toml` editing endpoints (Phase 8.12).
//!
//! Four endpoints:
//!
//! * `GET  /v0/connectors/status`            — describer state + manifest envelope.
//! * `GET  /v0/connectors/connectors.toml`   — raw blob with audit headers.
//! * `PUT  /v0/connectors/connectors.toml`   — update the blob, auto-trigger rebuild.
//! * `POST /v0/connectors/refresh`           — refresh the lock and rebuild.
//!
//! All endpoints are tenant-scoped via the existing auth context.
use actix_web::{
    get,
    http::header::{CacheControl, CacheDirective, ContentType, ETag, EntityTag},
    post, put,
    web::{self, Bytes, Data as WebData, ReqData},
    HttpRequest, HttpResponse,
};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::error::ApiError;
use crate::api::main::ServerState;
#[allow(unused_imports)] // used in utoipa schema doc attributes
use feldera_types::error::ErrorResponse;
use crate::compiler::connectors::{
    load_platform_manifest, merge_manifests, parse_manifest_json, spawn_describer_build,
    validate_connectors_toml_shape, ConnectorsTomlContent,
};
use crate::compiler::manifest_cache::TenantBuildState;
use crate::db::operations::tenant_connector_config::TenantConnectorConfig;
use crate::db::storage::Storage;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;

// ── Status envelope ──────────────────────────────────────────────────────

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StatusName {
    Ready,
    Building,
    Failed,
    NotConfigured,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ConnectorsStatusResponse {
    state: StatusName,
    /// `content_hash` of the tenant's `connectors.toml`. Absent for the
    /// `not_configured` state — there is no content to hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    version: i64,
    /// Wall-clock time of the last completed build. Absent when no build
    /// has run yet (e.g. `not_configured`, or first read of a tenant).
    #[serde(skip_serializing_if = "Option::is_none")]
    last_built_at: Option<DateTime<Utc>>,
    /// Captured stderr from a failed describer build. Present only when
    /// `state == "failed"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Raw JSON manifest emitted by the describer (each entry is a
    /// `ConnectorManifestEntry`). Present only when `state == "ready"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    descriptors: Option<serde_json::Value>,
}

// ── PUT response body ────────────────────────────────────────────────────

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ConnectorsConfigPutResponse {
    /// New `content_hash` of the row after the update. Use this as the
    /// next `If-Match` value when issuing a follow-up edit.
    content_hash: String,
    /// Monotonic per-tenant version, incremented on every successful PUT.
    version: i64,
}

// ── GET /v0/connectors/status ────────────────────────────────────────────

/// Get the current describer state and (when ready) the connector manifest.
///
/// Returns a unified envelope:
/// * `state` is one of `ready` | `building` | `failed` | `not_configured`.
/// * `content_hash` is the ETag value to echo on `If-Match` for a
///   subsequent `PUT /v0/connectors/connectors.toml`.
/// * `descriptors` is the parsed JSON manifest, present only when `state`
///   is `ready`.
///
/// Honors `If-None-Match`: a request whose value matches the current
/// `content_hash` receives `304 Not Modified` with no body — cheap to
/// poll for the web console status badge.
#[utoipa::path(
    get,
    path = "/v0/connectors/status",
    responses(
        (status = 200, description = "Status retrieved.", body = ConnectorsStatusResponse),
        (status = 304, description = "Not modified."),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connector plugin management"
)]
#[get("/connectors/status")]
pub(crate) async fn get_connectors_status(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    let tenant_id = *tenant_id;

    // Read the tenant's row, lazily seeding it from
    // `connectors_toml_path` on first access.
    let row = read_tenant_row(&state, tenant_id).await?;

    // Make sure the cache reflects the current row. If not, kick off the
    // appropriate state transition (NotConfigured for empty content,
    // Building for non-empty).
    materialize_cache(&state, tenant_id, &row).await;

    // Re-read state after materialization.
    let cache_state = state.manifest_cache.get(tenant_id).await;
    let envelope = build_status_envelope(&state, &row, cache_state.as_ref());

    // ETag handling — use the row's hash. Empty content also has a hash
    // (sha256("") is a stable constant), so the ETag is always present.
    let etag = EntityTag::new_strong(row.content_hash.clone());
    if request_matches(req.headers().get(actix_web::http::header::IF_NONE_MATCH), &etag) {
        return Ok(HttpResponse::NotModified()
            .insert_header(ETag(etag))
            .finish());
    }

    Ok(HttpResponse::Ok()
        .insert_header(ETag(etag))
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .json(envelope))
}

/// Merge the per-tenant user manifest JSON with the platform manifest
/// and return a JSON array suitable for the `descriptors` field.
/// Returns `None` only when the user JSON is unparseable (a programming
/// error — the describer validated it before the cache transition);
/// the API layer falls back to a `descriptors: null` payload in that
/// case rather than a 500.
fn merge_user_with_platform(
    state: &ServerState,
    user_manifest_json: &str,
) -> Option<serde_json::Value> {
    let _ = state;
    let user = parse_manifest_json(user_manifest_json).ok()?;
    let platform = load_platform_manifest();
    let merged: Vec<_> = merge_manifests(platform, user).into_values().collect();
    serde_json::to_value(merged).ok()
}

fn build_status_envelope(
    state: &ServerState,
    row: &TenantConnectorConfig,
    cache_state: Option<&TenantBuildState>,
) -> ConnectorsStatusResponse {
    // `not_configured` is determined by the row content, not by cache
    // state — even when the cache hasn't been populated yet, an empty
    // row is unambiguously "not configured".
    if row.content.is_empty() {
        return ConnectorsStatusResponse {
            state: StatusName::NotConfigured,
            content_hash: None,
            version: row.version,
            last_built_at: None,
            error: None,
            descriptors: None,
        };
    }

    match cache_state {
        Some(TenantBuildState::Ready {
            content_hash,
            version,
            manifest_json,
            last_built_at,
        }) => ConnectorsStatusResponse {
            state: StatusName::Ready,
            content_hash: Some(content_hash.clone()),
            version: *version,
            last_built_at: Some(*last_built_at),
            error: None,
            // The user describer's manifest contains only user-listed
            // connectors; merge with the platform manifest so the
            // surface returned here matches the SQL compiler's view.
            descriptors: merge_user_with_platform(state, manifest_json),
        },
        Some(TenantBuildState::Failed {
            content_hash,
            version,
            error,
            last_built_at,
        }) => ConnectorsStatusResponse {
            state: StatusName::Failed,
            content_hash: Some(content_hash.clone()),
            version: *version,
            last_built_at: Some(*last_built_at),
            error: Some(error.clone()),
            descriptors: None,
        },
        // Building, NotConfigured-in-cache (stale), or no cache entry: report Building
        // (a build is/will be in flight after `materialize_cache`).
        _ => ConnectorsStatusResponse {
            state: StatusName::Building,
            content_hash: Some(row.content_hash.clone()),
            version: row.version,
            last_built_at: None,
            error: None,
            descriptors: None,
        },
    }
}

// ── GET /v0/connectors/connectors.toml ───────────────────────────────────

/// Download the raw `connectors.toml` blob.
///
/// Headers:
///   * `ETag` — the row's `content_hash`. Echo this in `If-Match` on PUT.
///   * `X-Connectors-Version` — monotonic per-tenant version.
///   * `X-Connectors-Edited-At` / `X-Connectors-Edited-By` — audit trail.
#[utoipa::path(
    get,
    path = "/v0/connectors/connectors.toml",
    responses(
        (status = 200, description = "Blob retrieved.", content_type = "text/plain"),
    ),
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connector plugin management"
)]
#[get("/connectors/connectors.toml")]
pub(crate) async fn get_connectors_toml(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let row = read_tenant_row(&state, *tenant_id).await?;
    let etag = EntityTag::new_strong(row.content_hash.clone());

    Ok(HttpResponse::Ok()
        .insert_header(ETag(etag))
        .insert_header(ContentType::plaintext())
        .insert_header(CacheControl(vec![CacheDirective::NoCache]))
        .insert_header(("X-Connectors-Version", row.version.to_string()))
        .insert_header(("X-Connectors-Edited-At", row.edited_at.to_rfc3339()))
        .insert_header(("X-Connectors-Edited-By", row.edited_by.clone()))
        .body(row.content))
}

// ── PUT /v0/connectors/connectors.toml ───────────────────────────────────

/// Replace the tenant's `connectors.toml` blob and trigger a rebuild.
///
/// `If-Match` is required — its value is the `content_hash` from the
/// most recent GET. A mismatch means another operator updated the row
/// since this client read it; the response is `412 Precondition Failed`.
///
/// The body is plain text (the raw toml). Empty body clears the blob and
/// transitions the tenant back to the `not_configured` state.
///
/// On success the response is `202 Accepted` — the describer rebuild
/// runs in the background. Poll `GET /v0/connectors/status` to observe
/// the transition `building → ready` (or `failed`).
#[utoipa::path(
    put,
    path = "/v0/connectors/connectors.toml",
    request_body(content = String, content_type = "text/plain"),
    responses(
        (status = 202, description = "Update accepted; rebuild in progress.", body = ConnectorsConfigPutResponse),
        (status = 400, description = "Body fails the line-shape check."),
        (status = 412, description = "If-Match hash does not match the current row."),
    ),
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connector plugin management"
)]
#[put("/connectors/connectors.toml")]
pub(crate) async fn put_connectors_toml(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
    req: HttpRequest,
    body: Bytes,
) -> Result<HttpResponse, ManagerError> {
    let tenant_id = *tenant_id;

    // Body must be valid UTF-8 text and pass the line-shape check.
    let new_content = String::from_utf8(body.to_vec()).map_err(|_| {
        ApiError::ConnectorsConfigInvalidShape {
            line: 0,
            reason: "body is not valid UTF-8".to_string(),
        }
    })?;
    if let Err((line, reason)) = validate_connectors_toml_shape(&new_content) {
        return Err(ApiError::ConnectorsConfigInvalidShape { line, reason }.into());
    }

    // `If-Match` is mandatory.
    let expected_hash = required_if_match(&req)?;

    // Bootstrap the row if this tenant has never issued a GET. Without this
    // the UPDATE inside `put_connectors_config` finds no row and the
    // optimistic-concurrency check degrades to an "unknown tenant" 401.
    read_tenant_row(&state, tenant_id).await?;

    // Use the tenant ID as `edited_by` until a richer auth identity is
    // plumbed through (Phase 8.12).
    let edited_by = tenant_id.0.to_string();

    let updated = state
        .db
        .lock()
        .await
        .put_connectors_config(tenant_id, new_content.clone(), edited_by, &expected_hash)
        .await?;

    // Decide what to do with the cache + describer.
    if updated.content.is_empty() {
        state
            .manifest_cache
            .mark_not_configured(tenant_id, updated.version)
            .await;
    } else if let Some(config) = state.compiler_config.as_ref() {
        spawn_describer_build(
            state.manifest_cache.clone(),
            state.build_log_bus.clone(),
            config.clone(),
            tenant_id,
            ConnectorsTomlContent(updated.content.clone()),
            updated.content_hash.clone(),
            updated.version,
            false,
        )
        .await;
    }
    // No `compiler_config`: the API server is running standalone (tests
    // or a deployment without a paired compiler). The cache stays
    // unpopulated for the new hash — `GET /status` reports `building`
    // until a compiler-equipped manager picks it up.

    Ok(HttpResponse::Accepted()
        .insert_header(ETag(EntityTag::new_strong(updated.content_hash.clone())))
        .insert_header(("Location", "/v0/connectors/status"))
        .json(ConnectorsConfigPutResponse {
            content_hash: updated.content_hash,
            version: updated.version,
        }))
}

// ── GET /v0/connectors/build-log ─────────────────────────────────────────

/// Stream the live describer build log for the requesting tenant.
///
/// The response is a chunked `text/plain` body, one log line per
/// newline (matching the runner's `/v0/pipelines/{name}/logs`
/// endpoint). Subscribers are caught up with the buffered history of
/// the current session before receiving live lines; the stream ends
/// when the session terminates (build finished, or replaced by a
/// newer build).
///
/// When the tenant has never had a build, the response is a 200 with
/// an empty body — same shape as a finished session so the client
/// does not need to special-case "no build yet".
///
/// `Content-Encoding: identity` is forced to avoid gzip frame
/// buffering that would otherwise stall live tailing in browsers.
#[utoipa::path(
    get,
    path = "/v0/connectors/build-log",
    responses(
        (status = 200, description = "Live tail of the describer build log.", content_type = "text/plain"),
        (status = INTERNAL_SERVER_ERROR, body = ErrorResponse),
    ),
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connector plugin management"
)]
#[get("/connectors/build-log")]
pub(crate) async fn get_connectors_build_log(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let receiver = state.build_log_bus.subscribe(*tenant_id).await;

    let stream = try_stream! {
        if let Some(mut rx) = receiver {
            while let Some(line) = rx.recv().await {
                yield web::Bytes::from(format!("{line}\n"));
            }
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("text/plain; charset=utf-8")
        .insert_header(actix_http::ContentEncoding::Identity)
        .insert_header(("X-Content-Type-Options", "nosniff"))
        .streaming::<_, actix_web::Error>(stream))
}

// ── POST /v0/connectors/refresh ──────────────────────────────────────────

/// Refresh the describer's `Cargo.lock` (run `cargo update`) and rebuild
/// the manifest. Use this to pick up upstream patch releases without
/// editing the blob.
///
/// Returns `202 Accepted` immediately; poll
/// `GET /v0/connectors/status` to observe the rebuild outcome.
#[utoipa::path(
    post,
    path = "/v0/connectors/refresh",
    responses(
        (status = 202, description = "Refresh accepted; rebuild in progress."),
        (status = 404, description = "Compiler config is not available."),
    ),
    security(("JSON web token (JWT) or API key" = [])),
    tag = "Connector plugin management"
)]
#[post("/connectors/refresh")]
pub(crate) async fn post_connectors_refresh(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let tenant_id = *tenant_id;

    let Some(ref config) = state.compiler_config else {
        return Err(ApiError::CompilerConfigNotAvailable.into());
    };

    let row = read_tenant_row(&state, tenant_id).await?;

    if row.content.is_empty() {
        // Nothing to rebuild for bundled-only tenants.
        state
            .manifest_cache
            .mark_not_configured(tenant_id, row.version)
            .await;
    } else {
        spawn_describer_build(
            state.manifest_cache.clone(),
            state.build_log_bus.clone(),
            config.clone(),
            tenant_id,
            ConnectorsTomlContent(row.content.clone()),
            row.content_hash.clone(),
            row.version,
            true,
        )
        .await;
    }

    Ok(HttpResponse::Accepted()
        .insert_header(("Location", "/v0/connectors/status"))
        .finish())
}

// ── Shared helpers ───────────────────────────────────────────────────────

/// Read the tenant's `tenant_connector_config` row, lazily inserting one
/// from `connectors_toml_path` on first access.
async fn read_tenant_row(
    state: &ServerState,
    tenant_id: TenantId,
) -> Result<TenantConnectorConfig, ManagerError> {
    let seed = read_seed(state)?;
    let row = state
        .db
        .lock()
        .await
        .get_or_bootstrap_connectors_config(tenant_id, &seed)
        .await?;
    Ok(row)
}

/// Read the bootstrap seed from `connectors_toml_path`. Empty when the
/// field is unset, the file is absent, or no compiler config is wired
/// up at all.
fn read_seed(state: &ServerState) -> Result<String, ManagerError> {
    let Some(config) = state.compiler_config.as_ref() else {
        return Ok(String::new());
    };
    let Some(path) = config.connectors_toml_path.as_deref() else {
        return Ok(String::new());
    };
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(ApiError::ConnectorManifestBuildFailed {
            error: format!("reading bootstrap seed file: {e}"),
        }
        .into()),
    }
}

/// Ensure the manifest cache holds a state matching the current row.
///
/// On first access for a tenant (cache miss), or after the row's hash
/// has moved past whatever the cache last knew, this either marks the
/// tenant `NotConfigured` (empty content) or kicks off a background
/// describer build.
async fn materialize_cache(
    state: &ServerState,
    tenant_id: TenantId,
    row: &TenantConnectorConfig,
) {
    let current = state.manifest_cache.get(tenant_id).await;
    let cache_matches_row = current
        .as_ref()
        .and_then(|s| s.content_hash())
        .is_some_and(|h| h == row.content_hash);
    let cache_marks_not_configured = matches!(current, Some(TenantBuildState::NotConfigured { .. }));
    if cache_matches_row || (row.content.is_empty() && cache_marks_not_configured) {
        return;
    }

    if row.content.is_empty() {
        state
            .manifest_cache
            .mark_not_configured(tenant_id, row.version)
            .await;
        return;
    }

    let Some(config) = state.compiler_config.as_ref() else {
        return;
    };
    spawn_describer_build(
        state.manifest_cache.clone(),
        state.build_log_bus.clone(),
        config.clone(),
        tenant_id,
        ConnectorsTomlContent(row.content.clone()),
        row.content_hash.clone(),
        row.version,
        false,
    )
    .await;
}

/// Extract the `If-Match` header and reject any request that omits it
/// or supplies the wildcard `*`. Plain ETag values are returned with
/// surrounding `"…"` and the optional weak prefix `W/` removed.
fn required_if_match(req: &HttpRequest) -> Result<String, ManagerError> {
    let header = req
        .headers()
        .get(actix_web::http::header::IF_MATCH)
        .ok_or(ApiError::ConnectorsIfMatchRequired)?;
    let s = header.to_str().map_err(|_| {
        ApiError::ConnectorsConfigInvalidShape {
            line: 0,
            reason: "If-Match header is not valid ASCII".to_string(),
        }
    })?;
    let trimmed = s.trim();
    if trimmed == "*" {
        return Err(ApiError::ConnectorsIfMatchWildcardRejected.into());
    }
    Ok(trimmed
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_string())
}

fn request_matches(header: Option<&actix_web::http::header::HeaderValue>, etag: &EntityTag) -> bool {
    let Some(h) = header else { return false };
    let Ok(s) = h.to_str() else { return false };
    let stripped = s.trim().trim_start_matches("W/").trim_matches('"');
    stripped == etag.tag()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::main::ServerState;
    use crate::auth::TenantRecord;
    use crate::db::test::setup_pg;
    use actix_web::body::to_bytes;
    use actix_web::dev::{Service, ServiceResponse};
    use actix_web::http::header::HeaderValue;
    use actix_web::http::StatusCode;
    use actix_web::HttpMessage;
    use actix_web::{test, web, App};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Holder returned by [`build_app`] so the embedded Postgres temp
    /// directory stays alive for the duration of the test. Dropping
    /// `tmp` removes the directory, taking the database files with it.
    struct TestApp<S> {
        app: S,
        _state: Arc<ServerState>,
        _tmp: tempfile::TempDir,
    }

    async fn build_app(
        compiler_config: Option<crate::config::CompilerConfig>,
    ) -> TestApp<
        impl Service<actix_http::Request, Response = ServiceResponse, Error = actix_web::Error>,
    > {
        crate::ensure_default_crypto_provider();
        let (db, _tmp) = setup_pg().await;
        let db = Arc::new(Mutex::new(db));
        let state = Arc::new(
            ServerState::test_state_with_compiler(db, compiler_config).await,
        );
        let tenant_id = TenantRecord::default().id;
        let state_for_app = web::Data::from(state.clone());
        let app = test::init_service(
            App::new()
                .app_data(state_for_app)
                // Inject TenantId into request extensions so `ReqData<TenantId>`
                // resolves without bringing in the auth middleware.
                .wrap_fn(move |req, srv| {
                    req.extensions_mut().insert(tenant_id);
                    srv.call(req)
                })
                .service(get_connectors_status)
                .service(get_connectors_toml)
                .service(put_connectors_toml)
                .service(post_connectors_refresh),
        )
        .await;
        TestApp {
            app,
            _state: state,
            _tmp,
        }
    }

    fn etag(value: &HeaderValue) -> String {
        value
            .to_str()
            .unwrap()
            .trim_start_matches("W/")
            .trim_matches('"')
            .to_string()
    }

    /// Status endpoint reports `not_configured` for an empty blob and
    /// echoes the row's content_hash as the ETag.
    #[actix_web::test]
    async fn status_reports_not_configured_for_empty_blob() {
        let ta = build_app(None).await;
        let req = test::TestRequest::get()
            .uri("/connectors/status")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        let status = resp.status();
        let body = to_bytes(resp.into_body()).await.unwrap();
        if status != StatusCode::OK {
            panic!(
                "expected 200, got {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["state"], "not_configured");
        assert_eq!(v["version"], 1);
    }

    /// `GET /connectors/connectors.toml` returns the row's content with
    /// audit headers and a strong ETag.
    #[actix_web::test]
    async fn get_toml_returns_blob_with_headers() {
        let ta = build_app(None).await;
        let req = test::TestRequest::get()
            .uri("/connectors/connectors.toml")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get("etag").is_some());
        assert_eq!(
            resp.headers().get("X-Connectors-Version").unwrap(),
            HeaderValue::from_static("1")
        );
        assert!(resp.headers().get("X-Connectors-Edited-By").is_some());
        let body = to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(body.as_ref(), b"");
    }

    /// PUT requires an `If-Match` header.
    #[actix_web::test]
    async fn put_without_if_match_returns_428() {
        let ta = build_app(None).await;
        let req = test::TestRequest::put()
            .uri("/connectors/connectors.toml")
            .set_payload(Vec::<u8>::new())
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::PRECONDITION_REQUIRED);
    }

    /// PUT with a stale `If-Match` returns 412.
    #[actix_web::test]
    async fn put_with_stale_if_match_returns_412() {
        let ta = build_app(None).await;
        let req = test::TestRequest::put()
            .uri("/connectors/connectors.toml")
            .insert_header(("If-Match", "\"00000000-stale-stale-stale-000000000000\""))
            .set_payload(b"acme = \"1.0\"\n".to_vec())
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
    }

    /// Wildcard `If-Match: *` is rejected.
    #[actix_web::test]
    async fn put_with_wildcard_if_match_rejected() {
        let ta = build_app(None).await;
        let req = test::TestRequest::put()
            .uri("/connectors/connectors.toml")
            .insert_header(("If-Match", "*"))
            .set_payload(b"acme = \"1.0\"\n".to_vec())
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
    }

    /// PUT with a malformed body returns 400.
    #[actix_web::test]
    async fn put_with_malformed_body_returns_400() {
        let ta = build_app(None).await;
        // First read to obtain the current ETag.
        let req = test::TestRequest::get()
            .uri("/connectors/connectors.toml")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        let etag_value = etag(resp.headers().get("etag").unwrap());

        // Body without `=` is malformed.
        let req = test::TestRequest::put()
            .uri("/connectors/connectors.toml")
            .insert_header(("If-Match", format!("\"{etag_value}\"")))
            .set_payload(b"this_line_has_no_equals\n".to_vec())
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Happy path: GET → PUT → GET reflects the new content, the
    /// version increments and the ETag changes.
    #[actix_web::test]
    async fn put_updates_blob_and_increments_version() {
        let ta = build_app(None).await;
        // Read initial state.
        let req = test::TestRequest::get()
            .uri("/connectors/connectors.toml")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        let initial_etag = etag(resp.headers().get("etag").unwrap());

        // PUT with valid content.
        let new_content = b"acme = \"1.0\"\n";
        let req = test::TestRequest::put()
            .uri("/connectors/connectors.toml")
            .insert_header(("If-Match", format!("\"{initial_etag}\"")))
            .set_payload(new_content.to_vec())
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            HeaderValue::from_static("/v0/connectors/status")
        );
        let body = to_bytes(resp.into_body()).await.unwrap();
        let put_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(put_resp["version"], 2);
        let new_etag = put_resp["content_hash"].as_str().unwrap().to_string();
        assert_ne!(new_etag, initial_etag);

        // GET reflects the new content + version.
        let req = test::TestRequest::get()
            .uri("/connectors/connectors.toml")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(
            resp.headers().get("X-Connectors-Version").unwrap(),
            HeaderValue::from_static("2")
        );
        assert_eq!(etag(resp.headers().get("etag").unwrap()), new_etag);
        let body = to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(body.as_ref(), new_content);
    }

    /// Status endpoint returns 304 when `If-None-Match` matches.
    #[actix_web::test]
    async fn status_honors_if_none_match() {
        let ta = build_app(None).await;
        let req = test::TestRequest::get()
            .uri("/connectors/status")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        let etag_value = etag(resp.headers().get("etag").unwrap());

        let req = test::TestRequest::get()
            .uri("/connectors/status")
            .insert_header(("If-None-Match", format!("\"{etag_value}\"")))
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    }

    /// PUT to empty content transitions back to `not_configured`.
    #[actix_web::test]
    async fn put_empty_returns_to_not_configured() {
        let ta = build_app(None).await;
        // First add some content.
        let req = test::TestRequest::get()
            .uri("/connectors/connectors.toml")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        let etag1 = etag(resp.headers().get("etag").unwrap());

        let req = test::TestRequest::put()
            .uri("/connectors/connectors.toml")
            .insert_header(("If-Match", format!("\"{etag1}\"")))
            .set_payload(b"plugin_a = \"1.0\"\n".to_vec())
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        let body = to_bytes(resp.into_body()).await.unwrap();
        let put_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let etag2 = put_resp["content_hash"].as_str().unwrap().to_string();

        // Now PUT empty.
        let req = test::TestRequest::put()
            .uri("/connectors/connectors.toml")
            .insert_header(("If-Match", format!("\"{etag2}\"")))
            .set_payload(Vec::<u8>::new())
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // Status now reports not_configured.
        let req = test::TestRequest::get()
            .uri("/connectors/status")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        let body = to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["state"], "not_configured");
        assert_eq!(v["version"], 3);
    }

    /// Refresh endpoint returns 404 when no compiler config is wired.
    #[actix_web::test]
    async fn refresh_without_compiler_config_returns_404() {
        let ta = build_app(None).await;
        let req = test::TestRequest::post()
            .uri("/connectors/refresh")
            .to_request();
        let resp = test::call_service(&ta.app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
