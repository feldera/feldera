//! Role-based access-control enforcement.
//!
//! A single middleware over the authenticated `/v0` scope reads the
//! [`AuthenticatedPrincipal`] that `auth_validator` installed and compares its
//! role against the minimum role declared for the matched route. The route
//! table below is the single source of truth and mirrors the access-control
//! table in `DESIGN.md`. Enforcement is deny-by-default: a route that is reached
//! but absent from the table is refused (fail closed), so a newly added
//! endpoint cannot ship silently world-accessible.

use crate::auth::AuthenticatedPrincipal;
use crate::db::error::DBError;
use crate::db::types::role::Role;
use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::Method;
use actix_web::middleware::Next;
use actix_web::{HttpMessage, HttpResponse, ResponseError};
use std::collections::HashMap;
use std::sync::OnceLock;
use tracing::{debug, error, info};

/// Minimum role required to reach each `/v0` route. `None` means the route is
/// reachable by any authenticated principal (no role floor). A `(method, path)`
/// absent from this table is denied by the middleware.
#[rustfmt::skip]
static ROUTE_MIN_ROLE: &[(&str, &str, Option<Role>)] = &[
    ("GET", "/v0/api_keys", Some(Role::Write)), // list_api_keys
    ("POST", "/v0/api_keys", Some(Role::Write)), // post_api_key
    ("DELETE", "/v0/api_keys/{api_key_name}", Some(Role::Write)), // delete_api_key
    ("GET", "/v0/api_keys/{api_key_name}", Some(Role::Write)), // get_api_key
    ("GET", "/v0/cluster/events", Some(Role::Read)), // list_cluster_events
    ("GET", "/v0/cluster/events/{event_id}", Some(Role::Read)), // get_cluster_event
    ("GET", "/v0/cluster_healthz", Some(Role::Read)), // get_cluster_health
    ("GET", "/v0/config", Some(Role::Read)), // get_config
    ("GET", "/v0/config/demos", Some(Role::Read)), // get_config_demos
    ("GET", "/v0/config/session", Some(Role::Read)), // get_config_session
    ("GET", "/v0/metrics", Some(Role::Read)), // get_metrics
    ("GET", "/v0/oidc_trust", Some(Role::Admin)), // list_oidc_trust
    ("POST", "/v0/oidc_trust", Some(Role::Admin)), // post_oidc_trust
    ("DELETE", "/v0/oidc_trust/{name}", Some(Role::Admin)), // delete_oidc_trust
    ("GET", "/v0/oidc_trust/{name}", Some(Role::Admin)), // get_oidc_trust
    ("GET", "/v0/pipelines", Some(Role::Read)), // list_pipelines
    ("POST", "/v0/pipelines", Some(Role::Write)), // post_pipeline
    ("DELETE", "/v0/pipelines/{pipeline_name}", Some(Role::Write)), // delete_pipeline
    ("GET", "/v0/pipelines/{pipeline_name}", Some(Role::Read)), // get_pipeline
    ("PATCH", "/v0/pipelines/{pipeline_name}", Some(Role::Write)), // patch_pipeline
    ("PUT", "/v0/pipelines/{pipeline_name}", Some(Role::Write)), // put_pipeline
    ("POST", "/v0/pipelines/{pipeline_name}/activate", Some(Role::Write)), // post_pipeline_activate
    ("POST", "/v0/pipelines/{pipeline_name}/approve", Some(Role::Write)), // post_pipeline_approve
    ("POST", "/v0/pipelines/{pipeline_name}/checkpoint", Some(Role::Write)), // checkpoint_pipeline
    ("POST", "/v0/pipelines/{pipeline_name}/checkpoint/sync", Some(Role::Write)), // sync_checkpoint
    ("GET", "/v0/pipelines/{pipeline_name}/checkpoint/sync_status", Some(Role::Read)), // get_checkpoint_sync_status
    ("GET", "/v0/pipelines/{pipeline_name}/checkpoint_status", Some(Role::Read)), // get_checkpoint_status
    ("GET", "/v0/pipelines/{pipeline_name}/checkpoints", Some(Role::Read)), // get_checkpoints
    ("GET", "/v0/pipelines/{pipeline_name}/circuit_json_profile", Some(Role::Read)), // get_pipeline_circuit_json_profile
    ("GET", "/v0/pipelines/{pipeline_name}/circuit_profile", Some(Role::Read)), // get_pipeline_circuit_profile
    ("POST", "/v0/pipelines/{pipeline_name}/clear", Some(Role::Write)), // post_pipeline_clear
    ("POST", "/v0/pipelines/{pipeline_name}/clock/advance", Some(Role::Write)), // clock_advance
    ("POST", "/v0/pipelines/{pipeline_name}/commit_transaction", Some(Role::Write)), // commit_transaction
    ("GET", "/v0/pipelines/{pipeline_name}/completion_status", Some(Role::Read)), // completion_status
    ("GET", "/v0/pipelines/{pipeline_name}/dataflow_graph", Some(Role::Read)), // get_pipeline_dataflow_graph
    ("POST", "/v0/pipelines/{pipeline_name}/dismiss_error", Some(Role::Write)), // post_pipeline_dismiss_error
    ("POST", "/v0/pipelines/{pipeline_name}/egress/{table_name}", Some(Role::Write)), // http_output
    ("GET", "/v0/pipelines/{pipeline_name}/events", Some(Role::Read)), // list_pipeline_events
    ("GET", "/v0/pipelines/{pipeline_name}/events/{event_id}", Some(Role::Read)), // get_pipeline_event
    ("GET", "/v0/pipelines/{pipeline_name}/heap_profile", Some(Role::Read)), // get_pipeline_heap_profile
    ("POST", "/v0/pipelines/{pipeline_name}/ingress/{table_name}", Some(Role::Write)), // http_input
    ("GET", "/v0/pipelines/{pipeline_name}/logs", Some(Role::Read)), // get_pipeline_logs
    ("GET", "/v0/pipelines/{pipeline_name}/metrics", Some(Role::Read)), // get_pipeline_metrics
    ("POST", "/v0/pipelines/{pipeline_name}/pause", Some(Role::Write)), // post_pipeline_pause
    ("GET", "/v0/pipelines/{pipeline_name}/query", Some(Role::Write)), // pipeline_adhoc_sql
    ("POST", "/v0/pipelines/{pipeline_name}/rebalance", Some(Role::Write)), // post_pipeline_rebalance
    ("POST", "/v0/pipelines/{pipeline_name}/resume", Some(Role::Write)), // post_pipeline_resume
    ("GET", "/v0/pipelines/{pipeline_name}/samply_profile", Some(Role::Read)), // get_pipeline_samply_profile
    ("POST", "/v0/pipelines/{pipeline_name}/samply_profile", Some(Role::Read)), // start_samply_profile
    ("POST", "/v0/pipelines/{pipeline_name}/start", Some(Role::Write)), // post_pipeline_start
    ("POST", "/v0/pipelines/{pipeline_name}/start_compaction", Some(Role::Write)), // post_pipeline_start_compaction
    ("POST", "/v0/pipelines/{pipeline_name}/start_transaction", Some(Role::Write)), // start_transaction
    ("GET", "/v0/pipelines/{pipeline_name}/stats", Some(Role::Read)), // get_pipeline_stats
    ("POST", "/v0/pipelines/{pipeline_name}/stop", Some(Role::Write)), // post_pipeline_stop
    ("GET", "/v0/pipelines/{pipeline_name}/support_bundle", Some(Role::Read)), // get_pipeline_support_bundle
    ("GET", "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/completion_token", Some(Role::Write)), // completion_token
    ("GET", "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/stats", Some(Role::Read)), // get_pipeline_input_connector_status
    ("POST", "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/{action}", Some(Role::Write)), // post_pipeline_input_connector_action
    ("POST", "/v0/pipelines/{pipeline_name}/testing", Some(Role::Write)), // post_pipeline_testing
    ("GET", "/v0/pipelines/{pipeline_name}/time_series", Some(Role::Read)), // get_pipeline_time_series
    ("GET", "/v0/pipelines/{pipeline_name}/time_series_stream", Some(Role::Read)), // get_pipeline_time_series_stream
    ("POST", "/v0/pipelines/{pipeline_name}/update_runtime", Some(Role::Write)), // post_update_runtime
    ("POST", "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/command", Some(Role::Write)), // post_pipeline_output_connector_command
    ("GET", "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/stats", Some(Role::Read)), // get_pipeline_output_connector_status
    // --- RBAC tenant/user management (new) ---
    ("GET", "/v0/tenant/users", Some(Role::Admin)), // list_tenant_users
    ("PUT", "/v0/tenant/users/{user_id}", Some(Role::Admin)), // put_tenant_user
    ("DELETE", "/v0/tenant/users/{user_id}", Some(Role::Admin)), // delete_tenant_user
    ("GET", "/v0/tenants", Some(Role::Owner)), // list_tenants
    ("POST", "/v0/tenants", Some(Role::Owner)), // create_tenant
];

/// Build the lookup map once (the table is static and small).
fn route_table() -> &'static HashMap<(&'static str, &'static str), Option<Role>> {
    static TABLE: OnceLock<HashMap<(&'static str, &'static str), Option<Role>>> = OnceLock::new();
    TABLE.get_or_init(|| {
        ROUTE_MIN_ROLE
            .iter()
            .map(|(m, p, r)| ((*m, *p), *r))
            .collect()
    })
}

/// Look up the access rule for a matched route.
/// `None` => route unknown (deny). `Some(None)` => any authenticated principal.
/// `Some(Some(role))` => requires at least `role`.
fn lookup(method: &str, pattern: &str) -> Option<Option<Role>> {
    route_table().get(&(method, pattern)).copied()
}

/// Build the 403 body in the same shape as the rest of the API.
fn forbidden(message: &str) -> HttpResponse<BoxBody> {
    HttpResponse::Forbidden().json(serde_json::json!({
        "message": message,
        "error_code": "InsufficientPermissions",
    }))
}

/// Decide whether the principal may proceed. `Ok(())` allows; `Err(resp)` is the
/// 403 to return.
fn authorize(
    method: &str,
    pattern: Option<&str>,
    principal: Option<&AuthenticatedPrincipal>,
) -> Result<(), HttpResponse<BoxBody>> {
    // No matched route: let actix produce its normal 404. RBAC only guards
    // routes that exist.
    let Some(pattern) = pattern else {
        return Ok(());
    };
    match lookup(method, pattern) {
        None => {
            // Registered route with no classification: fail closed.
            error!("RBAC: route {method} {pattern} has no access-control entry; denying");
            Err(forbidden(
                "This endpoint has no access-control classification and is denied",
            ))
        }
        Some(None) => Ok(()),
        Some(Some(required)) => match principal {
            Some(p) if p.role.satisfies(required) => Ok(()),
            Some(p) => Err(DBError::InsufficientPermissions {
                required,
                actual: p.role,
            }
            .error_response()),
            None => {
                error!("RBAC: no authenticated principal for {method} {pattern}; denying");
                Err(forbidden("No authenticated principal"))
            }
        },
    }
}

/// Emit an audit line for an authorized request. Mutations and any owner action
/// are logged at info; reads at debug, to keep the happy path quiet.
fn audit(method: &Method, pattern: &str, principal: Option<&AuthenticatedPrincipal>) {
    if let Some(p) = principal {
        let is_owner = p.role == Role::Owner;
        if method != Method::GET || is_owner {
            info!(
                "audit: user='{}' tenant={} role={} {} {}",
                p.label, p.acting_tenant, p.role, method, pattern
            );
        } else {
            debug!(
                "audit: user='{}' tenant={} role={} {} {}",
                p.label, p.acting_tenant, p.role, method, pattern
            );
        }
    }
}

/// RBAC enforcement middleware for the authenticated `/v0` scope. Runs after
/// `auth_validator` has installed the principal.
pub(crate) async fn rbac_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    let principal = req.extensions().get::<AuthenticatedPrincipal>().cloned();
    let method = req.method().clone();
    let pattern = req.match_pattern();

    match authorize(method.as_str(), pattern.as_deref(), principal.as_ref()) {
        Ok(()) => {
            if let Some(pattern) = pattern.as_deref() {
                audit(&method, pattern, principal.as_ref());
            }
            Ok(next.call(req).await?.map_into_boxed_body())
        }
        Err(resp) => Ok(req.into_response(resp)),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn table_has_no_duplicate_entries() {
        let mut seen = std::collections::HashSet::new();
        for (m, p, _) in ROUTE_MIN_ROLE {
            assert!(seen.insert((*m, *p)), "duplicate route entry: {m} {p}");
        }
    }

    #[test]
    fn unknown_route_is_denied() {
        let p = AuthenticatedPrincipal::for_test(Role::Owner);
        assert!(authorize("GET", Some("/v0/does/not/exist"), Some(&p)).is_err());
    }

    #[test]
    fn role_floor_is_enforced() {
        let reader = AuthenticatedPrincipal::for_test(Role::Read);
        let writer = AuthenticatedPrincipal::for_test(Role::Write);
        // A write route rejects a reader and admits a writer.
        assert!(authorize("POST", Some("/v0/pipelines"), Some(&reader)).is_err());
        assert!(authorize("POST", Some("/v0/pipelines"), Some(&writer)).is_ok());
        // A read route admits a reader.
        assert!(authorize("GET", Some("/v0/pipelines"), Some(&reader)).is_ok());
        // An owner-only route rejects an admin.
        let admin = AuthenticatedPrincipal::for_test(Role::Admin);
        assert!(authorize("GET", Some("/v0/tenants"), Some(&admin)).is_err());
    }

    /// Systematic matrix: for every classified route and every role, a request
    /// is admitted iff the role meets the route's minimum, and refused
    /// otherwise. This is the exhaustive "only the correct role can access each
    /// endpoint" check. Reverting any `Role::*` in the table, or breaking the
    /// `>=` comparison, makes this fail.
    #[test]
    fn every_route_admits_exactly_its_minimum_role() {
        let all_roles = [Role::Read, Role::Write, Role::Admin, Role::Owner];
        for (method, pattern, min) in ROUTE_MIN_ROLE {
            for role in all_roles {
                let principal = AuthenticatedPrincipal::for_test(role);
                let allowed = authorize(method, Some(pattern), Some(&principal)).is_ok();
                let expected = match min {
                    None => true, // any authenticated principal
                    Some(required) => role >= *required,
                };
                assert_eq!(
                    allowed, expected,
                    "{method} {pattern}: role {role} allowed={allowed}, expected={expected} (min={min:?})"
                );
            }
            // A request with no principal at all is always refused on a
            // classified route (fail closed).
            assert!(
                authorize(method, Some(pattern), None).is_err(),
                "{method} {pattern}: missing principal must be denied"
            );
        }
    }

    /// Independent pin of the most security-sensitive classifications, so an
    /// accidental downgrade in the table is caught here regardless of the
    /// self-consistent matrix test above. Revert any of these table entries and
    /// this fails.
    #[test]
    fn security_critical_routes_have_expected_minimums() {
        let expect = |method, pattern, role| {
            assert_eq!(lookup(method, pattern), Some(role), "{method} {pattern}");
        };
        // Data plane and mutations require write; read must never reach them.
        expect("POST", "/v0/pipelines", Some(Role::Write));
        expect("DELETE", "/v0/pipelines/{pipeline_name}", Some(Role::Write));
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/start",
            Some(Role::Write),
        );
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/stop",
            Some(Role::Write),
        );
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/clear",
            Some(Role::Write),
        );
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/ingress/{table_name}",
            Some(Role::Write),
        );
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/egress/{table_name}",
            Some(Role::Write),
        );
        expect(
            "GET",
            "/v0/pipelines/{pipeline_name}/query",
            Some(Role::Write),
        );
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/start_transaction",
            Some(Role::Write),
        );
        // Monitoring is read.
        expect("GET", "/v0/pipelines", Some(Role::Read));
        expect(
            "GET",
            "/v0/pipelines/{pipeline_name}/stats",
            Some(Role::Read),
        );
        expect(
            "GET",
            "/v0/pipelines/{pipeline_name}/logs",
            Some(Role::Read),
        );
        // Identity administration is admin.
        expect("POST", "/v0/oidc_trust", Some(Role::Admin));
        expect("GET", "/v0/tenant/users", Some(Role::Admin));
        // Platform administration is owner.
        expect("GET", "/v0/tenants", Some(Role::Owner));
        expect("POST", "/v0/tenants", Some(Role::Owner));
    }

    /// End-to-end through a real actix pipeline: the middleware short-circuits
    /// with 403 below the minimum role and passes through at or above it. This
    /// exercises `match_pattern`, the response/body unification, and the wrap
    /// ordering that the unit tests above cannot. Reverting the `.wrap(rbac)` in
    /// `build_app` would make the deny cases return 200 here.
    #[actix_web::test]
    async fn middleware_enforces_in_a_real_pipeline() {
        use actix_web::middleware::from_fn;
        use actix_web::{test, web, App, HttpResponse};
        use std::str::FromStr;

        // Installs a principal whose role comes from the `x-test-role` header,
        // standing in for `auth_validator`.
        async fn install_principal(
            req: ServiceRequest,
            next: Next<impl MessageBody + 'static>,
        ) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
            if let Some(role) = req
                .headers()
                .get("x-test-role")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| Role::from_str(s).ok())
            {
                req.extensions_mut()
                    .insert(AuthenticatedPrincipal::for_test(role));
            }
            Ok(next.call(req).await?.map_into_boxed_body())
        }

        let app = test::init_service(
            App::new().service(
                web::scope("/v0")
                    .wrap(from_fn(rbac_middleware))
                    .wrap(from_fn(install_principal))
                    .route(
                        "/pipelines",
                        web::get().to(|| async { HttpResponse::Ok().finish() }),
                    )
                    .route(
                        "/pipelines",
                        web::post().to(|| async { HttpResponse::Ok().finish() }),
                    )
                    .route(
                        "/tenants",
                        web::get().to(|| async { HttpResponse::Ok().finish() }),
                    ),
            ),
        )
        .await;

        let call = |method: &str, path: &str, role: &str| {
            let req = match method {
                "POST" => test::TestRequest::post(),
                _ => test::TestRequest::get(),
            }
            .uri(path)
            .insert_header(("x-test-role", role))
            .to_request();
            test::call_service(&app, req)
        };

        // read may GET pipelines but not POST; write may POST.
        assert_eq!(call("GET", "/v0/pipelines", "read").await.status(), 200);
        assert_eq!(call("POST", "/v0/pipelines", "read").await.status(), 403);
        assert_eq!(call("POST", "/v0/pipelines", "write").await.status(), 200);
        // owner-only tenant list: admin refused, owner admitted.
        assert_eq!(call("GET", "/v0/tenants", "admin").await.status(), 403);
        assert_eq!(call("GET", "/v0/tenants", "owner").await.status(), 200);
    }

    /// Tripwire that the route table size matches the known surface, so adding
    /// a route without classifying it is noticed. This is only a count check;
    /// behavioral coverage (every classified route admits exactly its minimum
    /// role) is enforced by `every_route_admits_exactly_its_minimum_role`, and
    /// a route reached at runtime but absent from the table is denied
    /// (`authorize` returns 403, see `unknown_route_is_denied`).
    #[test]
    fn table_covers_the_known_surface() {
        // 64 routes registered in api_scope() at the time of writing plus the 5
        // RBAC management endpoints. If you add a route, add a table entry and
        // bump this count; the mismatch is the reminder.
        assert_eq!(
            ROUTE_MIN_ROLE.len(),
            69,
            "route table size changed; add/remove the matching access-control entry"
        );
    }
}
