//! Role-based access-control enforcement.
//!
//! A single middleware over the authenticated `/v0` scope reads the
//! `AuthenticatedPrincipal` that `auth_validator` installed and compares its
//! role against the minimum role declared for the matched route. The
//! `ROUTE_MIN_ROLE` table below is the single source of truth for the
//! access-control model. Enforcement is deny-by-default with no pass-through:
//! a handler runs only after a table entry admits the request. A registered
//! route absent from the table is refused, so a newly added endpoint cannot
//! ship silently world-accessible, and a path no route claims is answered 404
//! here rather than handed on.

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
use tracing::{debug, error};

/// Minimum role required to reach each `/v0` route. A `(method, path)` absent
/// from this table is denied by the middleware.
#[rustfmt::skip]
static ROUTE_MIN_ROLE: &[(&str, &str, Role)] = &[
    ("GET", "/v0/api_keys", Role::Write), // list_api_keys
    ("POST", "/v0/api_keys", Role::Write), // post_api_key
    ("DELETE", "/v0/api_keys/{api_key_name}", Role::Write), // delete_api_key
    ("GET", "/v0/api_keys/{api_key_name}", Role::Write), // get_api_key
    ("GET", "/v0/cluster/events", Role::Read), // list_cluster_events
    ("GET", "/v0/cluster/events/{event_id}", Role::Read), // get_cluster_event
    ("GET", "/v0/cluster_healthz", Role::Read), // get_cluster_health
    ("GET", "/v0/config", Role::Read), // get_config
    ("GET", "/v0/config/demos", Role::Read), // get_config_demos
    ("GET", "/v0/config/session", Role::Read), // get_config_session
    ("GET", "/v0/config/owners", Role::Owner), // get_config_owners
    ("GET", "/v0/metrics", Role::Read), // get_metrics
    ("GET", "/v0/oidc_trust", Role::Admin), // list_oidc_trust
    ("POST", "/v0/oidc_trust", Role::Admin), // post_oidc_trust
    ("DELETE", "/v0/oidc_trust/{name}", Role::Admin), // delete_oidc_trust
    ("GET", "/v0/oidc_trust/{name}", Role::Admin), // get_oidc_trust
    ("GET", "/v0/pipelines", Role::Read), // list_pipelines
    ("POST", "/v0/pipelines", Role::Write), // post_pipeline
    ("DELETE", "/v0/pipelines/{pipeline_name}", Role::Write), // delete_pipeline
    ("GET", "/v0/pipelines/{pipeline_name}", Role::Read), // get_pipeline
    ("PATCH", "/v0/pipelines/{pipeline_name}", Role::Write), // patch_pipeline
    ("PUT", "/v0/pipelines/{pipeline_name}", Role::Write), // put_pipeline
    ("POST", "/v0/pipelines/{pipeline_name}/activate", Role::Write), // post_pipeline_activate
    ("POST", "/v0/pipelines/{pipeline_name}/approve", Role::Write), // post_pipeline_approve
    ("POST", "/v0/pipelines/{pipeline_name}/checkpoint", Role::Write), // checkpoint_pipeline
    ("POST", "/v0/pipelines/{pipeline_name}/checkpoint/sync", Role::Write), // sync_checkpoint
    ("GET", "/v0/pipelines/{pipeline_name}/checkpoint/sync_status", Role::Read), // get_checkpoint_sync_status
    ("GET", "/v0/pipelines/{pipeline_name}/checkpoint_status", Role::Read), // get_checkpoint_status
    ("GET", "/v0/pipelines/{pipeline_name}/checkpoints", Role::Read), // get_checkpoints
    ("GET", "/v0/pipelines/{pipeline_name}/checkpoints/remote", Role::Read), // get_remote_checkpoints
    ("GET", "/v0/pipelines/{pipeline_name}/circuit_json_profile", Role::Read), // get_pipeline_circuit_json_profile
    ("GET", "/v0/pipelines/{pipeline_name}/circuit_profile", Role::Read), // get_pipeline_circuit_profile
    ("POST", "/v0/pipelines/{pipeline_name}/clear", Role::Write), // post_pipeline_clear
    ("POST", "/v0/pipelines/{pipeline_name}/clock/advance", Role::Write), // clock_advance
    ("POST", "/v0/pipelines/{pipeline_name}/commit_transaction", Role::Write), // commit_transaction
    ("GET", "/v0/pipelines/{pipeline_name}/completion_status", Role::Read), // completion_status
    ("GET", "/v0/pipelines/{pipeline_name}/dataflow_graph", Role::Read), // get_pipeline_dataflow_graph
    ("POST", "/v0/pipelines/{pipeline_name}/diff", Role::Write), // post_pipeline_diff (submits a candidate program to the shared compiler)
    ("POST", "/v0/pipelines/{pipeline_name}/dismiss_error", Role::Write), // post_pipeline_dismiss_error
    ("POST", "/v0/pipelines/{pipeline_name}/egress/{table_name}", Role::Write), // http_output
    ("GET", "/v0/pipelines/{pipeline_name}/events", Role::Read), // list_pipeline_events
    ("GET", "/v0/pipelines/{pipeline_name}/events/{event_id}", Role::Read), // get_pipeline_event
    ("GET", "/v0/pipelines/{pipeline_name}/heap_profile", Role::Read), // get_pipeline_heap_profile
    ("POST", "/v0/pipelines/{pipeline_name}/ingress/{table_name}", Role::Write), // http_input
    ("GET", "/v0/pipelines/{pipeline_name}/logs", Role::Read), // get_pipeline_logs
    ("GET", "/v0/pipelines/{pipeline_name}/metrics", Role::Read), // get_pipeline_metrics
    ("POST", "/v0/pipelines/{pipeline_name}/pause", Role::Write), // post_pipeline_pause
    ("GET", "/v0/pipelines/{pipeline_name}/query", Role::Write), // pipeline_adhoc_sql
    ("POST", "/v0/pipelines/{pipeline_name}/rebalance", Role::Write), // post_pipeline_rebalance
    ("POST", "/v0/pipelines/{pipeline_name}/resume", Role::Write), // post_pipeline_resume
    ("GET", "/v0/pipelines/{pipeline_name}/samply_profile", Role::Read), // get_pipeline_samply_profile
    ("POST", "/v0/pipelines/{pipeline_name}/samply_profile", Role::Read), // start_samply_profile
    ("POST", "/v0/pipelines/{pipeline_name}/start", Role::Write), // post_pipeline_start
    ("POST", "/v0/pipelines/{pipeline_name}/start_compaction", Role::Write), // post_pipeline_start_compaction
    ("POST", "/v0/pipelines/{pipeline_name}/start_transaction", Role::Write), // start_transaction
    ("GET", "/v0/pipelines/{pipeline_name}/stats", Role::Read), // get_pipeline_stats
    ("POST", "/v0/pipelines/{pipeline_name}/stop", Role::Write), // post_pipeline_stop
    ("GET", "/v0/pipelines/{pipeline_name}/support_bundle", Role::Read), // get_pipeline_support_bundle
    ("GET", "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/completion_token", Role::Write), // completion_token
    ("GET", "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/stats", Role::Read), // get_pipeline_input_connector_status
    ("POST", "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/{action}", Role::Write), // post_pipeline_input_connector_action
    ("POST", "/v0/pipelines/{pipeline_name}/testing", Role::Write), // post_pipeline_testing
    ("GET", "/v0/pipelines/{pipeline_name}/time_series", Role::Read), // get_pipeline_time_series
    ("GET", "/v0/pipelines/{pipeline_name}/time_series_stream", Role::Read), // get_pipeline_time_series_stream
    ("POST", "/v0/pipelines/{pipeline_name}/update_runtime", Role::Write), // post_update_runtime
    ("POST", "/v0/validate_program", Role::Write), // post_validate_program (submits a program to the shared compiler)
    ("POST", "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/command", Role::Write), // post_pipeline_output_connector_command
    ("GET", "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/stats", Role::Read), // get_pipeline_output_connector_status
    ("POST", "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/{action}", Role::Write), // post_pipeline_output_connector_action
    // RBAC tenant/user management
    ("GET", "/v0/tenant/users", Role::Admin), // list_tenant_users
    ("POST", "/v0/tenant/users", Role::Admin), // add_tenant_user (pre-provision)
    ("PUT", "/v0/tenant/users/{user_id}", Role::Admin), // put_tenant_user
    ("DELETE", "/v0/tenant/users/{user_id}", Role::Admin), // delete_tenant_user
    ("GET", "/v0/tenants", Role::Owner), // list_tenants
    ("POST", "/v0/tenants", Role::Owner), // create_tenant
    ("GET", "/v0/tenants/{tenant_id}", Role::Owner), // get_tenant
    ("PATCH", "/v0/tenants/{tenant_id}", Role::Owner), // patch_tenant
    ("DELETE", "/v0/tenants/{tenant_id}", Role::Owner), // delete_tenant
];

/// [`ROUTE_MIN_ROLE`] indexed by `(method, pattern)`, built once on first use,
/// for callers that already hold a route template rather than a request path.
fn route_table() -> &'static HashMap<(&'static str, &'static str), Role> {
    static TABLE: OnceLock<HashMap<(&'static str, &'static str), Role>> = OnceLock::new();
    TABLE.get_or_init(|| {
        ROUTE_MIN_ROLE
            .iter()
            .map(|(m, p, r)| ((*m, *p), *r))
            .collect()
    })
}

/// The minimum role for a route template, or `None` when the table does not
/// classify it, which the middleware treats as a denial.
///
/// ```text
/// min_role_for("POST", "/v0/tenants")     => Some(Role::Owner)
/// min_role_for("GET",  "/v0/not-a-route") => None
/// ```
///
/// The OpenAPI annotation reads the same table as the middleware, so the API
/// reference cannot drift from the enforced policy.
pub(crate) fn min_role_for(method: &str, pattern: &str) -> Option<Role> {
    route_table().get(&(method, pattern)).copied()
}

/// Resolve a request path to its table entry, returning the pattern it matched
/// and that pattern's rule.
///
/// Actix's `match_pattern()` cannot serve here: it resolves by path alone,
/// ignoring the method, so it reports whichever registered pattern claims the
/// path first. Matching the table directly, method first, keeps the enforced
/// rule and the authored rule the same thing.
///
/// A literal segment beats a placeholder, as in any router:
///
/// ```text
/// GET  /v0/pipelines/p/tables/t/connectors/c/completion_token
///   => .../connectors/{connector_name}/completion_token   (literal wins)
/// POST /v0/pipelines/p/tables/t/connectors/c/start
///   => .../connectors/{connector_name}/{action}           (no literal claims it)
/// ```
fn classify(method: &str, path: &str) -> Option<(&'static str, Role)> {
    let segments: Vec<&str> = path.split('/').collect();
    let candidates = candidates_by_shape().get(&(method, segments.len()))?;
    let mut best: Option<(usize, &'static str, Role)> = None;
    for (pattern, role) in candidates {
        let Some(literals) = literal_segments_if_match(pattern, &segments) else {
            continue;
        };
        if best.is_none_or(|(best_literals, _, _)| literals > best_literals) {
            best = Some((literals, pattern, *role));
        }
    }
    best.map(|(_, pattern, role)| (pattern, role))
}

/// [`ROUTE_MIN_ROLE`] bucketed by `(method, segment count)`, built once on first
/// use. A path can only match a pattern of the same shape, so a request compares
/// itself against a handful of candidates rather than the whole table.
#[allow(clippy::type_complexity)]
fn candidates_by_shape() -> &'static HashMap<(&'static str, usize), Vec<(&'static str, Role)>> {
    static SHAPES: OnceLock<HashMap<(&'static str, usize), Vec<(&'static str, Role)>>> =
        OnceLock::new();
    SHAPES.get_or_init(|| {
        let mut shapes: HashMap<(&'static str, usize), Vec<(&'static str, Role)>> = HashMap::new();
        for (method, pattern, role) in ROUTE_MIN_ROLE {
            shapes
                .entry((method, pattern.split('/').count()))
                .or_default()
                .push((pattern, *role));
        }
        shapes
    })
}

/// Whether any route pattern matches `path`, disregarding the method. Tells a
/// wrong-method request for a real path apart from a path the table describes
/// under no method at all.
fn path_matches_any_route(path: &str) -> bool {
    let segments: Vec<&str> = path.split('/').collect();
    ROUTE_MIN_ROLE
        .iter()
        .any(|(_, pattern, _)| literal_segments_if_match(pattern, &segments).is_some())
}

/// How many literal segments `pattern` matches `segments` with, or `None` when
/// it does not match. A `{name}` segment matches any one non-empty segment.
fn literal_segments_if_match(pattern: &str, segments: &[&str]) -> Option<usize> {
    let parts: Vec<&str> = pattern.split('/').collect();
    if parts.len() != segments.len() {
        return None;
    }
    let mut literals = 0;
    for (part, segment) in parts.iter().zip(segments) {
        if part.starts_with('{') && part.ends_with('}') {
            if segment.is_empty() {
                return None;
            }
        } else if part != segment {
            return None;
        } else {
            literals += 1;
        }
    }
    Some(literals)
}

/// A 403 in the same JSON shape the rest of the API returns, so clients can
/// parse a permission denial the same way whether it came from here or a handler.
fn forbidden(message: &str) -> HttpResponse<BoxBody> {
    HttpResponse::Forbidden().json(serde_json::json!({
        "message": message,
        "error_code": "InsufficientPermissions",
    }))
}

/// The 404 for a `/v0` path no route claims, answered by the guard itself so
/// that an unknown path never reaches a handler. Carries the `details` field
/// that `feldera_types::error::ErrorResponse` requires, so a strict client can
/// deserialize it like any other API error.
fn not_found(method: &str, path: &str) -> HttpResponse<BoxBody> {
    HttpResponse::NotFound().json(serde_json::json!({
        "message": format!(
            "No endpoint for {method} {path}; see the API reference for the available /v0 endpoints"
        ),
        "error_code": "UnknownEndpoint",
        "details": {},
    }))
}

/// Decide whether the principal may proceed, returning the pattern the request
/// resolved to for the audit line. `Ok` allows; `Err(resp)` is the response
/// to return instead of calling the handler.
///
/// The handler is reachable only through `Ok`, which requires a table entry
/// for the request and a role that meets it. There is no pass-through: a path
/// the table cannot classify for this method is answered here. It is a 404
/// when no route serves that method on the path, whether the path is unknown
/// or exists only for other methods, and a 403 only when the path is one actix
/// would dispatch yet the table classifies under no method, a registered route
/// missing from the table that the guard denies rather than serve unchecked.
///
/// ```text
/// // a writer posting a pipeline
/// authorize("POST", true,  "/v0/pipelines", Some(&writer))  => Ok(..)
/// // a reader posting a pipeline
/// authorize("POST", true,  "/v0/pipelines", Some(&reader))  => Err(403)
/// // an admin listing tenants, which is owner-only
/// authorize("GET",  true,  "/v0/tenants",   Some(&admin))   => Err(403)
/// // a real path, wrong method: answered 404, no error logged
/// authorize("POST", true,  "/v0/cluster_healthz", Some(&r)) => Err(404)
/// // no route and no table entry: answered as 404, never passed through
/// authorize("GET",  false, "/v0/nonesuch",  Some(&reader))  => Err(404)
/// ```
fn authorize(
    method: &str,
    routed: bool,
    path: &str,
    principal: Option<&AuthenticatedPrincipal>,
) -> Result<&'static str, HttpResponse<BoxBody>> {
    let Some((pattern, required)) = classify(method, path) else {
        // No table entry for this method on this path. A path the table
        // describes under other methods is a wrong-method request (`routed` is
        // method-blind, so it is true here even for a method the path does not
        // serve). Answer it 404 like an unknown path: neither reaches a
        // handler, and a legitimate HEAD or an unsupported verb must not log an
        // error. Only a path actix would dispatch yet the table classifies
        // under no method is the registered-route bug.
        if !routed || path_matches_any_route(path) {
            return Err(not_found(method, path));
        }
        error!("RBAC: route {method} {path} has no access-control entry; denying");
        return Err(forbidden(
            "This endpoint has no access-control classification and is denied",
        ));
    };
    match principal {
        Some(p) if p.role.satisfies(required) => Ok(pattern),
        Some(p) => Err(DBError::InsufficientPermissions {
            required,
            actual: p.role,
        }
        .error_response()),
        None => {
            error!("RBAC: no authenticated principal for {method} {pattern}; denying");
            Err(forbidden("No authenticated principal"))
        }
    }
}

/// Record who reached which route, in which tenant, at what role.
fn audit(method: &Method, pattern: &str, principal: Option<&AuthenticatedPrincipal>) {
    let Some(p) = principal else { return };
    debug!(
        "audit: user='{}' tenant={} role={} {} {}",
        p.label, p.acting_tenant, p.role, method, pattern
    );
}

/// Refuse any `/v0` request whose principal is below the role its route
/// requires, and record the ones that pass. Runs after `auth_validator` has
/// installed the principal, so the role is already resolved here.
pub(crate) async fn rbac_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    let principal = req.extensions().get::<AuthenticatedPrincipal>().cloned();
    let method = req.method().clone();
    // Classify the path actix routes on. actix dispatches on the
    // percent-decoded path, so the guard reads that same view rather than the
    // raw URI, which the router could spell differently. `match_info().as_str()`
    // is actix's decoded path.
    let path = req.match_info().as_str().to_string();
    // Only picks 404 versus 403 for an unclassified path; it never lets a
    // request through, so a wrong answer here cannot reach a handler.
    let routed = req.resource_map().match_pattern(&path).is_some();

    match authorize(method.as_str(), routed, &path, principal.as_ref()) {
        Ok(pattern) => {
            audit(&method, pattern, principal.as_ref());
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
        assert!(authorize("GET", true, "/v0/does/not/exist", Some(&p)).is_err());
    }

    /// A path with no route and no table entry is answered by the guard as a
    /// 404 rather than handed on, so nothing reaches a handler unclassified.
    #[test]
    fn unrouted_path_is_answered_not_passed_through() {
        let p = AuthenticatedPrincipal::for_test(Role::Owner);
        let resp = authorize("GET", false, "/v0/nonesuch", Some(&p)).unwrap_err();
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
    }

    /// A real path reached with a method it does not serve is a 404, not a 403:
    /// `routed` is method-blind, but a wrong-method request is neither a
    /// permission denial nor the registered-route bug that logs an error.
    #[test]
    fn wrong_method_on_a_real_path_is_not_found() {
        let p = AuthenticatedPrincipal::for_test(Role::Owner);
        // `/v0/cluster_healthz` exists for GET only.
        let resp = authorize("POST", true, "/v0/cluster_healthz", Some(&p)).unwrap_err();
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn role_floor_is_enforced() {
        let reader = AuthenticatedPrincipal::for_test(Role::Read);
        let writer = AuthenticatedPrincipal::for_test(Role::Write);
        // A write route rejects a reader and admits a writer.
        assert!(authorize("POST", true, "/v0/pipelines", Some(&reader)).is_err());
        assert!(authorize("POST", true, "/v0/pipelines", Some(&writer)).is_ok());
        // A read route admits a reader.
        assert!(authorize("GET", true, "/v0/pipelines", Some(&reader)).is_ok());
        // An owner-only route rejects an admin.
        let admin = AuthenticatedPrincipal::for_test(Role::Admin);
        assert!(authorize("GET", true, "/v0/tenants", Some(&admin)).is_err());
    }

    /// A concrete request path for a route template, with each `{name}`
    /// placeholder filled by a value that cannot be mistaken for a literal
    /// segment of some other pattern.
    fn concrete_path(pattern: &str) -> String {
        pattern
            .split('/')
            .map(|part| {
                if part.starts_with('{') && part.ends_with('}') {
                    format!("sample-{}", part.trim_matches(|c| c == '{' || c == '}'))
                } else {
                    part.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Every entry must be reachable from a real request path, under its own
    /// method: an entry that another pattern shadows is enforced with the wrong
    /// rule, or denied as unclassified. For example, `GET
    /// .../connectors/{connector_name}/completion_token` shares a path shape
    /// with `POST .../connectors/{connector_name}/{action}`.
    #[test]
    fn every_table_entry_resolves_from_a_concrete_path() {
        for (method, pattern, role) in ROUTE_MIN_ROLE {
            let path = concrete_path(pattern);
            let resolved = classify(method, &path);
            assert_eq!(
                resolved,
                Some((*pattern, *role)),
                "{method} {path} resolved to {resolved:?}, expected {pattern}"
            );
        }
    }

    /// A literal segment wins over a placeholder, and a placeholder still
    /// catches everything else, so both siblings keep their own rule.
    #[test]
    fn a_literal_route_wins_over_a_placeholder_sibling() {
        let base = "/v0/pipelines/p/tables/t/connectors/c";
        assert_eq!(
            classify("GET", &format!("{base}/completion_token")),
            Some((
                "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/completion_token",
                Role::Write
            ))
        );
        assert_eq!(
            classify("GET", &format!("{base}/stats")),
            Some((
                "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/stats",
                Role::Read
            ))
        );
        assert_eq!(
            classify("POST", &format!("{base}/start")),
            Some((
                "/v0/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/{action}",
                Role::Write
            ))
        );
        // The method is part of the match: no GET entry has that shape.
        assert_eq!(classify("GET", &format!("{base}/start")), None);

        // The output connector routes overlap the same way, with `command` as
        // the literal sibling of the action placeholder.
        let view_base = "/v0/pipelines/p/views/v/connectors/c";
        assert_eq!(
            classify("POST", &format!("{view_base}/command")),
            Some((
                "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/command",
                Role::Write
            ))
        );
        assert_eq!(
            classify("POST", &format!("{view_base}/pause")),
            Some((
                "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/{action}",
                Role::Write
            ))
        );
    }

    /// Systematic matrix: for every classified route and every role, a request
    /// is admitted iff the role meets the route's minimum, and refused
    /// otherwise. This is the exhaustive "only the correct role can access each
    /// endpoint" check. Reverting any `Role::*` in the table, or breaking the
    /// `>=` comparison, makes this fail.
    #[test]
    fn every_route_admits_exactly_its_minimum_role() {
        let all_roles = [Role::Read, Role::Write, Role::Admin, Role::Owner];
        for (method, pattern, required) in ROUTE_MIN_ROLE {
            for role in all_roles {
                let principal = AuthenticatedPrincipal::for_test(role);
                let allowed =
                    authorize(method, true, &concrete_path(pattern), Some(&principal)).is_ok();
                let expected = role >= *required;
                assert_eq!(
                    allowed, expected,
                    "{method} {pattern}: role {role} allowed={allowed}, expected={expected} (min={required})"
                );
            }
            // A request with no principal at all is always refused on a
            // classified route.
            assert!(
                authorize(method, true, &concrete_path(pattern), None).is_err(),
                "{method} {pattern}: missing principal must be denied"
            );
        }
    }

    /// A route that can change something never admits `read`. The exceptions
    /// are the POSTs that compile or profile: they take a body but leave no
    /// data or state behind.
    /// Profiling is the one POST a reader may reach: it acts on a pipeline the
    /// reader can already observe, and a bounded duration, not the role, is
    /// what limits its cost. The compile routes are not exempt, because they
    /// hand a caller-supplied program to the shared compiler.
    #[test]
    fn a_mutating_route_never_admits_read() {
        let posts_a_reader_may_make = ["/v0/pipelines/{pipeline_name}/samply_profile"];
        for (method, pattern, required) in ROUTE_MIN_ROLE {
            if *method == "GET" || posts_a_reader_may_make.contains(pattern) {
                continue;
            }
            assert!(
                *required >= Role::Write,
                "{method} {pattern} admits {required}; a mutating route needs at least write"
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
            assert_eq!(
                min_role_for(method, pattern),
                Some(role),
                "{method} {pattern}"
            );
        };
        // Data plane and mutations require write; read must never reach them.
        expect("POST", "/v0/pipelines", Role::Write);
        expect("DELETE", "/v0/pipelines/{pipeline_name}", Role::Write);
        expect("POST", "/v0/pipelines/{pipeline_name}/start", Role::Write);
        expect("POST", "/v0/pipelines/{pipeline_name}/stop", Role::Write);
        expect("POST", "/v0/pipelines/{pipeline_name}/clear", Role::Write);
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/ingress/{table_name}",
            Role::Write,
        );
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/egress/{table_name}",
            Role::Write,
        );
        expect("GET", "/v0/pipelines/{pipeline_name}/query", Role::Write);
        // Compiling a caller-supplied program is write authority, even though
        // neither route persists anything.
        expect("POST", "/v0/validate_program", Role::Write);
        expect("POST", "/v0/pipelines/{pipeline_name}/diff", Role::Write);
        expect(
            "POST",
            "/v0/pipelines/{pipeline_name}/start_transaction",
            Role::Write,
        );
        // Monitoring is read.
        expect("GET", "/v0/pipelines", Role::Read);
        expect("GET", "/v0/pipelines/{pipeline_name}/stats", Role::Read);
        expect("GET", "/v0/pipelines/{pipeline_name}/logs", Role::Read);
        // Identity administration is admin.
        expect("POST", "/v0/oidc_trust", Role::Admin);
        expect("GET", "/v0/tenant/users", Role::Admin);
        // Platform administration is owner.
        expect("GET", "/v0/tenants", Role::Owner);
        expect("POST", "/v0/tenants", Role::Owner);
        expect("GET", "/v0/tenants/{tenant_id}", Role::Owner);
    }

    /// End-to-end through a real actix pipeline: the middleware short-circuits
    /// with 403 below the minimum role and passes through at or above it. This
    /// exercises `match_pattern`, the response/body unification, and the wrap
    /// ordering that the unit tests above cannot. Reverting the `.wrap(rbac)` in
    /// `build_app` would make the deny cases return 200 here.
    #[actix_web::test]
    async fn middleware_enforces_in_a_real_pipeline() {
        use actix_web::middleware::from_fn;
        use actix_web::{App, HttpResponse, test, web};
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
                    )
                    // Registered in the same order as the real app: the
                    // placeholder route first, the literal one behind it.
                    .route(
                        "/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/{action}",
                        web::post().to(|| async { HttpResponse::Ok().finish() }),
                    )
                    .route(
                        "/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/completion_token",
                        web::get().to(|| async { HttpResponse::Ok().finish() }),
                    )
                    // Stands in for "a handler ran": a request the guard
                    // passes through without classifying lands here as 200.
                    .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
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

        // Shadowed route: `GET .../completion_token` shares its path shape with
        // `POST .../{action}`, registered first, and keeps its own rule anyway.
        let token = "/v0/pipelines/p/tables/t/connectors/c/completion_token";
        assert_eq!(call("GET", token, "write").await.status(), 200);
        assert_eq!(call("GET", token, "read").await.status(), 403);
        assert_eq!(
            call(
                "POST",
                "/v0/pipelines/p/tables/t/connectors/c/start",
                "write"
            )
            .await
            .status(),
            200
        );

        // An alternate spelling of a route that actix still dispatches to the
        // same handler carries the same role floor: the guard classifies the
        // decoded path the router uses, so it holds for either spelling.
        assert_eq!(call("POST", "/v0/%70ipelines", "read").await.status(), 403);
        assert_eq!(call("POST", "/v0/%70ipelines", "write").await.status(), 200);
        assert_eq!(call("GET", "/v0/%74enants", "admin").await.status(), 403);
        assert_eq!(call("GET", "/v0/%74enants", "owner").await.status(), 200);

        // No pass-through: a path nothing routes is answered 404 by the guard
        // and never reaches the scope's fallback handler, even for an owner.
        assert_eq!(call("GET", "/v0/nonesuch", "owner").await.status(), 404);
    }

    /// The authenticated `/v0` surface, enumerated from the generated OpenAPI
    /// document (the single source of truth for what `api_scope()` registers).
    /// Paths outside `/v0` are unauthenticated (public scope) and not gated.
    fn openapi_v0_routes() -> Vec<(String, String)> {
        use crate::api::main::ApiDoc;
        use utoipa::OpenApi;
        use utoipa::openapi::PathItemType;

        let method = |t: &PathItemType| match t {
            PathItemType::Get => "GET",
            PathItemType::Post => "POST",
            PathItemType::Put => "PUT",
            PathItemType::Delete => "DELETE",
            PathItemType::Patch => "PATCH",
            PathItemType::Head => "HEAD",
            PathItemType::Options => "OPTIONS",
            PathItemType::Trace => "TRACE",
            PathItemType::Connect => "CONNECT",
        };
        ApiDoc::openapi()
            .paths
            .paths
            .into_iter()
            .filter(|(path, _)| path.starts_with("/v0"))
            .flat_map(|(path, item)| {
                item.operations
                    .keys()
                    .map(|t| (method(t).to_string(), path.clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Routes registered in `api_scope()` but absent from the OpenAPI document
    /// (`ApiDoc::paths()`), so the OpenAPI-driven meta-tests must account for
    /// them explicitly. Both are still classified in `ROUTE_MIN_ROLE`, so RBAC
    /// covers them; they are only invisible to the OpenAPI enumeration:
    /// - `.../testing`: a test-only hook intentionally hidden from the spec.
    /// - `.../command`: declares a non-standard `text/json` content type the
    ///   client generator rejects, so it is left out of the spec for now.
    const REGISTERED_BUT_UNDOCUMENTED: &[(&str, &str)] = &[
        ("POST", "/v0/pipelines/{pipeline_name}/testing"),
        (
            "POST",
            "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/command",
        ),
    ];

    /// Deny-by-default has teeth only if every registered route is classified.
    /// Enumerate the `/v0` surface from the OpenAPI document (which mirrors
    /// `api_scope()`, save the explicit `REGISTERED_BUT_UNDOCUMENTED` set) and
    /// fail the build if any route lacks a `ROUTE_MIN_ROLE` entry. A new endpoint
    /// added without a classification breaks this test rather than silently
    /// shipping 403.
    #[test]
    fn every_registered_v0_route_is_classified() {
        let table: std::collections::HashSet<(&str, &str)> =
            ROUTE_MIN_ROLE.iter().map(|(m, p, _)| (*m, *p)).collect();
        let mut unclassified = vec![];
        for (method, path) in openapi_v0_routes() {
            if !table.contains(&(method.as_str(), path.as_str())) {
                unclassified.push(format!("{method} {path}"));
            }
        }
        unclassified.sort();
        assert!(
            unclassified.is_empty(),
            "these registered /v0 routes have no ROUTE_MIN_ROLE entry (add one, or they 403 for everyone):\n  {}",
            unclassified.join("\n  ")
        );
    }

    /// The reverse guard: every table entry must name a real registered route,
    /// so a stale entry (renamed/removed endpoint) is caught instead of lingering.
    #[test]
    fn no_stale_route_table_entries() {
        let mut registered: std::collections::HashSet<(String, String)> =
            openapi_v0_routes().into_iter().collect();
        registered.extend(
            REGISTERED_BUT_UNDOCUMENTED
                .iter()
                .map(|(m, p)| (m.to_string(), p.to_string())),
        );
        let mut stale = vec![];
        for (method, path, _) in ROUTE_MIN_ROLE {
            if !registered.contains(&(method.to_string(), path.to_string())) {
                stale.push(format!("{method} {path}"));
            }
        }
        stale.sort();
        assert!(
            stale.is_empty(),
            "these ROUTE_MIN_ROLE entries do not match any registered /v0 route (remove or fix them):\n  {}",
            stale.join("\n  ")
        );
    }

    /// `MinRoleAddon` stamps every documented `/v0` operation with its minimum
    /// role, so the API reference shows the policy. Removing the modifier from
    /// `ApiDoc`'s `modifiers(...)` makes this fail.
    #[test]
    fn every_v0_operation_documents_its_min_role() {
        use crate::api::main::ApiDoc;
        use utoipa::OpenApi;
        use utoipa::openapi::PathItemType;

        let method = |t: &PathItemType| match t {
            PathItemType::Get => "GET",
            PathItemType::Post => "POST",
            PathItemType::Put => "PUT",
            PathItemType::Delete => "DELETE",
            PathItemType::Patch => "PATCH",
            PathItemType::Head => "HEAD",
            PathItemType::Options => "OPTIONS",
            PathItemType::Trace => "TRACE",
            PathItemType::Connect => "CONNECT",
        };
        let doc = ApiDoc::openapi();
        let mut missing = vec![];
        for (path, item) in doc.paths.paths.iter() {
            if !path.starts_with("/v0") {
                continue;
            }
            for (t, operation) in item.operations.iter() {
                let has_note = operation
                    .description
                    .as_deref()
                    .is_some_and(|d| d.contains("Required role:"));
                if !has_note {
                    missing.push(format!("{} {path}", method(t)));
                }
            }
        }
        missing.sort();
        assert!(
            missing.is_empty(),
            "these /v0 operations are missing the minimum-role annotation:\n  {}",
            missing.join("\n  ")
        );
    }
}
