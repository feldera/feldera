//! Per-tenant describer manifest cache.
//!
//! Tracks the current build state of each tenant's describer manifest so
//! that `GET /v0/connectors/status` can report it without blocking and so
//! `PUT /v0/connectors/connectors.toml` and `POST /v0/connectors/refresh`
//! can return `202 Accepted` while the describer rebuilds in the
//! background (Phase 8.11 / 8.12).
//!
//! The cache is intentionally a dumb state container. It does *not* own
//! the database, the compiler config, or the describer build pipeline —
//! callers (the API endpoints) drive state transitions. Two reasons:
//!   * The cache stays unit-testable without spawning Cargo or hitting the
//!     filesystem.
//!   * The endpoint layer keeps full visibility over what runs in
//!     a `tokio::spawn` and what runs synchronously on the request path.
//!
//! State machine:
//!
//! ```text
//!   ┌──────────────────┐  PUT empty content
//!   │ NotConfigured    │ ◄─────────────┐
//!   └──────────────────┘               │
//!            │                          │
//!            │ PUT non-empty            │
//!            ▼                          │
//!   ┌──────────────────┐                │
//!   │ Building         │                │
//!   └──────────────────┘                │
//!         │      │                       │
//!    ok   │      │ err                   │
//!         ▼      ▼                       │
//!   ┌─────────┐ ┌────────┐                │
//!   │ Ready   │ │ Failed │                │
//!   └─────────┘ └────────┘                │
//!         └──── new content_hash ────────┘
//! ```
//!
//! Transitions out of `Building` are guarded against staleness: a
//! completion callback only takes effect if the cached state still
//! references the same `content_hash`. If a newer PUT raced in and
//! kicked off a fresh build with a different hash, the older
//! completion is ignored.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::db::types::tenant::TenantId;

/// Snapshot of a tenant's describer build state.
///
/// All variants carry the `version` of the `tenant_connector_config` row
/// that produced them so the API layer can echo it back to clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantBuildState {
    /// Tenant has empty `connectors.toml` content. The describer is not
    /// run; the manifest is constructed in-process from the bundled
    /// connector inventory by callers that need it.
    NotConfigured { version: i64 },

    /// A describer rebuild is currently in flight for `content_hash`.
    Building {
        content_hash: String,
        version: i64,
        since: DateTime<Utc>,
    },

    /// The latest successful manifest is available.
    Ready {
        content_hash: String,
        version: i64,
        manifest_json: String,
        last_built_at: DateTime<Utc>,
    },

    /// The most recent describer build for `content_hash` failed.
    Failed {
        content_hash: String,
        version: i64,
        error: String,
        last_built_at: DateTime<Utc>,
    },
}

impl TenantBuildState {
    /// Return the `content_hash` if the state has one (every variant
    /// except `NotConfigured`).
    pub fn content_hash(&self) -> Option<&str> {
        match self {
            Self::NotConfigured { .. } => None,
            Self::Building { content_hash, .. }
            | Self::Ready { content_hash, .. }
            | Self::Failed { content_hash, .. } => Some(content_hash),
        }
    }

    /// Return the version associated with this state.
    pub fn version(&self) -> i64 {
        match self {
            Self::NotConfigured { version }
            | Self::Building { version, .. }
            | Self::Ready { version, .. }
            | Self::Failed { version, .. } => *version,
        }
    }
}

/// Deployment-wide map of `TenantId → TenantBuildState`.
///
/// All methods are async because the inner map is behind a tokio Mutex.
/// The Mutex is fine-grained: each method holds it only long enough to
/// read or update one entry. Background build tasks acquire it just to
/// transition state; they do not hold it across the actual `cargo
/// build`.
#[derive(Debug, Default)]
pub struct ManifestCache {
    tenants: Mutex<HashMap<TenantId, TenantBuildState>>,
}

impl ManifestCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the current state for `tenant_id`, or `None` if no entry
    /// has been initialised yet. Callers materialise the entry via one
    /// of the `mark_*` methods below.
    pub async fn get(&self, tenant_id: TenantId) -> Option<TenantBuildState> {
        self.tenants.lock().await.get(&tenant_id).cloned()
    }

    /// Mark the tenant as having empty `connectors.toml` content.
    /// Idempotent.
    pub async fn mark_not_configured(&self, tenant_id: TenantId, version: i64) {
        self.tenants
            .lock()
            .await
            .insert(tenant_id, TenantBuildState::NotConfigured { version });
    }

    /// Try to acquire the build slot for `tenant_id` at `content_hash`.
    ///
    /// Returns `true` when the caller should run (or spawn) a describer
    /// build and call [`mark_ready`] or [`mark_failed`] on completion.
    ///
    /// Returns `false` when a build is unnecessary:
    ///   * State is already `Ready { content_hash: same }` and `force` is `false`.
    ///   * State is already `Building { content_hash: same }`.
    ///
    /// In every other case (no entry, `Failed`, different hash, or
    /// `force = true` while `Ready`) the state is set to `Building` and
    /// `true` is returned.
    ///
    /// `force = true` is used by `POST /v0/connectors/refresh`, which
    /// must run `cargo update` even when the blob is unchanged. It does
    /// **not** override an already in-flight `Building` for the same
    /// hash — the refresh waits for the in-flight build instead of
    /// racing it.
    ///
    /// [`mark_ready`]: ManifestCache::mark_ready
    /// [`mark_failed`]: ManifestCache::mark_failed
    pub async fn try_begin_build(
        &self,
        tenant_id: TenantId,
        content_hash: String,
        version: i64,
        force: bool,
    ) -> bool {
        let mut guard = self.tenants.lock().await;
        match guard.get(&tenant_id) {
            Some(TenantBuildState::Building { content_hash: h, .. }) if h == &content_hash => {
                return false;
            }
            Some(TenantBuildState::Ready { content_hash: h, .. }) if h == &content_hash && !force => {
                return false;
            }
            _ => {}
        }
        guard.insert(
            tenant_id,
            TenantBuildState::Building {
                content_hash,
                version,
                since: Utc::now(),
            },
        );
        true
    }

    /// Record a successful describer build.
    ///
    /// The transition only takes effect if the current state's
    /// `content_hash` matches `content_hash` — otherwise the completion
    /// is stale (a newer PUT has already replaced the in-flight build)
    /// and is silently dropped.
    pub async fn mark_ready(
        &self,
        tenant_id: TenantId,
        content_hash: String,
        version: i64,
        manifest_json: String,
    ) {
        let mut guard = self.tenants.lock().await;
        if !current_hash_matches(&guard, tenant_id, &content_hash) {
            return;
        }
        guard.insert(
            tenant_id,
            TenantBuildState::Ready {
                content_hash,
                version,
                manifest_json,
                last_built_at: Utc::now(),
            },
        );
    }

    /// Record a failed describer build. Same staleness rule as
    /// [`mark_ready`].
    pub async fn mark_failed(
        &self,
        tenant_id: TenantId,
        content_hash: String,
        version: i64,
        error: String,
    ) {
        let mut guard = self.tenants.lock().await;
        if !current_hash_matches(&guard, tenant_id, &content_hash) {
            return;
        }
        guard.insert(
            tenant_id,
            TenantBuildState::Failed {
                content_hash,
                version,
                error,
                last_built_at: Utc::now(),
            },
        );
    }
}

fn current_hash_matches(
    map: &HashMap<TenantId, TenantBuildState>,
    tenant_id: TenantId,
    expected: &str,
) -> bool {
    map.get(&tenant_id)
        .and_then(|s| s.content_hash())
        .is_some_and(|h| h == expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn tenant(n: u128) -> TenantId {
        TenantId(Uuid::from_u128(n))
    }

    #[tokio::test]
    async fn unseen_tenant_returns_none() {
        let cache = ManifestCache::new();
        assert!(cache.get(tenant(1)).await.is_none());
    }

    #[tokio::test]
    async fn mark_not_configured_records_state() {
        let cache = ManifestCache::new();
        cache.mark_not_configured(tenant(1), 7).await;
        let state = cache.get(tenant(1)).await.unwrap();
        assert_eq!(state, TenantBuildState::NotConfigured { version: 7 });
    }

    #[tokio::test]
    async fn try_begin_build_acquires_slot_when_unseen() {
        let cache = ManifestCache::new();
        assert!(cache.try_begin_build(tenant(1), "hash1".into(), 1, false).await);
        let state = cache.get(tenant(1)).await.unwrap();
        assert!(matches!(state, TenantBuildState::Building { .. }));
        assert_eq!(state.content_hash(), Some("hash1"));
        assert_eq!(state.version(), 1);
    }

    #[tokio::test]
    async fn try_begin_build_is_idempotent_for_in_flight_same_hash() {
        let cache = ManifestCache::new();
        assert!(cache.try_begin_build(tenant(1), "h".into(), 1, false).await);
        // Second call with the same hash while still Building must return
        // false — caller should not spawn another build.
        assert!(!cache.try_begin_build(tenant(1), "h".into(), 1, false).await);
    }

    #[tokio::test]
    async fn try_begin_build_is_idempotent_when_already_ready() {
        let cache = ManifestCache::new();
        cache.try_begin_build(tenant(1), "h".into(), 1, false).await;
        cache
            .mark_ready(tenant(1), "h".into(), 1, "{}".into())
            .await;
        assert!(!cache.try_begin_build(tenant(1), "h".into(), 1, false).await);
    }

    #[tokio::test]
    async fn try_begin_build_force_rebuilds_already_ready() {
        let cache = ManifestCache::new();
        cache.try_begin_build(tenant(1), "h".into(), 1, false).await;
        cache
            .mark_ready(tenant(1), "h".into(), 1, "{}".into())
            .await;
        // force=true: refresh-style call bypasses Ready idempotency.
        assert!(cache.try_begin_build(tenant(1), "h".into(), 2, true).await);
        let state = cache.get(tenant(1)).await.unwrap();
        assert!(matches!(state, TenantBuildState::Building { .. }));
    }

    #[tokio::test]
    async fn try_begin_build_does_not_displace_in_flight_with_force() {
        // Even force=true should not start a second concurrent build for
        // the same in-flight hash — the refresh waits instead of racing.
        let cache = ManifestCache::new();
        assert!(cache.try_begin_build(tenant(1), "h".into(), 1, false).await);
        assert!(!cache.try_begin_build(tenant(1), "h".into(), 1, true).await);
    }

    #[tokio::test]
    async fn try_begin_build_kicks_off_new_hash_after_failure() {
        let cache = ManifestCache::new();
        cache.try_begin_build(tenant(1), "h".into(), 1, false).await;
        cache
            .mark_failed(tenant(1), "h".into(), 1, "boom".into())
            .await;
        // Same hash, no force: caller still gets the slot — Failed is
        // not idempotent in the way Ready is, so a retry is allowed.
        assert!(cache.try_begin_build(tenant(1), "h".into(), 1, false).await);
    }

    #[tokio::test]
    async fn mark_ready_drops_stale_completion() {
        // Build kicks off for hash1, then a new PUT replaces it with
        // hash2. The hash1 completion must be ignored.
        let cache = ManifestCache::new();
        cache.try_begin_build(tenant(1), "hash1".into(), 1, false).await;
        cache.try_begin_build(tenant(1), "hash2".into(), 2, false).await;
        cache
            .mark_ready(tenant(1), "hash1".into(), 1, "old".into())
            .await;
        let state = cache.get(tenant(1)).await.unwrap();
        // Still Building hash2 — old completion was dropped.
        match state {
            TenantBuildState::Building { content_hash, version, .. } => {
                assert_eq!(content_hash, "hash2");
                assert_eq!(version, 2);
            }
            other => panic!("expected Building hash2, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mark_failed_drops_stale_completion() {
        let cache = ManifestCache::new();
        cache.try_begin_build(tenant(1), "hash1".into(), 1, false).await;
        cache.try_begin_build(tenant(1), "hash2".into(), 2, false).await;
        cache
            .mark_failed(tenant(1), "hash1".into(), 1, "boom".into())
            .await;
        // Still Building hash2.
        let state = cache.get(tenant(1)).await.unwrap();
        assert_eq!(state.content_hash(), Some("hash2"));
        assert!(matches!(state, TenantBuildState::Building { .. }));
    }

    #[tokio::test]
    async fn happy_path_building_to_ready() {
        let cache = ManifestCache::new();
        cache.try_begin_build(tenant(1), "h".into(), 1, false).await;
        cache
            .mark_ready(tenant(1), "h".into(), 1, "{\"ok\":true}".into())
            .await;
        let state = cache.get(tenant(1)).await.unwrap();
        match state {
            TenantBuildState::Ready { manifest_json, version, content_hash, .. } => {
                assert_eq!(manifest_json, "{\"ok\":true}");
                assert_eq!(version, 1);
                assert_eq!(content_hash, "h");
            }
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tenants_are_isolated() {
        let cache = ManifestCache::new();
        cache.mark_not_configured(tenant(1), 1).await;
        cache.try_begin_build(tenant(2), "h2".into(), 5, false).await;
        assert_eq!(
            cache.get(tenant(1)).await.unwrap(),
            TenantBuildState::NotConfigured { version: 1 }
        );
        let state2 = cache.get(tenant(2)).await.unwrap();
        assert_eq!(state2.content_hash(), Some("h2"));
        assert_eq!(state2.version(), 5);
    }
}
