// Client-side RBAC: the role to permission map that web-console gates UI on.
//
// The backend enforces a single ordered role per route (read < write < admin <
// owner). It exposes only the caller's own role (via the session payload, see
// AUTH_AND_TENANCY.md), never a route to role table, so the client cannot
// re-derive per-feature authorization from the API. Instead it keeps this
// hardcoded map. A gate then reads as "this feature needs `exec:runtime_upgrade`"
// and the role that grants it is one lookup, not a rank comparison at the call
// site. The map mirrors the backend today; `rbac.spec.ts` guards against the
// backend introducing a role the client does not model.

export type Role = 'read' | 'write' | 'admin' | 'owner'

/**
 * What a session reports when it holds no role, which the server does exactly
 * when it resolved no acting tenant: a role is granted per membership and means
 * nothing outside one.
 *
 * A named case rather than `undefined`, so `page.data.feldera.role` is total and
 * a consumer that forgets this case gets a type error instead of `undefined`
 * flowing through. `Role` itself stays the four roles the backend grants, since
 * that is what the permission map and the drift guard are about.
 */
export const NO_ROLE = 'no_role'
export type SessionRole = Role | typeof NO_ROLE

export type Permission =
  | 'read:pipeline'
  | 'read:pipeline_code'
  | 'read:pipeline_config'
  | 'read:support_bundle'
  | 'read:cluster_health'
  | 'write:pipeline'
  | 'write:pipeline_code'
  | 'write:pipeline_config'
  | 'write:pipeline_meta'
  | 'exec:pipeline'
  | 'exec:checkpoint'
  | 'exec:runtime_upgrade'
  | 'exec:pipeline_data'
  | 'write:api_key'
  | 'write:tenant_member'
  | 'write:oidc_trust'
  | 'write:tenant'
  | 'write:owner_trust'

// Ordered low to high. A role grants everything the roles before it grant.
export const ROLES: Role[] = ['read', 'write', 'admin', 'owner']

// What each role adds on top of the role below it (cumulative, see below).
const GRANTS: Record<Role, Permission[]> = {
  read: [
    'read:pipeline',
    'read:pipeline_code',
    'read:pipeline_config',
    'read:support_bundle',
    'read:cluster_health'
  ],
  write: [
    'write:pipeline',
    'write:pipeline_code',
    'write:pipeline_config',
    'write:pipeline_meta',
    'exec:pipeline',
    'exec:checkpoint',
    'exec:runtime_upgrade',
    'exec:pipeline_data',
    'write:api_key'
  ],
  admin: ['write:tenant_member', 'write:oidc_trust'],
  owner: ['write:tenant', 'write:owner_trust']
}

// Precompute the cumulative permission set per role once.
const PERMISSIONS: Record<Role, ReadonlySet<Permission>> = (() => {
  const acc: Permission[] = []
  const out = {} as Record<Role, Set<Permission>>
  for (const role of ROLES) {
    acc.push(...GRANTS[role])
    out[role] = new Set(acc)
  }
  return out
})()

export const hasPermission = (role: Role, permission: Permission): boolean =>
  PERMISSIONS[role].has(permission)

/**
 * The permissions a role grants, and none for {@link NO_ROLE}. `+layout.ts`
 * materializes this into `page.data.feldera.permissions` at session-config init,
 * so UI gates read a data field (as if the server sent it) instead of applying
 * the map at every call site.
 */
export const permissionsOf = (role: SessionRole): Permission[] =>
  role === NO_ROLE ? [] : [...PERMISSIONS[role]]

/**
 * The role a session holds, or {@link NO_ROLE} when it holds none.
 *
 * The server sends `role: null` exactly when no acting tenant resolved, together
 * with `tenant_id` and `tenant_name`. Reporting `read` for that would advertise
 * features whose every request fails, since such a session is refused everywhere
 * but `/v0/config/session`.
 *
 * An unrecognized role reads the same way: the backend gaining a role this
 * client does not model grants nothing until the map catches up, rather than
 * silently unlocking a feature. `rbac.spec.ts` guards against that drift.
 */
export const roleOf = (role: string | null | undefined): SessionRole =>
  (ROLES as string[]).includes(role ?? '') ? (role as Role) : NO_ROLE

// A session holding nothing. Frozen so a consumer reading the shared value
// cannot mutate it.
export const NO_PERMISSIONS: readonly Permission[] = Object.freeze([])

// Whether the session grants a permission. Reads the permission list
// materialized into `page.data.feldera` (see +layout.ts). An absent `feldera`
// means no session resolved a tenant — booting, unauthenticated, a config-load
// error, or no tenant picked yet — and grants nothing, so gates deny by default
// the way the backend's route table does. This is the session-facing check; the
// role-facing `hasPermission` above is used at init and in tests.
export const hasPermissions = (
  feldera: { permissions: readonly Permission[] } | undefined,
  permission: Permission
): boolean => (feldera?.permissions ?? NO_PERMISSIONS).includes(permission)
