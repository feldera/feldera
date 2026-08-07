// Unit tests for the client RBAC map plus a drift guard against the backend.
//
// The map in `rbac.ts` mirrors the backend's per-route minimum roles, which the
// API exposes only as prose ("Required role: `write` or higher.") in
// `openapi.json`. The drift guard parses those phrases and asserts the backend
// uses no role the client fails to model, catching the one silent way this
// mirror can rot: a new backend role.

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import {
  hasPermission,
  hasPermissions,
  NO_PERMISSIONS,
  NO_ROLE,
  type Permission,
  permissionsOf,
  ROLES,
  type Role,
  roleOf
} from './rbac'

// Cumulative expectation, stated independently of the map's internal wiring so a
// bug in the precompute cannot pass by matching itself.
const EXPECTED: Record<Role, Permission[]> = {
  read: [
    'read:pipeline',
    'read:pipeline_code',
    'read:pipeline_config',
    'read:support_bundle',
    'read:cluster_health'
  ],
  write: [
    'read:pipeline',
    'read:pipeline_code',
    'read:pipeline_config',
    'read:support_bundle',
    'read:cluster_health',
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
  admin: [], // filled below (write + admin adds)
  owner: [] // filled below (admin + owner adds)
}
EXPECTED.admin = [...EXPECTED.write, 'write:tenant_member', 'write:oidc_trust']
EXPECTED.owner = [...EXPECTED.admin, 'write:tenant', 'write:owner_trust']

const ALL_PERMISSIONS = EXPECTED.owner

describe('hasPermission', () => {
  it('grants each role exactly its cumulative permission set', () => {
    for (const role of ROLES) {
      const granted = ALL_PERMISSIONS.filter((p) => hasPermission(role, p))
      expect(new Set(granted)).toEqual(new Set(EXPECTED[role]))
    }
  })

  it('is monotonic: a higher role never loses a lower role permission', () => {
    for (let i = 1; i < ROLES.length; i++) {
      const lower = ROLES[i - 1]
      const higher = ROLES[i]
      for (const p of ALL_PERMISSIONS) {
        if (hasPermission(lower, p)) {
          expect(hasPermission(higher, p)).toBe(true)
        }
      }
    }
  })

  it('denies write and admin permissions to read', () => {
    expect(hasPermission('read', 'write:pipeline')).toBe(false)
    expect(hasPermission('read', 'exec:pipeline')).toBe(false)
    expect(hasPermission('read', 'write:api_key')).toBe(false)
    expect(hasPermission('read', 'write:tenant_member')).toBe(false)
  })

  it('reserves tenant and owner-trust management for owner', () => {
    expect(hasPermission('admin', 'write:tenant')).toBe(false)
    expect(hasPermission('admin', 'write:owner_trust')).toBe(false)
    expect(hasPermission('owner', 'write:tenant')).toBe(true)
    expect(hasPermission('owner', 'write:owner_trust')).toBe(true)
  })
})

describe('permissionsOf', () => {
  it('returns the cumulative permission set for each role', () => {
    for (const role of ROLES) {
      expect(new Set(permissionsOf(role))).toEqual(new Set(EXPECTED[role]))
    }
  })

  it('agrees with hasPermission for every role and permission', () => {
    for (const role of ROLES) {
      const set = new Set(permissionsOf(role))
      for (const p of ALL_PERMISSIONS) {
        expect(set.has(p)).toBe(hasPermission(role, p))
      }
    }
  })

  it('returns a fresh array each call so a caller cannot corrupt the map', () => {
    const first = permissionsOf('read')
    first.push('write:pipeline')
    expect(permissionsOf('read')).not.toContain('write:pipeline')
  })

  it('grants nothing for NO_ROLE', () => {
    // A session with no role resolved no acting tenant, and a role means nothing
    // outside one. This is what closes every `<RBAC>` gate on the tenant page.
    expect(permissionsOf(NO_ROLE)).toEqual([])
  })
})

describe('hasPermissions (session-facing)', () => {
  it('reads the granted list off the session feldera data', () => {
    const write = { permissions: permissionsOf('write') }
    expect(hasPermissions(write, 'write:pipeline')).toBe(true)
    expect(hasPermissions({ permissions: permissionsOf('read') }, 'write:pipeline')).toBe(false)
  })

  it('grants nothing when the session is absent', () => {
    // No `feldera` means no session resolved a tenant (booting, unauthenticated,
    // a config-load error, or no tenant picked yet). Every route but
    // /config/session refuses such a session, so reads are denied too, and
    // nothing throws.
    for (const permission of ALL_PERMISSIONS) {
      expect(hasPermissions(undefined, permission), permission).toBe(false)
    }
  })
})

// How `+layout.ts` materializes `page.data.feldera.permissions`: the session's
// role string in, the gated permission list out. Asserted as a pair because the
// no-tenant case only stays closed while both halves hold.
describe('roleOf composed with permissionsOf (what the layout does)', () => {
  it('grants the named role its permissions', () => {
    for (const role of ROLES) {
      expect(new Set(permissionsOf(roleOf(role)))).toEqual(new Set(EXPECTED[role]))
    }
  })

  it('grants nothing for the session the server sends without a tenant', () => {
    // `role`, `tenant_id` and `tenant_name` are null together in that state.
    for (const noRole of [null, undefined, '', 'superuser']) {
      expect(permissionsOf(roleOf(noRole)), String(noRole)).toEqual([])
    }
  })
})

describe('NO_PERMISSIONS', () => {
  it('is empty', () => {
    expect(NO_PERMISSIONS).toEqual([])
  })

  it('is frozen so a consumer reading the shared value cannot mutate it', () => {
    expect(Object.isFrozen(NO_PERMISSIONS)).toBe(true)
  })
})

describe('roleOf', () => {
  it('passes through every known role', () => {
    for (const role of ROLES) {
      expect(roleOf(role)).toBe(role)
    }
  })

  it('yields NO_ROLE for absent input, since the server omits it with the tenant', () => {
    // `role: null` arrives exactly when no acting tenant resolved. Reporting
    // `read` there would grant a session that is refused everywhere but
    // /v0/config/session the read role's permissions.
    expect(roleOf(undefined)).toBe(NO_ROLE)
    expect(roleOf(null)).toBe(NO_ROLE)
    expect(roleOf('')).toBe(NO_ROLE)
  })

  it('yields NO_ROLE for a role this client does not model', () => {
    // A backend role the map has not caught up with grants nothing rather than
    // silently unlocking a feature. The drift guard below is what catches it.
    expect(roleOf('superuser')).toBe(NO_ROLE)
  })

  it('never yields undefined, so `feldera.role` is always a case to handle', () => {
    for (const input of [undefined, null, '', 'superuser', ...ROLES]) {
      expect(roleOf(input), String(input)).not.toBeUndefined()
    }
  })

  it('maps the sentinel string to NO_ROLE rather than treating it as a role', () => {
    // Belt and braces: `no_role` arriving as a role name still denies.
    expect(roleOf(NO_ROLE)).toBe(NO_ROLE)
    expect(permissionsOf(roleOf(NO_ROLE))).toEqual([])
  })
})

// Locate the repo-root openapi.json by walking up from this file, so the guard
// survives a move of the package within the tree.
const findOpenapi = (): string => {
  let dir = path.dirname(fileURLToPath(import.meta.url))
  for (let i = 0; i < 8; i++) {
    const candidate = path.join(dir, 'openapi.json')
    if (fs.existsSync(candidate)) {
      return candidate
    }
    dir = path.dirname(dir)
  }
  throw new Error('openapi.json not found walking up from rbac.spec.ts')
}

describe('drift guard against openapi.json', () => {
  it('models every role the backend requires on a route', () => {
    const spec = fs.readFileSync(findOpenapi(), 'utf8')
    const backendRoles = new Set([...spec.matchAll(/Required role: `([a-z]+)`/g)].map((m) => m[1]))

    expect(backendRoles.size).toBeGreaterThan(0)
    const unmodeled = [...backendRoles].filter((r) => !(ROLES as string[]).includes(r))
    expect(unmodeled).toEqual([])
  })
})
