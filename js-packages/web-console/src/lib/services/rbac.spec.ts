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
  DEFAULT_PERMISSIONS,
  hasPermission,
  hasPermissions,
  type Permission,
  permissionsOf,
  ROLES,
  type Role,
  roleOf
} from './rbac'

// Cumulative expectation, stated independently of the map's internal wiring so a
// bug in the precompute cannot pass by matching itself.
const EXPECTED: Record<Role, Permission[]> = {
  read: ['read:pipeline', 'read:pipeline_code', 'read:pipeline_config', 'read:support_bundle'],
  write: [
    'read:pipeline',
    'read:pipeline_code',
    'read:pipeline_config',
    'read:support_bundle',
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
})

describe('hasPermissions (session-facing)', () => {
  it('reads the granted list off the session feldera data', () => {
    const write = { permissions: permissionsOf('write') }
    expect(hasPermissions(write, 'write:pipeline')).toBe(true)
    expect(hasPermissions({ permissions: permissionsOf('read') }, 'write:pipeline')).toBe(false)
  })

  it('falls back to the read floor when the session is absent', () => {
    // A rendered app shell with no session config (config-load error): reads are
    // permitted, writes denied, and nothing throws.
    expect(hasPermissions(undefined, 'read:pipeline')).toBe(true)
    expect(hasPermissions(undefined, 'write:pipeline')).toBe(false)
    expect(hasPermissions(undefined, 'write:tenant_member')).toBe(false)
  })
})

describe('DEFAULT_PERMISSIONS', () => {
  it('equals the read role grant', () => {
    expect(new Set(DEFAULT_PERMISSIONS)).toEqual(new Set(permissionsOf('read')))
  })

  it('is frozen so a consumer reading the shared default cannot mutate it', () => {
    expect(Object.isFrozen(DEFAULT_PERMISSIONS)).toBe(true)
  })
})

describe('roleOf', () => {
  it('passes through every known role', () => {
    for (const role of ROLES) {
      expect(roleOf(role)).toBe(role)
    }
  })

  it('defaults unknown or missing input to the least-privileged role', () => {
    expect(roleOf(undefined)).toBe('read')
    expect(roleOf('')).toBe('read')
    expect(roleOf('superuser')).toBe('read')
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
