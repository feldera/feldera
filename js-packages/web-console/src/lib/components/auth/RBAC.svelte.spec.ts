// Component tests for the RBAC gating wrapper: hide vs disable modes and the
// disabledProps contract, across roles. The role comes from a mocked
// `$app/state` page, read at render time.

import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '$lib/services/rbac'

const roleState = vi.hoisted(() => ({ current: 'read' as 'read' | 'write' | 'admin' | 'owner' }))
vi.mock('$app/state', () => ({
  page: {
    data: {
      get feldera() {
        return { role: roleState.current, permissions: permissionsOf(roleState.current) }
      }
    }
  }
}))

import RBACHarness from './RBACHarness.svelte'

afterEach(() => {
  roleState.current = 'read'
})

describe('RBAC.svelte', () => {
  describe('mode="hide" (default)', () => {
    it('renders the child when the role grants the permission', async () => {
      roleState.current = 'write'
      await render(RBACHarness, { require: 'write:pipeline' })
      await expect.element(page.getByTestId('child')).toBeInTheDocument()
    })

    it('omits the child when the role lacks the permission', async () => {
      roleState.current = 'read'
      await render(RBACHarness, { require: 'write:pipeline' })
      await expect.element(page.getByTestId('child')).not.toBeInTheDocument()
    })

    it('always renders read-floor permissions', async () => {
      roleState.current = 'read'
      await render(RBACHarness, { require: 'read:pipeline' })
      await expect.element(page.getByTestId('child')).toBeInTheDocument()
    })
  })

  describe('mode="disable"', () => {
    it('renders the child enabled and clean when allowed', async () => {
      roleState.current = 'write'
      await render(RBACHarness, { require: 'write:pipeline', mode: 'disable' })
      const child = page.getByTestId('child')
      await expect.element(child).toBeInTheDocument()
      await expect.element(child).not.toBeDisabled()
      await expect.element(child).toHaveAttribute('data-allowed', 'true')
    })

    it('renders the child but applies the read-only look when disallowed', async () => {
      roleState.current = 'read'
      await render(RBACHarness, { require: 'write:pipeline', mode: 'disable' })
      const child = page.getByTestId('child')
      await expect.element(child).toBeInTheDocument()
      await expect.element(child).toBeDisabled()
      await expect.element(child).toHaveAttribute('aria-disabled', 'true')
      await expect.element(child).toHaveAttribute('data-allowed', 'false')
    })
  })
})
