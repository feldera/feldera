import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { Configuration } from '$lib/services/manager'
import type { UserProfile } from '$lib/types/auth'

// Capture calls to the underlying loader without injecting its <script>.
const init = vi.fn()
vi.mock('product-fruits', () => ({ productFruits: { init, safeExec: vi.fn() } }))

// The one-shot `initialized` flag is module-level, so each case re-imports a
// fresh module with its own stubbed `window` global.
const freshModule = async (browser: boolean) => {
  vi.resetModules()
  init.mockClear()
  vi.stubGlobal('window', browser ? {} : undefined)
  return import('./productFruits')
}

const profile: UserProfile = { id: 'user-1', email: 'a@b.com', name: 'Ann' }

// Only `product_fruits` is read; the rest is filler to satisfy the type.
const config = (product_fruits: string) => ({ product_fruits }) as Configuration

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('toProductFruitsUser', () => {
  it('keys username on email so identity matches PostHog', async () => {
    const { toProductFruitsUser } = await freshModule(true)
    expect(toProductFruitsUser(profile)).toEqual({
      username: 'a@b.com',
      email: 'a@b.com',
      firstname: 'Ann'
    })
  })

  it('falls back to id, then to anonymous, when email is missing', async () => {
    const { toProductFruitsUser } = await freshModule(true)
    expect(toProductFruitsUser({ id: 'user-1' }).username).toBe('user-1')
    expect(toProductFruitsUser({}).username).toBe('anonymous')
  })
})

describe('initProductFruits', () => {
  beforeEach(() => {
    init.mockClear()
  })

  it('initializes once with the workspace code, language, and user', async () => {
    const { initProductFruits } = await freshModule(true)
    initProductFruits(config('wc-code'), profile)
    expect(init).toHaveBeenCalledExactlyOnceWith('wc-code', 'en', {
      username: 'a@b.com',
      email: 'a@b.com',
      firstname: 'Ann'
    })
  })

  it('is idempotent: a second call after success is a no-op', async () => {
    const { initProductFruits } = await freshModule(true)
    initProductFruits(config('wc-code'), profile)
    initProductFruits(config('wc-code'), profile)
    expect(init).toHaveBeenCalledOnce()
  })

  it('does nothing when the workspace code is empty', async () => {
    const { initProductFruits } = await freshModule(true)
    initProductFruits(config(''), profile)
    expect(init).not.toHaveBeenCalled()
  })

  it('does nothing outside the browser', async () => {
    const { initProductFruits } = await freshModule(false)
    initProductFruits(config('wc-code'), profile)
    expect(init).not.toHaveBeenCalled()
  })
})
