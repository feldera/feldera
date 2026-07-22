import { afterEach, describe, expect, it, vi } from 'vitest'

import type { Configuration } from '$lib/services/manager'
import type { UserProfile } from '$lib/types/auth'

// vi.mock is hoisted above imports, so its factory may only touch vars created
// with vi.hoisted (also hoisted), not plain module-level consts.
const { init, identify, capture } = vi.hoisted(() => ({
  init: vi.fn(),
  identify: vi.fn(),
  capture: vi.fn()
}))
vi.mock('posthog-js', () => ({ default: { init, identify, capture } }))

const profile: UserProfile = { id: 'user-1', email: 'a@b.com', name: 'Ann' }

// Only `posthog` is read; the rest is filler to satisfy the type.
const config = (posthog: string) => ({ posthog }) as Configuration

// Each case re-imports a fresh module so the one-shot `initialized` flag resets.
const freshModule = async (browser: boolean) => {
  vi.resetModules()
  init.mockClear()
  identify.mockClear()
  capture.mockClear()
  vi.stubGlobal('window', browser ? {} : undefined)
  return import('./posthog')
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('initPosthog', () => {
  it('starts the SDK, identifies the user, and captures signin', async () => {
    const { initPosthog } = await freshModule(true)
    initPosthog(config('ph-key'), profile)
    expect(init).toHaveBeenCalledExactlyOnceWith('ph-key', {
      api_host: 'https://us.i.posthog.com',
      person_profiles: 'identified_only',
      capture_pageview: false,
      capture_pageleave: false
    })
    expect(identify).toHaveBeenCalledExactlyOnceWith('a@b.com', {
      email: 'a@b.com',
      name: 'Ann',
      auth_id: 'user-1'
    })
    expect(capture).toHaveBeenCalledExactlyOnceWith('signin')
  })

  it('captures signin even when the profile has no email (skips identify)', async () => {
    const { initPosthog } = await freshModule(true)
    initPosthog(config('ph-key'), { id: 'user-1' })
    expect(identify).not.toHaveBeenCalled()
    expect(capture).toHaveBeenCalledExactlyOnceWith('signin')
  })

  it('is idempotent: a second call reports signin once', async () => {
    const { initPosthog } = await freshModule(true)
    initPosthog(config('ph-key'), profile)
    initPosthog(config('ph-key'), profile)
    expect(init).toHaveBeenCalledOnce()
    expect(capture).toHaveBeenCalledOnce()
  })

  it('does nothing when the key is empty', async () => {
    const { initPosthog } = await freshModule(true)
    initPosthog(config(''), profile)
    expect(init).not.toHaveBeenCalled()
    expect(capture).not.toHaveBeenCalled()
  })

  it('does nothing outside the browser', async () => {
    const { initPosthog } = await freshModule(false)
    initPosthog(config('ph-key'), profile)
    expect(init).not.toHaveBeenCalled()
  })
})
