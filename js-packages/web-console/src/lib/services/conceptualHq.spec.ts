import { afterEach, describe, expect, it, vi } from 'vitest'

import type { Configuration } from '$lib/services/manager'
import type { UserProfile } from '$lib/types/auth'

const profile: UserProfile = { id: 'user-1', email: 'a@b.com', name: 'Ann' }

// Only `conceptualhq` is read; the rest is filler to satisfy the type.
const config = (conceptualhq: string) => ({ conceptualhq }) as Configuration

// Fake DOM that records the injected <script>, so we can assert on it without a
// real document. `getElementsByTagName` must return an existing script whose
// parent receives the insert.
const stubDom = () => {
  const script: Record<string, unknown> = {}
  const insertBefore = vi.fn()
  const existing = { parentNode: { insertBefore } }
  vi.stubGlobal('document', {
    createElement: () => script,
    getElementsByTagName: () => [existing]
  })
  return { script, insertBefore }
}

// Each case re-imports a fresh module so the one-shot `initialized` flag and
// `window.ca` start clean.
const freshModule = async (browser: boolean) => {
  vi.resetModules()
  vi.stubGlobal('window', browser ? {} : undefined)
  return import('./conceptualHq')
}

// Queue entries are `arguments` objects; normalize to plain arrays for assertions.
const queued = () => (window.ca?.q ?? []).map((a) => Array.from(a))

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('initConceptualHq', () => {
  it('injects the loader keyed by config, then identifies and tracks signin', async () => {
    const { script } = stubDom()
    const { initConceptualHq } = await freshModule(true)
    initConceptualHq(config('my-key'), profile)

    expect(script.src).toBe('https://oqiset.feldera.com/analytics/loader-v1.js?key=my-key&v=1.1.0')
    expect(queued()).toEqual([
      ['identify', 'a@b.com', { email: 'a@b.com', name: 'Ann' }],
      ['track', 'signin']
    ])
  })

  it('does not track signup (OIDC login cannot distinguish it)', async () => {
    stubDom()
    const { initConceptualHq } = await freshModule(true)
    initConceptualHq(config('my-key'), profile)
    expect(queued().some((call) => call[1] === 'signup')).toBe(false)
  })

  it('falls back to the user id when email is missing', async () => {
    stubDom()
    const { initConceptualHq } = await freshModule(true)
    initConceptualHq(config('my-key'), { id: 'user-1' })
    expect(queued()[0]).toEqual(['identify', 'user-1', { email: undefined, name: undefined }])
  })

  it('is idempotent: a second call after success is a no-op', async () => {
    stubDom()
    const { initConceptualHq } = await freshModule(true)
    initConceptualHq(config('my-key'), profile)
    const afterFirst = queued().length
    initConceptualHq(config('my-key'), profile)
    expect(queued().length).toBe(afterFirst)
  })

  it('does nothing when the key is empty', async () => {
    stubDom()
    const { initConceptualHq } = await freshModule(true)
    initConceptualHq(config(''), profile)
    expect(window.ca).toBeUndefined()
  })

  it('does nothing outside the browser', async () => {
    stubDom()
    const { initConceptualHq } = await freshModule(false)
    // Must not throw when `window` is undefined.
    expect(() => initConceptualHq(config('my-key'), profile)).not.toThrow()
  })
})
