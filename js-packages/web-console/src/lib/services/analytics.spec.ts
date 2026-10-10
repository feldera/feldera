import { afterEach, describe, expect, it, vi } from 'vitest'

// vi.mock is hoisted above imports, so its factory may only touch vars created
// with vi.hoisted (also hoisted), not plain module-level consts.
const { capture, trackConceptualHq } = vi.hoisted(() => ({
  capture: vi.fn(),
  trackConceptualHq: vi.fn()
}))
vi.mock('posthog-js', () => ({ default: { capture } }))
vi.mock('$lib/services/conceptualHq', () => ({ trackConceptualHq }))

import type { UserProfile } from '$lib/types/auth'

import { captureEvent } from './analytics'

afterEach(() => {
  capture.mockClear()
  trackConceptualHq.mockClear()
  vi.unstubAllGlobals()
})

describe('captureEvent', () => {
  it('forwards the event and properties to both PostHog and ConceptualHQ', () => {
    const props = { demo: 'fraud', already_created: true, source: 'home' }
    captureEvent('demo_opened', props)
    expect(capture).toHaveBeenCalledExactlyOnceWith('demo_opened', props)
    expect(trackConceptualHq).toHaveBeenCalledExactlyOnceWith('demo_opened', props)
  })

  it('forwards events with no properties to both backends', () => {
    captureEvent('signin')
    expect(capture).toHaveBeenCalledExactlyOnceWith('signin', undefined)
    expect(trackConceptualHq).toHaveBeenCalledExactlyOnceWith('signin', undefined)
  })
})

describe('reportLogin', () => {
  const profile: UserProfile = { id: 'user-1', email: 'a@b.com' }

  const stubStorage = () => {
    const store = new Map<string, string>()
    vi.stubGlobal('localStorage', {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => store.set(k, v)
    })
  }

  // A new module instance acts as a page load. localStorage keeps its entries,
  // but `loginReported` resets.
  const pageLoad = async () => {
    vi.resetModules()
    return (await import('./analytics')).reportLogin
  }

  const signins = () => capture.mock.calls.filter(([event]) => event === 'signin').length

  it('sends signin to both backends', async () => {
    stubStorage()
    ;(await pageLoad())(profile, 1000)
    expect(capture).toHaveBeenCalledExactlyOnceWith('signin', undefined)
    expect(trackConceptualHq).toHaveBeenCalledExactlyOnceWith('signin', undefined)
  })

  it('sends signin once per authentication, not per page load', async () => {
    stubStorage()
    ;(await pageLoad())(profile, 1000)
    // Reload the page.
    ;(await pageLoad())(profile, 1000)
    expect(signins()).toBe(1)
    // Log out, then log in again.
    ;(await pageLoad())(profile, 2000)
    expect(signins()).toBe(2)
  })

  it('keeps one marker per user, so switching accounts sends signin', async () => {
    stubStorage()
    ;(await pageLoad())(profile, 1000)
    ;(await pageLoad())({ id: 'user-2', email: 'c@d.com' }, 1500)
    expect(signins()).toBe(2)
    // The first user has the same `auth_time`, so no second `signin`.
    ;(await pageLoad())(profile, 1000)
    expect(signins()).toBe(2)
  })

  it('reports once per page load', async () => {
    stubStorage()
    const reportLogin = await pageLoad()
    reportLogin(profile, undefined)
    reportLogin(profile, undefined)
    expect(signins()).toBe(1)
  })

  it('sends signin on each page load without auth_time or storage', async () => {
    stubStorage()
    ;(await pageLoad())(profile, undefined)
    ;(await pageLoad())(profile, undefined)
    vi.stubGlobal('localStorage', {
      getItem: () => {
        throw new Error('blocked')
      }
    })
    ;(await pageLoad())(profile, 1000)
    expect(signins()).toBe(3)
  })
})
