import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest'

import type { load } from './+layout'

// The fake PostHog drops events sent before `init`, the same as the real SDK.
const m = vi.hoisted(() => {
  const state = {
    posthogReady: false,
    sent: [] as string[],
    // Each full-page navigation, with the URL it leaves (the next page's `document.referrer`).
    navigations: [] as { from: string; to: string }[]
  }
  return {
    state,
    initConceptualHq: vi.fn(),
    initProductFruits: vi.fn(),
    loginCallbackAsync: vi.fn(async () => {}),
    posthog: {
      init: () => {
        state.posthogReady = true
      },
      identify: () => {},
      reset: () => {},
      capture: (event: string) => {
        if (state.posthogReady) {
          state.sent.push(event)
        }
      }
    },
    getCachedConfigsForRender: vi.fn(),
    fetchConfigs: vi.fn()
  }
})

vi.mock('posthog-js', () => ({ default: m.posthog }))
vi.mock('#lib/services/conceptualHq.ts', () => ({
  initConceptualHq: m.initConceptualHq,
  isConceptualHqInitialized: () => false,
  trackConceptualHq: () => {},
  trackConceptualHqSignup: () => {}
}))
vi.mock('#lib/services/productFruits.ts', () => ({ initProductFruits: m.initProductFruits }))
vi.mock('$app/navigation', () => ({ goto: vi.fn(), refreshAll: vi.fn() }))
vi.mock('#lib/compositions/auth.ts', () => ({
  loadAuthConfig: async () => ({ oidc: { redirect_uri: 'http://console/auth/callback' } })
}))
vi.mock('@axa-fr/oidc-client', () => ({
  TokenAutomaticRenewMode: {},
  OidcLocation: class {},
  OidcClient: {
    getOrCreate: () => () => ({
      tryKeepExistingSessionAsync: async () => {},
      loginCallbackAsync: m.loginCallbackAsync,
      tokens: { idTokenPayload: { auth_time: 1000 }, accessToken: 'token' },
      userInfoAsync: async () => ({ sub: 'user-1', email: 'a@b.com' })
})
  }
}))
vi.mock('#lib/compositions/configCache.ts', () => (
{
  clearConfigCaches: () => {}, configChanged
  : () => true,
  fetchConfigs: m.fetchConfigs,
  getCachedConfigsForRender: m.getCachedConfigsForRender,
  getConfigFromCache: () => undefined,
  getSessionConfigFromCache: () => undefined
}
))

const memoryStorage = () => {
  const store = new Map<string, string>()
  return {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => store.set(k, v),
    removeItem: (k: string) => store.delete(k)
  }
}

// A page load, by default in a new browser with no stored state. The layout,
// analytics and PostHog modules keep state in module variables, so each load
// imports new instances.
const pageLoad = async (localStorage = memoryStorage(), href = 'http://console/') => {
  m.state.posthogReady = false
  m.state.sent = []
  m.state.navigations = []
  vi.clearAllMocks()
  vi.stubGlobal('localStorage', localStorage)
  const location = {
    href,
    get pathname() {
      return new URL(location.href).pathname
    },
    replace: (to: string) => {
      m.state.navigations.push({ from: location.href, to })
    }
  }
  vi.stubGlobal('window', {
    location,
    history: {
      replaceState: (_data: unknown, _unused: string, url: string) => {
        location.href = new URL(url, location.href).href
      }
    },
    localStorage,
    sessionStorage: memoryStorage()
  })
  vi.resetModules()
  return (await import('./+layout')).load({} as Parameters<typeof load>[0])
}

const config = (posthog: string) => ({ version: '1', edition: 'Open source', posthog })

// The first import of `+layout` transforms its whole module graph. This takes
// about 0.5 s locally and can exceed the 5 s test timeout on a loaded CI runner
// (#7503). `vi.resetModules()` keeps the transforms, so pay this cost once here.
beforeAll(async () => {
  m.getCachedConfigsForRender.mockReturnValue({})
  m.fetchConfigs.mockResolvedValue({ config: config(''), sessionConfig: undefined })
  await pageLoad()
  vi.unstubAllGlobals()
}, 60_000)

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('root layout load', () => {
  it('sends signin on the cold path', async () => {
    m.getCachedConfigsForRender.mockReturnValue({})
    m.fetchConfigs.mockResolvedValue({ config: config('key'), sessionConfig: undefined })

    await pageLoad()

    expect(m.state.sent).toEqual(['signin'])
  })

  it('sends signin on the warm path when only the fresh config enables PostHog', async () => {
    vi.useFakeTimers()
    m.getCachedConfigsForRender.mockReturnValue({ config: config('') })
    m.fetchConfigs.mockResolvedValue({ config: config('key'), sessionConfig: undefined })

    await pageLoad()
    await vi.runAllTimersAsync()

    expect(m.state.sent).toEqual(['signin'])
  })

  it('sends signin on the warm path when the background refresh fails', async () => {
    vi.useFakeTimers()
    m.getCachedConfigsForRender.mockReturnValue({ config: config('key') })
    m.fetchConfigs.mockRejectedValue(new Error('down'))

    await pageLoad()
    await vi.runAllTimersAsync()

    expect(m.state.sent).toEqual(['signin'])
  })

  it('sends signin after a page load with no analytics backend', async () => {
    const localStorage = memoryStorage()
    m.getCachedConfigsForRender.mockReturnValue({})
    m.fetchConfigs.mockResolvedValue({ config: config(''), sessionConfig: undefined })
    await pageLoad(localStorage)
    expect(m.state.sent).toEqual([])

    // The deployment enables PostHog. The user reloads the page in the same session.
    m.fetchConfigs.mockResolvedValue({ config: config('key'), sessionConfig: undefined })
    await pageLoad(localStorage)
    expect(m.state.sent).toEqual(['signin'])
  })

  it('sends signin on the cold path after the user selects a tenant', async () => {
    m.getCachedConfigsForRender.mockReturnValue({})
    m.fetchConfigs.mockResolvedValue({ config: undefined, sessionConfig: { memberships: [] } })
    const data = await pageLoad()
    expect(data.unresolvedTenant).toBeDefined()
    expect(m.state.sent).toEqual([])

    // Selecting a tenant runs `load()` again in the same page.
    m.fetchConfigs.mockResolvedValue({ config: config('key'), sessionConfig: undefined })
    await (await import('./+layout')).load({} as Parameters<typeof load>[0])
    expect(m.state.sent).toEqual(['signin'])
  })
})

describe('root layout load on the OIDC callback page', () => {
  const callbackUrl = 'http://console/auth/callback/?code=secret-code&state=secret-state'

  const loadCallbackPage = () => {
    m.getCachedConfigsForRender.mockReturnValue({})
    m.fetchConfigs.mockResolvedValue({ config: config('key'), sessionConfig: undefined })
    return pageLoad(memoryStorage(), callbackUrl)
  }

  it('goes to the app root with the code and state removed from the URL', async () => {
    await loadCallbackPage()

    await vi.waitFor(() =>
      expect(m.state.navigations).toEqual([{ from: 'http://console/auth/callback/', to: '/' }])
    )
  })

  // The fake OIDC client has tokens, as with a session that is still valid
  // when the user opens the callback URL again.
  it('starts no analytics while the URL holds the code and state', async () => {
    // The code exchange does not finish, so the URL keeps the code and state.
    m.loginCallbackAsync.mockReturnValueOnce(new Promise(() => {}))

    await loadCallbackPage()

    expect(m.state.posthogReady).toBe(false)
    expect(m.initConceptualHq).not.toHaveBeenCalled()
    expect(m.state.sent).toEqual([])
  })

  it('still starts the analytics on the app root', async () => {
    m.getCachedConfigsForRender.mockReturnValue({})
    m.fetchConfigs.mockResolvedValue({ config: config('key'), sessionConfig: undefined })

    await pageLoad()

    expect(m.state.posthogReady).toBe(true)
    expect(m.initConceptualHq).toHaveBeenCalledOnce()
  })
})
