import { OidcClient, type OidcLocation } from '@axa-fr/oidc-client'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { toAxaOidcConfig, withoutRevocationRetry } from '#lib/compositions/@axa-fr/auth.ts'
import type { OidcConfig } from '#lib/compositions/auth.ts'

const idp = 'https://idp.example.com/'
const revocationEndpoint = `${idp}oauth2/revoke`
const endSessionEndpoint = `${idp}logout`

/** The OIDC client aborts each request after this time. */
const CLIENT_REQUEST_TIMEOUT_MS = 10_000

/** An AWS Cognito configuration in the form that `loadAuthConfig` builds. */
const oidcConfig = (overrides: { revocation_endpoint?: string } = {}): OidcConfig =>
  ({
    authority: idp.replace(/\/+$/g, ''),
    client_id: 'test-client',
    scope: 'openid profile email',
    redirect_uri: 'https://console.example.com/auth/callback/',
    authority_configuration: {
      issuer: idp,
      authorization_endpoint: `${idp}authorize`,
      token_endpoint: `${idp}token`,
      revocation_endpoint: revocationEndpoint,
      userinfo_endpoint: `${idp}oauth2/userInfo`,
      end_session_endpoint: endSessionEndpoint,
      ...overrides
    },
    storage: memoryStorage()
  }) as OidcConfig

/** The OIDC client reads and writes storage keys as properties, not with `getItem`, so a plain object works as storage. */
const memoryStorage = () => ({}) as Storage

/** Store a signed-in session where the client looks for it on page load. */
function storeSession(
  storage: Storage,
  configurationName: string,
  tokens: { accessToken: string; refreshToken?: string }
) {
  const nowSeconds = Math.floor(Date.now() / 1000)
  storage[`oidc.${configurationName}`] = JSON.stringify({
    status: 'LOGGED_IN',
    tokens: {
      ...tokens,
      idToken: 'id-token',
      tokenType: 'Bearer',
      issuedAt: nowSeconds,
      expiresIn: 3600
    }
  })
}

type Network = {
  /** Revocation requests, each as its decoded form body. */
  revocations: Record<string, string>[]
  /** Every network request and redirect, in order. */
  events: string[]
}

/**
 * Restore a session from storage the way the console does on page load, then
 * sign out. `respondToRevocation` answers the revocation request, and
 * `logoutExtras` are the extra parameters the console passes to `logoutAsync`.
 */
async function signOut(options: {
  config?: OidcConfig
  tokens?: { accessToken: string; refreshToken?: string }
  respondToRevocation?: () => Promise<Response>
  logoutExtras?: Record<string, string>
}): Promise<Network & { storage: Storage }> {
  const config = options.config ?? oidcConfig()
  const configurationName = `test-${crypto.randomUUID()}`
  storeSession(
    config.storage!,
    configurationName,
    options.tokens ?? {
      accessToken: 'access-token',
      refreshToken: 'refresh-token'
    }
  )
  const respondToRevocation =
    options.respondToRevocation ?? (async () => new Response(null, { status: 200 }))

  const network: Network = { revocations: [], events: [] }
  const fetchMock = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = input.toString()
    network.events.push(`fetch ${url}`)
    if (url === revocationEndpoint) {
      network.revocations.push(Object.fromEntries(new URLSearchParams(String(init?.body))))
      return abortable(respondToRevocation(), init?.signal)
    }
    throw new Error(`Unexpected request to ${url}`)
  }
  const location: OidcLocation = {
    open: (url: string) => void network.events.push(`open ${url.split('?')[0]}`),
    reload: () => void network.events.push('reload'),
    getCurrentHref: () => 'https://console.example.com/',
    getPath: () => '/',
    getOrigin: () => 'https://console.example.com'
  }

  const client = OidcClient.getOrCreate(() => withoutRevocationRetry(fetchMock), location)(
    toAxaOidcConfig(config),
    configurationName
  )
  await client.tryKeepExistingSessionAsync()
  expect(client.tokens?.refreshToken, 'stored session was not restored').toBe(
    options.tokens ? options.tokens.refreshToken : 'refresh-token'
  )
  await client.logoutAsync('/', options.logoutExtras)
  return { ...network, storage: config.storage! }
}

/** Reject `response` with the abort reason when `signal` aborts, as `fetch` does. */
function abortable(response: Promise<Response>, signal: AbortSignal | null | undefined) {
  return new Promise<Response>((resolve, reject) => {
    signal?.addEventListener('abort', () => reject(signal.reason))
    response.then(resolve, reject)
  })
}

describe('sign-out', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('revokes the refresh token before redirecting to the end-session endpoint', async () => {
    const { revocations, events } = await signOut({})

    expect(revocations).toEqual([
      { token: 'refresh-token', token_type_hint: 'refresh_token', client_id: 'test-client' }
    ])
    expect(events).toEqual([`fetch ${revocationEndpoint}`, `open ${endSessionEndpoint}`])
  })

  it('does not ask to revoke the access token', async () => {
    const { revocations } = await signOut({})

    expect(revocations.map((r) => r.token_type_hint)).not.toContain('access_token')
  })

  it('still signs out when the provider rejects the revocation', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { events, storage } = await signOut({
      respondToRevocation: async () =>
        new Response(JSON.stringify({ error: 'unsupported_token_type' }), { status: 400 })
    })

    expect(events.at(-1)).toBe(`open ${endSessionEndpoint}`)
    expect(storedTokens(storage)).toBeUndefined()
  })

  it('still signs out when the revocation request fails', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const { events, storage } = await signOut({
      respondToRevocation: () => Promise.reject(new TypeError('Failed to fetch'))
    })

    expect(events.at(-1)).toBe(`open ${endSessionEndpoint}`)
    expect(storedTokens(storage)).toBeUndefined()
  })

  it('signs out after one revocation attempt when the provider does not answer', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
    let isSignedOut = false
    const signingOut = signOut({ respondToRevocation: () => new Promise(() => {}) }).then(
      (network) => {
        isSignedOut = true
        return network
      }
    )

    await vi.advanceTimersByTimeAsync(CLIENT_REQUEST_TIMEOUT_MS - 1)
    expect(isSignedOut).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    expect(isSignedOut).toBe(true)
    const { revocations, events, storage } = await signingOut
    expect(revocations).toHaveLength(1)
    expect(events.at(-1)).toBe(`open ${endSessionEndpoint}`)
    expect(storedTokens(storage)).toBeUndefined()
  })

  it('sends only the revocation parameters when the console passes Cognito logout extras', async () => {
    const { revocations } = await signOut({
      logoutExtras: {
        client_id: 'test-client',
        redirect_uri: 'https://console.example.com/auth/callback/',
        response_type: 'code',
        nonce: 'nonce'
      }
    })

    expect(revocations).toEqual([
      { token: 'refresh-token', token_type_hint: 'refresh_token', client_id: 'test-client' }
    ])
  })

  it('skips revocation when the provider has no revocation endpoint', async () => {
    const { revocations, events } = await signOut({
      config: oidcConfig({ revocation_endpoint: undefined })
    })

    expect(revocations).toEqual([])
    expect(events).toEqual([`open ${endSessionEndpoint}`])
  })

  it('skips revocation when the session has no refresh token', async () => {
    const { revocations, events } = await signOut({
      tokens: { accessToken: 'access-token' }
    })

    expect(revocations).toEqual([])
    expect(events).toEqual([`open ${endSessionEndpoint}`])
  })
})

describe('withoutRevocationRetry', () => {
  const revocationBody = 'token=t&token_type_hint=refresh_token'
  const tokenBody = 'grant_type=refresh_token&refresh_token=t'

  /** Send `body` through the wrapper to a `fetch` that fails with `error`. */
  const errorOf = (body: string, error: Error) =>
    withoutRevocationRetry(() => Promise.reject(error))('https://idp.example.com/endpoint', {
      method: 'POST',
      body
    }).catch((e: Error) => e)

  const abortError = () => new DOMException('This operation was aborted', 'AbortError')

  it('rejects an aborted revocation request with a TimeoutError', async () => {
    expect(await errorOf(revocationBody, abortError())).toMatchObject({ name: 'TimeoutError' })
  })

  it('keeps the AbortError of an aborted token request', async () => {
    expect(await errorOf(tokenBody, abortError())).toMatchObject({ name: 'AbortError' })
  })

  it('keeps other errors of a revocation request', async () => {
    const networkError = new TypeError('Failed to fetch')
    expect(await errorOf(revocationBody, networkError)).toBe(networkError)
  })
})

/** The tokens left in storage under any OIDC key, if any. */
function storedTokens(storage: Storage): unknown {
  return Object.entries(storage)
    .filter(([key]) => key.startsWith('oidc.'))
    .map(([, value]) => JSON.parse(value)?.tokens)
    .find(Boolean)
}
