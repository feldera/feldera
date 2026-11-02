import { OidcClient, type OidcLocation } from '@axa-fr/oidc-client'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { toAxaOidcConfig } from '#lib/compositions/@axa-fr/auth.ts'
import type { OidcConfig } from '#lib/compositions/auth.ts'

const idp = 'https://idp.example.com/'
const revocationEndpoint = `${idp}oauth2/revoke`
const endSessionEndpoint = `${idp}logout`

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
 * sign out. `respondToRevocation` answers the revocation request.
 */
async function signOut(options: {
  config?: OidcConfig
  tokens?: { accessToken: string; refreshToken?: string }
  respondToRevocation?: () => Promise<Response>
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
      return respondToRevocation()
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

  const client = OidcClient.getOrCreate(() => fetchMock as typeof fetch, location)(
    toAxaOidcConfig(config),
    configurationName
  )
  await client.tryKeepExistingSessionAsync()
  expect(client.tokens?.refreshToken, 'stored session was not restored').toBe(
    options.tokens ? options.tokens.refreshToken : 'refresh-token'
  )
  await client.logoutAsync('/')
  return { ...network, storage: config.storage! }
}

describe('sign-out', () => {
  afterEach(() => {
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

/** The tokens left in storage under any OIDC key, if any. */
function storedTokens(storage: Storage): unknown {
  return Object.entries(storage)
    .filter(([key]) => key.startsWith('oidc.'))
    .map(([, value]) => JSON.parse(value)?.tokens)
    .find(Boolean)
}
