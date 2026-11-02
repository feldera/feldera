import { expect, type Page, test } from '@playwright/test'
import { loginAs } from './login'

// Regression test for INT-30989: Sign Out must revoke the refresh token at the
// provider. Before the fix the console only cleared its own storage, and a copy
// of the refresh token kept minting access tokens after sign-out.

/** The refresh token the OIDC client keeps in `sessionStorage`. */
const storedRefreshToken = async (page: Page) => {
  const token = await page.evaluate(() => {
    const raw = sessionStorage.getItem('oidc.default')
    const stored = raw ? (JSON.parse(raw) as { tokens?: { refreshToken?: string } }) : null
    return stored?.tokens?.refreshToken
  })
  if (!token) {
    throw new Error('no refresh token in sessionStorage after login')
  }
  return token
}

const signOut = async (page: Page) => {
  await page.getByLabel('Open settings popup').click()
  await page.getByRole('button', { name: 'Sign Out' }).click()
}

test.describe('Sign Out', () => {
  test('revokes the refresh token at the provider', async ({ page, request }) => {
    await loginAs(page, 'writer')
    const refreshToken = await storedRefreshToken(page)

    const revokePromise = page.waitForRequest(
      (req) => req.method() === 'POST' && /\/revoke$/.test(req.url())
    )
    await signOut(page)
    const revoke = await revokePromise
    const params = new URLSearchParams(revoke.postData() ?? '')
    expect(params.get('token')).toBe(refreshToken)
    expect(params.get('token_type_hint')).toBe('refresh_token')

    // The provider has processed the revocation once the console is back on
    // its login page. A replay of the refresh token must fail from then on.
    await page.waitForURL((url) => url.pathname === '/authorize' || url.pathname === '/')
    const tokenEndpoint = new URL('/token', revoke.url()).toString()
    const replay = await request.post(tokenEndpoint, {
      form: { grant_type: 'refresh_token', refresh_token: refreshToken }
    })
    expect(replay.status()).toBe(400)
    expect(await replay.json()).toMatchObject({ error: 'invalid_grant' })
  })
})
