import { expect, type Page, test } from '@playwright/test'
import { loginAs } from './login'

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
  await page.getByTestId('btn-sign-out').click()
}

test.describe('Sign Out', () => {
  test('revokes the refresh token at the provider', async ({ page, request }) => {
    await loginAs(page, 'writer')
    const refreshToken = await storedRefreshToken(page)

    // Wait for the response, not the request: the provider must finish the
    // revocation before the replay below reaches it.
    const revokeResponse = page.waitForResponse(
      (res) => res.request().method() === 'POST' && /\/revoke$/.test(res.url())
    )
    await signOut(page)
    const revoke = await revokeResponse
    expect(revoke.status()).toBe(200)
    const params = new URLSearchParams(revoke.request().postData() ?? '')
    expect(params.get('token')).toBe(refreshToken)
    expect(params.get('token_type_hint')).toBe('refresh_token')

    const tokenEndpoint = new URL('/token', revoke.url()).toString()
    const replay = await request.post(tokenEndpoint, {
      form: { grant_type: 'refresh_token', refresh_token: refreshToken }
    })
    expect(replay.status()).toBe(400)
    expect(await replay.json()).toMatchObject({ error: 'invalid_grant' })
  })
})
