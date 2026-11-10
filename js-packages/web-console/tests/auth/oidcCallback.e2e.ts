import { expect, test } from '@playwright/test'
import { loginAs } from './login'

// The analytics scripts send `document.referrer` to third parties, so it must
// not hold the OIDC `code` and `state` of the callback URL.
test('the page after login gets only the console origin as its referrer', async ({
  page,
  baseURL
}) => {
  await loginAs(page, 'reader')

  const referrer = await page.evaluate(() => document.referrer)

  expect(referrer).toBe(`${new URL(baseURL!).origin}/`)
})
