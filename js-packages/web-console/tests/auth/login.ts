import { type Page, test } from '@playwright/test'

/** Identities on the login page of `scripts/dummy_oidc.py`. */
export type DummyOidcIdentity = 'reader' | 'writer' | 'admin' | 'owner'

/**
 * Sign in through the console's real login flow: the console redirects to the
 * provider, the dummy provider's login page grants `identity` on click, and the
 * callback route exchanges the code and navigates back into the console.
 */
export async function loginAs(page: Page, identity: DummyOidcIdentity) {
  const baseURL = test.info().project.use.baseURL
  if (!baseURL) {
    throw new Error('loginAs needs a baseURL to tell the console from the provider')
  }
  const consoleOrigin = new URL(baseURL).origin
  await page.goto('/')

  await page.getByRole('link', { name: new RegExp(`^${identity}\\b`, 'i') }).click()
  await page.waitForURL(
    (url) => url.origin === consoleOrigin && !url.pathname.includes('/auth/callback')
  )
}
