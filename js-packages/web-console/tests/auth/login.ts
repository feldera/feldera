import type { Page } from '@playwright/test'

/** Identities on the login page of `scripts/dummy_oidc.py`. */
export type DummyOidcIdentity = 'reader' | 'writer' | 'admin' | 'owner'

/**
 * Sign in through the console's real login flow: the console redirects to the
 * provider, the dummy provider's login page grants `identity` on click, and the
 * callback route exchanges the code and navigates back into the console.
 */
export async function loginAs(page: Page, identity: DummyOidcIdentity) {
  const response = await page.goto('/')
  if (!response) {
    throw new Error('Opening the console did not start a navigation')
  }
  const consoleOrigin = new URL(response.url()).origin

  await page.getByRole('link', { name: new RegExp(`^${identity}\\b`, 'i') }).click()
  await page.waitForURL(
    (url) => url.origin === consoleOrigin && !url.pathname.includes('/auth/callback')
  )
}
