import { expect, test } from '@playwright/test'
import { loginAs } from './login'

test.describe('Home page demos on an authenticated instance', () => {
  // The home page renders the first 9 tiles at 1280px and wider, 5 below that.
  // "Feldera Basics" is the 7th demo, so the desktop layout is what shows it.
  test.use({ viewport: { width: 1440, height: 900 } })

  test('a signed-in user sees the Feldera Basics tutorial', async ({ page }) => {
    await loginAs(page, 'admin')

    // `/v0/config/demos` requires a bearer token, so this fails whenever the
    // console fetches the demo list before its auth interceptor is in place.
    await expect(page.getByText('Explore use cases and tutorials')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Feldera Basics', exact: true })).toBeVisible()
  })
})
