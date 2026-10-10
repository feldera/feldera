import { expect, type Page, type Route, test } from '@playwright/test'
import { loginAs } from './login'

const CALLS_KEY = 'e2e_conceptualhq_calls'
const SESSION_URL = /\/v0\/config\/session(?:\?|$)/

// Replaces the ConceptualHQ loader. It records each `ca(...)` call in
// localStorage, so the calls survive page loads.
const FAKE_LOADER = `(() => {
  const record = (args) => {
    const calls = JSON.parse(localStorage.getItem('${CALLS_KEY}') ?? '[]')
    calls.push(args)
    localStorage.setItem('${CALLS_KEY}', JSON.stringify(calls))
  }
  const queue = window.ca?.q ?? []
  window.ca = Object.assign((...args) => record(args), { getDeviceId: () => 'e2e-device' })
  queue.forEach(record)
})()`

const trackedEvents = (page: Page) =>
  page.evaluate(
    (key) =>
      (JSON.parse(localStorage.getItem(key) ?? '[]') as unknown[][])
        .filter((call) => call[0] === 'track')
        .map((call) => call.slice(1)),
    CALLS_KEY
  )

// The console sends the host that the test opens.
const signup = (email: string) => {
  const host = new URL(test.info().project.use.baseURL!).hostname
  return ['signup', { host, dedupe_id: `signup:${host}:${email}` }]
}

type Body = Record<string, unknown>

const fulfillPatched = async (route: Route, patch: Body | ((body: Body) => Body)) => {
  const response = await route.fetch()
  const body: Body = await response.json()
  const headers = { ...response.headers() }
  // The patched body is not compressed and has a new length.
  delete headers['content-encoding']
  delete headers['content-length']
  await route.fulfill({
    response,
    headers,
    body: JSON.stringify({ ...body, ...(typeof patch === 'function' ? patch(body) : patch) })
  })
}

// The manager has no ConceptualHQ key, so give one to the console.
const enableConceptualHq = async (page: Page) => {
  await page.route(/\/v0\/config(?:\?|$)/, (route) =>
    fulfillPatched(route, { conceptualhq: 'e2e-key' })
  )
  await page.route('https://oqiset.feldera.com/analytics/loader-v1.js*', (route) =>
    route.fulfill({ contentType: 'text/javascript', body: FAKE_LOADER })
  )
}

// Signs in as `writer`, and the browser caches a session with a recent
// `user_created_at`. The manager can have created `writer` long ago, so the
// test sets the time to now. The manager must still send the field.
const loginNewUser = async (page: Page) => {
  await page.route(SESSION_URL, (route) =>
    fulfillPatched(route, (body) => ({
      user_created_at: body.user_created_at && new Date().toISOString()
    }))
  )
  await loginAs(page, 'writer')
  await expect.poll(() => trackedEvents(page)).toEqual([signup('writer@example.com'), ['signin']])
}

// Ends the OIDC session but keeps localStorage, as when the user closes the tab.
// The console logout is not used, because it clears the config caches. The
// recorded calls are cleared, so the checks see only the calls for `reader`.
const switchToReader = async (page: Page) => {
  await page.evaluate((key) => {
    sessionStorage.clear()
    localStorage.removeItem(key)
  }, CALLS_KEY)
  await loginAs(page, 'reader')
}

test.describe('ConceptualHQ on an authenticated instance', () => {
  test('a new user sends signin and signup once', async ({ page }) => {
    await enableConceptualHq(page)
    await loginNewUser(page)

    // The reload renders from the cached config and refreshes it in the
    // background. The console reports the login after that refresh.
    const refreshed = page.waitForResponse(SESSION_URL)
    await page.reload()
    await refreshed
    await page.waitForTimeout(1_000)
    expect(await trackedEvents(page)).toEqual([signup('writer@example.com'), ['signin']])
  })

  test('the cached session of the previous user is not a signup', async ({ page }) => {
    await enableConceptualHq(page)
    await loginNewUser(page)

    // Make `reader` an old user. Then only the cached session of `writer` has a
    // recent `user_created_at`. This route takes precedence over the one for `writer`.
    await page.route(SESSION_URL, (route) =>
      fulfillPatched(route, { user_created_at: '2020-01-01T00:00:00Z' })
    )
    const refreshed = page.waitForResponse(SESSION_URL)
    await switchToReader(page)
    await refreshed

    // `reportLogin` sends `signup` before `signin`, so this check also
    // covers the signup.
    await expect.poll(() => trackedEvents(page)).toEqual([['signin']])
  })

  test('a failed session refresh is not a signup', async ({ page }) => {
    await enableConceptualHq(page)
    await loginNewUser(page)

    // The warm path renders from the cached session of `writer`. The background
    // refresh fails, so no session of `reader` is known.
    await page.route(SESSION_URL, (route) => route.abort())
    await switchToReader(page)

    // The background refresh starts 2 s after the login. `reportLogin` sends
    // `signup` before `signin`, so this check also covers the signup.
    await expect.poll(() => trackedEvents(page), { timeout: 15_000 }).toEqual([['signin']])
  })
})
