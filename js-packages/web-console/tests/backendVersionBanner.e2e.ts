import { expect, test } from '@playwright/test'
import { configureTestClient } from '$lib/services/testPipelineHelpers'

configureTestClient()

test.describe('Backend version banner', () => {
  test.setTimeout(60_000)

  test('polling refreshes feldera_config_cache when backend version changes', async ({ page }) => {
    // Intercept /v0/config and overwrite version/revision. Direct localStorage
    // poisoning does not work — `lazyUpdateConfig()` is scheduled ~2s after
    // any warm-cache load() and calls `fetchConfigs()`, which overwrites the
    // cache from the real backend. Mocking the response means every cache
    // write — boot, lazyUpdate, and polling — sees the version we control.
    const OLD_VERSION = '99.99.0-banner-e2e-old'
    const NEW_VERSION = '99.99.1-banner-e2e-new'
    let currentVersion = OLD_VERSION

    const configRequests: { url: string; status: number; intercepted: boolean }[] = []

    let configIntercepts = 0
    await page.route(/\/v0\/config(?:\?|$)/, async (route) => {
      configIntercepts++
      const url = route.request().url()
      try {
        const response = await route.fetch()
        const body = await response.json()
        body.version = currentVersion
        body.revision = `rev-${currentVersion}`
        // Strip headers that don't survive body rewriting: Content-Encoding
        // (the original may be gzip; our new body is plain) and Content-Length
        // (now wrong). Leaving them in causes the browser to fail decompression
        // or truncate the response, which surfaces as an empty `data.feldera`
        // and silently suppresses the banner.
        const headers = { ...response.headers() }
        delete headers['content-encoding']
        delete headers['content-length']
        configRequests.push({ url, status: response.status(), intercepted: true })
        console.log('sending value', body.version)
        await route.fulfill({
          status: response.status(),
          headers,
          body: JSON.stringify(body)
        })
      } catch (e) {
        configRequests.push({ url, status: -1, intercepted: false })
        await route.abort()
      }
    })

    // Cold-cache boot: load() calls fetchConfigs() → intercepted → writes
    // OLD into feldera_config_cache and into data.feldera.version.
    await page.goto('/demos')
    await page.waitForFunction(() => !!localStorage.getItem('feldera_config_cache'), null, {
      timeout: 3_000
    })

    // Sanity check: the boot fetch wrote OLD. If this is wrong the route
    // mock isn't intercepting, and every later assertion is a false positive.
    const bootCachedVersion = await page.evaluate(
      () => JSON.parse(localStorage.getItem('feldera_config_cache')!).version
    )
    expect(bootCachedVersion).toBe(OLD_VERSION)

    // Flip the live response. The next 10s polling tick must call
    // `fetchConfigs()` and rewrite the cache to NEW — that's the fix's whole
    // mechanism. The buggy code path called bare `getConfig()`, which never
    // wrote the cache, so `feldera_config_cache.version` would stay at OLD
    // forever and `data.feldera.version` would keep mismatching the live
    // response, re-firing `invalidateAll()` every 10s.
    currentVersion = NEW_VERSION
    const tFlip = Date.now()

    await page.waitForFunction(
      (newVer) => {
        const raw = localStorage.getItem('feldera_config_cache')
        return raw ? JSON.parse(raw).version === newVer : false
      },
      NEW_VERSION,
      { timeout: 12_000 }
    )
    console.log(`[banner] cache updated to NEW after ${Date.now() - tFlip}ms`)
  })
})
