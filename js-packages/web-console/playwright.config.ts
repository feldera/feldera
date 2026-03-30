import type { PlaywrightTestConfig } from '@playwright/test'

const appOrigin = process.env.PLAYWRIGHT_APP_ORIGIN

const config: PlaywrightTestConfig = {
  // The globalSetup only runs when PLAYWRIGHT_APP_ORIGIN is set (i.e., dedicated Feldera instance), not during the local build+preview mode.
  globalSetup: appOrigin ? './tests/global-setup.ts' : undefined,
  ...(appOrigin
    ? { use: { baseURL: appOrigin } }
    : {
        webServer: {
          command: 'npm run build && npm run preview',
          port: 4173
        }
      }),
  testDir: 'tests',
  testMatch: /(.+\.)?e2e\.[jt]s/,
  snapshotDir: 'playwright-snapshots/e2e',
  expect: {
    toHaveScreenshot: {
      // Threshold for pixel-level comparison (0 = exact, 1 = any)
      maxDiffPixelRatio: 0.01
    }
  }
}

export default config
