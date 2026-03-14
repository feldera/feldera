import type { PlaywrightTestConfig } from '@playwright/test'

const config: PlaywrightTestConfig = {
  webServer: {
    command: 'npm run build && npm run preview',
    port: 4173
  },
  testDir: 'tests',
  testMatch: /(.+\.)?(test|spec)\.[jt]s/,
  snapshotDir: 'playwright-snapshots/e2e',
  expect: {
    toHaveScreenshot: {
      // Threshold for pixel-level comparison (0 = exact, 1 = any)
      maxDiffPixelRatio: 0.01
    }
  }
}

export default config
