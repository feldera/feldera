import type { PlaywrightTestConfig } from '@playwright/test'

// Captures the screenshots for docs.feldera.com. See screenshots/README.md.

// With DOCS_SCREENSHOTS_BUILD=1, the app is built from this tree and served on port 4173, and it
// sends its API calls to localhost:8080 (see felderaEndpoint.ts). Otherwise the shots load the app
// that the pipeline manager serves.
const build = process.env.DOCS_SCREENSHOTS_BUILD === '1'

const config: PlaywrightTestConfig = {
  testDir: 'screenshots',
  outputDir: 'test-results-screenshots',
  projects: [
    // Needs no pipeline manager. The PR workflow runs only this project.
    { name: 'profile-viewer', testMatch: /(?<!\.web-console)\.shot\.ts$/ },
    // Needs a pipeline manager.
    { name: 'web-console', testMatch: /\.web-console\.shot\.ts$/ }
  ],
  webServer: build
    ? {
        command: 'bun run build && bun run preview -- --port 4173 --strictPort',
        port: 4173,
        timeout: 300_000
      }
    : undefined,
  workers: 1,
  // The UI shows a change in 0.2 to 0.5 seconds. So short limits make a broken shot fail fast.
  // Slower steps, such as loading the bundle, set their own limits.
  timeout: 15_000,
  use: {
    actionTimeout: 500,
    navigationTimeout: 5_000,
    baseURL: build
      ? 'http://localhost:4173'
      : (process.env.PLAYWRIGHT_APP_ORIGIN ?? 'http://localhost:8080'),
    viewport: { width: 1280, height: 1000 },
    deviceScaleFactor: 2,
    colorScheme: 'light',
    locale: 'en-US',
    timezoneId: 'UTC'
  },
  // `toHaveScreenshot(['operations', 'x.png'])` reads and writes docs.feldera.com/docs/operations/x.png.
  snapshotPathTemplate: '{testDir}/../../../docs.feldera.com/docs/{arg}{ext}',
  expect: {
    timeout: 500,
    toHaveScreenshot: {
      // A shot is rewritten only when more than this share of its pixels changed.
      // This ignores font antialiasing noise between machines.
      maxDiffPixelRatio: 0.005,
      scale: 'device',
      animations: 'disabled',
      caret: 'hide'
    }
  }
}

export default config
