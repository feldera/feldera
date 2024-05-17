import { resolve } from 'path'
import { FileSystemIconLoader } from 'unplugin-icons/loaders'
import UnpluginIcons from 'unplugin-icons/vite'

import { defineConfig, devices } from '@playwright/experimental-ct-react'
import react from '@vitejs/plugin-react'

/**
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig({
  testDir: './src/lib/components',
  /* The base directory, relative to the config file, for snapshot files created with toMatchSnapshot and toHaveScreenshot. */
  snapshotDir: './playwright-snapshots/ct',
  /* Maximum time one test can run for. */
  timeout: 5 * 1000,
  /* Run tests in files in parallel */
  fullyParallel: true,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  /* Retry on CI only */
  retries: process.env.CI ? 0 : 0,
  /* Opt out of parallel tests on CI. */
  workers: process.env.CI ? 1 : undefined,
  /* Reporter to use. See https://playwright.dev/docs/test-reporters */
  reporter: 'html',
  /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
  use: {
    /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
    trace: 'on-first-retry',

    /* Port to use for Playwright component endpoint. */
    ctPort: 3100,
    ctViteConfig: {
      resolve: {
        alias: {
          '$lib': resolve(__dirname, './src/lib'),
          '$tests': resolve(__dirname, './tests'),
          'src/lib': resolve(__dirname, './src/lib'),
        },
      },
      plugins: [
        react(),
        UnpluginIcons({
          compiler: 'jsx', jsx: 'react',
          customCollections: {
            'vendors': FileSystemIconLoader(
              "./public/icons/vendors",
            ),
            'generic': FileSystemIconLoader(
              "./public/icons/generic",
            ),
          }
         }),
      ],
    },
    testIdAttribute: 'data-testid',
  },

  /* Configure projects for major browsers */
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
  ],
});