import type { PlaywrightTestConfig } from '@playwright/test'
import baseConfig from './playwright.config'

/**
 * e2e tests against a pipeline-manager that authenticates through an OIDC
 * provider. CI serves the provider with `scripts/dummy_oidc.py`, whose login
 * page grants a chosen identity on click (see `tests/auth/login.ts`).
 */
const config: PlaywrightTestConfig = {
  ...baseConfig,
  // The base setup warms the compilation cache through the API without a
  // token, which an authenticated manager refuses. These tests compile nothing.
  globalSetup: undefined,
  testDir: 'tests/auth',
  testIgnore: undefined
}

export default config
