/**
 * Vitest globalSetup for integration test projects.
 *
 * Configures the API client and warms the Rust compilation cache,
 * mirroring what tests/global-setup.ts does for Playwright.
 */

import { configureTestClient, warmCompilationCache } from '$lib/services/testPipelineHelpers'

export async function setup() {
  configureTestClient()
  await warmCompilationCache()
}
