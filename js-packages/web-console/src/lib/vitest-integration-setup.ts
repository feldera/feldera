/**
 * Vitest setupFile for the `integration` test project.
 *
 * Configures the API client and warms the Rust compilation cache once per
 * worker, mirroring what tests/global-setup.ts does for Playwright.
 *
 * Each `integration` test file also calls `configureTestClient()` itself, so
 * only the compilation-cache warmup needs the once-per-worker guard here.
 */

import { configureTestClient, warmCompilationCache } from '$lib/services/testPipelineHelpers'

declare global {
  // eslint-disable-next-line no-var
  var __feldera_warmup: Promise<void> | undefined
}

configureTestClient()
globalThis.__feldera_warmup ??= warmCompilationCache()
await globalThis.__feldera_warmup
