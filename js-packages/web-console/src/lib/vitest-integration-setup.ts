/**
 * Vitest setupFile for the `integration` test project.
 *
 * Configures the API client and warms the Rust compilation cache once per
 * worker, mirroring what tests/global-setup.ts does for Playwright.
 *
 * Each `integration` test file also calls `configureTestClient()` itself, so
 * only the compilation-cache warmup needs the once-per-worker guard here.
 */

import { beforeAll } from 'vitest'
import { configureTestClient, warmCompilationCache } from '#lib/services/testPipelineHelpers.ts'

declare global {
  // eslint-disable-next-line no-var
  var __feldera_warmup: Promise<void> | undefined
}

configureTestClient()
// A hook, so that `vitest list` loads the test files
// without a Feldera instance. The timeout exceeds the 10 minutes that
// `waitForCompilation` waits.
beforeAll(async () => {
  globalThis.__feldera_warmup ??= warmCompilationCache()
  await globalThis.__feldera_warmup
}, 11 * 60_000)
