/**
 * Playwright global setup: warm the Rust compilation cache.
 *
 * The first SQL compilation in a fresh Feldera OSS image compiles many Rust
 * crates from scratch, which can take several minutes. Running this once before
 * the test suite keeps individual test timeouts tight.
 */

import { configureTestClient, warmCompilationCache } from '$lib/services/testPipelineHelpers'

export default async function globalSetup() {
  configureTestClient()
  await warmCompilationCache()
}
