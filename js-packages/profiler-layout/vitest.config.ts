import { playwright } from '@vitest/browser-playwright'
import { defineConfig } from 'vitest/config'

// Tests import from `profiler-lib` and `monaco-editor`, both of which touch real browser
// APIs at import time (cytoscape; Monaco's clipboard / DOM probes). Run them in a real
// Chromium via @vitest/browser + playwright so we exercise actual browser semantics — that
// also lets the SQL view's theme test read live computed styles when needed.
export default defineConfig({
  test: {
    include: ['src/**/*.test.ts'],
    setupFiles: ['./test/setup.ts'],
    browser: {
      enabled: true,
      provider: playwright(),
      headless: true,
      instances: [{ browser: 'chromium' }]
    }
  }
})
