import { defineConfig } from 'vitest/config'

// Helpers under `src/lib/functions` and `src/lib/components/metrics` import from
// `profiler-lib`, whose main entry pulls in cytoscape modules that touch `window` at
// import time. Using the jsdom environment gives those modules a DOM globals shim so
// unit tests can import them without spinning up a real browser.
export default defineConfig({
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.ts']
  }
})
