import { sveltekit } from '@sveltejs/kit/vite'
import tailwindcss from '@tailwindcss/vite'
import { playwright } from '@vitest/browser-playwright'
import { defineConfig } from 'vitest/config'

// Cap test workers in CI, where vitest's autodetection sees every host core and oversubscribes the
// pod, starving browser tests into per-test timeouts.
const testMaxWorkers = process.env.VITEST_MAX_WORKERS
  ? Math.max(1, Number(process.env.VITEST_MAX_WORKERS))
  : undefined

export default defineConfig({
  plugins: [tailwindcss(), sveltekit()],
  optimizeDeps: {
    // Rolldown's dep pre-bundling breaks svelte/internal/client — its @__PURE__ inlining can
    // reorder get_first_child() ahead of init_operations(), causing "Cannot read properties of
    // undefined (reading 'call')".
    exclude: ['svelte'],
    ...(process.env.VITEST
      ? // The dep scan fails on svelte component virtual-module exports, aborting all
        // pre-bundling; entries:[] skips it. noDiscovery then prevents the runtime discovery that
        // reloads a page mid-test, so anything the suites import has to be listed here.
        { entries: [], noDiscovery: true, include: ['fancy-ansi', 'fancy-ansi > escape-html', 'strip-ansi', 'virtua/svelte'] }
      : {})
  },
  test: {
    // These suites assert on geometry, and a spec that measures nothing passes silently.
    expect: { requireAssertions: true },
    watch: false,
    maxWorkers: testMaxWorkers,
    browser: {
      enabled: true,
      provider: playwright({ contextOptions: {} }),
      instances: [{ browser: 'chromium', headless: true }]
    },
    setupFiles: ['tests/setup.ts'],
    include: ['tests/**/*.svelte.spec.{js,ts}']
  }
})
