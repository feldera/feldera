import { playwright } from '@vitest/browser-playwright'
import { defineConfig } from 'vitest/config'

// Real-Chromium browser project, shaped like web-console's `browserTestProject`. Tests import
// from `profiler-lib` and `monaco-editor`, both of which touch real browser APIs at import
// time (cytoscape; Monaco's clipboard / DOM probes), so they run in a real browser via
// @vitest/browser + playwright.
const browserTestProject = (name: string, include: string[]) => ({
  // extends the app Vite config so the Svelte + Tailwind plugins compile component imports.
  extends: './vite.config.ts',
  test: {
    name,
    include,
    setupFiles: ['./test/setup.ts'],
    browser: {
      enabled: true,
      provider: playwright({ contextOptions: {} }),
      headless: true,
      instances: [{ browser: 'chromium' as const }]
    }
  }
})

// Naming split mirrors web-console:
//   *.test.ts        existing function / theme suites (browser)
//   *.svelte.spec.ts Svelte component unit tests (browser)
//   *.spec.ts        pure unit tests that need no browser (node)
export default defineConfig({
  test: {
    // The `unit` project has no files yet; don't fail the run until one lands.
    passWithNoTests: true,
    projects: [
      browserTestProject('browser', ['src/**/*.test.ts']),
      browserTestProject('component', ['src/**/*.svelte.spec.ts']),
      {
        test: {
          name: 'unit',
          environment: 'node',
          include: ['src/**/*.spec.ts'],
          exclude: ['src/**/*.svelte.spec.ts']
        }
      }
    ]
  }
})
