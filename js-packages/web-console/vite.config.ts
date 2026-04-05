import path from 'node:path'
import { monaco } from '@bithero/monaco-editor-vite-plugin'
import svg from '@poppanator/sveltekit-svg'
import { sveltekit } from '@sveltejs/kit/vite'
import tailwindcss from '@tailwindcss/vite'
import { playwright } from '@vitest/browser-playwright'
import { type PluginOption } from 'vite'
import devtoolsJson from 'vite-plugin-devtools-json'
import virtual from 'vite-plugin-virtual'
import {
  defineConfig,
  type TestProjectInlineConfiguration,
  type ViteUserConfigExport
} from 'vitest/config'
import { felderaApiJsonSchemas } from './src/lib/functions/felderaApiJsonSchemas'
import { svelteCssVirtualModuleFallback } from './src/lib/vite-plugins/svelte-css-virtual-module-fallback'

const snapshotsDir = path.resolve(__dirname, 'playwright-snapshots')

// Deps that vitest browser tests need pre-bundled. With noDiscovery: true
// (below), only these explicit deps are pre-bundled during tests.
// If a new test imports a dep that causes "new dependencies optimized"
// warnings or test failures, add it here.
const testOptimizeDepsInclude = [
  '@axa-fr/oidc-client',
  '@monaco-editor/loader',
  '@skeletonlabs/skeleton-svelte',
  '@streamparser/json',
  '@svelte-bin/clipboard',
  '@square/svelte-store',
  'bignumber.js',
  'dayjs',
  'flowbite-svelte',
  'new-github-issue-url',
  'paneforge',
  'runed',
  'sort-on',
  'svelte-french-toast',
  'tiny-invariant',
  'but-unzip',
  'colorizr',
  'cookie-storage',
  'cytoscape-dblclick',
  'cytoscape-elk',
  'cytoscape',
  'd3-format',
  'dayjs/plugin/duration',
  'echarts/charts',
  'echarts/components',
  'echarts/core',
  'echarts/renderers',
  'fancy-ansi',
  'formsnap',
  'jwt-decode',
  'nprogress',
  'posthog-js',
  'strip-ansi',
  'svelte-attr',
  'svelte-echarts',
  'sveltekit-superforms',
  'sveltekit-superforms/adapters',
  'true-json-bigint',
  'ts-pattern',
  'valibot',
  'virtua/svelte'
]

const browserTestProject = ({
  name,
  include,
  exclude
}: {
  name: string
  include: string[]
  exclude?: string[]
}): TestProjectInlineConfiguration => ({
  extends: './vite.config.ts',
  test: {
    name,
    browser: {
      enabled: true,
      provider: playwright({ contextOptions: {} }),
      instances: [{ browser: 'chromium', headless: true }],
      expect: {
        toMatchScreenshot: {
          resolveScreenshotPath({ testFileName, arg, ext }) {
            return path.join(snapshotsDir, 'component', testFileName, `${arg}${ext}`)
          }
        }
      }
    },
    setupFiles: ['src/lib/vitest-browser-setup.ts'],
    include,
    exclude: exclude ?? ['src/lib/server/**']
  }
})

// TODO: remove Prettier
export default defineConfig(async () => {
  return {
    plugins: [
      // Address vite-pugin-svelte bug; see the implementation for details.
      svelteCssVirtualModuleFallback(),
      tailwindcss(),
      sveltekit(),
      svg(),
      virtual({
        'virtual:felderaApiJsonSchemas.json': JSON.stringify(felderaApiJsonSchemas),
        // The plugins module is loaded from the cloud repo via package name
        'virtual:feldera-triage-plugins': process.env.FELDERA_PLUGINS_MODULE
          ? `export { default, createBundle, TriageResults } from '${process.env.FELDERA_PLUGINS_MODULE}'`
          : `export default []; export async function createBundle() { return {} }; export class TriageResults { constructor() { this.results = [] } }`
      }),

      // '@bithero/monaco-editor-vite-plugin' is used to only bundle monaco-editor features that are actually used to reduce the total bundle size
      monaco({
        // Only include languages that are actually used
        languages: ['json', 'sql', 'rust', 'graphql'],
        // Only include features that are used
        features: [
          'browser',
          'clipboard',
          'comment',
          'find',
          'folding',
          'format',
          'gotoLine',
          'gotoSymbol',
          'hover',
          'inPlaceReplace',
          'inspectTokens',
          'iPadShowKeyboard',
          'linesOperations',
          'links',
          'multicursor',
          'parameterHints',
          'quickOutline',
          'smartSelect',
          'suggest',
          'wordHighlighter',
          'wordOperations'
        ]
      }),
      devtoolsJson()
    ] as PluginOption[],
    build: { minify: false },
    optimizeDeps: {
      // Rolldown's dep pre-bundling breaks svelte/internal/client — its @__PURE__
      // inlining can reorder get_first_child() ahead of init_operations(),
      // causing "Cannot read properties of undefined (reading 'call')".
      exclude: ['svelte'],
      // During vitest: the dep scan fails on svelte component virtual-module
      // exports, aborting ALL pre-bundling. entries:[] skips the failing scan;
      // noDiscovery prevents runtime discovery that triggers flaky mid-test
      // reloads ("Vite unexpectedly reloaded a test"). All deps used by tests
      // must be listed in testOptimizeDepsInclude explicitly.
      // During vite dev: leave defaults so CJS packages get auto-discovered.
      ...(process.env.VITEST
        ? { entries: [], noDiscovery: true, include: testOptimizeDepsInclude }
        : {})
    },
    resolve: {
      // When support-bundle-triage is symlinked into node_modules, vite dereferences
      // the symlink and resolves its imports from the real path outside this workspace.
      // dedupe forces these packages to always resolve from this workspace root.
      dedupe: ['profiler-lib', 'triage-types', 'but-unzip']
    },
    server: {
      watch: {
        // Bun hoists deps to repo-root node_modules/.bun, causing vite to
        // resolve paths outside web-console and watch the entire monorepo,
        // exhausting the inotify limit (65 536 default, half used by VS Code).
        // Exclude everything outside js-packages/ to keep watches minimal.
        ignored: [
          // The repo-root node_modules/.bun has 10K+ dirs — must be excluded
          // both by glob and absolute path (chokidar resolves some paths before
          // glob matching, so the glob alone may not catch the root node_modules).
          '**/node_modules/**',
          path.resolve(__dirname, '../../node_modules') + '/**',
          '**/dist/**',
          '**/.svelte-kit/**',
          '**/build/**',
          '**/playwright-snapshots/**',
          '**/crates/**',
          '**/python/**',
          '**/sql-to-dbsp-compiler/**',
          '**/deploy/**',
          '**/benchmark/**',
          '**/docs.feldera.com/**',
          '**/scripts/**',
          '**/.github/**',
          '**/target/**'
        ]
        //Polling can be used instead of system watch to avoid hitting ENOSPC inotify error of watchers limit
        // usePolling: true,
        // interval: 2000,
      }
    },
    test: {
      expect: { requireAssertions: true },
      watch: false,
      resolveSnapshotPath(testPath, snapExtension) {
        // Svelte component tests (client project) → playwright-snapshots/component/
        if (/\.svelte\.(test|spec)\.[jt]s$/.test(testPath)) {
          const rel = path.relative(__dirname, testPath)
          return path.join(snapshotsDir, 'component', rel + snapExtension)
        }
        // Server/node tests → default __snapshots__/ location
        return path.join(
          path.dirname(testPath),
          '__snapshots__',
          path.basename(testPath) + snapExtension
        )
      },
      projects: [
        // Unit tests: *.spec.ts (run with `bun run test`)
        browserTestProject({ name: 'client', include: ['src/**/*.svelte.spec.{js,ts}'] }),

        {
          extends: './vite.config.ts',
          test: {
            name: 'server',
            environment: 'node',
            include: ['src/**/*.spec.{js,ts}'],
            exclude: ['src/**/*.svelte.spec.{js,ts}']
          }
        },

        // Integration tests: *.test.ts (require a Feldera instance, run with `bun run test-integration`)
        {
          extends: './vite.config.ts',
          test: {
            name: 'integration',
            environment: 'node',
            globalSetup: ['src/lib/vitest-integration-setup.ts'],
            include: ['src/**/*.test.{js,ts}'],
            exclude: ['src/**/*.svelte.test.{js,ts}']
          }
        },

        browserTestProject({
          name: 'integration-client',
          include: ['src/**/*.svelte.test.{js,ts}']
        })
      ]
    }
  } satisfies ViteUserConfigExport
})
