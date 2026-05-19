import path from 'node:path'
import { monaco } from '@feldera/vite-plugin-monaco-editor'
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
  'apache-arrow',
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
  // Transitive deps. Bun's flat layout doesn't hoist these into
  // web-console/node_modules, so we give vite the `parent > child` path
  // so it can resolve them via the owning package.
  '@square/svelte-store > cookie-storage',
  'profiler-lib > cytoscape',
  'profiler-lib > cytoscape-dblclick',
  'profiler-lib > cytoscape-elk',
  'd3-format',
  'dayjs/plugin/duration',
  'echarts/charts',
  'echarts/components',
  'echarts/core',
  'echarts/renderers',
  'common-ui > fancy-ansi',
  'common-ui > fancy-ansi > escape-html',
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
  'virtua/svelte',
  // worker-timers and its transitive fast-unique-numbers ship UMD bundles
  // via their `browser` package.json field. Without pre-bundling, Vite
  // serves the raw UMD file and ESM named imports (generateUniqueNumber, …)
  // fail. Pre-bundling rewraps them into ESM with real named exports.
  'worker-timers',
  'worker-timers > fast-unique-numbers'
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

// Optional injection point for closed-source triage plugins. When set, must be
// an absolute filesystem path to a module exporting { default, createBundle,
// TriageResults }. Resolved via a `resolve.alias` entry below so the OSS tree's
// node_modules is never touched. When unset, the virtual module emits a stub
// and the alias is omitted entirely.
const pluginsEntry = process.env.FELDERA_PLUGINS_ENTRY

// TODO: remove Prettier
export default defineConfig(async () => {
  return {
    plugins: [
      // Address vite-pugin-svelte bug; see the implementation for details.
      svelteCssVirtualModuleFallback(),
      tailwindcss(),
      sveltekit(),
      svg(),
      monaco({
        languages: ['json', 'sql', 'rust', 'graphql'],
        features: [
          'browser',
          'clipboard',
          'comment',
          'contextmenu',
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
      virtual({
        'virtual:felderaApiJsonSchemas.json': JSON.stringify(felderaApiJsonSchemas),
        'virtual:feldera-triage-plugins': pluginsEntry
          ? `export { default, createBundle, TriageResults } from 'feldera-triage-plugins-impl'`
          : `export default []; export async function createBundle() { return {} }; export class TriageResults { constructor() { this.results = [] } }`
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
      // During vite dev we rely on auto-discovery to pre-bundle
      // svelte libs that ship raw .svelte files (e.g. @vincjo/datatables) —
      // without it, vite-plugin-svelte's "failed to load virtual css module"
      // bug starts firing for every unbundled .svelte file. The
      // svelteCssVirtualModuleFallback plugin catches the downstream damage
      // either way.
      ...(process.env.VITEST
        ? { entries: [], noDiscovery: true, include: testOptimizeDepsInclude }
        : {})
    },
    resolve: {
      // The plugins module (when present) lives outside this workspace and
      // re-imports profiler-lib/triage-types/but-unzip. dedupe forces those to
      // resolve from web-console's node_modules so we don't end up with two
      // copies in the bundle.
      dedupe: ['profiler-lib', 'triage-types', 'but-unzip'],
      alias: pluginsEntry
        ? [{ find: 'feldera-triage-plugins-impl', replacement: pluginsEntry }]
        : []
    },
    server: {
      // When pluginsEntry is set, it points outside this workspace. Allow
      // vite-dev to read from its directory; build mode is unaffected.
      fs: pluginsEntry ? { allow: ['..', path.dirname(pluginsEntry)] } : undefined,
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
        // Unit tests: *.spec.ts (run with `bun run test-unit`)
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
