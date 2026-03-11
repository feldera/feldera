import { monaco } from '@bithero/monaco-editor-vite-plugin'
import svg from '@poppanator/sveltekit-svg'
import { sveltekit } from '@sveltejs/kit/vite'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig, type PluginOption, type UserConfig } from 'vite'
import devtoolsJson from 'vite-plugin-devtools-json'
import virtual from 'vite-plugin-virtual'
import { felderaApiJsonSchemas } from './src/lib/functions/felderaApiJsonSchemas'

// TODO: remove Prettier

export default defineConfig(async () => {
  return {
    plugins: [
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
    build: {
      minify: false
    },
    resolve: {
      // When support-bundle-triage is symlinked into node_modules, vite dereferences
      // the symlink and resolves its imports from the real path outside this workspace.
      // dedupe forces these packages to always resolve from this workspace root.
      dedupe: ['profiler-lib', 'triage-types', 'but-unzip']
    },
    server: {
      watch: {
        ignored: ['**/dist/**', '**/node_modules/**', '**/.svelte-kit/**', '**/build/**']
      }
    }
  } satisfies UserConfig
})
