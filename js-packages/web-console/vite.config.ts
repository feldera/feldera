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
        'virtual:felderaApiJsonSchemas.json': JSON.stringify(felderaApiJsonSchemas)
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
      // },
      // css: {
      //   preprocessorOptions: {
      //     scss: {
      //       api: 'modern-compiler'
      //     },
      //     sass: {
      //       api: 'modern-compiler'
      //     }
      //   }
    }
  } satisfies UserConfig
})
