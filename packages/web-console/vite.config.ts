import { defineConfig, type UserConfig } from 'vite'

import svg from '@poppanator/sveltekit-svg'
import { sveltekit } from '@sveltejs/kit/vite'
import virtual from 'vite-plugin-virtual'
import { felderaApiJsonSchemas } from './src/lib/functions/felderaApiJsonSchemas'
import { monaco } from '@bithero/monaco-editor-vite-plugin'

export default defineConfig(async () => {
  return {
    plugins: [
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
          // 'quickCommand',
          'quickOutline',
          'smartSelect',
          'suggest',
          // 'toggleHighContrast',
          'wordHighlighter',
          'wordOperations'
          // 'wordPartOperations'
        ]
      })
    ],
    build: {
      minify: 'esbuild'
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
