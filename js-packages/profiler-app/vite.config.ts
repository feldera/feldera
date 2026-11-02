import { fileURLToPath } from 'node:url'
import { monaco } from '@feldera/vite-plugin-monaco-editor'
import svg from '@poppanator/sveltekit-svg'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig } from 'vite'
import { viteSingleFile } from 'vite-plugin-singlefile'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    tailwindcss(),
    svelte(),
    svg(),
    // Bundle only the Monaco languages and features that the read-only SourceCodeView of
    // profiler-layout uses, and inline the workers so that `viteSingleFile` makes one
    // self-contained file. SourceCodeView shows TOML with the `graphql` grammar.
    // The plugin neuters `vs/language/<x>/` dynamic imports for any language not
    // listed here, so MonacoEditor's optional JSON-schema field costs nothing.
    monaco({
      languages: ['sql', 'rust', 'graphql'],
      features: ['browser', 'clipboard', 'find', 'wordHighlighter'],
      inlineWorkers: true
    }),
    viteSingleFile()
  ],
  resolve: {
    alias: [
      { find: '$assets', replacement: fileURLToPath(new URL('./src/assets', import.meta.url)) }
    ]
  },
  server: {
    port: 5174
  },
  build: {
    target: 'esnext',
    assetsInlineLimit: 100000000, // Inline all assets
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        inlineDynamicImports: true
      }
    }
  }
})
