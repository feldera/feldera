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
    // Trim monaco to the bare minimum needed for read-only SQL viewing, and
    // inline the workers so `viteSingleFile` produces a true self-contained bundle.
    // The SqlCodeView (consumed via profiler-layout) renders SQL with line numbers,
    // range highlighting via decorations, and a small lookup-cycling search.
    // The plugin neuters `vs/language/<x>/` dynamic imports for any language not
    // listed here, so MonacoEditor's optional JSON-schema field costs nothing.
    monaco({
      languages: ['sql'],
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
