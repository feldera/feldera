import { fileURLToPath } from 'node:url'
import svg from '@poppanator/sveltekit-svg'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig } from 'vite'
import { viteSingleFile } from 'vite-plugin-singlefile'

// https://vite.dev/config/
export default defineConfig({
  plugins: [tailwindcss(), svelte(), svg(), viteSingleFile()],
  resolve: {
    alias: {
      $assets: fileURLToPath(new URL('./src/assets', import.meta.url))
    }
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
