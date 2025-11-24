import { defineConfig, type UserConfig } from 'vite'
import { viteSingleFile } from 'vite-plugin-singlefile'

export default defineConfig(async () => {
  return {
    plugins: [viteSingleFile()],
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
  } satisfies UserConfig
})
