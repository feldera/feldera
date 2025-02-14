import { defineConfig, type UserConfig } from 'vite'

import svg from '@poppanator/sveltekit-svg'
import { sveltekit } from '@sveltejs/kit/vite'

export default defineConfig(async () => {
  return {
    plugins: [sveltekit(), svg()],
    build: {
      minify: 'esbuild'
    },
    css: {
      preprocessorOptions: {
        scss: {
          api: 'modern-compiler'
        },
        sass: {
          api: 'modern-compiler'
        }
      }
    }
  } satisfies UserConfig
})
