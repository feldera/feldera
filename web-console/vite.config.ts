import { defineConfig, type UserConfig } from 'vite'

import svg from '@poppanator/sveltekit-svg'
import { sveltekit } from '@sveltejs/kit/vite'
import virtual from 'vite-plugin-virtual'
import { felderaApiJsonSchemas } from './src/lib/functions/felderaApiJsonSchemas'

export default defineConfig(async () => {
  return {
    plugins: [
      sveltekit(),
      svg(),
      virtual({
        'virtual:felderaApiJsonSchemas.json': JSON.stringify(felderaApiJsonSchemas)
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
