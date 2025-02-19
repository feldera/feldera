import { defineConfig, type UserConfig } from 'vite'

import svg from '@poppanator/sveltekit-svg'
import { sveltekit } from '@sveltejs/kit/vite'
import viteSvgToWebfont from 'vite-svg-2-webfont'
import { resolve } from 'path'

export default defineConfig(async () => {
  return {
    plugins: [
      sveltekit(),
      svg(),
      viteSvgToWebfont({
        context: resolve(__dirname, 'src/assets/icons/feldera-material-icons'),
        fontName: 'FelderaMaterialIconsFont',
        baseSelector: '.fd',
        classPrefix: 'fd-',
        moduleId: 'feldera-material-icons-webfont.css',
        cssFontsUrl: '/'
      }),
      viteSvgToWebfont({
        context: resolve(__dirname, 'src/assets/icons/generic'),
        fontName: 'FelderaGenericIconsFont',
        baseSelector: '.gc',
        classPrefix: 'gc-',
        moduleId: 'generic-icons-webfont.css',
        cssFontsUrl: '/'
      })
    ],
    build: {
      minify: 'esbuild'
    }
  } satisfies UserConfig
})
