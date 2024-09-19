import { defineConfig } from 'vite'

import svg from '@poppanator/sveltekit-svg'
import { sveltekit } from '@sveltejs/kit/vite'
import { monaco } from '@bithero/monaco-editor-vite-plugin'

import viteSvgToWebfont from 'vite-svg-2-webfont'
import { resolve } from 'path'
import { existsSync, mkdirSync } from 'fs'
import SvgFixer from 'oslllo-svg-fixer'

export default defineConfig(async () => {
  if (!existsSync('tmp/assets/icons/feldera-material-icons')) {
    mkdirSync('tmp/assets/icons/feldera-material-icons', { recursive: true })
    await SvgFixer(
      'src/assets/icons/feldera-material-icons',
      'tmp/assets/icons/feldera-material-icons'
    ).fix()
  }
  return {
    plugins: [
      sveltekit(),
      svg(),
      viteSvgToWebfont({
        context: resolve(__dirname, 'tmp/assets/icons/feldera-material-icons'),
        fontName: 'FelderaIconsFont',
        baseSelector: '.fd',
        classPrefix: 'fd-',
        moduleId: 'vite-svg-2-webfont.css',
        cssFontsUrl: '/'
      }),
      monaco({
        features: ['clipboard', 'dropOrPasteInto', 'find', 'lineSelection', 'wordHighlighter'],
        languages: ['sql', 'json', 'rust']
      })
    ],
    build: {
      minify: 'esbuild'
    }
  }
})
