import adapter from '@sveltejs/adapter-static'
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte'

/** @type {import('@sveltejs/kit').Config} */
const config = {
  // Consult https://kit.svelte.dev/docs/integrations#preprocessors
  // for more information about preprocessors
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter({
      fallback: 'index.html',
      pages: process.env.BUILD_DIR || 'build' // built webapp static files output directory
    }),
    alias: {
      $assets: 'src/assets'
    },
    output: {
      bundleStrategy: 'split'
    }
  }
}

export default config
