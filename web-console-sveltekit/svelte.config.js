import adapter from '@sveltejs/adapter-static'
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte'

/** @type {import('@sveltejs/kit').Config} */
const config = {
  // Consult https://kit.svelte.dev/docs/integrations#preprocessors
  // for more information about preprocessors
  preprocess: vitePreprocess(),
  kit: {
    // adapter-auto only supports some environments, see https://kit.svelte.dev/docs/adapter-auto for a list.
    // If your environment is not supported, or you settled on a specific environment, switch out the adapter.
    // See https://kit.svelte.dev/docs/adapters for more information about adapters.
    adapter: adapter({
      fallback: 'index.html',
      pages: process.env.BUILD_DIR || 'build' // built webapp static files output directory
    }),
    alias: {
      $assets: 'src/assets'
    },
    paths: {
      // server subdirectory where the root index.html will be served from
      // comment out when webapp gets served from root path
      base: '/new'
    }
  }
}

export default config
