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
    // Base path the app is served from. Empty for a root deployment and for
    // local `bun run dev`. When pipeline-manager builds the embedded bundle it
    // sets WEBCONSOLE_BASE_PATH to a sentinel, which the manager rewrites at
    // serve time to the operator-configured prefix
    // (WEBCONSOLE_BASE_PATH_PLACEHOLDER in crates/pipeline-manager/src/api/main.rs).
    // The base reaches the built output in three places, all of them literal
    // text the rewrite reaches: the `__sveltekit_*.base`/`.assets` runtime
    // globals in index.html, the asset URLs in that same file, and the
    // fallback base in the bundle's `$app/paths` module. Worker and font URLs
    // are resolved relative to the bundle, so they need no prefix.
    paths: {
      base: process.env.WEBCONSOLE_BASE_PATH || ''
    },
    alias: {
      $assets: 'src/assets'
    },
    output: {
      bundleStrategy: 'single'
    }
  }
}

export default config
