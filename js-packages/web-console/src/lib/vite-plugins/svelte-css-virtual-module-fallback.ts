/**
 * Workaround: vite-plugin-svelte CSS virtual module fallback
 *
 * Bug: vite-plugin-svelte's load hook for CSS virtual modules
 * (?svelte&type=style&lang.css) can fail silently for .svelte files inside
 * node_modules. When it fails, Vite falls back to serving the raw .svelte
 * source as the CSS module content. Any downstream CSS transform plugin
 * (e.g. @tailwindcss/vite) then tries to parse the full .svelte file —
 * including <script>, HTML, and comments — as CSS, and crashes.
 *
 * Example error:
 *   [vite-plugin-svelte:load] failed to load virtual css module
 *     …/node_modules/.bun/flowbite-svelte@1.31.0/…/Arrow.svelte?svelte&type=style&lang.css
 *   [vite] Pre-transform error: Invalid declaration: `Side`
 *     Plugin: @tailwindcss/vite:generate:serve
 *
 * Root cause: the load hook likely fails a file-existence or path-resolution
 * check for packages resolved through bun's hoisted symlink layout
 * (node_modules/.bun/pkg@version/…). A similar path-resolution bug was fixed
 * in https://github.com/sveltejs/vite-plugin-svelte/pull/247.
 *
 * This plugin runs only during vitest (process.env.VITEST) because:
 * - During `vite dev`, the dep optimizer pre-bundles svelte libraries so
 *   their .svelte files are never loaded individually — the bug never fires.
 * - During vitest browser tests, the optimizer's dep scan fails on svelte
 *   component virtual-module exports, and in CI (cold cache) the include
 *   list alone isn't enough to produce pre-bundled output. Components are
 *   then loaded individually, triggering the bug.
 *
 * Fix: an enforce:'pre' transform hook detects when a CSS virtual module
 * received the raw .svelte source (via `code.includes('<script')`), compiles
 * the component with the Svelte compiler, and returns the real extracted CSS.
 * This preserves component styles for screenshot tests.
 *
 * ---
 * Minimal reproduction (for reporting to vite-plugin-svelte):
 *
 * 1. Use bun as the package manager in a monorepo. Bun hoists dependencies
 *    into `<repo-root>/node_modules/.bun/<pkg>@<version>/node_modules/<pkg>`
 *    and symlinks them back into each workspace's node_modules.
 *
 * 2. Install a svelte component library that ships raw .svelte files and
 *    includes a <style> block (e.g. flowbite-svelte which has Arrow.svelte
 *    with a :global(.clip) style).
 *
 * 3. Install @tailwindcss/vite (v4) — or any vite CSS transform plugin that
 *    parses CSS strictly — and add it to the vite plugins list.
 *
 * 4. Configure vitest with browser mode (playwright provider) and add a test
 *    that renders a component importing from the library (e.g. a component
 *    using flowbite-svelte's Tooltip or Popover, which internally imports
 *    Arrow.svelte).
 *
 * 5. Run `vitest` with a cold .vite cache (rm -rf node_modules/.vite).
 *    The dep optimizer will NOT pre-bundle the library (its dep scan fails
 *    on svelte virtual-module exports). vite-plugin-svelte then tries to
 *    load the CSS virtual module for Arrow.svelte but its load hook fails.
 *    The raw .svelte source is served as CSS, and tailwindcss crashes with
 *    "Invalid declaration" errors.
 *
 * Key conditions: bun's symlink layout, cold cache (no pre-bundled deps),
 * vitest browser mode, a node_modules svelte component with a <style> block.
 */

import type { Plugin } from 'vite'

export function svelteCssVirtualModuleFallback(): Plugin {
  return {
    name: 'svelte-css-virtual-module-fallback',
    enforce: 'pre',
    apply() {
      return !!process.env.VITEST
    },
    async transform(code, id) {
      if (
        id.includes('node_modules') &&
        id.includes('.svelte') &&
        id.includes('type=style') &&
        code.includes('<script')
      ) {
        const { compile } = await import('svelte/compiler')
        const svelteFile = id.replace(/\?.*$/, '')
        const result = compile(code, {
          css: 'external',
          filename: svelteFile,
          dev: true
        })
        return { code: result.css?.code ?? '', map: null }
      }
    }
  }
}
