// `$app/paths` is a SvelteKit virtual module, only resolvable when this file
// is loaded through SvelteKit's Vite plugin. This module is also reachable
// from plain Node/bun scripts that never go through that plugin —
// Playwright's global setup, via testPipelineHelpers.ts, pipelineManager.ts
// and auth.ts — so the import is dynamic and gated behind a browser check,
// letting those non-browser callers skip it instead of failing to resolve it.
// SvelteKit 3 no longer exports `base`. Resolving the home page's route ID gives
// the base followed by `/`.
export const base =
  'window' in globalThis
    ? (await import('$app/paths'))
        .resolve('/(system)/(authenticated)/(authorized)/(shell)')
        .slice(0, -1)
    : ''

export const resolve = (path: string) => `${base}${path}`
