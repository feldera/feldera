// `$app/paths` is a SvelteKit virtual module, only resolvable when this file
// is loaded through SvelteKit's Vite plugin. This module is also reachable
// from plain Node/bun scripts that never go through that plugin —
// Playwright's global setup, via testPipelineHelpers.ts, pipelineManager.ts
// and auth.ts — so the import is dynamic and gated behind a browser check,
// letting those non-browser callers skip it instead of failing to resolve it.
const { base } = 'window' in globalThis ? await import('$app/paths') : { base: '' }

export const resolve = (path: string) => `${base}${path}`
