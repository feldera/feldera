// `base` is the path prefix the app is served from (empty for a root
// deployment, e.g. `/feldera` behind a reverse proxy). It must be appended to
// the origin so REST and WebSocket calls target `<origin><base>/v0/...` rather
// than bypassing the proxy prefix. `base` is resolved at runtime from the
// SvelteKit payload the manager injects, so a single embedded bundle serves
// any subpath.
//
// `$app/paths` is a SvelteKit virtual module, only resolvable when this file
// is loaded through SvelteKit's Vite plugin. This module is also imported
// from plain Node/bun scripts that never go through that plugin — Playwright's
// global setup, via testPipelineHelpers.ts and pipelineManager.ts — so the
// import is dynamic and gated behind the same `window` check as its only use,
// letting those non-browser callers skip it instead of failing to resolve it.
export const felderaEndpoint =
  'window' in globalThis && window.location.origin
    ? // If we're running locally with `bun run dev`, we point to the
      // backend server running on port 8080
      // Otherwise the API and UI URL will be the same
      window.location.origin.replace(/:([45]17[34])$/, ':8080') + (await import('$app/paths')).base
    : 'http://localhost:8080'
