/**
 * A proxy for SvelteKit's `invalidateAll`, re-running every `load()`. The root
 * layout registers the real function at startup.
 *
 * The indirection keeps `$app/navigation` out of the import tree of
 * the API code layer. SvelteKit's app modules resolve only inside a Vite
 * build, and both Playwright's loader and vitest's globalSetup (pure Node) import that
 * layer with no such build, so a module-scope import of any "$app" module fails there.
 *
 * Import it from here, not from `$app/navigation`, in anything the API layer
 * reaches.
 */
let registered: () => Promise<void> = async () => {}

export const setInvalidateAll = (invalidateAll: () => Promise<void>) => {
  registered = invalidateAll
}

/** Resolves once every `load()` has re-run, as SvelteKit's `invalidateAll` does. */
export const invalidateAll = () => registered()
