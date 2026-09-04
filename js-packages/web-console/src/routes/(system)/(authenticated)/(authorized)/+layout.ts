import { redirect } from '@sveltejs/kit'
import { resolve } from '$lib/functions/svelte'
import { stashRedirectTarget } from '$lib/services/redirectTarget'

/**
 * Everything in this group needs an acting tenant: the `(shell)` group's layout
 * mounts the app shell and its pollers, and the pages fetch tenant-scoped
 * resources. While the session resolves no tenant, every route but
 * /config/session refuses to answer, so this redirects instead of letting any of
 * that run. A redirect (rather than a branch in a layout component) is what keeps
 * the nested `load` functions from firing too, since it aborts the navigation
 * before they run.
 *
 * `profile-viewer` sits in this group but outside `(shell)`: it needs the tenant
 * to download a bundle from a pipeline, but it reads the bundle offline, so
 * nothing on its path asks the manager for anything else. Loaders that fetch
 * shell content, such as the demo list, belong in `(shell)`.
 *
 * The page asked for is stashed here, in the one place the gate fires, so
 * whichever route triggered it is the one the user returns to after picking a
 * tenant. `/select-tenant` sits outside this group, so it is reachable while the
 * gate is closed.
 */
export const load = async ({ parent, url }) => {
  const data = await parent()
  if (data.unresolvedTenant) {
    stashRedirectTarget(url.href)
    throw redirect(307, resolve('/select-tenant/'))
  }
  return {}
}
