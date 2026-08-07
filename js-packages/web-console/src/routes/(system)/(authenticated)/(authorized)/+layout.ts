import { redirect } from '@sveltejs/kit'
import { loadDemos } from '$lib/compositions/useDemos.svelte'
import { resolve } from '$lib/functions/svelte'
import { stashRedirectTarget } from '$lib/services/redirectTarget'

/**
 * Everything in this group needs an acting tenant: its layout mounts the app
 * shell and its pollers, and its pages fetch tenant-scoped resources. While the
 * session resolves no tenant, every route but /config/session refuses to answer,
 * so this redirects instead of letting any of that run. A redirect (rather than
 * a branch in the layout component) is what keeps the sibling `load` functions
 * from firing too, since it aborts the navigation before they run.
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
  loadDemos()
  return {}
}
