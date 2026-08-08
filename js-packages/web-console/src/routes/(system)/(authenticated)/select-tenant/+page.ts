import { redirect } from '@sveltejs/kit'
import { resolve } from '$lib/functions/svelte'
import { takeRedirectTarget } from '$lib/services/redirectTarget'

/**
 * The page the `(authorized)` gate redirects to. Reaching it with a tenant
 * already resolved means there is nothing to pick — a stale history entry, or a
 * second tab that picked first — so it hands the user on to wherever the gate
 * interrupted them, consuming the stash so no later redirect reuses it.
 */
export const load = async ({ parent }) => {
  const data = await parent()
  if (!data.unresolvedTenant) {
    throw redirect(307, takeRedirectTarget() ?? resolve('/'))
  }
  return { memberships: data.unresolvedTenant.memberships }
}
