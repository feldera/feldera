import type { LoadEvent } from '@sveltejs/kit'
import { redirect } from '@sveltejs/kit'
import { resolve } from '$lib/functions/svelte'

// Gate the admin area: only admins and owners may enter; everyone else goes home.
export const load = async ({ parent }: LoadEvent) => {
  const data = await parent()
  const role = data.feldera?.role
  if (role !== 'admin' && role !== 'owner') {
    throw redirect(307, resolve('/'))
  }
  return {}
}
