import type { LoadEvent } from '@sveltejs/kit'
import { redirect } from '@sveltejs/kit'
import { resolve } from '#lib/functions/svelte.ts'
import { hasPermissions } from '#lib/services/rbac.ts'

// Gate the admin area on the lowest permission any admin section needs
// (`write:tenant_member`); everyone below admin goes home.
export const load = async ({ parent }: LoadEvent) => {
  const data = await parent()
  if (!hasPermissions(data.feldera, 'write:tenant_member')) {
    throw redirect(307, resolve('/'))
  }
  return {}
}
