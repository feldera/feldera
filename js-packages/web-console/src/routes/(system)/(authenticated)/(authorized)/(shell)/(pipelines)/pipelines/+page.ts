import { redirect } from '@sveltejs/kit'
import { resolve } from '$lib/functions/svelte'
import type { PageLoad } from './$types'

export const load: PageLoad = () => {
  throw redirect(308, resolve('/'))
}
