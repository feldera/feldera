import { resolve } from '$lib/functions/svelte'
import { redirect } from '@sveltejs/kit'
import type { PageLoad } from './$types'

export const load: PageLoad = () => {
  throw redirect(308, resolve('/'))
}
