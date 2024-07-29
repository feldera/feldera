import type { Handle } from '@sveltejs/kit'
import { svelteAttr } from 'svelte-attr'

import '$lib/compositions/setupHttpClient'

export const handle: Handle = async ({ event, resolve }) => {
  const response = resolve(event)
  return svelteAttr(response)
}
