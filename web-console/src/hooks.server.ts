import type { Handle } from '@sveltejs/kit'
import { svelteAttr } from 'svelte-attr'

import '$lib/compositions/setupHttpClient'

export const handle: Handle = async ({ event, resolve }) => {
  const response = resolve(event, {
    preload: ({ type }) => type === 'js' || type === 'css' || type === 'font'
  })
  return svelteAttr(response)
}
