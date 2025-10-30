import type { Handle } from '@sveltejs/kit'
import { svelteAttr } from 'svelte-attr'

export const handle: Handle = async ({ event, resolve }) => {
  const response = resolve(event)
  return svelteAttr(response)
}
