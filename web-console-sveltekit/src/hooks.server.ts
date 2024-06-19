import type { Handle } from '@sveltejs/kit'
import { svelteAttr } from 'svelte-attr'

export const handle: Handle = async ({ event, resolve }) => {
  return svelteAttr(resolve(event))
}
