import type { Handle } from '@sveltejs/kit'
import { svelteAttr } from 'svelte-attr'
import * as auth from '$lib/compositions/auth'

export const handle: Handle = async (input) => {
  const response = auth.handle(input)
  return svelteAttr(response)
}
