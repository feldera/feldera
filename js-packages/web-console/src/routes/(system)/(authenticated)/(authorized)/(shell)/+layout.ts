import { loadDemos } from '$lib/compositions/useDemos.svelte'

/**
 * The home page and /demos show demo tiles, so the shell starts the fetch. A page
 * outside the shell, such as the profile viewer, must not ask the manager for it.
 */
export const load = async () => {
  loadDemos()
  return {}
}
