import { loadDemos } from '$lib/compositions/useDemos.svelte'

export const load = async ({ parent }) => {
  const data = await parent()
  if (typeof data.auth === 'object' && 'login' in data.auth) {
    data.auth.login()
    await new Promise(() => {}) // Await indefinitely to avoid loading the page - until redirected to auth page
  }
  loadDemos()
  return { ...data }
}
