/**
 * Being logged in is all this level requires, so `/select-tenant` lives here
 * too. Anything that needs an acting tenant belongs in the `(authorized)` group,
 * whose gate redirects here when none resolves.
 */
export const load = async ({ parent }) => {
  const data = await parent()
  if (typeof data.auth === 'object' && 'login' in data.auth) {
    data.auth.login()
    await new Promise(() => {}) // Await indefinitely to avoid loading the page - until redirected to auth page
  }
  return { ...data }
}
