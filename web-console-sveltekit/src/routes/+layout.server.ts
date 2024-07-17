// import { signIn, signOut } from "@auth/sveltekit/client"

export const load = async (event) => {

  const xx = await event.locals.auth().catch(e => { console.log('eee', e); throw e })
  console.log('loading layout', event.locals)
  return {
    session: xx, // Provides $page.data.session with data from @auth/sveltekit
    authEnabled: event.locals.authEnabled
  }
}