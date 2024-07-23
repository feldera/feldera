// import { signIn, signOut } from "@auth/sveltekit/client"

export const load = async (event) => {
  return {
    session: await event.locals.auth(), // Provides $page.data.session with data from @auth/sveltekit
    authDetails: event.locals.authDetails
  }
}
