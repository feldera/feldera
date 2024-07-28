import '$lib/compositions/setupHttpClient'

import { signIn } from '@auth/sveltekit/client'
import { error } from '@sveltejs/kit'
import { client } from '@hey-api/client-fetch'

export const ssr = false
export const trailingSlash = 'always'

let accessToken: string | undefined
const authMiddleware = (request: Request) => {
  if (accessToken) {
    request.headers.set('Authorization', `Bearer ${accessToken}`)
  }
  return request
}

export const load = async () => {
  const auth = authenticate()

  if (auth.authDetails.enabled && !auth.session && !(await signIn(auth.authDetails.providerId))) {
    error(401)
  }
  accessToken = auth.session?.accessToken
  client.interceptors.request.use(authMiddleware)
  return auth
}
