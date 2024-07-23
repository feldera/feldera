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

export const load = async ({ data, fetch }) => {
  if (data.authDetails.enabled && !data.session && !(await signIn(data.authDetails.providerId))) {
    error(401)
  }
  accessToken = data.session?.accessToken
  client.interceptors.request.use(authMiddleware)
}
