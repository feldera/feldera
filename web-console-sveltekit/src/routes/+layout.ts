import '$lib/compositions/setupHttpClient'

import { signIn } from '@auth/sveltekit/client'
import { error } from '@sveltejs/kit'
import { client } from '@hey-api/client-fetch';

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
  if (data.authDetails.enabled && !data.session && !await signIn(data.authDetails.providerId)) {
    error(401)
  }
  // if (data.session) {
  //   if (data.session.accessToken) {
  //     accessToken = data.session.accessToken
  //   } else {
  //     accessToken = undefined
  //   }
  // }
  accessToken = data.session?.accessToken
  client.interceptors.request.use(authMiddleware)
  // if (data.session?.accessToken) {
  //   client.interceptors.request.eject()
  // }
  console.log('root layout load', data.session)
}
