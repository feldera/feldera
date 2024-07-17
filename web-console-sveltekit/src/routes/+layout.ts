import { signIn } from '@auth/sveltekit/client'
import { redirect } from '@sveltejs/kit'

export const ssr = false
export const trailingSlash = 'always'
import '$lib/compositions/setupHttpClient'

export const load = async ({data}) => {
  if (data.authEnabled && !data.session) {
    // redirect(302, `${base}/login`)
    signIn('cognito')
  }
  console.log('f.data', data)
}