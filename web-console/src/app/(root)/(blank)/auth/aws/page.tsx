'use client'

import { useAuthStore } from '$lib/compositions/auth/useAuth'
import { useHashPart } from '$lib/compositions/useHashPart'
import { LS_PREFIX } from '$lib/types/localStorage'
import { jwtDecode, JwtPayload } from 'jwt-decode'
import { redirect } from 'next/navigation'

export default () => {
  const { setAuth } = useAuthStore()
  const params = (([path]) =>
    Object.fromEntries(Array.from(path.matchAll(/(\w+)=([\w.-]+)&?/g)).map(([, k, v]) => [k, v])))(useHashPart())
  const jwtIdPayload = jwtDecode<JwtPayload & Record<string, string>>(params['id_token'])
  if ('access_token' in params && 'state' in params) {
    const logoutUrlBase64 = params['state']
    const logoutUrl = Buffer.from(params['state'], 'base64')
      .toString('utf8')
      .replace('{redirectUri}', encodeURIComponent(window.location.origin + '/auth/aws/'))
      .replace('{state}', logoutUrlBase64)

    setAuth({
      Authenticated: {
        credentials: {
          bearer: params['access_token']
        },
        user: {
          avatar: jwtIdPayload['picture'],
          username: jwtIdPayload['cognito:username'] || 'anonynous',
          contacts: {
            email: jwtIdPayload['email'],
            phone: jwtIdPayload['phone_number']
          }
        },
        signOutUrl: logoutUrl
      }
    })
  }
  const redirectUrl = window.sessionStorage.getItem(LS_PREFIX + 'redirect')?.slice(1, -1) // Trim quotes of a raw string
  if (redirectUrl) {
    return redirect(redirectUrl)
  }
  redirect('/home')
}
