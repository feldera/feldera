'use client'

import { useAuthStore } from '$lib/compositions/auth/useAuth'
import { useHashPart } from '$lib/compositions/useHashPart'
import { redirect } from 'next/navigation'

export default () => {
  const { setAuth } = useAuthStore()
  const params = (([path]) =>
    Object.fromEntries(Array.from(path.matchAll(/(\w+)=([\w.-]+)&?/g)).map(([, k, v]) => [k, v])))(useHashPart())

  if ('access_token' in params && 'state' in params) {
    const logoutUrlBase64 = params['state']
    const logoutUrl = Buffer.from(params['state'], 'base64')
      .toString('utf8')
      .replace('{redirectUri}', encodeURIComponent(window.location.origin + '/auth/aws/'))
      .replace('{state}', logoutUrlBase64)

    setAuth({
      bearer: params['access_token'],
      user: {
        username: 'anonymous'
      },
      signOutUrl: logoutUrl
    })
  }
  redirect('/home')
}
