'use client'

import { authContext, useAuthStore } from '$lib/compositions/auth/useAuth'
import { OpenAPI } from '$lib/services/manager'
import { PublicPipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { ReactNode } from 'react'
import { match, P } from 'ts-pattern'

import { GoogleOAuthProvider } from '@react-oauth/google'
import { useQuery } from '@tanstack/react-query'

export const AuthenticationProvider = (props: { children: ReactNode }) => {
  const { data: auth } = useQuery(PublicPipelineManagerQuery.getAuthConfig())
  const { auth: authState, setAuth } = useAuthStore()
  if (typeof authState === 'object' && 'Authenticated' in authState) {
    OpenAPI.TOKEN = authState['Authenticated'].credentials.bearer
  }
  if (!auth) {
    return <></>
  }

  if (auth !== 'NoAuth' && authState === 'NoAuth') {
    setAuth('Unauthenticated')
    return <></>
  }
  if (auth === 'NoAuth' && authState === 'Unauthenticated') {
    setAuth('NoAuth')
    return <></>
  }

  return (
    <authContext.Provider value={authState}>
      {match(auth)
        .with({ AwsCognito: P._ }, () => {
          return props.children
        })
        .with({ GoogleIdentity: P.select() }, config => {
          return <GoogleOAuthProvider clientId={config.client_id}>{props.children}</GoogleOAuthProvider>
        })
        .with('NoAuth', () => props.children)
        .exhaustive()}
    </authContext.Provider>
  )
}
