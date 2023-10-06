'use client'
import { GridItems } from '$lib/components/common/GridItems'
import { useAuthStore } from '$lib/compositions/auth/useAuth'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import Image from 'next/image'
import { redirect } from 'next/navigation'
import { ReactNode } from 'react'
import { match, P } from 'ts-pattern'

import { Box, Button, Grid } from '@mui/material'
import { CredentialResponse, GoogleLogin, TokenResponse } from '@react-oauth/google'
import { useQuery } from '@tanstack/react-query'

export default () => {
  const { data: authConfig } = useQuery(PipelineManagerQuery.getAuthConfig())
  const { auth } = useAuthStore()
  if (typeof auth === 'object') {
    redirect('/home')
  }
  return (
    <Box sx={{ display: 'flex', height: '100vh', justifyContent: 'center' }}>
      {match(authConfig)
        .with(undefined, () => <></>)
        .with({ AwsCognito: P.select() }, config => (
          <Grid container spacing={6} sx={{ pt: 12, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
            <GridItems>
              <Image src={'/icons/vendors/aws-cognito-icon.png'} width={160} height={160} alt='AWS Cognito logo' />
              <AwsLoginButton loginUrl={config.login_url} logoutUrl={config.logout_url}>
                Login with AWS Cognito
              </AwsLoginButton>
            </GridItems>
          </Grid>
        ))
        .with({ GoogleIdentity: P._ }, () => (
          <Grid container spacing={6} sx={{ pt: 32, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
            <GridItems>
              <GoogleLoginButton></GoogleLoginButton>
            </GridItems>
          </Grid>
        ))
        .otherwise(() => {
          redirect('/home')
        })}
    </Box>
  )
}

const AwsLoginButton = (props: { loginUrl: string; logoutUrl: string; children: ReactNode }) => {
  const url = props.loginUrl
    .replace('{redirectUri}', encodeURIComponent(window.location.origin + '/auth/aws'))
    .replace('{state}', Buffer.from(props.logoutUrl, 'utf8').toString('base64'))
  return <Button href={url}>{props.children}</Button>
}

const useOnGoogleLogin = () => {
  const { setAuth } = useAuthStore()
  return (tokenResponse: CredentialResponse | TokenResponse) => {
    const bearer = 'access_token' in tokenResponse ? tokenResponse.access_token : tokenResponse.credential!
    setAuth({
      user: {
        username: 'Anonymous Google User'
      },
      bearer,
      // https://stackoverflow.com/a/23245957
      signOutUrl: 'https://accounts.google.com/o/oauth2/revoke?token=' + bearer
    })
  }
}

const GoogleLoginButton = () => {
  const onGoogleLogin = useOnGoogleLogin()
  return (
    <GoogleLogin
      onSuccess={r => {
        onGoogleLogin(r)
        redirect('/home')
      }}
    />
  )
}
