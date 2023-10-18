'use client'
import { GridItems } from '$lib/components/common/GridItems'
import { useAuthStore } from '$lib/compositions/auth/useAuth'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import Image from 'next/image'
import { redirect } from 'next/navigation'
import { ReactNode } from 'react'
import { match, P } from 'ts-pattern'
import IconLockAlt from '~icons/bx/lock-alt'

import { Box, Button, Grid, Typography, useTheme } from '@mui/material'
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
      <Grid container spacing={6} sx={{ pt: 24, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
        <GridItems>
          <Image src={'/images/feldera/LogoSolid.svg'} width={300} height='100' alt='AWS Cognito logo' />
          <IconLockAlt fontSize={64} />
          {match(authConfig)
            .with(undefined, () => <></>)
            .with({ AwsCognito: P.select() }, config => (
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <AwsLoginButton loginUrl={config.login_url} logoutUrl={config.logout_url}>
                  Login with AWS Cognito
                </AwsLoginButton>
              </Box>
            ))
            .with({ GoogleIdentity: P._ }, () => <GoogleLoginButton></GoogleLoginButton>)
            .otherwise(() => {
              redirect('/home')
            })}
        </GridItems>
      </Grid>
    </Box>
  )
}

const AwsLoginButton = (props: { loginUrl: string; logoutUrl: string; children: ReactNode }) => {
  const theme = useTheme()
  const url = (props.loginUrl + '&redirect_uri={redirectUri}&state={state}')
    .replace('{redirectUri}', encodeURIComponent(window.location.origin + '/auth/aws'))
    .replace(
      '{state}',
      Buffer.from(props.logoutUrl + '&redirect_uri={redirectUri}&state={state}', 'utf8').toString('base64')
    )
  return (
    <Button href={url} variant='outlined' sx={{ gap: 8, backgroundColor: theme.palette.background.paper }}>
      <Image src={'/icons/vendors/aws-cognito-icon.png'} width={27} height={32} alt='AWS Cognito logo' />
      <Typography variant='body1' style={{ textTransform: 'none' }}>
        {props.children}
      </Typography>{' '}
    </Button>
  )
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
      size='large'
      onSuccess={r => {
        onGoogleLogin(r)
        redirect('/home')
      }}
    />
  )
}
