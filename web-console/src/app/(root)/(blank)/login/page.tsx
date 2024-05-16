'use client'
import { GridItems } from '$lib/components/common/GridItems'
import { useAuthStore } from '$lib/compositions/auth/useAuth'
import { PublicPipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { jwtDecode, JwtPayload } from 'jwt-decode'
import Image from 'next/image'
import { redirect } from 'next/navigation'
import { match, P } from 'ts-pattern'

import { Box, Grid } from '@mui/material'
import { CredentialResponse, GoogleLogin, TokenResponse } from '@react-oauth/google'
import { useQuery } from '@tanstack/react-query'

export default () => {
  const { data: authConfig } = useQuery(PublicPipelineManagerQuery.getAuthConfig())
  const { auth } = useAuthStore()
  if (typeof auth === 'object' && 'Authenticated' in auth) {
    redirect('/home')
  }
  return (
    <Box sx={{ display: 'flex', height: '100vh', justifyContent: 'center' }}>
      <Grid container spacing={6} sx={{ pt: 24, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
        <GridItems>
          <Image src={'/images/feldera/LogoSolid.svg'} width={300} height='100' alt='AWS Cognito logo' />
          <i className={`bx bx-lock-alt`} style={{ fontSize: 64 }} />
          {match(authConfig)
            .with(undefined, () => <></>)
            .with({ AwsCognito: P.select() }, config => (
              <AwsCognitoRedirect loginUrl={config.login_url} logoutUrl={config.logout_url} />
            ))
            .with({ GoogleIdentity: P._ }, () => <GoogleLoginButton></GoogleLoginButton>)
            .with('NoAuth', () => {
              redirect('/home')
            })
            .exhaustive()}
        </GridItems>
      </Grid>
    </Box>
  )
}

const AwsCognitoRedirect = (props: { loginUrl: string; logoutUrl: string }) => {
  const url = (props.loginUrl + '&redirect_uri={redirectUri}&state={state}')
    .replace('{redirectUri}', encodeURIComponent(window.location.origin + '/auth/aws/'))
    .replace(
      '{state}',
      Buffer.from(props.logoutUrl + '&redirect_uri={redirectUri}&state={state}', 'utf8').toString('base64')
    )
  redirect(url)
}

const useOnGoogleLogin = () => {
  const { setAuth } = useAuthStore()
  return (tokenResponse: CredentialResponse | TokenResponse) => {
    const bearer = 'access_token' in tokenResponse ? tokenResponse.access_token : tokenResponse.credential!
    const jwtPayload = jwtDecode<JwtPayload & Record<string, string>>(bearer)
    setAuth({
      Authenticated: {
        user: {
          avatar: jwtPayload['picture'],
          username: jwtPayload['name'] || 'anonymous',
          contacts: {
            email: jwtPayload['email']
          }
        },
        credentials: {
          bearer
        },
        // https://stackoverflow.com/a/23245957
        signOutUrl: 'https://accounts.google.com/o/oauth2/revoke?token=' + bearer
      }
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
