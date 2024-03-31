'use client'
/** @jsxImportSource @emotion/react */

import StatusSnackBar from '$lib/components/common/errors/StatusSnackBar'
import { AuthenticationProvider } from '$lib/components/layouts/AuthProvider'
import { PostHogProvider } from '$lib/components/services/PostHogProvider'
import { OpenAPI } from '$lib/services/manager'
import { Next13ProgressBar as NextProgressBar } from 'next13-progressbar'
import posthog from 'posthog-js'
import { ReactNode } from 'react'
import { SettingsConsumer, SettingsProvider } from 'src/@core/context/settingsContext'
import ThemeComponent from 'src/@core/theme/ThemeComponent'

import { useTheme } from '@mui/material/styles'
import { LocalizationProvider } from '@mui/x-date-pickers'
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs'
import { LicenseInfo } from '@mui/x-license-pro'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import EmotionRootStyleRegistry from './EmotionRootStyleRegistry'

LicenseInfo.setLicenseKey(process.env.NEXT_PUBLIC_MUIX_PRO_KEY || 'unset')
if ('window' in globalThis) {
  // If we're running locally with `yarn dev` on port 3000, we point to the
  // backend server running on port 8080
  // Otherwise the API and UI URL will be the same
  OpenAPI.BASE = window.location.origin.replace(/:(300[0-9])$/, ':8080') + OpenAPI.BASE

  if (process.env.NEXT_PUBLIC_POSTHOG_API_KEY) {
    posthog.init(process.env.NEXT_PUBLIC_POSTHOG_API_KEY, {
      api_host: process.env.NEXT_PUBLIC_POSTHOG_HOST
    })
  }
}

// provide the default query function to your app with defaultOptions
const queryClient = new QueryClient({})

const Layout = (props: { children: ReactNode }) => {
  const theme = useTheme()
  return (
    <body>
      <NextProgressBar height='3px' color={theme.palette.primary.main} options={{ showSpinner: false }} showOnShallow />
      {props.children}
      <StatusSnackBar />
    </body>
  )
}

export default (props: { children: ReactNode }) => {
  return (
    <EmotionRootStyleRegistry>
      <LocalizationProvider dateAdapter={AdapterDayjs} adapterLocale='en-gb'>
        <QueryClientProvider client={queryClient}>
          <PostHogProvider client={posthog}>
            <AuthenticationProvider>
              <SettingsProvider>
                <SettingsConsumer>
                  {({ settings }) => {
                    return (
                      <ThemeComponent settings={settings}>
                        <Layout>{props.children}</Layout>
                      </ThemeComponent>
                    )
                  }}
                </SettingsConsumer>
              </SettingsProvider>
            </AuthenticationProvider>
          </PostHogProvider>
        </QueryClientProvider>
      </LocalizationProvider>
    </EmotionRootStyleRegistry>
  )
}
