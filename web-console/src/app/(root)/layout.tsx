'use client'
/** @jsxImportSource @emotion/react */

import StatusSnackBar from '$lib/components/common/errors/StatusSnackBar'
import { OpenAPI } from '$lib/services/manager'
import { Next13ProgressBar as NextProgressBar } from 'next13-progressbar'
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
OpenAPI.BASE =
  ('window' in globalThis && window.location.origin
    ? window.location.origin.endsWith(':3000')
      ? // If we're running locally with `yarn dev` on port 3000, we point to the
        // backend server running on port 8080
        window.location.origin.replace(':3000', ':8080')
      : // Otherwise the API and UI URL will be the same
        window.location.origin
    : '') + '/v0'

// provide the default query function to your app with defaultOptions
const queryClient = new QueryClient({})

export default (props: { children: ReactNode }) => {
  const theme = useTheme()
  return (
    <EmotionRootStyleRegistry>
      <SettingsProvider>
        <SettingsConsumer>
          {({ settings }) => {
            return (
              <LocalizationProvider dateAdapter={AdapterDayjs} adapterLocale='en-gb'>
                <ThemeComponent settings={settings}>
                  <QueryClientProvider client={queryClient}>
                    <NextProgressBar
                      height='3px'
                      color={theme.palette.primary.main}
                      options={{ showSpinner: false }}
                      showOnShallow
                    />
                    {props.children}
                    <StatusSnackBar />
                  </QueryClientProvider>
                </ThemeComponent>
              </LocalizationProvider>
            )
          }}
        </SettingsConsumer>
      </SettingsProvider>
    </EmotionRootStyleRegistry>
  )
}
