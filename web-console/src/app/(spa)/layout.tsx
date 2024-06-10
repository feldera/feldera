'use client'
/** @jsxImportSource @emotion/react */

import { OpenAPI } from '$lib/services/manager'
import { ReactNode } from 'react'

import { SettingsConsumer, SettingsProvider } from '@core/context/settingsContext'
import ThemeComponent from '@core/theme/ThemeComponent'
import { LocalizationProvider } from '@mui/x-date-pickers'
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs'
import { LicenseInfo } from '@mui/x-license'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import EmotionRootStyleRegistry from './EmotionRootStyleRegistry'

LicenseInfo.setLicenseKey(process.env.NEXT_PUBLIC_MUIX_PRO_KEY || 'unset')
OpenAPI.BASE =
  ('window' in globalThis && window.location.origin
    ? // If we're running locally with `yarn dev` on port 3000, we point to the
      // backend server running on port 8080
      // Otherwise the API and UI URL will be the same
      window.location.origin.replace(/:(300[0-9])$/, ':8080')
    : '') + OpenAPI.BASE

// provide the default query function to your app with defaultOptions
const queryClient = new QueryClient({})

export default (props: { children: ReactNode }) => {
  return (
    <EmotionRootStyleRegistry>
      <LocalizationProvider dateAdapter={AdapterDayjs} adapterLocale='en-gb'>
        <QueryClientProvider client={queryClient}>
          <SettingsProvider>
            <SettingsConsumer>
              {({ settings }) => {
                return <ThemeComponent settings={settings}>{props.children}</ThemeComponent>
              }}
            </SettingsConsumer>
          </SettingsProvider>
        </QueryClientProvider>
      </LocalizationProvider>
    </EmotionRootStyleRegistry>
  )
}
