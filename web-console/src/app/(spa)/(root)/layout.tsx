'use client'
import StatusSnackBar from '$lib/components/common/errors/StatusSnackBar'
import { AuthenticationProvider } from '$lib/components/layouts/AuthProvider'
import { Next13ProgressBar as NextProgressBar } from 'next13-progressbar'

import { useTheme } from '@mui/material/styles'

import type { ReactNode } from 'react'
export default (props: { children: ReactNode }) => {
  const theme = useTheme()
  return (
    <AuthenticationProvider>
      <NextProgressBar height='3px' color={theme.palette.primary.main} options={{ showSpinner: false }} showOnShallow />
      {props.children}
      <StatusSnackBar />
    </AuthenticationProvider>
  )
}
