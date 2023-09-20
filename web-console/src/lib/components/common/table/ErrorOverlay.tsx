// The error message that is shown instead of the EntityTable if we failed to
// load data for some reason (e.g. network error).
'use client'

import { ErrorResponse } from '$lib/services/manager'

import { Alert, AlertTitle } from '@mui/material'

export interface ErrorProps {
  error: ErrorResponse | Error | undefined
}

export const ErrorOverlay = (props: ErrorProps) => {
  if (!props?.error) {
    return <></>
  }
  return (
    <Alert severity='error'>
      <AlertTitle>Error</AlertTitle>
      Unable to load table content:
      <br />
      <strong>{props.error.message}</strong>
    </Alert>
  )
}
