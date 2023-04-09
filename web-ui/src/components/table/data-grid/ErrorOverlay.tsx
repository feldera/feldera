import { Alert, AlertTitle, Link } from '@mui/material'

export interface ErrorProps {
  isError: boolean
  error: any
}

export const ErrorOverlay = (props: ErrorProps) => {
  if (props.isError) {
    return (
      <Alert severity='error'>
        <AlertTitle>Error</AlertTitle>
        Can't display programs: <strong>{props.error.message}</strong>
        <br />
        Check network connectivity,{' '}
        <Link href='#' underline='always'>
          then try again
        </Link>
        .
      </Alert>
    )
  } else {
    return <></>
  }
}
