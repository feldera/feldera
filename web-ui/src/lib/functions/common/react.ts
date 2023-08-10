import { useState, useCallback } from 'react'

// A way to throw errors in async code so they can be caught with ErrorBoundary
// in react. Pretty silly, but it works.
//
// See:
// https://medium.com/trabe/catching-asynchronous-errors-in-react-using-error-boundaries-5e8a5fd7b971
export const useAsyncError = () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [_, setError] = useState()
  return useCallback(
    (e: any) => {
      setError(() => {
        throw e
      })
    },
    [setError]
  )
}
