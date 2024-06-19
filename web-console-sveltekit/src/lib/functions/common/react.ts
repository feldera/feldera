import { useCallback, useEffect, useState } from 'react'
import { create, StateCreator } from 'zustand'

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

// https://docs.pmnd.rs/zustand/integrations/persisting-store-data#usage-in-next.js
export const makeClientStore =
  <T, Init extends StateCreator<T, [], []> | undefined>(
    store: (selector: (state: T) => unknown) => unknown,
    initializer?: Init
  ) =>
  <R>(selector: (state: T) => R) => {
    const result = store(selector) as R
    /* eslint-disable react-hooks/rules-of-hooks */
    const [data, setData] = useState<R>()

    useEffect(() => {
      setData(result)
    }, [result])

    return (
      data ??
      ((initializer ? create<T>()(initializer)(selector) : undefined) as Init extends undefined
        ? R | undefined
        : R)
    )
  }
