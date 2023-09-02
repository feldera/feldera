import { ApiError } from '$lib/services/manager'

import { QueryClient, Updater, UseQueryOptions } from '@tanstack/react-query'

type FunctionType = (...args: any) => any
type Arguments<F extends FunctionType> = F extends (...args: infer A) => any ? A : never

type QueryType<U extends Record<string, FunctionType>, P extends keyof U> = {
  queryKey: readonly unknown[]
  queryFn: () => ReturnType<U[P]>
} & Pick<UseQueryOptions<ReturnType<U[P]>, ApiError, Awaited<ReturnType<U[P]>>, readonly unknown[]>, 'onError'>

export const mkQuery = <U extends Record<string, FunctionType>>(
  source: U
): { [P in keyof U]: (...args: Arguments<U[P]>) => QueryType<U, P> } =>
  Object.fromEntries(
    Object.entries(source).map(([key, value]) => {
      return [
        key,
        (...args: unknown[]) => {
          return {
            queryKey: [key, ...args], // key + '/' + args.map(String).join('/'),
            queryFn: () => value(...args)
          }
        }
      ]
    })
  ) as any

export const invalidateQuery = (queryClient: QueryClient, query: { queryKey: readonly unknown[] }) =>
  queryClient.invalidateQueries(query.queryKey)

export const setQueryData = <R>(
  queryClient: QueryClient,
  query: { queryKey: readonly unknown[]; queryFn: () => Promise<R> },
  data: Updater<R | undefined, R | undefined>
) => queryClient.setQueryData(query.queryKey, data)
