/**
 * This file contains utility functions that simplify usage of Tanstack Query
 * in a type-safe way
 */

import { ApiError } from '$lib/services/manager'
import { Arguments, FunctionType } from '$lib/types/common/function'

import { InvalidateQueryFilters, QueryClient, QueryFilters, Updater, UseQueryOptions } from '@tanstack/react-query'

type QueryType<U extends Record<string, FunctionType>, P extends keyof U> = {
  queryKey: readonly unknown[]
  queryFn: () => ReturnType<U[P]>
} & UseQueryOptions<Awaited<ReturnType<U[P]>>, ApiError, Awaited<ReturnType<U[P]>>, readonly unknown[]>

/**
 * Construct a set of query objects with queryKey and queryFn,
 * bound to the same names as the fields of an argument object
 *
 * Fields of source object become a prefix for a corresponding queryKey
 * Functions' arguments are prepended as-is in a list to the queryKey
 * @param source
 * @returns
 */
export const mkQuery = <U extends Record<string, FunctionType>>(
  source: U
): { [P in keyof U]: (...args: Arguments<U[P]>) => QueryType<U, P> } =>
  Object.fromEntries(
    Object.entries(source).map(([key, value]) => {
      return [
        key,
        (...args: unknown[]) => {
          return {
            queryKey: [key, ...args],
            queryFn: () => value(...args)
          }
        }
      ]
    })
  ) as any

export const invalidateQuery = (queryClient: QueryClient, query: InvalidateQueryFilters) =>
  queryClient.invalidateQueries(query)

export type QueryData<Query extends { queryFn: (...args: any) => any }> = Awaited<ReturnType<Query['queryFn']>>

export const setQueryData = <R>(
  queryClient: QueryClient,
  query: { queryKey: readonly unknown[]; queryFn: () => Promise<R> },
  data: Updater<R | undefined, R | undefined>
) => queryClient.setQueryData(query.queryKey, data)

export const getQueryData = <R>(
  queryClient: QueryClient,
  query: { queryKey: readonly unknown[]; queryFn: () => Promise<R> },
  filters?: QueryFilters
) => queryClient.getQueriesData<R>({ ...filters, queryKey: query.queryKey })[0][1]
