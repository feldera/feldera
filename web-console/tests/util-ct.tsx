import React, { ReactNode } from 'react'

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

const queryClient = new QueryClient({})

export const Provider = (props: { children: ReactNode }) => {
  return <QueryClientProvider client={queryClient}>{props.children}</QueryClientProvider>
}
