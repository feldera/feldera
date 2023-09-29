'use client'

import { useAuthentication } from '$lib/compositions/useAuth'
import { redirect } from 'next/navigation'

import type { ReactNode } from 'react'

export default (props: { children: ReactNode }) => {
  const auth = useAuthentication()

  if (!auth && (process.env.NODE_ENV !== 'development' || process.env.NEXT_PUBLIC_WEB_CONSOLE_TEST_AUTH)) {
    redirect('/login')
  }
  return props.children
}
