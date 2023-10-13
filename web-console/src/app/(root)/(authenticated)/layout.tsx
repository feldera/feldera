'use client'

import { useAuth } from '$lib/compositions/auth/useAuth'
import { redirect } from 'next/navigation'
import { ReactNode } from 'react'

export default (props: { children: ReactNode }) => {
  const { auth } = useAuth()
  if (auth === 'Unauthenticated') {
    redirect('/login')
  }

  return props.children
}
