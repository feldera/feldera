'use client'

import { useAuth } from '$lib/compositions/auth/useAuth'
import * as monaco from 'monaco-editor'
import { redirect } from 'next/navigation'
import { ReactNode } from 'react'

import { loader } from '@monaco-editor/react'

export default (props: { children: ReactNode }) => {
  const { auth } = useAuth()
  if (auth === 'Unauthenticated') {
    redirect('/login')
  }

  return props.children
}

// Configure monaco-editor to statically bundle required resources
// https://www.npmjs.com/package/@monaco-editor/react#use-monaco-editor-as-an-npm-package
loader.config({ monaco })
