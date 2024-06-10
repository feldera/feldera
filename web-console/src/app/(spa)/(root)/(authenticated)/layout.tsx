'use client'

import { useAuth } from '$lib/compositions/auth/useAuth'
import * as monaco from 'monaco-editor'
import { redirect } from 'next/navigation'
import { ReactNode } from 'react'
import { LS_PREFIX } from 'src/lib/types/localStorage'

import { useSessionStorage } from '@mantine/hooks'
import { loader } from '@monaco-editor/react'

export default (props: { children: ReactNode }) => {
  const [, setRedirectUrl, clearRedirectUrl] = useSessionStorage<string | undefined>({
    key: LS_PREFIX + 'redirect'
  })
  const { auth } = useAuth()
  if (auth === 'Unauthenticated') {
    setRedirectUrl(window.location.pathname + window.location.search + window.location.hash)
    redirect('/login')
  }
  const redirectUrl = window.sessionStorage.getItem(LS_PREFIX + 'redirect')?.slice(1, -1) // Trim quotes of a raw string
  if (redirectUrl && redirectUrl === window.location.pathname + window.location.search + window.location.hash) {
    clearRedirectUrl()
  }

  return props.children
}

// Configure monaco-editor to statically bundle required resources
// https://www.npmjs.com/package/@monaco-editor/react#use-monaco-editor-as-an-npm-package
loader.config({ monaco })
