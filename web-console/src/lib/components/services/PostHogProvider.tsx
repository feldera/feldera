'use client'

import { usePathname, useSearchParams } from 'next/navigation'
import posthog, { PostHog } from 'posthog-js'
import { PostHogProvider } from 'posthog-js/react'
import { useEffect } from 'react'

import type { ReactNode } from 'react'

const Provider = (props: { client?: PostHog; children: ReactNode }) => {
  const pathname = usePathname()
  const searchParams = useSearchParams().toString()
  useEffect(() => {
    if (!pathname) {
      return
    }
    const url = window.origin + pathname + (searchParams ? '?' : '') + searchParams
    posthog.capture('$pageview', { $current_url: url })
  }, [pathname, searchParams])

  return <PostHogProvider client={props.client}>{props.children}</PostHogProvider>
}

export { Provider as PostHogProvider }
