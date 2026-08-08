// The stash both the login redirect and the acting-tenant gate use to remember
// the page they interrupted.

import { beforeEach, describe, expect, it } from 'vitest'
import { stashRedirectTarget, takeRedirectTarget } from './redirectTarget'

const store = new Map<string, string>()

beforeEach(() => {
  store.clear()
  globalThis.window = {
    sessionStorage: {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, v),
      removeItem: (k: string) => void store.delete(k)
    }
  } as any
})

describe('redirect target', () => {
  it('is undefined when nothing was interrupted', () => {
    expect(takeRedirectTarget()).toBeUndefined()
  })

  it('round-trips the stashed page', () => {
    stashRedirectTarget('http://localhost/pipelines/foo/')
    expect(takeRedirectTarget()).toBe('http://localhost/pipelines/foo/')
  })

  it('keeps the first write, so a fallback navigation cannot overwrite it', () => {
    stashRedirectTarget('http://localhost/pipelines/foo/')
    stashRedirectTarget('http://localhost/')
    expect(takeRedirectTarget()).toBe('http://localhost/pipelines/foo/')
  })

  it('clears on read, so a later redirect cannot reuse a stale target', () => {
    stashRedirectTarget('http://localhost/pipelines/foo/')
    takeRedirectTarget()
    expect(takeRedirectTarget()).toBeUndefined()
  })
})
