/**
 * Tests how the profile viewer's loader parses the URL's query parameters.
 *
 * The profile loading logic branches on two query params:
 * - `source`: 'upload' | 'remote'. Any other value reads as 'remote'.
 * - `bundle`: a positive integer id | `undefined` when absent | 'invalid' otherwise.
 * If the `bundle` param is invalid an error message is shown.
 */

import type { LoadEvent } from '@sveltejs/kit'
import { describe, expect, it } from 'vitest'
import { load } from './+page'

const loadFrom = (query: string) =>
  load({ url: new URL(`http://localhost/profile-viewer${query}`) } as LoadEvent)

describe('profile viewer loader', () => {
  it('reads a bundle link', () => {
    const data = loadFrom('?source=upload&bundle=42')

    expect(data.source).toBe('upload')
    expect(data.bundle).toBe(42)
  })

  it('leaves the bundle unset when the URL names none', () => {
    const data = loadFrom('?source=upload&channel=c-1')

    expect(data.bundle).toBeUndefined()
    expect(data.channel).toBe('c-1')
  })

  it.each([
    'abc',
    '',
    '0',
    '-1',
    '1.5',
    '1e3000'
  ])('reports %o as an id that names no entry', (raw) => {
    // A truncated or hand-edited id must not read as an absent one: the page would
    // then wait for a handoff nobody is making, and time out with an error about a
    // tab the user never opened.
    expect(loadFrom(`?source=upload&bundle=${raw}`).bundle).toBe('invalid')
  })

  it('reads any source other than upload as remote', () => {
    // The page treats these two values as exhaustive, so a stale or mistyped source
    // has to land on one of them.
    expect(loadFrom('?source=uploaded').source).toBe('remote')
    expect(loadFrom('').source).toBe('remote')
    expect(loadFrom('?source=remote').source).toBe('remote')
  })

  it('collects new data unless the URL turns it off', () => {
    expect(loadFrom('').collect).toBe(true)
    expect(loadFrom('?collect=1').collect).toBe(true)
    expect(loadFrom('?collect=0').collect).toBe(false)
  })

  it('reads the pipeline name', () => {
    expect(loadFrom('?pipelineName=my%20pipeline').pipelineName).toBe('my pipeline')
    expect(loadFrom('').pipelineName).toBe('')
  })
})
