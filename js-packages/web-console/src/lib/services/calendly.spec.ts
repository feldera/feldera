import { describe, expect, it } from 'vitest'

import { bookADemoUrl } from './calendly'

const DEMO_URL = 'https://calendly.com/d/cxz2-37b-qqd/feldera-demo-30min'

const paramsOf = (url: string) => Object.fromEntries(new URL(url).searchParams)

describe('bookADemoUrl', () => {
  it('tags the link with the visitor ID and the placement', () => {
    expect(paramsOf(bookADemoUrl({ visitorId: 'dev-123', placement: 'footer' }))).toEqual({
      utm_content: 'dev-123',
      // The prefix keeps console placements apart from the website's.
      utm_term: 'try.feldera.com:footer'
    })
  })

  it('carries the placement even when the visitor is unknown', () => {
    // The booking cannot be tied to a visitor, but sales still sees its source.
    expect(bookADemoUrl({ visitorId: '', placement: 'footer' })).toBe(
      `${DEMO_URL}?utm_term=try.feldera.com%3Afooter`
    )
  })

  it('carries the visitor ID when no placement is given', () => {
    // No placement, no prefix: a bare prefix would name no button at all.
    expect(bookADemoUrl({ visitorId: 'dev-123' })).toBe(`${DEMO_URL}?utm_content=dev-123`)
  })

  it('percent-encodes values holding URL-significant characters', () => {
    const url = bookADemoUrl({ visitorId: 'a&b=c d', placement: 'x&y' })
    // A raw '&' or '=' would split into bogus extra parameters.
    expect(url).toBe(`${DEMO_URL}?utm_content=a%26b%3Dc+d&utm_term=try.feldera.com%3Ax%26y`)
    expect(paramsOf(url)).toEqual({ utm_content: 'a&b=c d', utm_term: 'try.feldera.com:x&y' })
  })

  it('leaves the link untagged when nothing is known', () => {
    expect(bookADemoUrl({ visitorId: '' })).toBe(DEMO_URL)
  })
})
