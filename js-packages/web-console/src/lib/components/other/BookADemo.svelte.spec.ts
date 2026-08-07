// Component tests for the demo link's visitor-ID tag: the Calendly URL must
// carry the analytics visitor ID so Calendly can forward it to Zapier/HubSpot
// with the booking. On a first visit the ID only exists once the analytics
// loader lands, which is after the first render, so the link has to retag
// itself reactively.

import { createRawSnippet } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { refreshConceptualHqDeviceId } from '$lib/compositions/useConceptualHq.svelte'

const captureEvent = vi.hoisted(() => vi.fn())

vi.mock('$lib/services/analytics', () => ({ captureEvent }))

import BookADemo from './BookADemo.svelte'

const DEMO_URL = 'https://calendly.com/d/cxz2-37b-qqd/feldera-demo-30min'

// Stands in for the loader script having installed the full `ca` API.
const loaderReports = (deviceId: string) => {
  window.ca = Object.assign(() => {}, { q: [], getDeviceId: () => deviceId })
  refreshConceptualHqDeviceId()
}

// Link label. The default icon alone is an empty webfont span, which playwright
// treats as invisible and refuses to click.
const label = createRawSnippet(() => ({ render: () => '<span>Book a demo</span>' }))

// Keep clicks from opening the real Calendly tab.
const swallowNavigation = () => {
  const cancel = (event: Event) => event.preventDefault()
  document.addEventListener('click', cancel, { capture: true })
  return () => document.removeEventListener('click', cancel, { capture: true })
}

afterEach(() => {
  window.ca = undefined
  refreshConceptualHqDeviceId()
  captureEvent.mockClear()
})

describe('BookADemo.svelte', () => {
  it('tags the link with the visitor ID known at render', async () => {
    loaderReports('dev-123')
    render(BookADemo, { children: label })

    await expect
      .element(page.getByRole('link'))
      .toHaveAttribute('href', `${DEMO_URL}?utm_content=dev-123`)
  })

  it('tags the link with the placement of the button', async () => {
    loaderReports('dev-123')
    render(BookADemo, { placement: 'footer', children: label })

    await expect
      .element(page.getByRole('link'))
      .toHaveAttribute('href', `${DEMO_URL}?utm_content=dev-123&utm_term=try.feldera.com%3Afooter`)
  })

  it('links to the untagged URL when neither the visitor nor the placement is known', async () => {
    render(BookADemo, { children: label })

    await expect.element(page.getByRole('link')).toHaveAttribute('href', DEMO_URL)
  })

  it('retags the link when the visitor ID arrives after rendering', async () => {
    render(BookADemo, { placement: 'footer', children: label })
    const link = page.getByRole('link')
    await expect
      .element(link)
      .toHaveAttribute('href', `${DEMO_URL}?utm_term=try.feldera.com%3Afooter`)

    loaderReports('late-loader')

    // Reading the ID once at render leaves the href unattributed here, and the
    // booking would reach HubSpot without a visitor.
    await expect
      .element(link)
      .toHaveAttribute(
        'href',
        `${DEMO_URL}?utm_content=late-loader&utm_term=try.feldera.com%3Afooter`
      )
  })

  it('reports the tagged URL and the placement to analytics on click', async () => {
    const restore = swallowNavigation()
    try {
      loaderReports('dev-123')
      render(BookADemo, { placement: 'footer', children: label })

      await page.getByRole('link').click()

      expect(captureEvent).toHaveBeenCalledWith('calendly_opened', {
        url: `${DEMO_URL}?utm_content=dev-123&utm_term=try.feldera.com%3Afooter`,
        placement: 'footer'
      })
    } finally {
      restore()
    }
  })
})
