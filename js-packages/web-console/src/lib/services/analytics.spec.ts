import { afterEach, describe, expect, it, vi } from 'vitest'

// vi.mock is hoisted above imports, so its factory may only touch vars created
// with vi.hoisted (also hoisted), not plain module-level consts.
const { capture, trackConceptualHq } = vi.hoisted(() => ({
  capture: vi.fn(),
  trackConceptualHq: vi.fn()
}))
vi.mock('posthog-js', () => ({ default: { capture } }))
vi.mock('$lib/services/conceptualHq', () => ({ trackConceptualHq }))

import { captureEvent } from './analytics'

afterEach(() => {
  capture.mockClear()
  trackConceptualHq.mockClear()
})

describe('captureEvent', () => {
  it('forwards the event and properties to both PostHog and ConceptualHQ', () => {
    const props = { demo: 'fraud', already_created: true, source: 'home' }
    captureEvent('demo_opened', props)
    expect(capture).toHaveBeenCalledExactlyOnceWith('demo_opened', props)
    expect(trackConceptualHq).toHaveBeenCalledExactlyOnceWith('demo_opened', props)
  })

  it('forwards events with no properties to both backends', () => {
    captureEvent('signin')
    expect(capture).toHaveBeenCalledExactlyOnceWith('signin', undefined)
    expect(trackConceptualHq).toHaveBeenCalledExactlyOnceWith('signin', undefined)
  })
})
