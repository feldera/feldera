/**
 * Component tests for the details of one incident or one timeline bar. Each event row has a dot
 * with the dot color of its status tone. The first row opens on mount and loads its details.
 */

import type { HealthEventBucket, HealthStatus } from 'common-ui/healthEvents'
import { HealthEventList } from 'common-ui'
import { statusToneColors } from 'common-ui/statusTone'
import { describe, expect, it, vi } from 'vitest'
import { page, userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const at = (hour: number) => new Date(Date.UTC(2026, 4, 1, hour))

const bucket: HealthEventBucket<HealthStatus, 'pipeline'> = {
  timestampFrom: at(1),
  timestampTo: at(2),
  type: 'major_issue',
  description: 'Pipeline incident',
  tag: 'pipeline',
  active: true,
  title: 'Pipeline deployment error',
  events: [
    { id: 'e2', timestamp: at(2), status: 'major_issue' },
    { id: 'e1', timestamp: at(1), status: 'transitioning' }
  ]
}

type EventDetail = { timestamp: Date; description: string }

const renderDetails = (
  loadEventDetail: (eventId: string) => Promise<EventDetail | null> = vi.fn(async (eventId) => ({
    timestamp: at(2),
    description: `details of ${eventId}`
  })),
  onClose = vi.fn()
) => {
  render(HealthEventList<HealthStatus, 'pipeline'>, {
    eventParts: bucket,
    loadEventDetail,
    onClose
  })
  return { loadEventDetail, onClose }
}

describe('HealthEventList', () => {
  it('marks each event with the dot color of its status tone', async () => {
    const { container } = render(HealthEventList<HealthStatus, 'pipeline'>, {
      eventParts: bucket,
      loadEventDetail: async () => null,
      onClose: () => {}
    })
    await expect.element(page.getByText('Pipeline deployment error')).toBeInTheDocument()
    const dots = [...container.querySelectorAll('.rounded-full')].map((e) => e.className)
    expect(dots).toEqual([
      expect.stringContaining(statusToneColors.error.healthDot),
      expect.stringContaining(statusToneColors.blue.healthDot)
    ])
  })

  it('loads and shows the details of the first event only', async () => {
    const { loadEventDetail } = renderDetails()
    await expect.element(page.getByText('details of e2')).toBeInTheDocument()
    expect(loadEventDetail).toHaveBeenCalledTimes(1)
    expect(loadEventDetail).toHaveBeenCalledWith('e2', bucket)
  })

  it.each([
    ['returns no details', async () => null],
    ['fails', async () => Promise.reject(new Error('not found'))]
  ])('asks for the details of an event once when the loader %s', async (_, outcome) => {
    let calls = 0
    // After a few calls the loader never settles, so a retry loop stops instead of hanging.
    const loadEventDetail = vi.fn(() => (++calls <= 3 ? outcome() : new Promise<never>(() => {})))
    renderDetails(loadEventDetail)
    await expect
      .element(page.getByText('The details of this event are not available.'))
      .toBeInTheDocument()
    expect(loadEventDetail).toHaveBeenCalledTimes(1)
  })

  it('closes on Escape', async () => {
    const { onClose } = renderDetails()
    await expect.element(page.getByText('Pipeline deployment error')).toBeInTheDocument()
    await userEvent.keyboard('{Escape}')
    expect(onClose).toHaveBeenCalled()
  })
})
