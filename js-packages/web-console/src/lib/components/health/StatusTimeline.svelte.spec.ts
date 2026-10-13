/**
 * Component tests for the status timeline.
 *
 * The header reports the status of the newest event. When the cluster monitor stops writing
 * events, that status can stay "Operational" for a long time. The `stale` prop replaces it with
 * "No data".
 *
 * The bars, the legend and the header take their colors from the shared status tone table, so
 * the tests compare them with `statusToneColors` instead of with literal classes.
 */

import type { HealthStatus } from 'common-ui/healthEvents'
import { StatusTimeline, type TimelineEvent } from 'common-ui'
import { statusToneColors } from 'common-ui/statusTone'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'

const recordedAt = new Date('2026-05-01T12:00:00Z')
const hourMs = 60 * 60 * 1000

const event = (type: HealthStatus, minutesAgo = 0): TimelineEvent<HealthStatus> => ({
  timestamp: new Date(recordedAt.getTime() - minutesAgo * 60 * 1000),
  type,
  description: ''
})

const renderTimeline = ({
  events = [event('healthy')],
  stale = false,
  updatedText = null,
  legend = []
}: {
  /** Newest first, as the API returns them. */
  events?: TimelineEvent<HealthStatus>[]
  stale?: boolean
  updatedText?: string | null
  legend?: HealthStatus[]
} = {}) =>
  render(StatusTimeline<HealthStatus>, {
    // `events` is both a prop here and a Svelte mount option, so every prop goes under `props`.
    props: {
      label: 'Runner',
      events,
      // Two one-hour bars: index 0 is 2h to 1h ago, index 1 is the last hour.
      startAt: new Date(recordedAt.getTime() - 2 * hourMs),
      endAt: recordedAt,
      unitDurationMs: hourMs,
      legend,
      stale,
      updatedText
    }
  })

/** The colored rect of a bar. The first rect of a bar only catches the pointer. */
const barRect = (index: number) =>
  document.querySelectorAll<SVGRectElement>(`#timeline-bar-${index} rect`)[1]

describe('StatusTimeline header', () => {
  it('reports the newest status while the data is fresh', async () => {
    renderTimeline()
    await expect.element(document.body).toHaveTextContent('Runner — Operational')
  })

  it('colors the status label with the text color of its tone', async () => {
    const { container } = renderTimeline({ events: [event('major_issue')] })
    await expect.element(document.body).toHaveTextContent('Runner — Major Issue')
    const label = [...container.querySelectorAll('span')].find(
      (s) => s.textContent === 'Major Issue'
    )
    expect(label?.className).toBe(statusToneColors.error.healthText)
  })

  it('reports no data instead of a stale status', async () => {
    renderTimeline({ stale: true })
    await expect.element(document.body).toHaveTextContent('Runner — No data')
    expect(document.body.textContent).not.toContain('Operational')
  })

  it('names the issue the stale data recorded', async () => {
    renderTimeline({ stale: true, events: [event('major_issue')] })
    await expect
      .element(document.body)
      .toHaveTextContent('Runner — No data (last recorded: Major Issue)')
  })

  it('does not name a stale status that is not an issue', async () => {
    renderTimeline({ stale: true, events: [event('idle')] })
    await expect.element(document.body).toHaveTextContent('Runner — No data')
    expect(document.body.textContent).not.toContain('last recorded')
  })

  it('shows the updated text after the status', async () => {
    renderTimeline({ updatedText: 'updated 10m ago' })
    await expect.element(document.body).toHaveTextContent('Runner — Operational — updated 10m ago')
  })
})

describe('StatusTimeline bars', () => {
  it('fills a bar with the bar color of its status tone', async () => {
    renderTimeline({ events: [event('healthy', 30)] })
    await expect
      .poll(() => barRect(1).getAttribute('class'))
      .toContain(statusToneColors.jade.healthBar)
  })

  it('fills a bar with mixed statuses with the color of the most severe one', async () => {
    renderTimeline({
      events: [event('healthy', 10), event('unhealthy', 20), event('transitioning', 30)]
    })
    await expect
      .poll(() => barRect(1).getAttribute('class'))
      .toContain(statusToneColors.warning.healthBar)
  })

  it('fills a bar without events with the "no data" color', async () => {
    renderTimeline({ events: [event('healthy', 30)] })
    await expect.poll(() => barRect(0).getAttribute('class')).toContain('fill-surface-300-700')
  })

  it('fills a bar under the pointer with the highlight color of its status tone', async () => {
    renderTimeline({ events: [event('healthy', 30)] })
    const bar = document.querySelector('#timeline-bar-1')!
    const { healthBar, healthBarHighlight } = statusToneColors.jade
    expect(barRect(1).getAttribute('class')).toContain(healthBar)
    bar.dispatchEvent(new MouseEvent('mouseenter'))
    await expect.poll(() => barRect(1).getAttribute('class')).toContain(healthBarHighlight)
    expect(barRect(1).getAttribute('class')).not.toContain(healthBar)
  })
})

describe('StatusTimeline legend', () => {
  it('lists "No data" and then each status with the dot color of its tone', async () => {
    const { container } = renderTimeline({ legend: ['healthy', 'major_issue'] })
    await expect.element(document.body).toHaveTextContent('No data Operational Major Issue')
    const swatches = [...container.querySelectorAll('.rounded-md')].map((e) => e.className)
    expect(swatches).toEqual([
      expect.stringContaining('bg-surface-300-700'),
      expect.stringContaining(statusToneColors.jade.healthDot),
      expect.stringContaining(statusToneColors.error.healthDot)
    ])
  })

  it('hides the legend when it lists no statuses', async () => {
    renderTimeline()
    await expect.element(document.body).toHaveTextContent('Runner — Operational')
    expect(document.body.textContent).not.toContain('No data')
  })
})
