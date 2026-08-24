/**
 * Component tests for the timeline header. The header reports the newest event's status,
 * which keeps claiming "Operational" long after the cluster monitor stopped writing; the
 * `stale` prop is what stops it.
 */
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import type { ClusterEventType, ClusterRawEvent } from '$lib/functions/pipelines/health'
import StatusTimeline from './StatusTimeline.svelte'

const recordedAt = new Date('2026-05-01T12:00:00Z')

const statusStyles: Record<ClusterEventType, { bg: string; text: string; label: string }> = {
  healthy: { bg: 'bg-green-500', text: 'text-green-500', label: 'Operational' },
  unhealthy: { bg: 'bg-yellow-500', text: 'text-yellow-500', label: 'Service degradation' },
  major_issue: { bg: 'bg-red-500', text: 'text-red-500', label: 'Major Issue' }
}

const healthyEvent: ClusterRawEvent = {
  timestamp: recordedAt,
  type: 'healthy',
  description: 'The runner is healthy.',
  tag: 'runner',
  id: 'evt-1'
}

const renderTimeline = (stale: boolean, event: ClusterRawEvent = healthyEvent) =>
  render(StatusTimeline<ClusterEventType>, {
    // `events` is both a prop here and a Svelte mount option, so every prop goes under `props`.
    props: {
      label: 'Runner',
      events: [event],
      startAt: new Date(recordedAt.getTime() - 60 * 60 * 1000),
      endAt: recordedAt,
      unitDurationMs: 60 * 60 * 1000,
      legend: [],
      stale,
      getSeverity: (status) => (status === 'healthy' ? 0 : 1),
      getBarColor: () => 'fill-green-500',
      getStatusStyle: (status) => statusStyles[status]
    }
  })

describe('StatusTimeline', () => {
  it('reports the newest status while the data is fresh', async () => {
    renderTimeline(false)
    await expect.element(document.body).toHaveTextContent('Runner — Operational')
  })

  it('reports no data instead of a stale status', async () => {
    renderTimeline(true)
    await expect.element(document.body).toHaveTextContent('Runner — No data')
    expect(document.body.textContent).not.toContain('Operational')
  })

  it('names the issue the stale data recorded', async () => {
    renderTimeline(true, { ...healthyEvent, type: 'major_issue' })
    await expect
      .element(document.body)
      .toHaveTextContent('Runner — No data (last recorded: Major Issue)')
  })
})
