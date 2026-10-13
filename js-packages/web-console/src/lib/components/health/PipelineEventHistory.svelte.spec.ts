/**
 * Component tests for the pipeline event history, which the pipeline Health tab and the
 * support bundle viewer show.
 */

import { formatDateTime } from 'common-lib/format'
import type { PipelineMonitorEvent } from 'common-ui/pipelineHealth'
import { PipelineEventHistory } from 'common-ui'
import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const now = new Date('2026-05-01T12:30:00Z')

let nextId = 1
/** An event of a running, healthy pipeline, recorded `minutesAgo` minutes before `now`. */
const makeEvent = (
  minutesAgo: number,
  overrides: Partial<PipelineMonitorEvent> = {}
): PipelineMonitorEvent => ({
  event_id: `e${nextId++}`,
  recorded_at: new Date(now.getTime() - minutesAgo * 60 * 1000).toISOString(),
  program_status: 'Success',
  storage_status: 'InUse',
  deployment_has_error: false,
  deployment_resources_status: 'Provisioned',
  deployment_resources_desired_status: 'Provisioned',
  deployment_runtime_status: 'Running',
  deployment_runtime_desired_status: 'Running',
  ...overrides
})

const failed = (minutesAgo: number) =>
  makeEvent(minutesAgo, {
    deployment_has_error: true,
    deployment_error: { message: 'boom', error_code: 'E1' }
  })

const renderHistory = (
  props: Partial<{
    events: PipelineMonitorEvent[] | null
    loading: boolean
    now: Date
    updatedText: string | null
    loadEvent: (eventId: string) => Promise<PipelineMonitorEvent | null | undefined>
  }> = {}
) =>
  render(PipelineEventHistory, {
    // `events` is both a prop here and a Svelte mount option, so every prop goes under `props`.
    props: { events: [], loading: false, now, ...props }
  })

/** Opens the details of the incident with this description. */
const openIncident = (description: string) =>
  // The list fills an absolute container, so a pointer click at screen coordinates can miss.
  (page.getByText(description).element() as HTMLElement).click()

describe('PipelineEventHistory', () => {
  it('reports the status of the newest event', async () => {
    renderHistory({ events: [makeEvent(10), failed(20)] })
    await expect.element(document.body).toHaveTextContent('Pipeline status — Operational')
  })

  it('lists an incident that has not ended as ongoing', async () => {
    renderHistory({ events: [failed(10), failed(20), makeEvent(30)] })
    await expect.element(page.getByText('1 ongoing incident')).toBeInTheDocument()
    await expect.element(page.getByText('Pipeline incident: deployment error')).toBeInTheDocument()
  })

  it('lists an incident that a healthy event ended as previous', async () => {
    renderHistory({ events: [makeEvent(10), failed(20)] })
    await expect.element(page.getByText('Previous incidents')).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('ongoing')
  })

  it('reports no issues when the events have no incidents', async () => {
    renderHistory({ events: [makeEvent(10)] })
    await expect
      .element(page.getByText('The pipeline experienced no issues in the observed period.'))
      .toBeInTheDocument()
  })

  it('does not report "no issues" before the events are loaded', async () => {
    renderHistory({ events: null, loading: true })
    await expect.element(page.getByRole('progressbar')).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('no issues')
  })

  it('shows a progress bar only while loading', async () => {
    renderHistory({ events: [makeEvent(10)] })
    await expect.element(document.body).toHaveTextContent('Pipeline status — Operational')
    expect(page.getByRole('progressbar').elements()).toHaveLength(0)
  })

  it('shows the updated text in the timeline header', async () => {
    renderHistory({ events: [makeEvent(10)], updatedText: 'updated 5m ago' })
    await expect
      .element(document.body)
      .toHaveTextContent('Pipeline status — Operational — updated 5m ago')
  })

  it.each([
    ['ends at the hour after now', [makeEvent(10)], new Date('2026-05-01T13:00:00Z')],
    // The clock of the browser can be behind the clock that recorded the events.
    [
      'ends at the hour after an event newer than now',
      [makeEvent(-60)],
      new Date('2026-05-01T14:00:00Z')
    ],
    ['ends at the hour after now without events', [], new Date('2026-05-01T13:00:00Z')]
  ])('shows 72 hours that %s', async (_, events, endAt) => {
    renderHistory({ events })
    const startAt = new Date(endAt.getTime() - 72 * 60 * 60 * 1000)
    await expect
      .element(document.body)
      .toHaveTextContent(`${formatDateTime(startAt)} - ${formatDateTime(endAt)}`)
  })

  it('shows the details of an incident from the events by default', async () => {
    renderHistory({ events: [failed(10), makeEvent(30)] })
    await openIncident('Pipeline incident: deployment error')
    await expect.element(page.getByText('Pipeline deployment error')).toBeInTheDocument()
    await expect.element(document.body).toHaveTextContent('deployment_error.message: boom')
  })

  it('loads the details of an event with loadEvent', async () => {
    const event = failed(10)
    const loadEvent = vi.fn(async () => ({
      ...event,
      deployment_error: { message: 'loaded from the API' }
    }))
    renderHistory({ events: [event, makeEvent(30)], loadEvent })
    await openIncident('Pipeline incident: deployment error')
    await expect
      .element(document.body)
      .toHaveTextContent('deployment_error.message: loaded from the API')
    expect(loadEvent).toHaveBeenCalledWith(event.event_id)
  })
})
