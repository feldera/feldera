/**
 * Component tests for the incident list. An incident's icon shows its status with the icon and
 * the text color of the status tone. A resolved incident keeps the icon, in a neutral color.
 */

import type { HealthEventBucket, HealthStatus } from 'common-ui/healthEvents'
import { EventLogList, healthStatusIcon } from 'common-ui'
import { statusToneColors } from 'common-ui/statusTone'
import { createRawSnippet } from 'svelte'
import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

type Bucket = HealthEventBucket<HealthStatus, 'pipeline'>

const incident = (
  type: HealthStatus,
  description: string,
  hour: number,
  active: boolean
): Bucket => {
  const timestamp = new Date(Date.UTC(2026, 4, 1, hour))
  return {
    timestampFrom: timestamp,
    timestampTo: timestamp,
    type,
    description,
    tag: 'pipeline',
    active,
    title: description,
    events: [{ id: `e${hour}`, timestamp, status: type }]
  }
}

const renderList = ({
  unresolvedEvents = [] as Bucket[],
  previousEvents = [] as Bucket[],
  onEventSelected = (_: Bucket) => {}
} = {}) =>
  render(EventLogList<HealthStatus, 'pipeline'>, {
    unresolvedEvents,
    previousEvents,
    onEventSelected,
    noIssues: createRawSnippet(() => ({ render: () => '<span>NO ISSUES</span>' }))
  })

/** The icon next to an incident description. */
const iconOf = (description: string) =>
  page.getByText(description).element().previousElementSibling!

describe('EventLogList', () => {
  it('shows an ongoing incident with the icon and the text color of its status', async () => {
    renderList({ unresolvedEvents: [incident('major_issue', 'Deployment failed', 1, true)] })
    await expect.element(page.getByText('1 ongoing incident')).toBeInTheDocument()
    const icon = iconOf('Deployment failed')
    expect(icon.className).toContain(healthStatusIcon('major_issue'))
    expect(icon.className).toContain(statusToneColors.error.healthIcon)
  })

  it('shows a resolved incident with the icon of its status and a neutral color', async () => {
    renderList({ previousEvents: [incident('unhealthy', 'Runtime unavailable', 1, false)] })
    await expect.element(page.getByText('Previous incidents')).toBeInTheDocument()
    const icon = iconOf('Runtime unavailable')
    expect(icon.className).toContain(healthStatusIcon('unhealthy'))
    expect(icon.className).toContain('text-surface-900-100')
  })

  it('counts several ongoing incidents', async () => {
    renderList({
      unresolvedEvents: [incident('major_issue', 'A', 2, true), incident('unhealthy', 'B', 1, true)]
    })
    await expect.element(page.getByText('2 ongoing incidents')).toBeInTheDocument()
  })

  it('shows the "no issues" content when there are no incidents', async () => {
    renderList()
    await expect.element(page.getByText('NO ISSUES')).toBeInTheDocument()
  })

  it('reports the incident that the user clicks', async () => {
    const onEventSelected = vi.fn()
    const bucket = incident('major_issue', 'Deployment failed', 1, true)
    renderList({ unresolvedEvents: [bucket], onEventSelected })
    // The list fills an absolute container of the size of its parent, which is zero here, so a
    // pointer click at screen coordinates misses the button.
    ;(page.getByText('Deployment failed').element() as HTMLElement).click()
    expect(onEventSelected).toHaveBeenCalledWith(bucket)
  })
})
