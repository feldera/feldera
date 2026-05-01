/**
 * Component tests for the Health monitoring tab. The tab is fed by
 * `usePipelineManager` (network) and `pipeline.current` (write-through state).
 * Both are mocked here so the test runs fully offline.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineMonitorEventSelectedInfo } from '$lib/services/manager'
import type { ExtendedPipeline } from '$lib/services/pipelineManager'

// --- Mock usePipelineManager so the tab can fetch events offline ---

const getPipelineEventsMock = vi.fn<(name: string) => Promise<PipelineMonitorEventSelectedInfo[]>>()
const getPipelineEventMock =
  vi.fn<(name: string, id: string) => Promise<PipelineMonitorEventSelectedInfo>>()

vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({
    getPipelineEvents: getPipelineEventsMock,
    getPipelineEvent: getPipelineEventMock
  })
}))

// Imported AFTER vi.mock so the mock takes effect.
import TabHealth from './TabHealth.svelte'

// --- Factories ---

let nextId = 1
const freshId = () => `evt-${nextId++}`

function makeEvent(
  overrides: Partial<PipelineMonitorEventSelectedInfo> = {}
): PipelineMonitorEventSelectedInfo {
  // Spread overrides last so explicit nullish overrides are preserved.
  return {
    event_id: freshId(),
    recorded_at: '2026-05-01T12:00:00Z',
    program_status: 'Success',
    deployment_resources_desired_status: 'Provisioned',
    deployment_resources_status: 'Provisioned',
    deployment_runtime_desired_status: 'Running',
    deployment_runtime_status: 'Running',
    deployment_has_error: false,
    storage_status: 'InUse',
    ...overrides
  }
}

function pipelineProp(name = 'p-test'): { current: ExtendedPipeline } {
  return { current: { name } as ExtendedPipeline }
}

async function renderTab(events: PipelineMonitorEventSelectedInfo[], deleted = false) {
  getPipelineEventsMock.mockResolvedValue(events)
  return render(TabHealth, { pipeline: pipelineProp(), deleted })
}

// --- Tests ---

describe('TabHealth.svelte', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    getPipelineEventMock.mockImplementation(async (_name, eventId) =>
      makeEvent({
        event_id: eventId,
        deployment_has_error: true,
        deployment_error: { message: 'kaboom', error_code: 'BoomCode', details: '' }
      })
    )
  })

  describe('A. Basic rendering', () => {
    it('renders the health tab container and the timeline label', async () => {
      await renderTab([makeEvent()])
      await expect.element(page.getByTestId('box-pipeline-health')).toBeInTheDocument()
      await expect.element(page.getByText('Pipeline status')).toBeInTheDocument()
    })

    it('shows the "no issues" message when every event is healthy', async () => {
      await renderTab([
        makeEvent({ recorded_at: '2026-05-01T10:00:00Z' }),
        makeEvent({ recorded_at: '2026-05-01T11:00:00Z' })
      ])
      await expect
        .element(page.getByText('The pipeline experienced no issues in the observed period.'))
        .toBeInTheDocument()
    })
  })

  describe('B. Incident classification', () => {
    it('renders an ongoing incident when the most recent event has deployment_has_error', async () => {
      await renderTab([
        // API returns newest-first; the first entry drives the timeline window.
        makeEvent({
          recorded_at: '2026-05-01T12:00:00Z',
          deployment_has_error: true,
          deployment_error: { message: 'pod crashed', error_code: 'PodCrashLoop', details: '' }
        }),
        makeEvent({ recorded_at: '2026-05-01T10:00:00Z' })
      ])

      await expect.element(page.getByText('1 ongoing incident')).toBeInTheDocument()
      await expect
        .element(page.getByText('Pipeline incident: deployment error'))
        .toBeInTheDocument()
    })

    it('lists incidents under "Previous incidents" once they have been resolved', async () => {
      await renderTab([
        // newest-first: a healthy event closes the prior incident
        makeEvent({ recorded_at: '2026-05-01T12:00:00Z' }),
        makeEvent({
          recorded_at: '2026-05-01T11:00:00Z',
          program_status: 'SqlError'
        })
      ])

      await expect.element(page.getByText('Previous incidents')).toBeInTheDocument()
      await expect
        .element(page.getByText('Pipeline incident: compilation error'))
        .toBeInTheDocument()
      await expect.element(page.getByText('1 ongoing incident')).not.toBeInTheDocument()
    })
  })

  describe('C. Event detail drawer', () => {
    it('opens the drawer when an incident is clicked and closes it via the X button', async () => {
      await renderTab([
        makeEvent({
          recorded_at: '2026-05-01T12:00:00Z',
          deployment_has_error: true,
          deployment_error: { message: 'kaboom', error_code: 'BoomCode', details: '' }
        })
      ])

      // Wait for the incident to appear, then issue a native DOM click on
      // its <button>. We bypass Playwright's pointer-event path because the
      // EventLogList content is laid out inside a collapsed-height container
      // in this test environment, which makes Playwright's actionability
      // checks reject the click.
      const incident = page.getByText('Pipeline incident: deployment error')
      await expect.element(incident).toBeInTheDocument()
      const incidentEl = incident.element()
      ;(incidentEl.closest('button') ?? (incidentEl as HTMLElement)).click()

      // Drawer header shows the incident title.
      await expect.element(page.getByText('Pipeline deployment error')).toBeInTheDocument()

      // The detail loader is exercised when the dropdown auto-opens the first
      // event — verify the formatted description reaches the panel.
      await expect.element(page.getByText('deployment_error.message: kaboom')).toBeInTheDocument()
      expect(getPipelineEventMock).toHaveBeenCalled()

      // Close via the X button (aria-label set in HealthEventList).
      ;(page.getByLabelText('Close event details').element() as HTMLButtonElement).click()
      await expect.element(page.getByText('Pipeline deployment error')).not.toBeInTheDocument()
    })
  })
})
