import { expect, test } from '@playwright/test'
import { putPipeline } from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  clearAndDeletePipeline,
  configureTestClient,
  killPipelineAndWaitForStopped,
  startPipelineAndWaitForRunning,
  TEST_COMPILATION_PROFILE,
  waitForCompilation,
  waitForPipeline
} from '$lib/services/testPipelineHelpers'

configureTestClient()

// The status chip follows `usePipelineList`, which polls every 2 s, so the DOM
// trails the API by up to one poll plus request latency.
const STATUS_CHIP_TIMEOUT_MS = 6_000

const PREFIX = `test-name-tooltip-${Date.now()}`
const PIPELINE_NAME = `${PREFIX}`

test.describe('Pipeline name edit tooltip', () => {
  test.setTimeout(120_000)

  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(60_000)
    await putPipeline(PIPELINE_NAME, {
      name: PIPELINE_NAME,
      description: 'E2E test pipeline for name-edit tooltip',
      program_code: 'create view v as (select 1)',
      program_config: { profile: TEST_COMPILATION_PROFILE }
    })
    await waitForPipeline(PIPELINE_NAME, (p) => p.status === 'Stopped', 60_000)
  })

  test.afterAll(async () => {
    await cleanupPipeline(PIPELINE_NAME)
  })

  test('shows no tooltip when stopped, running tooltip when running, deleted tooltip after out-of-band delete', async ({
    page
  }) => {
    await page.goto(`/pipelines/${PIPELINE_NAME}`)

    const editButton = page.getByRole('button', { name: 'Edit pipeline name', exact: true })
    const statusChip = page.getByTestId('box-pipeline-status')
    // The pipeline name display in the header breadcrumb (the tooltip trigger element).
    const pipelineNameTrigger = page
      .locator('span[role="button"]')
      .filter({ hasText: PIPELINE_NAME })

    // Move the pointer away and then on the target element to trigger `mouseenter` event.
    const hoverPipelineName = async () => {
      await page.mouse.move(0, 0)
      await pipelineNameTrigger.hover()
    }

    // Stopped: edit is enabled, no tooltip on hover.
    await expect(editButton).not.toBeDisabled()
    await hoverPipelineName()
    await expect(
      page.getByText("Cannot edit the pipeline's name while it's running")
    ).not.toBeVisible()
    await expect(page.getByText("Cannot edit the deleted pipeline's name")).not.toBeVisible()

    // Start the pipeline and wait for Running.
    await waitForCompilation(PIPELINE_NAME, 60_000)
    await startPipelineAndWaitForRunning(PIPELINE_NAME, 60_000)
    await expect(statusChip).toHaveText(/running/i, { timeout: STATUS_CHIP_TIMEOUT_MS })

    // Running: edit is disabled, "running" tooltip appears on hover.
    await expect(editButton).toBeDisabled()
    await hoverPipelineName()
    await expect(page.getByText("Cannot edit the pipeline's name while it's running")).toBeVisible({
      timeout: 5_000
    })

    // Kill the pipeline and wait for Stopped.
    await killPipelineAndWaitForStopped(PIPELINE_NAME)
    await expect(statusChip).not.toHaveText(/running/i, { timeout: STATUS_CHIP_TIMEOUT_MS })

    // Delete the pipeline out-of-band (simulates another tab / API client).
    // The server refuses a delete until storage is cleared, so clear it first.
    await clearAndDeletePipeline(PIPELINE_NAME)

    // Wait for the page to enter the frozen "Deleted" state.
    await expect(statusChip).toHaveText(/deleted/i, { timeout: 10_000 })

    // Deleted: edit is disabled, "deleted" tooltip appears on hover.
    await expect(editButton).toBeDisabled()
    await hoverPipelineName()
    await expect(page.getByText("Cannot edit the deleted pipeline's name")).toBeVisible({
      timeout: 5_000
    })
  })
})
