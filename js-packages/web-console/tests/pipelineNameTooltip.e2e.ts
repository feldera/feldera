import { expect, test } from '@playwright/test'
import { deletePipeline, putPipeline } from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  killPipelineAndWaitForStopped,
  startPipelineAndWaitForRunning,
  waitForCompilation,
  waitForPipeline
} from '$lib/services/testPipelineHelpers'

configureTestClient()

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
      program_config: { profile: 'unoptimized' }
    })
    await waitForPipeline(PIPELINE_NAME, (p) => p.status === 'Stopped', 60_000)
  })

  test.afterAll(async () => {
    await cleanupPipeline(PIPELINE_NAME)
  })

  test('shows no tooltip when stopped, running tooltip when running, deleted tooltip after out-of-band delete', async ({
    page
  }) => {
    test.skip()
    await page.goto(`/pipelines/${PIPELINE_NAME}`)

    const editButton = page.getByRole('button', { name: 'Edit pipeline name', exact: true })
    const statusChip = page.getByTestId('box-pipeline-status')
    // The pipeline name display in the header breadcrumb (the tooltip trigger element).
    const pipelineNameTrigger = page
      .locator('span[role="button"]')
      .filter({ hasText: PIPELINE_NAME })

    // Stopped: edit is enabled, no tooltip on hover.
    await expect(editButton).not.toBeDisabled()
    await pipelineNameTrigger.hover()
    await expect(
      page.getByText("Cannot edit the pipeline's name while it's running")
    ).not.toBeVisible()
    await expect(page.getByText("Cannot edit the deleted pipeline's name")).not.toBeVisible()

    // Start the pipeline and wait for Running.
    await waitForCompilation(PIPELINE_NAME, 60_000)
    await startPipelineAndWaitForRunning(PIPELINE_NAME, 20_000)
    await expect(statusChip).toHaveText(/running/i, { timeout: 2_000 })

    // Running: edit is disabled, "running" tooltip appears on hover.
    await expect(editButton).toBeDisabled()
    await pipelineNameTrigger.hover()
    await expect(page.getByText("Cannot edit the pipeline's name while it's running")).toBeVisible({
      timeout: 5_000
    })

    // Kill the pipeline and wait for Stopped.
    await killPipelineAndWaitForStopped(PIPELINE_NAME)
    await expect(statusChip).not.toHaveText(/running/i, { timeout: 2_000 })

    // Delete the pipeline out-of-band (simulates another tab / API client).
    await deletePipeline(PIPELINE_NAME)

    // Wait for the page to enter the frozen "Deleted" state.
    await expect(statusChip).toHaveText(/deleted/i, { timeout: 10_000 })

    // Deleted: edit is disabled, "deleted" tooltip appears on hover.
    await expect(editButton).toBeDisabled()
    await pipelineNameTrigger.hover()
    await expect(page.getByText("Cannot edit the deleted pipeline's name")).toBeVisible({
      timeout: 5_000
    })
  })
})
