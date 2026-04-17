import { expect, test } from '@playwright/test'
import { deletePipeline, putPipeline } from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  waitForPipeline
} from '$lib/services/testPipelineHelpers'

configureTestClient()

const PREFIX = `test-deleted-${Date.now()}`
const PIPELINE_A = `${PREFIX}-a`
const PIPELINE_B = `${PREFIX}-b`

test.describe('Pipeline deleted state', () => {
  test.setTimeout(180_000)

  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(120_000)
    for (const name of [PIPELINE_A, PIPELINE_B]) {
      await putPipeline(name, {
        name,
        description: 'E2E test pipeline for deleted-state chip',
        program_code: 'create view v as (select 1)',
        program_config: { profile: 'unoptimized' }
      })
      await waitForPipeline(name, (p) => p.status === 'Stopped')
    }
  })

  test.afterAll(async () => {
    await cleanupPipeline(PIPELINE_A)
    await cleanupPipeline(PIPELINE_B)
  })

  test('shows Deleted chip after out-of-band delete, and then the real status when switching to another pipeline', async ({
    page
  }) => {
    await page.goto(`/pipelines/${PIPELINE_A}`)

    const statusChip = page.getByTestId('box-pipeline-status')
    await expect(statusChip).toBeVisible()
    await expect(statusChip).not.toHaveText(/deleted/i)

    // Delete pipeline A out-of-band (simulates another tab / API client).
    await deletePipeline(PIPELINE_A)

    // The page should enter the frozen "Deleted" state.
    await expect(statusChip).toHaveText(/deleted/i, { timeout: 10_000 })

    // Navigate to the other (still-existing) pipeline via client-side nav
    // (sidebar link). page.goto() would do a full reload which masks the
    // bug — the real-world path is clicking a sidebar link, which reuses
    // +page.svelte since only the route param changes.
    await page.locator(`a[href$="/pipelines/${PIPELINE_B}/"]`).first().click()
    await expect(page).toHaveURL(new RegExp(`/pipelines/${PIPELINE_B}/?$`))

    await expect(statusChip).toBeVisible()
    await expect(statusChip).not.toHaveText(/deleted/i, { timeout: 10_000 })
  })
})
