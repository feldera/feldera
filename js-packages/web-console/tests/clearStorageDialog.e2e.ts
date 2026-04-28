import { expect, test } from '@playwright/test'
import { postPipelineAction, putPipeline } from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  waitForPipeline
} from '$lib/services/testPipelineHelpers'

configureTestClient()

const PIPELINE_NAME = `test-clear-storage-${Date.now()}`

async function createPipelineWithStorage() {
  await putPipeline(PIPELINE_NAME, {
    name: PIPELINE_NAME,
    description: 'E2E test pipeline for clear storage dialog',
    program_code: 'CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY);',
    runtime_config: {
      workers: 1,
      storage: { min_storage_bytes: 1048576 }
    },
    program_config: { profile: 'unoptimized' }
  })
  await waitForPipeline(PIPELINE_NAME, (p) => p.status === 'Stopped', 180_000)
}

async function startAndStopToCreateStorage() {
  await postPipelineAction(PIPELINE_NAME, 'start')
  await waitForPipeline(PIPELINE_NAME, (p) => p.status === 'Running')
  await postPipelineAction(PIPELINE_NAME, 'kill')
  await waitForPipeline(PIPELINE_NAME, (p) => p.status === 'Stopped')
  await waitForPipeline(PIPELINE_NAME, (p) => p.storageStatus !== 'Cleared', 10_000)
}

/** Intercept the next PATCH to return a storage-not-cleared error, then open
 *  the config dialog and click Apply. This triggers the clear-storage flow
 *  without needing to edit Monaco (which has unreliable value propagation). */
async function triggerClearStorageDialog(page: import('@playwright/test').Page) {
  // Intercept the PATCH request to return a storage-not-cleared error.
  // This simulates what happens when the user changes workers/storage config
  // on a pipeline with non-cleared storage.
  await page.route(`**/v0/pipelines/${encodeURIComponent(PIPELINE_NAME)}`, (route) => {
    if (route.request().method() === 'PATCH') {
      route.fulfill({
        status: 400,
        contentType: 'application/json',
        body: JSON.stringify({
          message:
            'The following pipeline edits are not allowed while storage is not cleared: `runtime_config.workers`',
          error_code: 'EditRestrictedToClearedStorage',
          details: { not_allowed: ['`runtime_config.workers`'] }
        })
      })
    } else {
      route.continue()
    }
  })

  // Open the configurations popup
  await page.locator('button[aria-label="Pipeline actions"]').click()
  await expect(page.getByTestId('box-generic-dialog')).toBeVisible({ timeout: 5000 })

  // Click Apply — the intercepted PATCH will return storage-not-cleared error
  await page.getByRole('button', { name: 'Apply' }).click()

  // Wait for clear storage confirm dialog
  await expect(page.getByTestId('box-clear-storage-confirm')).toBeVisible({ timeout: 10_000 })

  // Remove the route interception so subsequent requests go to the real API
  await page.unrouteAll()
}

test.describe('Clear storage dialog', () => {
  test.setTimeout(300_000)

  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(180_000)
    await cleanupPipeline(PIPELINE_NAME)
    await createPipelineWithStorage()
    await startAndStopToCreateStorage()
  })

  test.afterAll(async ({}, testInfo) => {
    testInfo.setTimeout(60_000)
    await cleanupPipeline(PIPELINE_NAME)
  })

  test('changing config triggers clear storage dialog and completes the flow', async ({ page }) => {
    await page.goto(`/pipelines/${PIPELINE_NAME}/`)
    await page.waitForLoadState('networkidle')

    await triggerClearStorageDialog(page)

    // === CONFIRM PHASE ===
    await expect(page.getByTestId('box-dialog-title')).toHaveText('Clear storage to apply changes?')
    await expect(page.getByTestId('box-dialog-description')).toContainText(
      'Storage must be cleared'
    )
    await expect(page.getByTestId('button-confirm-clear-storage')).toHaveText('Clear and apply')
    await expect(page.getByTestId('btn-dialog-cancel')).toHaveText('Back')

    // No X button (noclose mode)
    await expect(page.getByLabel('Close dialog')).not.toBeVisible()

    // Click "Clear and apply" — this calls the real API to clear storage then apply
    await page.getByTestId('button-confirm-clear-storage').click()

    // === PROGRESS PHASE ===
    await expect(page.getByTestId('box-clear-storage-progress')).toBeVisible({ timeout: 10_000 })
    await expect(page.getByTestId('box-clear-storage-progress-message')).toBeVisible()

    // === SUCCESS ===
    // Dialog should close after successful clear and apply
    await expect(page.getByTestId('box-clear-storage-progress')).not.toBeVisible({
      timeout: 60_000
    })
    await expect(page.getByTestId('box-clear-storage-confirm')).not.toBeVisible()
  })

  test('Back button in confirm phase returns to config dialog', async ({ page }) => {
    // Recreate storage state for this test
    await startAndStopToCreateStorage()

    await page.goto(`/pipelines/${PIPELINE_NAME}/`)
    await page.waitForLoadState('networkidle')

    await triggerClearStorageDialog(page)

    // Click "Back" to return to config dialog
    await page.getByTestId('btn-dialog-cancel').click()

    // Should return to the config dialog (MultiJSONDialog with Apply button)
    await expect(page.getByRole('button', { name: 'Apply' })).toBeVisible({ timeout: 5000 })
    await expect(page.getByTestId('box-clear-storage-confirm')).not.toBeVisible()
  })
})
