import { type BrowserContext, expect, test } from '@playwright/test'
import { putPipeline } from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  startPipelineAndWaitForRunning,
  waitForCompilation
} from '$lib/services/testPipelineHelpers'

configureTestClient()

const PIPELINE_NAME = `test-support-bundle-${Date.now()}`

/**
 * Walks the full support-bundle handoff sequence: download a bundle from a running
 * pipeline, open the remote profile viewer, then re-upload the saved bundle
 * into a fresh viewer tab. Both viewer tabs must render the Logs panel with
 * real log lines — that's the signal the bundle was correctly fetched (remote
 * path) or correctly transferred to the new tab via the BroadcastChannel +
 * postMessage handoff (upload path).
 */
test.describe('Support bundle download / view / upload / view', () => {
  test.setTimeout(360_000)

  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(300_000)
    await putPipeline(PIPELINE_NAME, {
      name: PIPELINE_NAME,
      description: 'E2E test pipeline for support bundle handoff',
      program_code: 'create view v as (select 1)',
      program_config: { profile: 'unoptimized' }
    })
    await waitForCompilation(PIPELINE_NAME, 240_000)
    await startPipelineAndWaitForRunning(PIPELINE_NAME, 60_000)
  })

  test.afterAll(async () => {
    await cleanupPipeline(PIPELINE_NAME)
  })

  test('download → view (logs load) → upload → view (logs load)', async ({
    page,
    context
  }, testInfo) => {
    await page.goto(`/pipelines/${PIPELINE_NAME}`)

    // MonitoringPanel defaults to the "Compiler" tab, whose tabBarEnd does NOT
    // include the support-bundle controls. Switching to the "Runtime" tab
    // surfaces TabBarEndPipelineInfo, which renders DownloadSupportBundle.
    await page.getByRole('tab', { name: 'Runtime' }).click()
    await expect(page.getByTestId('btn-view-profile')).toBeVisible({ timeout: 30_000 })

    // ── Phase A: download the support bundle archive via the dropdown dialog ─
    await page.getByLabel('Support bundle options').click()
    await page.getByTestId('btn-download-support-bundle').click()

    const downloadConfirm = page.getByRole('button', { name: 'Download', exact: true })
    await expect(downloadConfirm).toBeVisible()
    const downloadPromise = page.waitForEvent('download')
    await downloadConfirm.click()
    const download = await downloadPromise
    const bundlePath = testInfo.outputPath('support-bundle.zip')
    await download.saveAs(bundlePath)
    // Wait for the dialog to close before driving the rest of the page.
    await expect(downloadConfirm).toHaveCount(0, { timeout: 10_000 })

    // ── Phase B: open the remote profile viewer; Logs panel must populate ───
    await assertProfileViewerLoadsLogs(context, () => page.getByTestId('btn-view-profile').click())

    // ── Phase C: re-upload the saved bundle from disk ───────────────────────
    await page.getByLabel('Support bundle options').click()
    const fileInput = page.getByTestId('input-upload-support-bundle')
    await fileInput.setInputFiles(bundlePath)
    // The dropdown transitions to the confirmation view with the picked file.
    const confirmViewProfile = page.getByTestId('btn-confirm-view-profile')
    await expect(confirmViewProfile).toBeVisible({ timeout: 10_000 })

    // ── Phase D: viewer tab fed via cross-tab handoff also shows logs ───────
    await assertProfileViewerLoadsLogs(context, () => confirmViewProfile.click())
  })
})

/**
 * Click an element that opens /profile-viewer in a new tab, assert that the
 * viewer renders, switch to the Logs tab, and confirm at least one log line
 * rendered. The viewer tab is closed before this returns. The caller's `page`
 * still refers to the originating pipeline page.
 */
async function assertProfileViewerLoadsLogs(context: BrowserContext, trigger: () => Promise<void>) {
  const viewerPromise = context.waitForEvent('page')
  await trigger()
  const viewer = await viewerPromise
  try {
    await viewer.waitForLoadState('domcontentloaded')
    await expect(viewer.getByTestId('box-profile-viewer-page')).toBeVisible({ timeout: 30_000 })

    // The Logs tab label appears once the bundle finishes loading and the
    // SupportBundleViewerLayout mounts — wait for it before clicking.
    const logsTab = viewer.getByText('Logs', { exact: true }).first()
    await expect(logsTab).toBeVisible({ timeout: 60_000 })
    await logsTab.click()

    // At least one rendered log line means logText was extracted from the bundle.
    await expect(viewer.locator('.logline').first()).toBeVisible({ timeout: 15_000 })
  } finally {
    await viewer.close()
  }
}
