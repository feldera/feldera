import { type BrowserContext, expect, type Page, test } from '@playwright/test'
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
 * Exercises the profile viewer end to end against a single running pipeline.
 *
 * `beforeAll` does the expensive shared setup once: create + compile + start the
 * pipeline, then download its support bundle to disk so the upload-path test can
 * reuse it. Each test then opens the profile viewer through a different entry
 * point — a fresh remote download, or the saved bundle re-uploaded into a new
 * tab via the BroadcastChannel + postMessage handoff — and asserts the loaded
 * viewer renders its core panels (logs, per-node metrics).
 *
 * Serial: the tests share the pipeline created in `beforeAll`, and there's no
 * value in running the upload path once the remote path has already proven the
 * viewer broken.
 */
test.describe('Profile viewer', () => {
  test.describe.configure({ mode: 'serial' })
  test.setTimeout(360_000)

  // Path of the support bundle downloaded in `beforeAll`; consumed by the upload test.
  let bundlePath: string

  test.beforeAll(async ({ browser }, testInfo) => {
    testInfo.setTimeout(300_000)
    await putPipeline(PIPELINE_NAME, {
      name: PIPELINE_NAME,
      description: 'E2E test pipeline for the profile viewer',
      program_code: 'create view v as (select 1)',
      program_config: { profile: 'unoptimized' }
    })
    await waitForCompilation(PIPELINE_NAME, 240_000)
    await startPipelineAndWaitForRunning(PIPELINE_NAME, 60_000)

    // Download the support bundle once, so the upload-path test has a real archive
    // to feed back in without repeating the (slow) remote fetch.
    const page = await browser.newPage()
    try {
      await openSupportBundleControls(page)
      await page.getByTestId('btn-download-support-bundle').click()

      const downloadConfirm = page.getByRole('button', { name: 'Download', exact: true })
      await expect(downloadConfirm).toBeVisible()
      const downloadPromise = page.waitForEvent('download')
      await downloadConfirm.click()
      const download = await downloadPromise
      bundlePath = testInfo.outputPath('support-bundle.zip')
      await download.saveAs(bundlePath)
    } finally {
      await page.close()
    }
  })

  test.afterAll(async () => {
    await cleanupPipeline(PIPELINE_NAME)
  })

  test('remote profile download opens a working viewer', async ({ page, context }) => {
    await page.goto(`/pipelines/${PIPELINE_NAME}`)
    await page.getByRole('tab', { name: 'Runtime' }).click()
    await expect(page.getByTestId('btn-view-profile')).toBeVisible({ timeout: 30_000 })

    await withProfileViewer(context, () => page.getByTestId('btn-view-profile').click())
  })

  test('re-uploaded bundle opens a working viewer', async ({ page, context }) => {
    await openSupportBundleControls(page)
    await page.getByTestId('input-upload-support-bundle').setInputFiles(bundlePath)

    // The dropdown transitions to the confirmation view with the picked file.
    const confirmViewProfile = page.getByTestId('btn-confirm-view-profile')
    await expect(confirmViewProfile).toBeVisible({ timeout: 10_000 })

    await withProfileViewer(context, () => confirmViewProfile.click())
  })
})

/**
 * Navigate to the pipeline page and open the support-bundle dropdown.
 *
 * MonitoringPanel defaults to the "Compiler" tab, whose tabBarEnd does NOT
 * include the support-bundle controls. Switching to the "Runtime" tab surfaces
 * TabBarEndPipelineInfo, which renders DownloadSupportBundle.
 */
async function openSupportBundleControls(page: Page) {
  await page.goto(`/pipelines/${PIPELINE_NAME}`)
  await page.getByRole('tab', { name: 'Runtime' }).click()
  await expect(page.getByTestId('btn-view-profile')).toBeVisible({ timeout: 30_000 })
  await page.getByLabel('Support bundle options').click()
}

/**
 * Run `trigger` (which opens /profile-viewer in a new tab), then verify the
 * loaded viewer's core functionality before closing the tab. The caller's
 * `page` still refers to the originating pipeline page afterwards.
 */
async function withProfileViewer(context: BrowserContext, trigger: () => Promise<void>) {
  const viewerPromise = context.waitForEvent('page')
  await trigger()
  const viewer = await viewerPromise
  try {
    await viewer.waitForLoadState('domcontentloaded')
    await expect(viewer.getByTestId('box-profile-viewer-page')).toBeVisible({ timeout: 30_000 })

    await assertLogsPanelPopulates(viewer)
    await assertNodeMetricsShowPersistentId(viewer)
  } finally {
    await viewer.close()
  }
}

/**
 * Switch to the Logs tab and confirm at least one log line rendered — the signal
 * that `logText` was extracted from the bundle (remote fetch or cross-tab handoff).
 */
async function assertLogsPanelPopulates(viewer: Page) {
  // The Logs tab label appears once the bundle finishes loading and the
  // SupportBundleViewerLayout mounts — wait for it before clicking.
  const logsTab = viewer.getByText('Logs', { exact: true }).first()
  await expect(logsTab).toBeVisible({ timeout: 60_000 })
  await logsTab.click()
  await expect(viewer.locator('.logline').first()).toBeVisible({ timeout: 15_000 })
}

/**
 * Switch to the Metrics tab, click a leaf operator in the dataflow diagram, and
 * verify the metrics panel surfaces a `persistent ID` row beside the node title
 * (issue #6409). The diagram is canvas-rendered, so we reach into cytoscape via
 * the container's private `_cyreg.cy` field and emit a synthetic `click` on the
 * first leaf — the same event the user's mouse would fire.
 */
async function assertNodeMetricsShowPersistentId(viewer: Page) {
  await viewer.getByText('Metrics', { exact: true }).first().click()

  const diagram = viewer.getByTestId('visualizer-diagram')
  await expect(diagram).toBeVisible({ timeout: 30_000 })

  await viewer.waitForFunction(
    () => {
      const container = document.querySelector<HTMLElement>('.visualizer-graph')
      // biome-ignore lint/suspicious/noExplicitAny: cytoscape attaches `_cyreg` as a private field
      const cy = (container as any)?._cyreg?.cy
      if (!cy) {
        return false
      }
      return (
        cy
          .nodes()
          // biome-ignore lint/suspicious/noExplicitAny: cytoscape collection element type
          .filter((n: any) => !n.data('has_children') && !n.data('invisible')).length > 0
      )
    },
    undefined,
    { timeout: 60_000 }
  )

  await viewer.evaluate(() => {
    const container = document.querySelector<HTMLElement>('.visualizer-graph')
    // biome-ignore lint/suspicious/noExplicitAny: cytoscape attaches `_cyreg` as a private field
    const cy = (container as any)._cyreg.cy
    const leaf = cy
      .nodes()
      // biome-ignore lint/suspicious/noExplicitAny: cytoscape collection element type
      .filter((n: any) => !n.data('has_children') && !n.data('invisible'))
      .first()
    leaf.emit('click')
  })

  await expect(viewer.getByText(/persistent ID:/i)).toBeVisible({ timeout: 10_000 })
}
