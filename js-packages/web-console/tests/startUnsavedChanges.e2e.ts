/**
 * Start saves the unsaved code before it starts the pipeline, and the pipeline starts
 * after the code compiles. Start is disabled for a program with errors until the user
 * edits the code.
 */
import { expect, test } from '@playwright/test'
import {
  getExtendedPipeline,
  postPipelineAction,
  putPipeline
} from '#lib/services/pipelineManager.ts'
import {
  clearAndDeletePipeline,
  TEST_COMPILATION_PROFILE,
  waitForPipeline
} from '#lib/services/testPipelineHelpers.ts'

const PIPELINE_NAME = `test-start-unsaved-${Date.now()}`
const FIXED_SQL = 'create view v as select 1'

test.describe('Start with unsaved changes', () => {
  test.setTimeout(180_000)

  test.afterAll(async () => {
    // `cleanupPipeline` waits for the status Stopped, which a compiling pipeline does not
    // show. This hook waits for the deployment status instead.
    await postPipelineAction(PIPELINE_NAME, 'kill').catch(() => {})
    await expect
      .poll(async () => {
        const { deploymentStatus, deploymentDesiredStatus } =
          await getExtendedPipeline(PIPELINE_NAME)
        return [deploymentStatus, deploymentDesiredStatus]
      })
      .toEqual(['Stopped', 'Stopped'])
    await clearAndDeletePipeline(PIPELINE_NAME)
  })

  test('a fix to a program with errors enables Start, and Start saves it', async ({ page }) => {
    await putPipeline(PIPELINE_NAME, {
      name: PIPELINE_NAME,
      description: '',
      program_code: 'create view v as selec 1',
      program_config: { profile: TEST_COMPILATION_PROFILE }
    })
    await waitForPipeline(PIPELINE_NAME, (p) => p.status === 'SqlError')
    // Turn autosave off, so that only Start can save the edit.
    await page.addInitScript(() => localStorage.setItem('layout/pipelines/autosave', 'false'))
    await page.goto(`/pipelines/${PIPELINE_NAME}/`)

    const actions = page.getByTestId('box-action-buttons')
    const start = actions.getByRole('button', { name: 'Start', exact: true })
    const startOptions = actions.getByRole('button', { name: 'See start options' })
    await expect(start).toHaveClass(/\bdisabled\b/)
    await expect(startOptions).toBeDisabled()

    await page.locator('.monaco-editor .view-lines').first().click()
    await page.keyboard.press('ControlOrMeta+a')
    await page.keyboard.insertText(FIXED_SQL)
    await expect(start).not.toHaveClass(/\bdisabled\b/)
    await expect(startOptions).toBeEnabled()

    await start.click()
    await expect
      .poll(async () => (await getExtendedPipeline(PIPELINE_NAME)).programCode)
      .toBe(FIXED_SQL)
    await expect(page.getByTestId('box-generic-dialog')).not.toBeVisible()
    await expect
      .poll(async () => (await getExtendedPipeline(PIPELINE_NAME)).deploymentDesiredStatus)
      .not.toBe('Stopped')
  })
})
