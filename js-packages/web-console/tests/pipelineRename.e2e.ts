import { expect, test } from '@playwright/test'
import { postPipelineAction, putPipeline } from '#lib/services/pipelineManager.ts'
import {
  cleanupPipeline,
  killPipelineAndWaitForStopped,
  startPipelineAndWaitForRunning,
  TEST_COMPILATION_PROFILE,
  waitForCompilation,
  waitForPipeline
} from '#lib/services/testPipelineHelpers.ts'

const OLD_NAME = `test-rename-${Date.now()}`
const NEW_NAME = `${OLD_NAME}-renamed`
const TABLE = 'transactions'
const QUERY = 'SELECT 42 AS answer'
// The name to clean up, which the test changes when it renames the pipeline
let currentName = OLD_NAME

// The generator keeps producing rows, so the change stream gets rows after each start
const PROGRAM_CODE = `
CREATE TABLE ${TABLE} (id BIGINT NOT NULL) WITH (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": { "plan": [{ "limit": 1000, "rate": 5, "fields": { "id": { "range": [0, 1000000] } } }] }
    }
  }]'
);
`

test.describe('Pipeline rename', () => {
  test.setTimeout(300_000)

  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(600_000)
    await putPipeline(OLD_NAME, {
      name: OLD_NAME,
      description: 'E2E test for the state of the pipeline page across a rename',
      program_code: PROGRAM_CODE,
      program_config: { profile: TEST_COMPILATION_PROFILE }
    })
    await waitForCompilation(OLD_NAME)
  })

  test.afterAll(async () => {
    await cleanupPipeline(currentName)
  })

  test('keeps the ad-hoc queries and the followed table, and streams under the new name', async ({
    page
  }) => {
    const queryBoxes = page.getByPlaceholder('SELECT * FROM ...')
    const changeRows = page.getByTestId('box-changestream-row')
    const followTable = page.getByTestId(`input-changestream-relation-${TABLE}`)

    // Make the state that the rename must keep: a query with a result, and a followed table with rows
    await startPipelineAndWaitForRunning(OLD_NAME)
    await page.goto(`/pipelines/${OLD_NAME}`)
    await page.getByRole('tab', { name: 'Ad-Hoc Queries' }).click()
    await queryBoxes.first().fill(QUERY)
    await page.getByRole('button', { name: 'Run query' }).first().click()
    await expect(page.getByTestId('box-result-row').getByText('42')).toBeVisible()
    await page.getByRole('tab', { name: 'Change Stream' }).click()
    await followTable.check()
    await expect(changeRows.first()).toBeVisible({ timeout: 30_000 })
    await killPipelineAndWaitForStopped(OLD_NAME)
    // The manager accepts a new name only after the storage is cleared
    await postPipelineAction(OLD_NAME, 'clear')
    await waitForPipeline(OLD_NAME, (p) => p.storageStatus === 'Cleared')

    // The edit button stays disabled until the page sees the pipeline stop
    await page.getByRole('button', { name: 'Edit pipeline name', exact: true }).click()
    const nameInput = page.locator('input:focus')
    await nameInput.fill(NEW_NAME)
    await nameInput.press('Enter')
    await expect(page).toHaveURL(new RegExp(`/pipelines/${NEW_NAME}/?$`))
    currentName = NEW_NAME

    await expect(followTable).toBeChecked()
    await expect(changeRows.first()).toBeVisible()
    await page.getByRole('tab', { name: 'Ad-Hoc Queries' }).click()
    // The newest query box is first, so the query that ran is second
    await expect(queryBoxes.nth(1)).toHaveValue(QUERY)
    await expect(page.getByTestId('box-result-row').getByText('42')).toBeVisible()

    // A start from the page restarts the stream of the followed table
    const egressRequest = page.waitForRequest((request) =>
      request.url().includes(`/v0/pipelines/${NEW_NAME}/egress/${TABLE}`)
    )
    await page
      .getByTestId('box-action-buttons')
      .getByRole('button', { name: 'Start', exact: true })
      .click()
    await egressRequest
    await page.getByRole('tab', { name: 'Change Stream' }).click()
    await expect(changeRows.first()).toBeVisible({ timeout: 30_000 })
  })
})
