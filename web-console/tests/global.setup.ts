import { Page, test as setup } from '@playwright/test'

import { appOrigin } from '../playwright.config'

const deleteRows = async (page: Page, regex: RegExp) => {
  while (true) {
    try {
      // Wait for atleast one row, if timed out - no rows left
      await page.getByTestId(regex).waitFor({ timeout: 2000 })
    } catch {}
    const buttonDelete = await page.getByTestId(regex).first().getByTestId('button-delete')
    if (!(await buttonDelete.isVisible())) {
      // Exit if no more rows left
      break
    }
    await buttonDelete.click()
    await page.getByTestId('button-confirm-delete').click()
  }
}

setup('Global prepare', async ({ page }) => {
  await page.goto(appOrigin, { timeout: 3000 })

  await setup.step('Prepare: Delete pipelines', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()

    await deleteRows(page, /box-pipeline-actions-/)
  })

  await setup.step('Prepare: Delete connectors', async () => {
    await page.getByTestId('button-vertical-nav-connectors').click()

    await deleteRows(page, /box-connector-actions-/)
  })

  await setup.step('Prepare: Delete programs', async () => {
    await page.getByTestId('button-vertical-nav-sql-programs').click()

    await deleteRows(page, /box-program-actions-/)
  })
})
