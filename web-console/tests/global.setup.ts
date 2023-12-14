import { test as setup } from '@playwright/test'

import { appOrigin } from '../playwright.config'

setup('Global prepare', async ({ page }) => {
  await page.goto(appOrigin)

  await setup.step('Prepare: Delete pipelines', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()
    await page
      .getByTestId(/box-pipeline-actions-/)
      .first()
      .waitFor({ timeout: 2000 })
      .catch(() => {})

    while (true) {
      const buttonDelete = await page.getByTestId(/box-pipeline-actions-/).first().getByTestId('button-delete')
      if ((await buttonDelete.count()) === 0) {
        break
      }
      await buttonDelete.click()
      await page.getByTestId('button-confirm-delete').click()
    }
  })

  await setup.step('Prepare: Delete connectors', async () => {
    await page.getByTestId('button-vertical-nav-connectors').click()
    await page
      .getByTestId(/box-connector-actions-/)
      .first()
      .waitFor({ timeout: 2000 })
      .catch(() => {})

    while (true) {
      const buttonDelete = await page.getByTestId(/box-connector-actions-/).first().getByTestId('button-delete')
      if ((await buttonDelete.count()) === 0) {
        break
      }
      await buttonDelete.click()
      await page.getByTestId('button-confirm-delete').click()
    }
  })

  await setup.step('Prepare: Delete programs', async () => {
    await page.getByTestId('button-vertical-nav-sql-programs').click()
    await page
      .getByTestId(/box-program-actions-/)
      .first()
      .waitFor({ timeout: 2000 })
      .catch(() => {})

    while (true) {
      const buttonDelete = await page.getByTestId(/box-program-actions-/).first().getByTestId('button-delete')
      if ((await buttonDelete.count()) === 0) {
        break
      }

      await buttonDelete.click()
      await page.getByTestId('button-confirm-delete').click()
    }
  })
})
