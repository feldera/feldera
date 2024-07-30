import { Page, test as setup } from '@playwright/test'

import { appOrigin } from '../../playwright-e2e.config'

const rowsAction = async (page: Page, regex: RegExp, actionTestId: string, postProcedure = async () => {}) => {
  while (true) {
    try {
      // Wait for at least one row, if timed out - no rows left
      await page.getByTestId(regex).waitFor({ timeout: 2000 })
    } catch {}
    const buttonDelete = await page.getByTestId(regex).first().getByTestId(actionTestId)
    if (!(await buttonDelete.isVisible())) {
      // Exit if no more rows left
      break
    }
    await buttonDelete.click()
    await postProcedure()
  }
}

setup('Global prepare', async ({ page, request }) => {
  for (let i = 0; i < 5; ++i) {
    // Try to ping Feldera instance before opening the WebConsole,
    // which may sporadically fail the first time (transient CI issue)
    try {
      await page.waitForTimeout(1000)
      await request.get(appOrigin + 'config/authentication').then(r => r.json())
      break
    } catch {}
  }

  await page.goto(appOrigin)

  await setup.step('Prepare: Delete pipelines', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()

    await rowsAction(page, /box-pipeline-actions-/, 'button-shutdown')
    await page.waitForTimeout(4000)
    await rowsAction(page, /box-pipeline-actions-/, 'button-delete', () =>
      page.getByTestId('button-confirm-delete').click()
    )
  })

  await setup.step('Prepare: Delete connectors', async () => {
    await page.getByTestId('button-vertical-nav-connectors').click()

    await rowsAction(page, /box-connector-actions-/, 'button-delete', () =>
      page.getByTestId('button-confirm-delete').click()
    )
  })

  await setup.step('Prepare: Delete programs', async () => {
    await page.getByTestId('button-vertical-nav-sql-programs').click()

    await rowsAction(page, /box-program-actions-/, 'button-delete', () =>
      page.getByTestId('button-confirm-delete').click()
    )
  })
})
