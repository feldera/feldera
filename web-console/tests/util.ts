import { Page } from '@playwright/test'

export const deleteProgram = async (page: Page, programName: string) => {
  await page.getByTestId('button-vertical-nav-sql-programs').click()
  await page
    .getByTestId('box-program-actions-' + programName)
    .getByTestId('button-delete')
    .click()
  await page.getByTestId('button-confirm-delete').click()
}

export const deletePipeline = async (page: Page, pipelineName: string) => {
  await page.getByTestId('button-vertical-nav-pipelines').click()
  await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-delete').click()
  await page.getByTestId('button-confirm-delete').click()
}

export const deleteConnectors = async (page: Page, connectorNames: string[]) => {
  await page.getByTestId('button-vertical-nav-connectors').click()
  for (const connectorName of connectorNames) {
    await page.getByTestId(`box-connector-actions-${connectorName}`).getByTestId('button-delete').click()
    await page.getByTestId('button-confirm-delete').click()
  }
}
