import invariant from 'tiny-invariant'

import { expect, Page, test } from '@playwright/test'

import { apiOrigin, appOrigin } from '../../playwright.config'
import felderaBasicsTutorialSql from './felderaBasicsTutorial.sql'

// Insert data rows into an empty table
const insertRows = async (page: Page, data: (string | number)[][]) => {
  for (let i = 0; i < data.length; ++i) {
    await page.getByTestId('button-add-1-empty').click()
  }
  const rows = await page.getByTestId('box-data-row').all()
  for (const [rowNum, rowData] of data.entries()) {
    const cellLocators = await rows[rowNum].getByRole('cell').all()
    for (const [colNum, cellData] of rowData.entries()) {
      await cellLocators[colNum].dblclick()
      // Finding currently focused element: https://github.com/microsoft/playwright/issues/15865
      await page.locator('*:focus').fill(String(cellData))
      await page.getByTestId('box-inspection-background').click() // Loose focus on the cell
    }
  }
  await page.getByTestId('button-insert-rows').click()
}

const addUrlConnector = async (page: Page, params: { name: string; url: string }) => {
  await page.getByTestId('box-connector-url').getByTestId('button-add-input').click()
  await page.getByTestId('input-datasource-name').fill(params.name)
  await page.getByTestId('input-datasource-url').fill(params.url)
  await page.getByTestId('button-next').click()
  await page.getByTestId('input-update-format-display').click()
  await page.getByTestId('box-update-format-options').locator('[data-value="insert_delete"]').click()
  await page.getByTestId('button-create').click()
  // Connector created successfully!
}

const pipelineName = 'Supply Chain Test Pipeline'
const programName = 'Supply Chain Analytics'

/**
 * Example setup

test.beforeAll('Prepare', async ({browser}) => {
  const page = await browser.newPage()
  await page.goto(appOrigin)
  ...
  await page.close()
})
 */

test.skip('Supply Chain Analytics Tutorial', async ({ page, request }) => {
  test.setTimeout(150000)
  await page.goto(appOrigin)

  await test.step('Part 1: Create a program', async () => {
    await expect(page).toHaveURL(appOrigin + 'home/')
    await expect(page).toHaveScreenshot('clean home.png')
    await page.getByTestId('button-vertical-nav-sql-programs').click()
    await page.getByTestId('button-add-sql-program').first().click()
    await page.getByTestId('input-program-name').fill(programName)
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').waitFor({ state: 'attached' })
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').fill(felderaBasicsTutorialSql)
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').blur()
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('box-compile-status-success').waitFor()
    await expect(page).toHaveScreenshot('saved sql program.png')
  })

  await test.step('Part 1: Create a pipeline', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()
    await page.getByTestId('button-add-pipeline').first().click()
    await page.getByTestId('input-pipeline-name').fill(pipelineName)
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('input-builder-select-program').locator('button').click()
    await page.getByTestId('box-builder-program-options').getByRole('option', { name: programName }).click()
    await page.getByTestId('box-save-saved').waitFor()
  })
  await test.step('Part 1: Start the pipeline', async () => {
    await page.getByTestId('button-breadcrumb-pipelines').click()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).waitFor()
    await expect(page).toHaveScreenshot('compiling program binary.png')
    await page.getByTestId(`box-pipeline-${pipelineName}-status-Ready to run`).waitFor({ timeout: 60000 })
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-start').click()
  })
  const pipelineUUID = await test.step('Part 1: Populate PART table', async () => {
    await page.getByTestId(`button-expand-pipeline-${pipelineName}`).click()
    await expect(page).toHaveScreenshot('pipeline details.png', {
      mask: ['box-pipeline-id', 'box-pipeline-date-created', 'box-pipeline-port', 'box-pipeline-memory-graph'].map(id =>
        page.getByTestId(id)
      )
    })
    await page
      .getByTestId(`box-details-${pipelineName}`)
      .getByTestId(`box-relation-actions-PART`)
      .getByTestId(`button-import`)
      .click()
    const pipelineUUID = page.url().match(/pipeline_name=([\w-]+)/)?.[1]
    expect(pipelineUUID).toBeTruthy()
    invariant(pipelineUUID)

    await insertRows(page, [
      [1, 'Flux Capacitor'],
      [2, 'Warp Core'],
      [3, 'Kyber Crystal']
    ])

    await expect(page.getByTestId('box-snackbar-popup')).toHaveText('3 Row(s) inserted')
    await page.getByTestId('button-tab-browse').click()
    await expect(page).toHaveScreenshot('part data 0.png')
    await page.getByTestId('button-tab-insert').click()
    return pipelineUUID
  })
  await test.step('Part 1: Populate VENDOR table', async () => {
    await page.getByTestId('button-expand-relations').click()
    await page.getByTestId('box-relation-options').getByTestId(`button-option-relation-VENDOR`).click()
    await insertRows(page, [
      [1, 'Gravitech Dynamics', '222 Graviton Lane'],
      [2, 'HyperDrive Innovations', '456 Warp Way'],
      [3, 'DarkMatter Devices', '333 Singularity Street']
    ])
    await expect(page).toHaveScreenshot('vendor data 0.png')
  })
  await test.step('Part 1: Populate PRICE table', async () => {
    await page.getByTestId('button-expand-relations').click()
    await page.getByTestId('box-relation-options').getByTestId(`button-option-relation-PRICE`).click()
    await insertRows(page, [
      [1, 2, 10000],
      [2, 1, 15000],
      [3, 3, 9000]
    ])
    await page.getByTestId('button-tab-browse').click()
    await expect(page).toHaveScreenshot('price-data-0.png')
    await page.getByTestId('button-tab-insert').click()
  })
  await test.step('Part 1: Check PREFERRED_VENDOR view', async () => {
    await page.getByTestId('button-expand-relations').click()
    await page.getByTestId('box-relation-options').getByTestId(`button-option-relation-PREFERRED_VENDOR`).click()
    await expect(page).toHaveScreenshot('preferred_vendor data 0.png')
  })
  await test.step('Part 1: Update PRICE table', async () => {
    await page.getByTestId('button-expand-relations').click()
    await page.getByTestId('box-relation-options').getByTestId(`button-option-relation-PRICE`).click()
    await page.getByTestId('button-tab-insert').click()
    await insertRows(page, [
      [1, 2, 10000],
      [2, 1, 15000],
      [3, 3, 9000]
    ])

    await page.getByTestId('button-expand-relations').click()
    await page.getByTestId('box-relation-options').getByTestId(`button-option-relation-PREFERRED_VENDOR`).click()
    await expect(page).toHaveScreenshot('preferred_vendor data 1.png')
  })
  await test.step('Part 1: Stop the pipeline', async () => {
    await page.getByTestId('button-current-pipeline').click()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-shutdown').click()
    await page.getByTestId(`box-pipeline-${pipelineName}-status-Ready to run`).waitFor()
  })
  await test.step('Part 2: Restart the pipeline', async () => {
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-start').click()
    await page.getByTestId(`box-pipeline-${pipelineName}-status-Running`).waitFor()
  })
  await test.step('Part 2: Repopulate PART table', async () => {
    await request.post(apiOrigin + `v0/pipelines/${pipelineUUID}/ingress/PART?format=json`, {
      data: `
      {"insert": {"id": 1, "name": "Flux Capacitor"}}
      {"insert": {"id": 2, "name": "Warp Core"}}
      {"insert": {"id": 3, "name": "Kyber Crystal"}}
    `
    })
  })
  await test.step('Part 2: Repopulate VENDOR table', async () => {
    await request.post(apiOrigin + `v0/pipelines/${pipelineUUID}/ingress/PART?format=json`, {
      data: `
      {"insert": {"id": 1, "name": "Gravitech Dynamics", "address": "222 Graviton Lane"}}
      {"insert": {"id": 2, "name": "HyperDrive Innovations", "address": "456 Warp Way"}}
      {"insert": {"id": 3, "name": "DarkMatter Devices", "address": "333 Singularity Street"}}
    `
    })
  })
  await test.step('Part 2: Repopulate PRICE table', async () => {
    await request.post(apiOrigin + `v0/pipelines/${pipelineUUID}/ingress/PART?format=json`, {
      data: `
      {"insert": {"part": 1, "vendor": 2, "price": 10000}}
      {"insert": {"part": 2, "vendor": 1, "price": 15000}}
      {"insert": {"part": 3, "vendor": 3, "price": 9000}}
    `
    })
  })
  await test.step('Part 2: Update PRICE table', async () => {
    await request.post(apiOrigin + `v0/pipelines/${pipelineUUID}/ingress/PRICE?format=json`, {
      data: `
      {"delete": {"part": 1, "vendor": 2, "price": 10000}}
      {"insert": {"part": 1, "vendor": 2, "price": 30000}}
      {"delete": {"part": 2, "vendor": 1, "price": 15000}}
      {"insert": {"part": 2, "vendor": 1, "price": 50000}}
      {"insert": {"part": 1, "vendor": 3, "price": 20000}}
      {"insert": {"part": 2, "vendor": 3, "price": 11000}}'
    `
    })
  })
  await test.step('Part 2: View updated data', async () => {
    // await page.getByTestId(`button-expand-pipeline-${pipelineName}`).click()
    await page
      .getByTestId(`box-details-${pipelineName}`)
      .getByTestId(`box-relation-actions-PRICE`)
      .getByTestId(`button-inspect`)
      .click()
    await expect(page).toHaveScreenshot('price-data-1.png')
    await page.getByTestId('button-expand-relations').click()
    await page.getByTestId('box-relation-options').getByTestId(`button-option-relation-PREFERRED_VENDOR`).click()
    await expect(page).toHaveScreenshot('preferred_vendor data 2.png')
  })
  await test.step('Part 2: Shutdown pipeline', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-shutdown').click()
    await page.getByTestId(`box-pipeline-${pipelineName}-status-Ready to run`).waitFor()
  })
  await test.step('Part 3: Create HTTP GET connectors', async () => {
    await page.getByTestId('button-vertical-nav-connectors').click()
    await page.getByTestId('button-add-connector').first().click()
    await addUrlConnector(page, { name: 'parts-s3', url: 'https://feldera-basics-tutorial.s3.amazonaws.com/part.json' })
    await addUrlConnector(page, {
      name: 'vendors-s3',
      url: 'https://feldera-basics-tutorial.s3.amazonaws.com/vendor.json'
    })
    await addUrlConnector(page, {
      name: 'prices-s3',
      url: 'https://feldera-basics-tutorial.s3.amazonaws.com/price.json'
    })
    await expect(page).toHaveScreenshot('http connectors.png')
  })
  await test.step('Part 3: Attach GET connectors to the pipeline', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-edit').click()
    for (const name of ['parts-s3', 'vendors-s3', 'prices-s3']) {
      await page.getByTestId('button-builder-add-input').click()
      await page.getByTestId('box-connector-HTTP_GET').getByTestId('button-select-connector').click()
      await page.getByTestId('button-add-connector-' + name).click()
    }
    for (const [input, table] of [
      ['parts-s3', 'PART'],
      ['vendors-s3', 'VENDOR'],
      ['prices-s3', 'PRICE']
    ]) {
      await page.getByTestId('box-handle-input-' + input).dragTo(page.getByTestId('box-handle-table-' + table))
    }
    await expect(page).toHaveScreenshot('connected inputs.png')
  })
  await test.step('Part 3: Create Kafka input', async () => {
    await page.getByTestId('button-builder-add-input').click()
    await expect(page).toHaveScreenshot('input connectors drawer.png')
    await page
      .getByTestId('box-connector-' + 'KafkaIn')
      .getByTestId('button-add-connector')
      .click()
    await page.getByTestId('input-datasource-name').fill('price-redpanda')
    await page.getByTestId('button-next').click()
    await page.getByTestId('input-server-hostname').fill('redpanda:9092')
    await page.getByTestId('input-group-id').fill('price')
    await page.getByTestId('input-wrapper-topics').locator('input').fill('price')
    await page.getByTestId('button-tab-auth').click()
    await page.getByTestId('button-next').click()
    await page.getByTestId('input-update-format-display').click()
    await page.getByTestId('box-update-format-options').locator('[data-value="insert_delete"]').click()
    await page.getByTestId('button-create').click()
    await page
      .getByTestId('box-handle-input-' + 'price-redpanda')
      .dragTo(page.getByTestId('box-handle-table-' + 'PRICE'))
  })
  await test.step('Part 3: Create Redpanda output', async () => {
    await page.getByTestId('button-builder-add-output').click()
    await expect(page).toHaveScreenshot('output connectors drawer.png')
    await page
      .getByTestId('box-connector-' + 'KafkaOut')
      .getByTestId('button-add-connector')
      .click()
    await page.getByTestId('input-datasource-name').fill('preferred_vendor-redpanda')
    await page.getByTestId('button-next').click()
    await page.getByTestId('input-server-hostname').fill('redpanda:9092')
    await page.getByTestId('input-topic').fill('preferred_vendor')
    await page.getByTestId('button-tab-auth').click()
    await page.getByTestId('button-next').click()
    await page.getByTestId('button-create').click()
    await page
      .getByTestId('box-handle-output-' + 'preferred_vendor-redpanda')
      .dragTo(page.getByTestId('box-handle-view-' + 'PREFERRED_VENDOR'))
    await expect(page).toHaveScreenshot('complete pipeline.png')
  })
  await test.step('Part 3: Start the pipleine', async () => {
    await page.getByTestId('button-breadcrumb-pipelines').click()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-start').click()
    await page.getByTestId(`box-pipeline-${pipelineName}-status-Running`).waitFor()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-shutdown').click()
    await page.getByTestId(`box-pipeline-${pipelineName}-status-Ready to run`).waitFor()
  })

  await test.step('Cleanup: Delete pipeline', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-delete').click()
    await page.getByTestId('button-confirm-delete').click()
  })
  await test.step('Cleanup: Delete connectors', async () => {
    await page.getByTestId('button-vertical-nav-connectors').click()
    for (const connectorName of [
      'price-redpanda',
      'preferred_vendor-redpanda',
      'parts-s3',
      'vendors-s3',
      'prices-s3'
    ]) {
      await page.getByTestId(`box-connector-actions-${connectorName}`).getByTestId('button-delete').click()
      await page.getByTestId('button-confirm-delete').click()
    }
  })
  await test.step('Cleanup: Delete program', async () => {
    await page.getByTestId('button-vertical-nav-sql-programs').click()
    await page
      .getByTestId('box-program-actions-' + programName)
      .getByTestId('button-delete')
      .click()
    await page.getByTestId('button-confirm-delete').click()
  })
})
