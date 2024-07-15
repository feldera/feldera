import { expect, test } from '@playwright/test'

import { appOrigin } from '../../playwright-e2e.config'
import { deleteConnectors, deletePipeline, deleteProgram } from '../util'
import felderaBasicsTutorialSql from './felderaBasicsTutorial.sql'

/**
 * The usage of API entity names as identifiers introduces more complex UI states when editing the names.
 * This test performs renames of program, connector and pipeline entities in various circumstances,
 * checking if all the views update accordingly and without fail.
 */

test('Entity rename test', async ({ page, request }) => {
  test.setTimeout(120000)
  await page.goto(appOrigin)

  await test.step('Create program1', async () => {
    await page.getByTestId('button-vertical-nav-sql-programs').click()
    await page.getByTestId('button-add-sql-program').first().click()
    await expect(page).toHaveScreenshot('1-1-create-program1.png')

    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').waitFor({ state: 'attached' })
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').fill(felderaBasicsTutorialSql)
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').blur()
    await expect(page).toHaveScreenshot('1-2-create-program1.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })

    await page.getByTestId('input-program-name').fill('program1_1')
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('box-compile-status-success').waitFor()
    await expect(page).toHaveScreenshot('1-3-create-program1.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })

    await page.getByTestId('input-program-name').fill('program1_2')
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('box-compile-status-success').waitFor()
    await expect(page).toHaveScreenshot('1-4-create-program1.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })

    await page.getByTestId('input-program-description').fill('Description of program1')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('1-5-create-program1.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })
  })

  await test.step('Create program2', async () => {
    await page.getByTestId('button-breadcrumb-sql-programs').click()
    await page.getByTestId('button-add-sql-program').first().click()

    await page.getByTestId('input-program-name').fill('program1_2')

    await expect(page).toHaveScreenshot('2-1-saved-program2.png')
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').waitFor({ state: 'attached' })
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').fill(felderaBasicsTutorialSql)
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').blur()

    await page.getByTestId('input-program-name').fill('program2_0')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('2-2-saved-program2.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })

    await page.getByTestId('input-program-name').fill('')
    await page.getByTestId('box-save-modified').waitFor()
    await expect(page).toHaveScreenshot('2-3-saved-program2.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })

    await page.getByTestId('input-program-name').fill('program2_0')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('2-4-saved-program2.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })

    await page.getByTestId('input-program-name').fill('program2_1')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('2-5-saved-program2.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })
  })

  await test.step('Create program3', async () => {
    await page.getByTestId('button-breadcrumb-sql-programs').click()
    await page.getByTestId('button-add-sql-program').first().click()

    await page.getByTestId('input-program-description').fill('Description of program_3')
    await expect(page).toHaveScreenshot('3-1-saved-program3.png')

    await page.getByTestId('input-program-name').fill('program3_1')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('3-2-saved-program3.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })

    await page.getByTestId('input-program-description').fill('Description of program3')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('3-3-saved-program3.png', {
      mask: ['box-spinner'].map(id => page.getByTestId(id))
    })
  })

  await test.step('Rename program2 in the list', async () => {
    await page.getByTestId('button-breadcrumb-sql-programs').click()
    await page.getByTestId('box-column-header-name').click()
    await expect(page).toHaveScreenshot('4-1-program-list.png', {
      mask: [/box-status-/].map(id => page.getByTestId(id))
    })

    await page.getByTestId(`box-program-name-program2_1`).dblclick()
    await page.getByTestId(`box-grid-row-program2_1`).locator('input').fill('program1_2')
    await page.getByTestId(`box-grid-row-program2_1`).locator('input').press('Enter')
    await expect(page).toHaveScreenshot('4-2-program-list.png', {
      mask: [/box-status-/].map(id => page.getByTestId(id))
    })

    await page.getByTestId(`box-program-name-program2_1`).dblclick()
    await page.getByTestId(`box-grid-row-program2_1`).locator('input').fill('program2_2')
    await page.getByTestId(`box-grid-row-program2_1`).locator('input').press('Enter')
    await expect(page).toHaveScreenshot('4-3-program-list.png', {
      mask: [/box-status-/].map(id => page.getByTestId(id))
    })

    await page.getByTestId(`box-program-description-program2_2`).locator('..').dblclick()
    await page.getByTestId(`box-grid-row-program2_2`).locator('input').fill('A description for program2')
    await page.getByTestId(`box-grid-row-program2_2`).locator('input').press('Enter')
    await expect(page).toHaveScreenshot('4-4-program-list.png', {
      mask: [/box-status-/].map(id => page.getByTestId(id))
    })
  })

  await test.step('Wait for all programs to compile', async () => {
    await page.getByTestId('box-grid-row-program2_2').getByTestId('box-status-compiling-binary').waitFor()
  })

  await test.step('Create pipeline1', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()
    await page.getByTestId('button-add-pipeline').first().click()
    await expect(page).toHaveScreenshot('6-1-create-pipeline1.png')

    await page.getByTestId('input-pipeline-name').fill('pipeline2')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('6-2-create-pipeline1.png')

    await page.getByTestId('input-builder-select-program').locator('button').click()
    await page.getByTestId('box-builder-program-options').getByRole('option', { name: 'program2_2' }).click()
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('6-3-create-pipeline1.png')

    await page.getByTestId('button-remove-program').click()
    await expect(page).toHaveScreenshot('6-4-create-pipeline1.png')
    await page.getByTestId('button-confirm-delete').click()
    await expect(page).toHaveScreenshot('6-5-create-pipeline1.png')

    await page.getByTestId('input-builder-select-program').locator('button').click()
    await page.getByTestId('box-builder-program-options').getByRole('option', { name: 'program1_2' }).click()
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('6-6-create-pipeline1.png')

    await page.getByTestId('input-pipeline-name').fill('')
    await page.getByTestId('box-error-name').waitFor()
    await expect(page).toHaveScreenshot('6-7-create-pipeline1.png')

    await page.getByTestId('input-pipeline-description').fill('Description for pipeline1')
    await expect(page).toHaveScreenshot('6-8-create-pipeline1.png')

    await page.getByTestId('input-pipeline-name').fill('pipeline1')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('6-9-create-pipeline1.png')
  })

  await test.step('Create pipeline2', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()
    await page.getByTestId('button-add-pipeline').first().click()

    await page.getByTestId('input-pipeline-name').fill('pipeline2')
    await expect(page).toHaveScreenshot('7-1-create-pipeline2.png')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('7-2-create-pipeline2.png')

    await page.getByTestId('input-pipeline-description').fill('Description for pipeline2')

    await page.getByTestId('input-builder-select-program').locator('button').click()
    await page.getByTestId('box-builder-program-options').getByRole('option', { name: 'program2_2' }).click()
    await page.waitForTimeout(1000)
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('7-3-create-pipeline2.png')
  })

  await test.step('Add kafka input to pipeline2', async () => {
    await page.getByTestId('button-builder-add-input').click()
    await page.getByTestId('box-connector-KafkaIn').getByTestId('button-add-connector').click()

    await page.getByTestId('input-datasource-name').fill('kafka_in_1')
    await page.getByTestId('input-datasource-description').fill('Description for kafka_in_1')
    await page.getByTestId('button-tab-server').click()
    await page.getByTestId('input-bootstrap_servers').fill('redpanda:9092')
    await page.getByTestId('input-group_id').fill('topic-1')
    await page.getByTestId('input-topics').fill('topic-1')
    await page.getByTestId('button-tab-format').click()
    await page.getByTestId('button-create').click()
    await expect(page).toHaveScreenshot('8-1-edit-pipeline2.png')
    await page.getByTestId('box-handle-input-' + 'kafka_in_1').dragTo(page.getByTestId('box-handle-table-' + 'vendor'))
    await expect(page).toHaveScreenshot('8-2-edit-pipeline2.png')
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('box-snackbar-popup').waitFor({ state: 'hidden' })
    await expect(page).toHaveScreenshot('8-3-edit-pipeline2.png')
  })

  await test.step('Add kafka output to pipeline2', async () => {
    await page.getByTestId('button-builder-add-output').click()
    await page
      .getByTestId('box-connector-' + 'KafkaOut')
      .getByTestId('button-add-connector')
      .hover()
    await expect(page).toHaveScreenshot('9-1-output-connectors-drawer.png')
    await page
      .getByTestId('box-connector-' + 'KafkaOut')
      .getByTestId('button-add-connector')
      .click()
    await page.getByTestId('input-datasource-name').fill('preferred_vendor-redpanda')
    await page.getByTestId('button-next').click()
    await page.getByTestId('input-bootstrap_servers').fill('redpanda:9092')
    await page.getByTestId('input-topic').fill('preferred_vendor')
    await page.getByTestId('button-tab-auth').click()
    await page.getByTestId('button-next').click()
    await page.getByTestId('button-next').click()
    await page.getByTestId('button-create').click()
    await page
      .getByTestId('box-handle-output-' + 'preferred_vendor-redpanda')
      .dragTo(page.getByTestId('box-handle-view-' + 'preferred_vendor'))
    await expect(page).toHaveScreenshot('9-2-edit-pipeline2.png')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('9-3-edit-pipeline2.png')
  })

  await test.step('Replace program in pipeline 2', async () => {
    await page.locator('.react-flow__edgeupdater-target').nth(1).click()
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('box-snackbar-popup').waitFor({ state: 'hidden' })
    await expect(page).toHaveScreenshot('a-1-edit-pipeline2.png')
    await page.getByTestId('input-pipeline-name').fill('pipeline1')
    await page.getByTestId('box-error-name').waitFor()
    await expect(page).toHaveScreenshot('a-2-edit-pipeline2.png')
    await page
      .getByTestId('box-handle-output-' + 'preferred_vendor-redpanda')
      .dragTo(page.getByTestId('box-handle-view-' + 'preferred_vendor'))
    await expect(page).toHaveScreenshot('a-3-edit-pipeline2.png')
    await page.getByTestId('button-remove-program').click()
    await page.getByTestId('button-confirm-delete').click()
    await expect(page).toHaveScreenshot('a-4-edit-pipeline2.png')
    await page.getByTestId('input-pipeline-name').fill('pipeline2')
    await page.getByTestId('box-error-name').waitFor({ state: 'detached' })
    await expect(page).toHaveScreenshot('a-5-edit-pipeline2.png')
    await page.getByTestId('box-save-saved').waitFor()
    await expect(page).toHaveScreenshot('a-6-edit-pipeline2.png')
  })

  await test.step('Rename pipeline in pipelines list', async () => {
    await page.getByTestId('button-breadcrumb-pipelines').click()
    await page.getByTestId('box-column-header-name').click()
    await expect(page).toHaveScreenshot('b-1-edit-pipeline-in-list.png')
    await page.getByTestId('box-pipeline-name-pipeline1').dblclick()
    await page.getByTestId(`box-grid-row-pipeline1`).locator('input').fill('pipeline2')
    await page.getByTestId(`box-grid-row-pipeline1`).locator('input').press('Enter')
    await expect(page).toHaveScreenshot('b-2-edit-pipeline-in-list.png')
    await page.getByTestId('box-pipeline-name-pipeline1').dblclick()
    await page.getByTestId(`box-grid-row-pipeline1`).locator('input').fill('pipeline3')
    await page.getByTestId(`box-grid-row-pipeline1`).locator('input').press('Enter')
    await expect(page).toHaveScreenshot('b-3-edit-pipeline-in-list.png')
    await page.getByTestId(`box-grid-row-pipeline3`).getByTestId('button-start').click()
    await page.getByTestId(`box-status-pipeline-pipeline3-Running`).waitFor()
    await expect(page).toHaveScreenshot('b-4-edit-pipeline-in-list.png')
    await page.getByTestId('box-pipeline-name-pipeline3').dblclick()
    await page.getByTestId(`box-grid-row-pipeline3`).locator('input').fill('pipeline4')
    await page.getByTestId(`box-grid-row-pipeline3`).locator('input').press('Enter')
    await expect(page).toHaveScreenshot('b-5-edit-pipeline-in-list.png')
    await page.getByTestId(`box-grid-row-pipeline4`).getByTestId('button-shutdown').click()
    await page.getByTestId(`box-status-pipeline-pipeline4-Ready to run`).waitFor()
    await expect(page).toHaveScreenshot('b-6-edit-pipeline-in-list.png')
  })

  await test.step('Cleanup: Delete pipelines', async () => {
    await deletePipeline(page, 'pipeline2')
    await deletePipeline(page, 'pipeline4')
  })

  await test.step('Cleanup: Delete programs', async () => {
    await deleteProgram(page, 'program1_2')
    await deleteProgram(page, 'program2_2')
    await deleteProgram(page, 'program3_1')
  })

  await test.step('Cleanup: Delete connectors', async () => {
    await deleteConnectors(page, ['kafka_in_1', 'preferred_vendor-redpanda'])
  })
})
