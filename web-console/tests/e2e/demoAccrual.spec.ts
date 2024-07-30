import invariant from 'tiny-invariant'

import { faker } from '@faker-js/faker'
import { expect, test } from '@playwright/test'

import { apiOrigin, appOrigin } from '../../playwright-e2e.config'
import { deletePipeline, deleteProgram } from '../util'
import demoAccrualSql from './demoAccrual.sql'

const programName = 'Accrual demo'
const pipelineName = 'Accrual demo'

test('Accrual demo test', async ({ page, request }) => {
  test.setTimeout(240000)
  await page.goto(appOrigin)

  await test.step('Create a program', async () => {
    await page.getByTestId('button-vertical-nav-sql-programs').click()
    await page.getByTestId('button-add-sql-program').first().click()
    await page.getByTestId('input-program-name').fill(programName)
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').waitFor({ state: 'attached' })
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').fill(demoAccrualSql)
    await page.getByTestId('box-program-code-wrapper').getByRole('textbox').blur()
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('box-compile-status-success').waitFor()
    await expect(page).toHaveScreenshot('1-1-saved-sql-program.png')
  })

  const pipelineNameUrlEncoded = await test.step('Create a pipeline', async () => {
    await page.getByTestId('button-vertical-nav-pipelines').click()
    await page.getByTestId('button-add-pipeline').first().click()
    await page.getByTestId('input-pipeline-name').fill(pipelineName)
    await page.getByTestId('box-save-saved').waitFor()
    await page.getByTestId('input-builder-select-program').locator('button').click()
    await page.getByTestId('box-builder-program-options').getByRole('option', { name: programName }).click()
    await page.getByTestId('box-save-saved').waitFor()

    // Ensure storage is disabled
    await page.getByTestId('button-configure-resources').click()
    if ((await page.getByTestId('input-enable-storage').inputValue()) === 'false') {
      // Double switch to force `false` value from an undefined state
      await page.waitForTimeout(200)
      await page.getByTestId('input-enable-storage').click()
    }
    await page.waitForTimeout(200)
    await page.getByTestId('input-enable-storage').click()
    await page.waitForTimeout(200)
    await expect(page).toHaveScreenshot('2-1.png')
    await page.getByTestId('button-apply').click()

    await page.getByTestId('button-breadcrumb-pipelines').click()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).waitFor()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-edit').click()
    const pipelineNameUrlEncoded = page.url().match(/pipeline_name=([\w-%]+)/)?.[1] as string
    expect(pipelineNameUrlEncoded).toBeTruthy()
    invariant(pipelineNameUrlEncoded)
    return pipelineNameUrlEncoded
  })

  await test.step('Start the pipeline', async () => {
    await page.getByTestId('box-resources-thumb').waitFor()
    await page.getByTestId('button-breadcrumb-pipelines').click()
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).waitFor()
    await expect(page).toHaveScreenshot('3-1-compiling-program-binary.png')
    await page.getByTestId(`box-status-pipeline-${pipelineName}-Ready to run`).waitFor({ timeout: 270000 })
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-start').click()
    await page.getByTestId(`box-status-pipeline-${pipelineName}-Running`).waitFor({ timeout: 10000 })
  })

  await test.step('Post data to tables', async () => {
    faker.seed(123456789)

    // rows per table
    const num = {
      customer: 10,
      workspace: 100,
      work: 1000,
      credit: 100,
      user: 10000,
      task: 100000
    }
    // Batch size for insertion
    const batchSize = 1000

    const customers = (
      await inBatches(
        num.customer,
        batchSize,
        i => ({ insert: { id: i, name: faker.company.name() } }),
        data =>
          request.post(apiOrigin + `v0/pipelines/${pipelineNameUrlEncoded}/ingress/CUSTOMER_T?format=json&array=true`, {
            data
          })
      )
    ).map(r => r.insert)

    const workspaces = (
      await inBatches(
        num.workspace,
        batchSize,
        i => ({
          insert: { id: i, name: faker.company.catchPhrase(), customer_id: faker.helpers.arrayElement(customers).id }
        }),
        data =>
          request.post(
            apiOrigin + `v0/pipelines/${pipelineNameUrlEncoded}/ingress/workspace_t?format=json&array=true`,
            { data }
          )
      )
    ).map(r => r.insert)

    const works = (
      await inBatches(
        num.work,
        batchSize,
        i => ({
          insert: { id: i, name: faker.company.catchPhrase(), workspace_id: faker.helpers.arrayElement(workspaces).id }
        }),
        data =>
          request.post(apiOrigin + `v0/pipelines/${pipelineNameUrlEncoded}/ingress/WORK_T?format=json&array=true`, {
            data
          })
      )
    ).map(r => r.insert)

    await inBatches(
      num.credit,
      batchSize,
      i => ({
        insert: {
          id: i,
          total: faker.helpers.rangeToNumber({ min: 100, max: 10000 }),
          customer_id: faker.helpers.arrayElement(customers).id
        }
      }),
      data =>
        request.post(apiOrigin + `v0/pipelines/${pipelineNameUrlEncoded}/ingress/CREDIT_T?format=json&array=true`, {
          data
        })
    )

    const users = (
      await inBatches(
        num.user,
        batchSize,
        i => ({ insert: { id: i, name: faker.person.firstName() } }),
        data =>
          request.post(apiOrigin + `v0/pipelines/${pipelineNameUrlEncoded}/ingress/USER_T?format=json&array=true`, {
            data
          })
      )
    ).map(r => r.insert)

    await inBatches(
      num.task,
      batchSize,
      i => ({
        insert: {
          id: i,
          event_time: faker.date
            .between({ from: '2023-11-01T00:00:01.000Z', to: '2023-11-30T23:59:59.000Z' })
            .toISOString()
            .replace('T', ' ')
            .split('.')[0],
          user_id: faker.helpers.arrayElement(users).id,
          work_id: faker.helpers.arrayElement(works).id,
          total: faker.helpers.rangeToNumber({ min: 0, max: 20 })
        }
      }),
      data =>
        request.post(apiOrigin + `v0/pipelines/${pipelineNameUrlEncoded}/ingress/TASK_T?format=json&array=true`, {
          data
        })
    )
  })

  await test.step('Expand pipeline and open data browser', async () => {
    await page.getByTestId(`button-expand-pipeline-${pipelineName}`).click()
    await expect(page).toHaveScreenshot('4-1-pipeline details.png', {
      mask: [
        'box-pipeline-id',
        'box-pipeline-date-created',
        'box-pipeline-port',
        'box-pipeline-throughput-graph',
        'box-pipeline-throughput-value',
        'box-pipeline-memory-graph',
        'box-pipeline-memory-value'
      ].map(id => page.getByTestId(id))
    })
    await page
      .getByTestId(`box-details-${pipelineName}`)
      .getByTestId(`box-relation-actions-workspace_t`)
      .getByTestId(`button-inspect`)
      .click()
  })

  await test.step('View data in data browser', async () => {
    for (const relation of [
      'customer_t',
      'workspace_t',
      'work_t',
      'credit_t',
      'user_t',
      'task_t',
      'work_consumed_v',
      'top10_users',
      'workspace_consumed_v',
      'customer_consumed_v',
      'customer_total_credit_v',
      'customer_balance_v'
    ]) {
      await page.getByTestId('button-expand-relations').click()
      await page.getByTestId('box-relation-options').getByTestId(`button-option-relation-${relation}`).click()
      await page.getByTestId('button-expand-relations').hover() // Prevent the cursor causing flakes by tooltip popups
      await page.getByTestId('box-relation-options').waitFor({ state: 'hidden' })
      await page.getByTestId('box-relation-row').nth(2).waitFor()
      await page.waitForLoadState('domcontentloaded')
      await page.waitForTimeout(1000)
      await expect(page).toHaveScreenshot(`5-1-relation ${relation}.png`)
    }
  })

  await test.step('Stop the pipeline', async () => {
    await page.getByTestId('button-current-pipeline').click()
    await expect(page).toHaveScreenshot('6-1-pipeline details final.png', {
      mask: [
        'box-pipeline-id',
        'box-pipeline-date-created',
        'box-pipeline-port',
        'box-pipeline-throughput-graph',
        'box-pipeline-throughput-value',
        'box-pipeline-memory-graph',
        'box-pipeline-memory-value'
      ].map(id => page.getByTestId(id))
    })
    await page.getByTestId(`box-pipeline-actions-${pipelineName}`).getByTestId('button-shutdown').click()
    await page.getByTestId(`box-status-pipeline-${pipelineName}-Ready to run`).waitFor()
  })

  await test.step('Cleanup: Delete pipeline', async () => {
    await deletePipeline(page, pipelineName)
  })

  await test.step('Cleanup: Delete program', async () => {
    await deleteProgram(page, programName)
  })
})

const inBatches = async <R>(
  count: number,
  batchSize: number,
  getElem: (i: number) => R,
  action: (rs: R[]) => Promise<unknown>
) => {
  let leftover = count
  let result = Array<R>()
  while (leftover > 0) {
    const currentBatch = Math.min(leftover, batchSize)
    const data = Array(currentBatch)
      .fill(undefined)
      .map((v, i) => getElem(i + count - leftover))
    await action(data)
    leftover -= currentBatch
    result.push(...data)
  }
  return result
}
