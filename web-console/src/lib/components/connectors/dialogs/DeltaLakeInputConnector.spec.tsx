import Layout from '$app/(spa)/layout'
import { NewConnectorResponse } from '$lib/services/manager'
import BigNumber from 'bignumber.js/bignumber.js'

import { Button } from '@mui/material'
import { expect, test } from '@playwright/experimental-ct-react'

import { DeltaLakeInputConnectorDialog } from './DeltaLakeInputConnector'

test.use({ viewport: { width: 1000, height: 800 } })

test('DeltaLake input creation', async ({ mount, page }) => {
  test.setTimeout(20000)
  let config: any = null
  await mount(
    <Layout>
      <DeltaLakeInputConnectorDialog
        show={true}
        setShow={() => {}}
        existingTitle={null}
        onSuccess={result => {
          config = result
        }}
        submitButton={
          <Button
            variant='contained'
            color='success'
            endIcon={<i className={`bx bx-check`} style={{}} />}
            type='submit'
            data-testid='button-create'
          >
            Create
          </Button>
        }
      ></DeltaLakeInputConnectorDialog>
    </Layout>
  )
  await test.step('Name tab', async () => {
    await page.getByTestId('input-storage-uri').fill('s3://bucket-0_1/pathA/pathB')
    await expect(page).toHaveScreenshot('1-1.png')
    await page.getByTestId('input-storage-uri').fill('')
    await expect(page).toHaveScreenshot('1-2.png')
    await page.getByTestId('input-storage-uri').fill('gs://bucket-0_1/pathA/pathB')
    await expect(page).toHaveScreenshot('1-3.png')
    await page.getByTestId('button-next').click()
  })
  await test.step('Options tab', async () => {
    await page.getByTestId('input-select-field').click()
    await page.getByRole('option', { name: 'google_service_account', exact: true }).click()
    await page.getByTestId('input-google_service_account').fill('account1')
    await page.getByTestId('input-select-field').click()
    await page.getByRole('option', { name: 'google_service_account_key' }).click()
    await page.getByTestId('input-google_service_account_key').fill('pass')
    await expect(page).toHaveScreenshot('2-1.png')
    await page.getByTestId('input-edit-json').click()
    await page.locator('.monaco-editor').waitFor()
    await expect(page).toHaveScreenshot('2-2.png')
    await page.getByTestId('input-edit-json').click()
    await page.getByTestId('button-remove-google_service_account_key').click()
    await expect(page).toHaveScreenshot('2-3.png')
    await page.getByTestId('input-edit-json').click()
    await expect(page).toHaveScreenshot('2-4.png', {
      animations: 'disabled'
    })
    await page.getByTestId('input-edit-json').click()
  })
  await test.step('Ingest mode tab', async () => {
    await page.getByTestId('button-tab-ingest-mode').click()
    await expect(page).toHaveScreenshot('3-1.png')
    await page.getByTestId('input-snapshot-filter').fill('num BETWEEN 1 AND 10')
    await page.getByTestId('input-version').fill('11')
    await page.getByTestId('input-edit-json').click()
    await expect(page).toHaveScreenshot('3-2.png', { animations: 'disabled' })
    await page.getByTestId('input-edit-json').click()
    await page.getByTestId('input-datetime').fill('2024-06-10T13:08')
    await expect(page).toHaveScreenshot('3-3.png')
    await page.getByTestId('input-edit-json').click()
    await expect(page).toHaveScreenshot('3-4.png', { animations: 'disabled' })
    await page.getByTestId('input-edit-json').click()
    await page.getByTestId('input-timestamp-column').fill('time')
    await page.getByTestId('input-timestamp-column').fill('')
    await page.getByTestId('input-edit-json').click()
    await expect(page).toHaveScreenshot('3-5.png')
    await page.getByTestId('input-edit-json').click()
    await page.getByTestId('input-ingest-mode').click()
    await page.getByRole('option', { name: 'follow', exact: true }).click()
    await expect(page).toHaveScreenshot('3-6.png')
    await page.getByTestId('input-edit-json').click()
    await expect(page).toHaveScreenshot('3-7.png')
    await page.getByTestId('input-edit-json').click()
    await page.getByTestId('input-version').fill('12')
    await page.getByTestId('input-ingest-mode').click()
    await page.getByRole('option', { name: 'snapshot_and_follow' }).click()
    await expect(page).toHaveScreenshot('3-8.png')
    await page.getByTestId('input-edit-json').click()
    await expect(page).toHaveScreenshot('3-9.png')
    await page.getByTestId('input-edit-json').click()
  })
  await test.step('Try to submit form, fix missing field', async () => {
    await page.getByTestId('button-create').click()
    await expect(page).toHaveScreenshot('4-1.png')
    await page.getByTestId('input-datasource-name').fill('testSource')
    await page.getByTestId('button-next').click()
    await page.getByTestId('button-next').click()

    await page.route('*/**/connectors', async route => {
      const json: NewConnectorResponse = { connector_id: '00000000-0000-0000-0000-000000000000' }
      await route.fulfill({ json })
    })
    await page.getByTestId('button-create').click()

    {
      // Need to wait for onSuccess callback when running `playwright test` without `--ui-host=0.0.0.0`
      await page.waitForLoadState('networkidle')
      await page.waitForTimeout(30)
    }

    await expect(config).toEqual({
      connector_id: '00000000-0000-0000-0000-000000000000',
      name: 'testSource',
      description: '',
      config: {
        transport: {
          name: 'delta_table_input',
          config: {
            uri: 'gs://bucket-0_1/pathA/pathB',
            mode: 'snapshot_and_follow',
            google_service_account: 'account1',
            snapshot_filter: 'num BETWEEN 1 AND 10',
            version: Object.fromEntries(Object.entries(new BigNumber(12))),
            timestamp_column: undefined,
            datetime: undefined
          }
        },
        enable_output_buffer: undefined,
        max_output_buffer_size_records: undefined,
        max_output_buffer_time_millis: undefined
      }
    })
  })
})
