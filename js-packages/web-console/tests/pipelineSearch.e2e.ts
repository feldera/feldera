import { expect, test } from '@playwright/test'
import { client } from '$lib/services/manager/client.gen'
import {
  deletePipeline,
  getExtendedPipeline,
  putPipeline
} from '$lib/services/pipelineManager'

const API_ORIGIN = (process.env.PLAYWRIGHT_API_ORIGIN ?? 'http://localhost:8080').replace(/\/$/, '')
client.setConfig({ baseUrl: API_ORIGIN })

const PREFIX = `test-search-${Date.now()}`
const PIPELINES = [`${PREFIX}-alpha`, `${PREFIX}-beta`, `${PREFIX}-gamma`] as const

async function cleanupPipelines() {
  for (const name of PIPELINES) {
    try {
      await deletePipeline(name)
    } catch {
      // Pipeline may not exist
    }
  }
}

/** Poll the API until a pipeline reaches "Stopped" status. */
async function waitForStopped(name: string, timeoutMs = 120_000) {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const p = await getExtendedPipeline(name)
    if (p.status === 'Stopped') return
    await new Promise((r) => setTimeout(r, 1000))
  }
  throw new Error(`Timed out waiting for ${name} to reach Stopped status`)
}

/** Navigate to the home page and wait until all test pipelines are visible. */
async function gotoAndWaitForPipelines(page: import('@playwright/test').Page) {
  await page.goto('/')
  for (const name of PIPELINES) {
    await expect(page.getByTestId(`box-row-${name}`)).toBeVisible()
  }
}

test.describe('Pipeline search', () => {
  test.setTimeout(180_000)

  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(120_000)
    await cleanupPipelines()
    for (const name of PIPELINES) {
      await putPipeline(name, {
        name,
        description: `E2E search test pipeline: ${name}`,
        program_code: 'create view test as (select 1)'
      })
    }
    // Wait for all pipelines to finish compiling via API polling,
    // so individual tests don't need to wait on DOM status changes.
    for (const name of PIPELINES) {
      await waitForStopped(name)
    }
  })

  test.afterAll(async ({}, testInfo) => {
    testInfo.setTimeout(60_000)
    await cleanupPipelines()
  })

  test('filters pipelines by name substring', async ({ page }) => {
    await gotoAndWaitForPipelines(page)

    const searchInput = page.getByTestId('input-pipeline-search')
    await searchInput.fill('alpha')

    await expect(page.getByTestId(`box-row-${PREFIX}-alpha`)).toBeVisible()
    await expect(page.getByTestId(`box-row-${PREFIX}-beta`)).not.toBeVisible()
    await expect(page.getByTestId(`box-row-${PREFIX}-gamma`)).not.toBeVisible()
  })

  test('search is case-insensitive', async ({ page }) => {
    await gotoAndWaitForPipelines(page)

    const searchInput = page.getByTestId('input-pipeline-search')
    await searchInput.fill(PREFIX.toUpperCase())

    // All three pipelines share the prefix, so all should be visible
    for (const name of PIPELINES) {
      await expect(page.getByTestId(`box-row-${name}`)).toBeVisible()
    }
  })

  test('shows empty state when no pipelines match', async ({ page }) => {
    await gotoAndWaitForPipelines(page)

    const searchInput = page.getByTestId('input-pipeline-search')
    await searchInput.fill('nonexistent-pipeline-xyz-999')

    await expect(page.getByText('No pipelines found')).toBeVisible()
  })

  test('clearing search shows all pipelines again', async ({ page }) => {
    await gotoAndWaitForPipelines(page)

    const searchInput = page.getByTestId('input-pipeline-search')
    await searchInput.fill('alpha')
    await expect(page.getByTestId(`box-row-${PREFIX}-beta`)).not.toBeVisible()

    await searchInput.clear()

    for (const name of PIPELINES) {
      await expect(page.getByTestId(`box-row-${name}`)).toBeVisible()
    }
  })

  test('search works together with status filter', async ({ page }) => {
    await gotoAndWaitForPipelines(page)

    // All test pipelines should be in "Stopped" (Ready To Start) status
    const statusSelect = page.getByTestId('select-pipeline-status')
    await statusSelect.selectOption('Ready To Start')

    const searchInput = page.getByTestId('input-pipeline-search')
    await searchInput.fill('beta')

    await expect(page.getByTestId(`box-row-${PREFIX}-beta`)).toBeVisible()
    await expect(page.getByTestId(`box-row-${PREFIX}-alpha`)).not.toBeVisible()

    // Switching to a non-matching status should hide everything
    await statusSelect.selectOption('Running')
    await expect(page.getByTestId(`box-row-${PREFIX}-beta`)).not.toBeVisible()
  })
})
