/**
 * A `/create/` link that carries code opens a new empty pipeline with the code as
 * unsaved edits. The console saves and compiles the code only after the user confirms.
 */
import { expect, type Page, test } from '@playwright/test'
import {
  deletePipeline,
  getDemos,
  getExtendedPipeline,
  getPipelines,
  postPipelineAction,
  putPipeline
} from '#lib/services/pipelineManager.ts'

const PREFIX = `test-link-${Date.now()}`
const createdPipelines: string[] = []

const SQL = 'create view v as select 1'
const RUST = '// udf.rs from a link'
const TOML = '# udf.toml from a link'

const linkCode = { 'program.sql': SQL, 'udf.rs': RUST, 'udf.toml': TOML }

/** Open a `/create/` link, and record the pipelines it may create for cleanup. */
const openLink = async (page: Page, name: string, code: Record<string, string> = {}) => {
  createdPipelines.push(name, `${name}-copy`)
  await page.goto(`/create/?${new URLSearchParams({ name, ...code })}`)
}

const savedCode = async (name: string) => {
  const { programCode, programUdfRs, programUdfToml } = await getExtendedPipeline(name)
  return { programCode, programUdfRs, programUdfToml }
}

const desiredStatus = async (name: string) =>
  (await getExtendedPipeline(name)).deploymentDesiredStatus

const emptyCode = { programCode: '', programUdfRs: '', programUdfToml: '' }

const fileTab = (page: Page, fileName: string) =>
  page.locator('button.tab', { hasText: new RegExp(`^\\s*${fileName.replace('.', '\\.')}\\s*$`) })

const expectUnsaved = (page: Page, fileName: string) =>
  expect(fileTab(page, fileName).getByTitle('Unsaved changes')).toBeVisible()

const pressCtrlS = async (page: Page) => {
  await page.locator('.monaco-editor .view-lines').first().click()
  await page.keyboard.press('ControlOrMeta+s')
}

test.describe('Pasted /create/ link', () => {
  test.setTimeout(120_000)

  test.afterAll(async () => {
    // The pipelines never ran, so they have no storage to clear. A failed test may
    // leave a start scheduled, which the kill cancels. `createdPipelines` also lists
    // names that no test created, so the cleanup ignores errors.
    for (const name of createdPipelines) {
      await postPipelineAction(name, 'kill').catch(() => {})
      await deletePipeline(name).catch(() => {})
    }
  })

  test('opens a new empty pipeline with the link code unsaved', async ({ page }) => {
    const name = `${PREFIX}-open`
    await openLink(page, name, linkCode)

    await expect(page).toHaveURL(new RegExp(`/pipelines/${name}/$`))
    await expect(page.locator('.monaco-editor .view-lines')).toContainText(SQL)
    for (const fileName of Object.keys(linkCode)) {
      await expectUnsaved(page, fileName)
    }
    expect(await savedCode(name)).toEqual(emptyCode)
  })

  test('saves the link code only after the user confirms', async ({ page }) => {
    const name = `${PREFIX}-save`
    await openLink(page, name, linkCode)
    await expect(page).toHaveURL(new RegExp(`/pipelines/${name}/$`))
    await expectUnsaved(page, 'program.sql')
    const cancelLinkDialog = async () => {
      await expect(page.getByTestId('box-dialog-title')).toHaveText('Compile code from a link?')
      await page.getByTestId('btn-dialog-cancel').click()
      await expect(page.getByTestId('box-generic-dialog')).not.toBeVisible()
    }

    // Leaving the page and declining the save keeps the user on the page.
    await page.locator('a[href="/"]').first().click()
    await page.getByTestId('btn-dialog-save-and-continue').click()
    await cancelLinkDialog()
    await expect(page).toHaveURL(new RegExp(`/pipelines/${name}/$`))

    await pressCtrlS(page)
    await cancelLinkDialog()

    expect(await savedCode(name)).toEqual(emptyCode)
    expect(await desiredStatus(name)).toBe('Stopped')
    await expectUnsaved(page, 'program.sql')

    // Confirming saves every file.
    await pressCtrlS(page)
    await page.getByTestId('btn-dialog-save-pasted-link').click()
    await expect
      .poll(() => savedCode(name))
      .toEqual({ programCode: SQL, programUdfRs: RUST, programUdfToml: TOML })
    await expect(fileTab(page, 'program.sql').getByTitle('Unsaved changes')).not.toBeVisible()
    expect(await desiredStatus(name)).toBe('Stopped')
  })

  test('opens a -copy pipeline when the name is taken', async ({ page }) => {
    const name = `${PREFIX}-taken`
    const existingCode = 'create view existing as select 2'
    await putPipeline(name, { name, description: '', program_code: existingCode })
    await openLink(page, name, { 'program.sql': SQL })

    await expect(page).toHaveURL(new RegExp(`/pipelines/${name}-copy/$`))
    await expectUnsaved(page, 'program.sql')
    expect(await savedCode(`${name}-copy`)).toEqual(emptyCode)
    expect((await savedCode(name)).programCode).toBe(existingCode)
  })

  test('opens a demo from a link without code as before', async ({ page }) => {
    const existing = new Set((await getPipelines()).map((pipeline) => pipeline.name))
    const demo = (await getDemos()).find((demo) => !existing.has(demo.name))
    test.skip(!demo, 'every demo already has a pipeline')
    createdPipelines.push(demo!.name)

    await page.goto(`/create/?name=${encodeURIComponent(demo!.name)}`)

    await expect(page).toHaveURL(new RegExp(`/pipelines/${demo!.name}/$`))
    expect((await savedCode(demo!.name)).programCode).toBe(demo!.program_code)
    await expect(fileTab(page, 'program.sql')).toBeVisible()
    await expect(fileTab(page, 'program.sql').getByTitle('Unsaved changes')).not.toBeVisible()
  })
})
