// The screenshots of docs.feldera.com/docs/operations/visualizing-profiles.md that need a pipeline
// manager. They run against a real instance; see README.md.

import path from 'node:path'
import { test as base, expect, type Page } from '@playwright/test'
import { putPipeline } from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  TEST_COMPILATION_PROFILE,
  waitForCompilation
} from '$lib/services/testPipelineHelpers'
import { annotate, boxOf, clearAnnotations, grow, union } from './annotate'
import { APP_START_TIMEOUT, docsImage, expectClip, navigate, waitForTransitions } from './capture'
import { BUNDLE_PATH } from './profileViewer'

configureTestClient()

const shot = (file: string) => docsImage('operations', file)

/** A name of its own, so that the shots never change a pipeline of the instance's user. */
const PIPELINE_NAME = 'docs-screenshots-visualizing-profiles'

/** Analytics and in-app guides add overlays at random times. */
const blockOverlays = (page: Page) => page.route(/posthog|productfruits/, (route) => route.abort())

/**
 * Makes `showOpenFilePicker` return a file named as the fixture bundle. The file is in the origin
 * private file system, so its handle is a real one, which the bundle history can store.
 */
const stubFilePicker = (page: Page) =>
  page.addInitScript((name) => {
    Object.assign(window, {
      showOpenFilePicker: async () => [
        await (await navigator.storage.getDirectory()).getFileHandle(name, { create: true })
      ]
    })
  }, path.basename(BUNDLE_PATH))

/**
 * The icon in the header. The right drawer has a button with the same test ID but no title. The
 * drawer is closed, but off screen, so Playwright counts its button as visible.
 */
const openBundleButton = (page: Page) =>
  page.getByTestId('btn-open-support-bundle').and(page.getByTitle('Open support bundle'))

/**
 * A `test` whose `page` is one app, shared by all the tests of a worker. The app starts once, in
 * about 2.5 seconds, and each test moves to its page inside it (`navigate`). Each test starts with
 * no annotations, and no menu or dialog open.
 */
const test = base.extend<object, { app: Page }>({
  app: [
    async ({ browser }, use) => {
      const page = await browser.newPage()
      await blockOverlays(page)
      await stubFilePicker(page)
      await page.goto('/')
      await expect(openBundleButton(page)).toBeVisible({ timeout: APP_START_TIMEOUT })
      await use(page)
      await page.close()
    },
    { scope: 'worker' }
  ],
  page: async ({ app }, use) => {
    await clearAnnotations(app)
    await app.keyboard.press('Escape')
    await use(app)
  }
})

test.describe('Opening a profile from a pipeline', () => {
  test.beforeAll(async () => {
    test.setTimeout(600_000)
    // The shots show only the monitoring panel, so the smallest program does. It also compiles fast.
    await putPipeline(PIPELINE_NAME, {
      name: PIPELINE_NAME,
      description: 'Shown in the docs screenshots',
      program_code: 'create view v as (select 1)',
      program_config: { profile: TEST_COMPILATION_PROFILE }
    })
    await waitForCompilation(PIPELINE_NAME)
  })

  test.afterAll(async () => {
    await cleanupPipeline(PIPELINE_NAME)
  })

  /** Opens the pipeline on the Runtime tab, and returns the box of the "View profile" button. */
  async function openPipeline(page: Page) {
    await navigate(page, `/pipelines/${PIPELINE_NAME}/`)
    await page.getByRole('tab', { name: 'Runtime' }).click()
    await expect(page.getByTestId('btn-view-profile')).toBeVisible({ timeout: 2_000 })
    return union(
      await boxOf(page.getByTestId('btn-view-profile')),
      await boxOf(page.getByLabel('Support bundle options'))
    )
  }

  /** Moves the pointer off the button, so that its tooltip closes. */
  async function parkPointer(page: Page) {
    const tabs = await boxOf(page.getByRole('tablist').first())
    await page.mouse.move(tabs.x, tabs.y + tabs.height + 100)
  }

  test('downloading a live profile', async ({ page }) => {
    const button = await openPipeline(page)
    await parkPointer(page)
    await annotate(page, [
      { kind: 'frame', box: grow(await boxOf(page.getByTestId('btn-view-profile')), 2) }
    ])
    await expectClip(page, shot('open-live-pipeline.png'), grow(button, 12))
  })

  test('uploading a bundle', async ({ page }) => {
    const button = await openPipeline(page)
    await page.getByLabel('Support bundle options').click()
    const upload = page.getByTestId('btn-upload-support-bundle')
    const menu = page.getByTestId('box-support-bundle-menu')
    await expect(upload).toBeVisible()
    // The menu grows from no height.
    await waitForTransitions(menu)
    await parkPointer(page)
    await annotate(page, [{ kind: 'frame', box: grow(await boxOf(upload), -1) }])
    await expectClip(
      page,
      shot('open-upload-pipeline.png'),
      grow(union(button, await boxOf(menu)), 8)
    )
  })
})

test.describe('Opening a profile from the home page', () => {
  /** Opens the "Open support bundle" dialog, and returns its box. */
  async function openDialog(page: Page) {
    await openBundleButton(page).click()
    const dialog = page.getByTestId('box-generic-dialog')
    await expect(dialog).toBeVisible()
    // Off the dialog, so that no row of it glows from a hover.
    await page.mouse.move(0, 0)
    return boxOf(dialog)
  }

  test.beforeEach(async ({ page }) => {
    await navigate(page, '/')
  })

  test('uploading a bundle', async ({ page }) => {
    const dialog = await openDialog(page)
    await annotate(page, [
      { kind: 'frame', box: grow(await boxOf(page.getByTestId('btn-pick-support-bundle')), 3) }
    ])
    await expectClip(page, shot('open-upload-home.png'), dialog)
  })

  test('opening a bundle from the history', async ({ page }) => {
    // Picking a bundle adds it to the history.
    await openDialog(page)
    await page.getByTestId('btn-pick-support-bundle').click()
    await expect(page.getByTestId('box-support-bundle-confirm')).toBeVisible()
    // The confirmation replaces the history. "Dismiss" shows the history again.
    await page.getByLabel('Dismiss').click()
    await expect(page.getByTestId('box-support-bundle-confirm')).toBeHidden()
    await page.mouse.move(0, 0)
    const dialog = await boxOf(page.getByTestId('box-generic-dialog'))
    const entry = page.getByTestId('btn-open-bundle-from-list')
    await expect(entry).toHaveCount(1)
    await annotate(page, [{ kind: 'frame', box: grow(await boxOf(entry), 1) }])
    await expectClip(page, shot('open-history-home.png'), dialog)
  })
})
