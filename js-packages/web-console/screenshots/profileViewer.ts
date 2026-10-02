// Helpers for the profile viewer shots: open the viewer on the bundle, and move and wait for its diagram.

import path from 'node:path'
import { test as base, expect, type Page } from '@playwright/test'
import { type Box, clearAnnotations } from './annotate'

/** The profile that the viewer shots load: a support bundle of the `fraud-detection` demo pipeline. */
export const BUNDLE_PATH = path.join(
  import.meta.dirname,
  'fixtures/fraud-detection-support-bundle.zip'
)

/** The limit to read the bundle or to lay out the diagram. Each takes about 0.8 seconds. */
const LOAD_TIMEOUT = 5_000

/**
 * Opens the profile viewer on the bundle, and waits until the diagram has its first layout.
 * The viewer only reads the bundle, so every pipeline manager request is refused. Then the shots
 * are the same with a manager, without one, and whatever the manager holds.
 */
export async function openProfile(page: Page) {
  await page.route(/\/v0\//, (route) => route.abort('connectionrefused'))
  // Analytics and in-app guides add overlays at random times.
  await page.route(/posthog|productfruits/, (route) => route.abort())
  await page.goto('/profile-viewer')
  await page.locator('input[type="file"]').setInputFiles(BUNDLE_PATH, { timeout: LOAD_TIMEOUT })
  await expect(page.getByTestId('visualizer-diagram')).toBeVisible({ timeout: LOAD_TIMEOUT })
  await waitForDiagram(page)
}

/**
 * A `test` whose `page` is one profile viewer, shared by all the tests of a worker. Starting the app
 * and loading the bundle take about 3 seconds, and a shot takes less than 0.5 seconds. Each test
 * gets the viewer back in its first state, at the viewport of the test.
 */
export const test = base.extend<object, { viewer: Page }>({
  viewer: [
    async ({ browser }, use) => {
      const page = await browser.newPage()
      await openProfile(page)
      const firstView = await page.evaluate(() => {
        // biome-ignore lint/suspicious/noExplicitAny: cytoscape keeps its instance in a private field
        const cy = (document.querySelector('.visualizer-graph') as any)._cyreg.cy
        return { zoom: cy.zoom(), pan: cy.pan() }
      })
      await page.exposeBinding('docsFirstView', () => firstView)
      await use(page)
      await page.close()
    },
    { scope: 'worker' }
  ],
  page: async ({ viewer, viewport }, use) => {
    if (viewport) {
      await viewer.setViewportSize(viewport)
    }
    await resetViewer(viewer)
    await use(viewer)
  }
})

/**
 * Undoes what a test did to the viewer: removes the annotations, closes a menu or a list, unpins the
 * node information, clears the search, moves the view back to where the profile opened, scrolls the
 * panes to the top, and shows the Overview of the metrics. A test that collapses a region or expands
 * a row of bars collapses it again.
 */
async function resetViewer(page: Page) {
  await clearAnnotations(page)
  await page.evaluate(async () => {
    for (const element of document.querySelectorAll('[data-pane] *')) {
      element.scrollTop = 0
    }
    const view = await (window as unknown as { docsFirstView(): Promise<object> }).docsFirstView()
    // biome-ignore lint/suspicious/noExplicitAny: cytoscape keeps its instance in a private field
    ;(document.querySelector('.visualizer-graph') as any)._cyreg.cy.viewport(view)
  })
  await page.keyboard.press('Escape')
  await page.getByPlaceholder('Search node').fill('')
  await page.locator('[data-pane]').nth(3).getByText('Overview', { exact: true }).click()
  await page.mouse.move(0, 0)
}

/** Waits until the diagram has nodes, and neither the nodes nor the view move between two animation frames. */
export async function waitForDiagram(page: Page) {
  await page.waitForFunction(
    () =>
      new Promise<boolean>((resolve) => {
        // biome-ignore lint/suspicious/noExplicitAny: cytoscape keeps its instance in a private field
        const cy = (document.querySelector('.visualizer-graph') as any)?._cyreg?.cy
        if (!cy || cy.nodes().length === 0) {
          return resolve(false)
        }
        // The view counts too: a search animates the pan and the zoom.
        const positions = () =>
          JSON.stringify([
            cy.pan(),
            cy.zoom(),
            cy.nodes().map((n: { position(): unknown }) => n.position())
          ])
        const before = positions()
        requestAnimationFrame(() => requestAnimationFrame(() => resolve(positions() === before)))
      }),
    undefined,
    { timeout: LOAD_TIMEOUT }
  )
}

/** The box of diagram node `id` on screen, in viewport coordinates. */
export async function nodeBox(page: Page, id: string): Promise<Box> {
  return page.evaluate((id) => {
    const container = document.querySelector('.visualizer-graph') as HTMLElement
    // biome-ignore lint/suspicious/noExplicitAny: cytoscape keeps its instance in a private field
    const cy = (container as any)._cyreg.cy
    const node = cy.getElementById(id)
    if (node.empty()) {
      throw new Error(`node ${id} is not in the diagram`)
    }
    const box = node.renderedBoundingBox({ includeLabels: false, includeOverlays: false })
    const origin = container.getBoundingClientRect()
    return { x: origin.x + box.x1, y: origin.y + box.y1, width: box.w, height: box.h }
  }, id)
}

/** Runs `action`, which starts a new layout, and waits until that layout has finished. */
export async function afterLayout(page: Page, action: () => Promise<void>) {
  await page.evaluate(() => {
    // biome-ignore lint/suspicious/noExplicitAny: cytoscape keeps its instance in a private field
    const cy = (document.querySelector('.visualizer-graph') as any)._cyreg.cy
    const done = window as unknown as { docsLayoutDone: boolean }
    done.docsLayoutDone = false
    cy.one('layoutstop', () => {
      done.docsLayoutDone = true
    })
  })
  await action()
  await page.waitForFunction(
    () => (window as unknown as { docsLayoutDone: boolean }).docsLayoutDone,
    undefined,
    { timeout: LOAD_TIMEOUT }
  )
  await waitForDiagram(page)
}

/**
 * Fits the view to the nodes `ids`, and to the nodes up to `hops` edges away from them. Unlike a
 * search, this adds no glow.
 */
export async function fitTo(page: Page, ids: string[], { hops = 0, padding = 30 } = {}) {
  await page.evaluate(
    ({ ids, hops, padding }) => {
      // biome-ignore lint/suspicious/noExplicitAny: cytoscape keeps its instance in a private field
      const cy = (document.querySelector('.visualizer-graph') as any)._cyreg.cy
      let nodes = cy.collection(ids.map((id: string) => cy.getElementById(id)))
      for (let hop = 0; hop < hops; hop++) {
        nodes = nodes.closedNeighborhood().nodes()
      }
      cy.fit(nodes, padding)
    },
    { ids, hops, padding }
  )
  await waitForDiagram(page)
}

/** Puts node `id` in the center of the view at `zoom`. Unlike a search, this adds no glow. */
export async function centerOn(page: Page, id: string, zoom: number) {
  await page.evaluate(
    ({ id, zoom }) => {
      // biome-ignore lint/suspicious/noExplicitAny: cytoscape keeps its instance in a private field
      const cy = (document.querySelector('.visualizer-graph') as any)._cyreg.cy
      cy.zoom(zoom)
      cy.center(cy.getElementById(id))
    },
    { id, zoom }
  )
  await waitForDiagram(page)
}

/**
 * Waits until the scrollbars of the SQL panel have faded. They show while the panel scrolls to a
 * selection, so without this wait a shot has them or not, at random. Monaco hides them 0.5 seconds
 * after the scroll stops.
 */
export async function waitForSqlScrollbarsToFade(page: Page) {
  await expect(page.locator('.monaco-editor .scrollbar.visible')).toHaveCount(0, { timeout: 2_000 })
}
