// The screenshots of docs.feldera.com/docs/operations/visualizing-profiles.md.
// Each test sets up one state of the UI, then compares it with the image in the docs.

import { expect, type Locator, type Page } from '@playwright/test'
import { annotate, type Box, boxOf, clearAnnotations, grow, union } from './annotate'
import { docsImage, expectClip as expectDocsClip, waitForTransitions } from './capture'
import {
  afterLayout,
  centerOn,
  chipBoxes,
  fitTo,
  nodeBox,
  test,
  waitForDiagram,
  waitForSqlScrollbarsToFade
} from './profileViewer'

const shot = (file: string) => docsImage('operations', file)

/** The region that the region shots expand and collapse: `input_zset`, which holds 4 operators. */
const REGION = 'n_r2'

/** Compares the area `clip` of the page with the image `file` of this docs page. */
const expectClip = (page: Page, file: string, clip: Box) => expectDocsClip(page, shot(file), clip)

const center = (box: Box) => ({ x: box.x + box.width / 2, y: box.y + box.height / 2 })

/** The panes of the viewer, in the default arrangement: the diagram on top, SQL and analysis below. */
const panes = (page: Page) => {
  const pane = page.locator('[data-pane]')
  return { diagram: pane.nth(0), sql: pane.nth(2), analysis: pane.nth(3) }
}

/** Types `query` in "Search node" and waits until the view is on the node it finds. */
async function searchNode(page: Page, query: string) {
  await page.getByPlaceholder('Search node').fill(query)
  await page.getByPlaceholder('Search node').press('Enter')
  await waitForDiagram(page)
}

/** Clicks node `id` with the pointer, in its center. */
async function clickNode(page: Page, id: string) {
  const { x, y } = center(await nodeBox(page, id))
  await page.mouse.click(x, y)
}

/** Moves the pointer off the diagram, so that no node glows from a hover. */
const parkPointer = (page: Page) => page.mouse.move(0, 0)

const scrollToTop = (pane: Locator) =>
  pane.evaluate((element) => {
    for (const child of element.querySelectorAll('*')) {
      child.scrollTop = 0
    }
  })

/** Clicks the "Skew" toggle of the row `metric` in the panel `analysis`, which expands or collapses its bars. */
const toggleBars = (analysis: Locator, metric: string) =>
  analysis
    .locator('.grid-cols-subgrid')
    .filter({ has: analysis.page().getByText(metric, { exact: true }) })
    .getByRole('button', { name: /^Skew/ })
    .click()

const metricsView = (page: Page, view: 'Overview' | 'Node' | 'Top nodes') =>
  panes(page).analysis.getByText(view, { exact: true }).click()

test.describe('Opening a profile from the viewer', () => {
  /** Opens the "Load profile" menu, frames `item` in it, and clips the button and the menu. */
  async function loadProfileMenu(page: Page, item: string, file: string) {
    const button = page.getByRole('button', { name: 'Load profile', exact: true })
    await button.click()
    const menu = page.getByTestId('btn-upload-support-bundle').locator('..')
    await expect(menu).toBeVisible()
    // The menu grows from no height.
    await waitForTransitions(menu)
    await parkPointer(page)
    await annotate(page, [
      {
        kind: 'frame',
        box: grow(await boxOf(menu.getByRole('button', { name: item, exact: true })), -1)
      }
    ])
    await expectClip(page, file, grow(union(await boxOf(button), await boxOf(menu)), 8))
    // The next tests share the viewer, and expect the menu closed.
    await button.click()
    await expect(menu).toBeHidden()
  }

  test('downloading a live profile', async ({ page }) => {
    await loadProfileMenu(page, 'Download profile', 'open-live-viewer.png')
  })

  test('uploading a bundle', async ({ page }) => {
    await loadProfileMenu(page, 'Open support bundle', 'open-upload-viewer.png')
  })
})

test.describe('The profile viewer', () => {
  test('the parts of the viewer', async ({ page }) => {
    const { diagram, sql, analysis } = panes(page)
    const toolbar = diagram.locator('div.flex-shrink-0').first()
    await annotate(page, [
      { kind: 'frame', box: grow(await boxOf(toolbar), -4), label: 'Toolbar', side: 'top' },
      {
        kind: 'frame',
        box: grow(await boxOf(page.locator('.visualizer-navigator')), 2),
        label: 'Minimap',
        side: 'right'
      },
      {
        kind: 'frame',
        box: grow(await boxOf(page.getByTestId('visualizer-diagram')), -4),
        label: 'Dataflow graph',
        side: 'bottom'
      },
      { kind: 'frame', box: grow(await boxOf(sql), -2), label: 'SQL', side: 'top' },
      { kind: 'frame', box: grow(await boxOf(analysis), -2), label: 'Analysis panel', side: 'top' }
    ])
    await waitForSqlScrollbarsToFade(page)
    await expect(page).toHaveScreenshot(shot('ui-structure.png'))
  })

  test('an expanded region', async ({ page }) => {
    await fitTo(page, [REGION], { padding: 60 })
    await expectClip(page, 'region-expanded.png', grow(await nodeBox(page, REGION), 40))
  })

  test('the paths through the selected node', async ({ page }) => {
    await centerOn(page, 'nn55', 0.8)
    await clickNode(page, 'nn55')
    await parkPointer(page)
    await expectClip(page, 'reachability.png', await boxOf(page.getByTestId('visualizer-diagram')))
  })

  test('nodes with a SQL source position', async ({ page }) => {
    await fitTo(page, ['nn20', 'nn21'], { padding: 80 })
    await expectClip(
      page,
      'source-chip.png',
      grow(union(await nodeBox(page, 'nn20'), await nodeBox(page, 'nn21')), 40)
    )
  })

  test('searching for a node', async ({ page }) => {
    await searchNode(page, 'customer')
    await parkPointer(page)
    const search = page.getByPlaceholder('Search node')
    await annotate(page, [
      { kind: 'frame', box: grow(await boxOf(search), 3), label: 'Search node', side: 'left' }
    ])
    await expectClip(page, 'search.png', await boxOf(panes(page).diagram))
  })

  test('the SQL of the selected node', async ({ page }) => {
    await searchNode(page, 'nn60')
    await clickNode(page, 'nn60')
    await parkPointer(page)
    await waitForSqlScrollbarsToFade(page)
    await expectClip(page, 'sources.png', await boxOf(panes(page).sql))
  })

  // Last of the tests that share this viewer: the layout after the region expands again moves some
  // edges, so a later shot of the diagram would differ.
  test('a collapsed region', async ({ page }) => {
    await fitTo(page, [REGION], { padding: 60 })
    const region = await nodeBox(page, REGION)
    // A double-click on the padding of the region, away from the nodes in it.
    await afterLayout(page, () => page.mouse.dblclick(region.x + 4, region.y + region.height / 2))
    await parkPointer(page)
    await centerOn(page, REGION, 1)
    const collapsed = await nodeBox(page, REGION)
    const chips = await chipBoxes(page, REGION)
    // Room right of the node for the labels.
    const clip = grow(union(collapsed, { ...chips.counter, width: chips.counter.width + 130 }), 40)
    await annotate(page, [
      { kind: 'frame', box: grow(chips.code, 2), label: 'Code chip', side: 'right' },
      { kind: 'frame', box: grow(chips.counter, 2), label: 'Child count chip', side: 'right' }
    ])
    await expectClip(page, 'region-collapsed.png', clip)
    await clearAnnotations(page)
    const { x, y } = center(collapsed)
    // The pointer on the region, so that it glows as a hovered node does. It first moves to an empty
    // spot of the dataflow graph: the graph still has the region as hovered, from the double-click,
    // and fires no new hover for it.
    await page.mouse.move(collapsed.x - 40, collapsed.y - 40)
    await page.mouse.move(x, y)
    await annotate(page, [
      { kind: 'frame', box: grow(chips.counter, 2), label: 'Expand', side: 'right' },
      {
        kind: 'arrow',
        from: { x: collapsed.x + 30, y: collapsed.y + collapsed.height + 30 },
        to: { x: collapsed.x + 30, y: collapsed.y + collapsed.height + 6 },
        label: 'Glow'
      }
    ])
    await expectClip(page, 'region-collapsed-hover.png', clip)
    // The next tests share the viewer, and expect the region expanded.
    await afterLayout(page, () => page.mouse.dblclick(x, y))
  })
})

test.describe('The Metrics tab', () => {
  // Taller, so that the analysis panel has room for its measurements.
  test.use({ viewport: { width: 1280, height: 1200 } })

  const expectAnalysis = async (page: Page, name: string) =>
    expectClip(page, name, await boxOf(panes(page).analysis))

  test('the Overview view', async ({ page }) => {
    await metricsView(page, 'Overview')
    await expectAnalysis(page, 'overall.png')
  })

  test.describe(() => {
    // Taller again, so that the panel shows the expanded bars of both rows.
    test.use({ viewport: { width: 1280, height: 1280 } })

    test('the measurements of a node', async ({ page }) => {
      await searchNode(page, 'nn77')
      await clickNode(page, 'nn77')
      const analysis = panes(page).analysis
      const metrics = ['Runtime percent', 'Input batches stats count']
      for (const metric of metrics) {
        await toggleBars(analysis, metric)
      }
      // A click scrolls its toggle into view. The shot shows the panel from the top.
      await scrollToTop(analysis)
      await waitForTransitions(analysis)
      await parkPointer(page)
      const panel = await boxOf(analysis)
      const first = async (locator: Locator) => grow(await boxOf(locator.first()), 2)
      // A margin left of the panel, over the edge of the SQL panel, for the labels.
      const MARGIN = 130
      const margin = { x: panel.x - MARGIN, y: panel.y, width: MARGIN, height: panel.height }
      const labelX = panel.x - 16
      await annotate(page, [
        { kind: 'cover', box: margin },
        {
          kind: 'callout',
          box: await first(analysis.getByTitle('Show this node in the diagram')),
          label: 'Title',
          x: labelX
        },
        {
          kind: 'callout',
          box: await first(analysis.getByText('persistent ID:')),
          label: 'persistent ID',
          x: labelX
        },
        {
          kind: 'callout',
          box: await first(analysis.locator('.metrics-block button[aria-expanded]')),
          label: 'Section',
          x: labelX
        },
        {
          kind: 'callout',
          box: await first(analysis.locator('.bar-chart')),
          label: 'Histogram',
          x: labelX
        },
        { kind: 'frame', box: await first(analysis.getByText(/^Skew/)), label: 'Skew', side: 'top' }
      ])
      await expectClip(page, 'node-metrics.png', union(margin, panel))
      // The next tests share the viewer, and expect the bars collapsed.
      for (const metric of metrics) {
        await toggleBars(analysis, metric)
      }
    })
  })

  test('selecting the metric', async ({ page }) => {
    const input = page.getByTitle('Select metric')
    await input.click()
    const list = page.getByRole('listbox')
    await expect(list).toBeVisible()
    const panel = await boxOf(panes(page).analysis)
    const listBox = await boxOf(list)
    await expectClip(page, 'metric-selection.png', {
      x: panel.x,
      y: panel.y,
      width: panel.width,
      height: Math.min(listBox.y + listBox.height + 16 - panel.y, panel.height)
    })
  })

  test('searching in a tab', async ({ page }) => {
    const analysis = panes(page).analysis
    const button = analysis.getByRole('button', { name: 'Search', exact: true })
    await button.click()
    const input = analysis.getByPlaceholder('Search metrics')
    await input.fill('records')
    await input.press('Enter')
    await waitForTransitions(analysis)
    await parkPointer(page)
    await annotate(page, [{ kind: 'frame', box: grow(await boxOf(button), 2) }])
    await expectAnalysis(page, 'search-tab.png')
    // The next tests share the viewer, and expect the search closed.
    await input.press('Escape')
  })

  test('the top nodes', async ({ page }) => {
    await metricsView(page, 'Top nodes')
    await expectAnalysis(page, 'important-nodes.png')
  })
})
