/**
 * Two regressions in the support bundle viewer layout: the vertical resizer's direction, and
 * which gesture navigates to a node's SQL.
 *
 * ## The resizer
 *
 * The graph pane is rendered inside `{#if hasProfile}`; the analysis pane below it is
 * unconditional. PaneForge orders panes by registration time unless each pane declares an
 * explicit `order`. When a profile loads *after* the layout has mounted (profileData
 * undefined -> defined), the graph pane registers second and, without `order`, PaneForge
 * treats it as the *lower* pane. The resizer then drives the panes backwards, so moving the
 * divider down shrinks the top pane instead of growing it - the reversal the user reported.
 *
 * The test drives the actual resize behavior rather than an internal-order proxy: it presses
 * ArrowDown on the handle (equivalent to dragging the divider down) and asserts the top pane
 * grows. PaneForge writes each pane's layout size straight to its `flex-grow` (see
 * computePaneFlexBoxStyle), so the rendered size is read directly - no pixel layout needed.
 * An inverted order shrinks the top pane and fails the assertion.
 *
 * ProfilerDiagram and the tab panels are stubbed so the test exercises only the pane layout,
 * not cytoscape or Monaco. To see the assertion fail, drop the `order` props from the two
 * vertical panes in SupportBundleViewerLayout.svelte.
 */

import type { SourcePositionRange } from 'profiler-lib'
import { TriageResults } from 'triage-types'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

// vi.mock is hoisted above module-scope bindings, so each factory imports the stub inline
// (no top-level references allowed). Stubbing ProfilerDiagram and the tab panels keeps the
// test on the pane layout, off cytoscape and Monaco.
vi.mock('./ProfilerDiagram.svelte', async () => ({
  default: (await import('../test-support/ComponentStub.svelte')).default
}))
vi.mock('./tabs/MetricsTab.svelte', async () => ({
  default: (await import('../test-support/ComponentStub.svelte')).default
}))
vi.mock('./tabs/LogsTab.svelte', async () => ({
  default: (await import('../test-support/ComponentStub.svelte')).default
}))
vi.mock('./tabs/ConfigTab.svelte', async () => ({
  default: (await import('../test-support/ComponentStub.svelte')).default
}))
vi.mock('./tabs/IssuesTab.svelte', async () => ({
  default: (await import('../test-support/ComponentStub.svelte')).default
}))
vi.mock('./tabs/SqlTab.svelte', async () => ({
  default: (await import('../test-support/ComponentStub.svelte')).default
}))

// Imported after vi.mock so the stubs take effect.
import { capture } from '../test-support/ComponentStub.svelte'
import SupportBundleViewerLayout from './SupportBundleViewerLayout.svelte'

const baseProps = () => ({
  profileData: undefined,
  dataflowData: undefined,
  programCode: undefined,
  triageResults: new TriageResults(),
  profileFiles: [],
  selectedTimestamp: null,
  onSelectTimestamp: () => { },
  sqlPanelFullHeight: false
})

// Any non-undefined value flips `hasProfile`; the parsed profile is only ever handed to the
// stubbed ProfilerDiagram, so its shape is irrelevant here.
const someProfile = {} as never

/**
 * Move the vertical divider down and assert the pane above it grows.
 *
 * The graph/analysis split is the only PaneGroup using `pane-divider-horizontal`. The top
 * pane is that group's first `[data-pane]` in DOM order (the graph pane). Pressing ArrowDown
 * on the handle nudges the divider down, which must enlarge the top pane; PaneForge mirrors
 * each pane's size onto its `flex-grow`, so we compare that before and after. When the panes
 * are inverted the top pane shrinks instead, exactly as a mouse drag would appear.
 */
const expectDividerDownGrowsTopPane = async (container: HTMLElement) => {
  const resizer = container.querySelector<HTMLElement>(
    '.pane-divider-horizontal[data-pane-resizer]'
  )
  if (!resizer) {
    throw new Error('vertical resize handle not found')
  }
  const groupId = resizer.getAttribute('data-pane-group-id')
  const topPane = container.querySelector<HTMLElement>(
    `[data-pane][data-pane-group-id="${groupId}"]`
  )
  if (!topPane) {
    throw new Error('top pane not found')
  }

  const topPaneSize = () => Number.parseFloat(topPane.style.flexGrow)
  // PaneForge assigns flex-grow from an effect after the pane registers; wait for it.
  await expect.poll(() => Number.isFinite(topPaneSize())).toBe(true)
  const sizeBefore = topPaneSize()

  resizer.focus()
  resizer.dispatchEvent(
    new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true, cancelable: true })
  )

  await expect.poll(topPaneSize).toBeGreaterThan(sizeBefore)
}

describe('SupportBundleViewerLayout vertical pane order', () => {
  it('grows the graph pane when the divider moves down after a profile loads late', async () => {
    const { container, rerender } = render(SupportBundleViewerLayout, baseProps())

    // Bundle without a circuit profile: no graph pane, no vertical resizer yet.
    expect(container.querySelector('.pane-divider-horizontal')).toBeNull()

    // Profile arrives -> graph pane mounts and registers after the analysis pane.
    await rerender({ profileData: someProfile })

    await expectDividerDownGrowsTopPane(container)
  })

  it('keeps the correct direction in the SQL full-height layout too', async () => {
    const { container, rerender } = render(SupportBundleViewerLayout, {
      ...baseProps(),
      sqlPanelFullHeight: true
    })

    await rerender({ profileData: someProfile })

    await expectDividerDownGrowsTopPane(container)
  })
})

/**
 * Issue 6994: navigating to a node's SQL with a single click on any node - region
 * or operator.
 *
 * The stub the diagram is mocked with records the callbacks the layout registers, so the test
 * can raise them the way the Visualizer does; cytoscape cannot run here.
 */
describe('SupportBundleViewerLayout source navigation', () => {
  const range = (line: number): SourcePositionRange =>
    ({ start: { line, column: 1 }, end: { line, column: 9 } }) as SourcePositionRange

  // The ids the fake profile knows: one operator, one region carrying the positions of the
  // operators inside it, and one node compiled from no SQL at all.
  const ranges: Record<string, SourcePositionRange[]> = {
    operator: [range(3)],
    region: [range(3), range(7)],
    internal: []
  }

  const mount = () => {
    const highlighted: SourcePositionRange[][] = []
    capture.profile = { getSourceRanges: (id: string) => ranges[id] ?? [] } as never
    render(SupportBundleViewerLayout, {
      props: {
        ...baseProps(),
        profileData: someProfile,
        programCode: ['a', 'b', 'c'],
        onHighlightSourceRanges: (r: SourcePositionRange[]) => highlighted.push(r)
      }
    })
    const click = capture.callbacks?.onNodeClick
    if (!click) {
      throw new Error('the layout registered no node-click callback')
    }
    return { click, highlighted }
  }

  beforeEach(() => {
    capture.callbacks = undefined
    capture.profile = null
  })

  it('navigates to the SQL of an operator on a single click', () => {
    const { click, highlighted } = mount()
    click('operator')
    expect(highlighted).toEqual([ranges.operator])
  })

  it('navigates to the SQL of a region, whose double click is taken by expanding it', () => {
    const { click, highlighted } = mount()
    click('region')
    expect(highlighted).toEqual([ranges.region])
  })

  it('keeps the previous highlight for a node compiled from no SQL', () => {
    const { click, highlighted } = mount()
    click('operator')
    click('internal')
    expect(highlighted).toEqual([ranges.operator])
  })
})
