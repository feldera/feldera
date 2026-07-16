/**
 * Regression test for the vertical resizer in the support bundle viewer layout.
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

import { TriageResults } from 'triage-types'
import { describe, expect, it, vi } from 'vitest'
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
import SupportBundleViewerLayout from './SupportBundleViewerLayout.svelte'

const baseProps = () => ({
  profileData: undefined,
  dataflowData: undefined,
  programCode: undefined,
  triageResults: new TriageResults(),
  profileFiles: [],
  selectedTimestamp: null,
  onSelectTimestamp: () => {},
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
