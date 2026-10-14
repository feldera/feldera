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

import { formatDateTime } from 'common-lib/format'
import type { SourcePositionRange } from 'profiler-lib'
import { TriageResults } from 'triage-types'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

// vi.mock is hoisted above module-scope bindings, so each factory imports the stub inline
// (no top-level references allowed). Stubbing ProfilerDiagram and the tab panels keeps the
// test on the pane layout, off cytoscape and Monaco.
vi.mock('./ProfilerDiagram.svelte', async () => ({
  default: (await import('../test-support/LayoutPanelStub.svelte')).default
}))
vi.mock('./tabs/MetricsTab.svelte', async () => ({
  default: (await import('../test-support/LayoutPanelStub.svelte')).default
}))
vi.mock('./tabs/LogsTab.svelte', async () => ({
  default: (await import('../test-support/LayoutPanelStub.svelte')).default
}))
vi.mock('./tabs/ConfigTab.svelte', async () => ({
  default: (await import('../test-support/LayoutPanelStub.svelte')).default
}))
vi.mock('./tabs/IssuesTab.svelte', async () => ({
  default: (await import('../test-support/LayoutPanelStub.svelte')).default
}))
vi.mock('./tabs/SourceTab.svelte', async () => ({
  default: (await import('../test-support/LayoutPanelStub.svelte')).default
}))

// Imported after vi.mock so the stubs take effect.
import { capture } from '../test-support/LayoutPanelStub.svelte'
import SupportBundleViewerLayout from './SupportBundleViewerLayout.svelte'

const baseProps = () => ({
  profileData: undefined,
  dataflowData: undefined,
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
        sourceFiles: [{ name: 'program.sql', language: 'sql', code: 'a\nb\nc' }],
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

  it('selects nothing for a node compiled from no SQL', () => {
    // The metrics panel has already switched to the clicked node, so leaving the previous
    // node's statements selected would show two different nodes at once.
    const { click, highlighted } = mount()
    click('operator')
    click('internal')
    expect(highlighted).toEqual([ranges.operator, []])
  })
})

describe('SupportBundleViewerLayout Connectors tab', () => {
  // The analysis tabs are a SegmentedControl; each item holds a hidden radio input.
  const connectorsTab = (container: HTMLElement) => {
    const tab = container.querySelector<HTMLInputElement>('input[value="Connectors"]')
    if (!tab) {
      throw new Error('Connectors tab not found')
    }
    return tab
  }

  it('is disabled when the bundle has no connector statistics', () => {
    const { container } = render(SupportBundleViewerLayout, baseProps())
    expect(connectorsTab(container).disabled).toBe(true)
  })

  it('is enabled when the bundle has connector statistics', () => {
    const { container } = render(SupportBundleViewerLayout, {
      ...baseProps(),
      connectorMetrics: { tables: new Map(), views: new Map() }
    })
    expect(connectorsTab(container).disabled).toBe(false)
  })
})

describe('SupportBundleViewerLayout Health tab', () => {
  const healthTab = () => page.getByRole('radio', { name: 'Health' })

  const event = (event_id: string, recorded_at: string) => ({
    event_id,
    recorded_at,
    program_status: 'Success' as const,
    storage_status: 'InUse' as const,
    deployment_has_error: true,
    deployment_resources_status: 'Provisioned' as const,
    deployment_resources_desired_status: 'Provisioned' as const,
    deployment_runtime_status: 'Running' as const
  })
  // The range label of the 72-hour timeline that ends at `endAt`.
  const timelineRange = (endAt: string) => {
    const end = new Date(endAt)
    const start = new Date(end.getTime() - 72 * 60 * 60 * 1000)
    return page.getByText(`${formatDateTime(start)} - ${formatDateTime(end)}`)
  }
  // Playwright never finds the segment label visible and stable here, so the test clicks it
  // through the DOM.
  const openHealthTab = () =>
    (page.getByText('Health', { exact: true }).element() as HTMLElement).click()

  it.each([
    ['has no pipeline_events.json', undefined],
    ['has an empty pipeline_events.json', []]
  ])('is disabled when the bundle %s', async (_, pipelineEvents) => {
    render(SupportBundleViewerLayout, { ...baseProps(), pipelineEvents })
    await expect.element(healthTab()).toBeDisabled()
  })

  it('shows the pipeline events of the bundle up to the time it was collected', async () => {
    render(SupportBundleViewerLayout, {
      ...baseProps(),
      selectedTimestamp: new Date('2026-05-01T12:30:00Z'),
      pipelineEvents: [event('e1', '2026-05-01T12:00:00Z')]
    })
    await expect.element(healthTab()).toBeEnabled()
    openHealthTab()
    await expect.element(page.getByText('1 ongoing incident')).toBeInTheDocument()
    // The timeline ends at the hour after the bundle was collected, not after the current time.
    await expect.element(timelineRange('2026-05-01T13:00:00Z')).toBeInTheDocument()
  })

  it('extends the timeline to an event recorded after the bundle was collected', async () => {
    render(SupportBundleViewerLayout, {
      ...baseProps(),
      selectedTimestamp: new Date('2026-05-01T12:30:00Z'),
      pipelineEvents: [event('e1', '2026-05-01T14:10:00Z')]
    })
    openHealthTab()
    await expect.element(timelineRange('2026-05-01T15:00:00Z')).toBeInTheDocument()
  })

  it('follows the selected snapshot while the tab is open', async () => {
    const { rerender } = render(SupportBundleViewerLayout, {
      ...baseProps(),
      selectedTimestamp: new Date('2026-05-01T12:30:00Z'),
      pipelineEvents: [event('e1', '2026-05-01T12:00:00Z')]
    })
    openHealthTab()
    await expect.element(timelineRange('2026-05-01T13:00:00Z')).toBeInTheDocument()
    await rerender({
      selectedTimestamp: new Date('2026-05-02T08:15:00Z'),
      pipelineEvents: [event('e2', '2026-05-02T08:00:00Z')]
    })
    await expect.element(timelineRange('2026-05-02T09:00:00Z')).toBeInTheDocument()
    await expect.element(timelineRange('2026-05-01T13:00:00Z')).not.toBeInTheDocument()
  })
})

describe('SupportBundleViewerLayout code panel', () => {
  const program = { name: 'program.sql', language: 'sql', code: 'a\nb\nc' } as const
  const udf = { name: 'udf.rs', language: 'rust', code: 'pub fn f() {}' } as const
  const operatorRanges = [
    { start: { line: 2, column: 1 }, end: { line: 2, column: 1 } }
  ] as SourcePositionRange[]
  // The code panel overlay has no size here, so it is hidden and `getByRole` cannot find its
  // tabs. The test finds them through the DOM.
  const fileTab = (name: string) =>
    [...document.querySelectorAll<HTMLElement>('[role="tab"]')].find(
      (tab) => tab.textContent?.trim() === name
    )
  const openTab = (name: string) => fileTab(name)?.click()
  const selectedTab = () =>
    document.querySelector('[role="tab"][aria-selected="true"]')?.textContent?.trim()

  beforeEach(() => {
    capture.callbacks = undefined
    capture.profile = { getSourceRanges: () => operatorRanges } as never
    capture.sourceTab = undefined
    capture.programCode = undefined
  })

  it('has a tab for each source file and opens the SQL program', async () => {
    render(SupportBundleViewerLayout, { ...baseProps(), sourceFiles: [program, udf] })
    await expect
      .poll(() => [...document.querySelectorAll('[role="tab"]')].map((t) => t.textContent?.trim()))
      .toEqual(['program.sql', 'udf.rs'])
    await expect.poll(() => capture.sourceTab?.file).toEqual(program)
  })

  it('has a single tab when the pipeline has no UDF files', async () => {
    render(SupportBundleViewerLayout, { ...baseProps(), sourceFiles: [program] })
    await expect
      .poll(() => [...document.querySelectorAll('[role="tab"]')].map((t) => t.textContent?.trim()))
      .toEqual(['program.sql'])
  })

  it('hands the diagram the lines of the SQL program', async () => {
    render(SupportBundleViewerLayout, {
      ...baseProps(),
      profileData: someProfile,
      sourceFiles: [udf, program]
    })
    await expect.poll(() => capture.programCode).toEqual(['a', 'b', 'c'])
  })

  it('hands the diagram no lines when the bundle has no source files', async () => {
    render(SupportBundleViewerLayout, { ...baseProps(), profileData: someProfile })
    await expect.poll(() => capture.callbacks).toBeDefined()
    expect(capture.programCode).toBeUndefined()
  })

  it('says that the source code is not in the bundle when it has no source files', async () => {
    render(SupportBundleViewerLayout, baseProps())
    await expect.poll(() => document.body.textContent).toContain('Profile does not contain the source code')
    expect(document.querySelector('[role="tab"]')).toBeNull()
  })

  it('opens the first file when the bundle has no SQL program', async () => {
    render(SupportBundleViewerLayout, { ...baseProps(), sourceFiles: [udf] })
    await expect.poll(() => capture.sourceTab?.file).toEqual(udf)
    await expect.poll(selectedTab).toBe('udf.rs')
    expect(document.body.textContent).not.toContain('Profile does not contain the source code')
  })

  it('shows the files when a bundle with source files follows one without', async () => {
    const { rerender } = render(SupportBundleViewerLayout, baseProps())
    await rerender({ sourceFiles: [program, udf] })
    await expect.poll(() => capture.sourceTab?.file).toEqual(program)
    expect(document.body.textContent).not.toContain('Profile does not contain the source code')
  })

  it('selects node ranges only in the SQL program', async () => {
    render(SupportBundleViewerLayout, {
      ...baseProps(),
      profileData: someProfile,
      sourceFiles: [program, udf]
    })
    capture.callbacks?.onNodeClick?.('operator')
    await expect.poll(() => capture.sourceTab?.highlightRanges).toEqual(operatorRanges)
    await expect.poll(() => fileTab('udf.rs')).toBeDefined()
    openTab('udf.rs')
    await expect.poll(() => capture.sourceTab?.file).toEqual(udf)
    expect(capture.sourceTab?.highlightRanges).toEqual([])
  })

  it('goes back to the SQL program when a node with SQL is clicked', async () => {
    render(SupportBundleViewerLayout, {
      ...baseProps(),
      profileData: someProfile,
      sourceFiles: [program, udf]
    })
    await expect.poll(() => fileTab('udf.rs')).toBeDefined()
    openTab('udf.rs')
    await expect.poll(() => capture.sourceTab?.file).toEqual(udf)
    capture.callbacks?.onNodeClick?.('operator')
    await expect.poll(() => capture.sourceTab?.file).toEqual(program)
    await expect.poll(selectedTab).toBe('program.sql')
  })

  it('stays on the open file when a node without SQL is clicked', async () => {
    capture.profile = { getSourceRanges: () => [] } as never
    render(SupportBundleViewerLayout, {
      ...baseProps(),
      profileData: someProfile,
      sourceFiles: [program, udf]
    })
    await expect.poll(() => fileTab('udf.rs')).toBeDefined()
    openTab('udf.rs')
    await expect.poll(() => capture.sourceTab?.file).toEqual(udf)
    capture.callbacks?.onNodeClick?.('internal')
    await expect.poll(selectedTab).toBe('udf.rs')
    expect(capture.sourceTab?.file).toEqual(udf)
  })

  it('stays on the open file when the next bundle has it, and shows its new code', async () => {
    const { rerender } = render(SupportBundleViewerLayout, {
      ...baseProps(),
      sourceFiles: [program, udf]
    })
    await expect.poll(() => fileTab('udf.rs')).toBeDefined()
    openTab('udf.rs')
    await expect.poll(() => capture.sourceTab?.file).toEqual(udf)
    const nextUdf = { ...udf, code: 'pub fn g() {}' }
    await rerender({ sourceFiles: [program, nextUdf] })
    await expect.poll(() => capture.sourceTab?.file).toEqual(nextUdf)
    expect(selectedTab()).toBe('udf.rs')
  })

  it('goes back to the SQL program when the next bundle does not have the open file', async () => {
    const { rerender } = render(SupportBundleViewerLayout, {
      ...baseProps(),
      sourceFiles: [program, udf]
    })
    await expect.poll(() => fileTab('udf.rs')).toBeDefined()
    openTab('udf.rs')
    await expect.poll(() => capture.sourceTab?.file).toEqual(udf)
    await rerender({ sourceFiles: [program] })
    await expect.poll(() => capture.sourceTab?.file).toEqual(program)
    await expect.poll(selectedTab).toBe('program.sql')
  })
})
