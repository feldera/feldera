/**
 * Two regressions in the support bundle viewer layout: the vertical resizer's direction, and
 * which gesture navigates to a node's SQL.
 *
 * ## The resizer
 *
 * The graph pane is rendered inside `{#if hasProfile}`; the analysis pane below it is
 * unconditional. PaneForge orders panes by registration time unless each pane declares an
 * explicit `order`. When a profile loads after the layout has mounted (profile
 * undefined -> defined), the graph pane registers second and, without `order`, PaneForge
 * treats it as the lower pane. The resizer then drives the panes backwards, so moving the
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
import { aggregateConnectorMetrics, type ConnectorStatus } from 'common-ui/connectorMetrics'
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
  profile: undefined,
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
    await rerender({ profile: someProfile })

    await expectDividerDownGrowsTopPane(container)
  })

  it('keeps the correct direction in the SQL full-height layout too', async () => {
    const { container, rerender } = render(SupportBundleViewerLayout, {
      ...baseProps(),
      sqlPanelFullHeight: true
    })

    await rerender({ profile: someProfile })

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
        profile: someProfile,
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

describe('SupportBundleViewerLayout diagram metric', () => {
  const option = (id: string) => ({ id, label: id })
  // runtime_percent is the diagram default and is not a basic metric.
  const metrics = ['runtime_percent', 'runtime_seconds', 'spine_storage_size_bytes'].map(option)

  const mount = async (selected: string) => {
    capture.callbacks = undefined
    capture.profile = { getSourceRanges: () => [] } as never
    capture.selectedMetrics = []
    const { container } = render(SupportBundleViewerLayout, {
      ...baseProps(),
      profile: someProfile
    })
    await expect.poll(() => capture.callbacks).toBeDefined()
    capture.callbacks!.onMetricsChanged(metrics, selected)
    const advanced = container.querySelector<HTMLInputElement>('input[name="showAdvancedMetrics"]')
    if (!advanced) {
      throw new Error('the "Show advanced" switch is not rendered')
    }
    const toggleAdvanced = async () => {
      const before = advanced.checked
      advanced.click()
      await expect.poll(() => advanced.checked).toBe(!before)
    }
    const diagramMetric = () => capture.selectedMetrics.at(-1)
    return { toggleAdvanced, diagramMetric }
  }

  it('keeps the diagram metric when the advanced metrics are shown and hidden', async () => {
    const { toggleAdvanced, diagramMetric } = await mount('runtime_percent')
    await expect.poll(diagramMetric).toBe('runtime_seconds')
    capture.selectedMetrics = []
    await toggleAdvanced()
    await toggleAdvanced()
    await toggleAdvanced()
    // A toggle that changed the metric would restyle every node of the diagram.
    expect(capture.selectedMetrics.filter((id) => id !== 'runtime_seconds')).toEqual([])
  })

  it('selects the runtime when the advanced metrics are hidden while one of them is selected', async () => {
    const { toggleAdvanced, diagramMetric } = await mount('runtime_percent')
    await toggleAdvanced()
    capture.callbacks!.onMetricsChanged(metrics, 'runtime_percent')
    await expect.poll(diagramMetric).toBe('runtime_percent')
    await toggleAdvanced()
    await expect.poll(diagramMetric).toBe('runtime_seconds')
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

  it('opens the first file when the bundle has no SQL program', async () => {
    render(SupportBundleViewerLayout, { ...baseProps(), sourceFiles: [udf] })
    await expect.poll(() => capture.sourceTab?.file).toEqual(udf)
    await expect.poll(selectedTab).toBe('udf.rs')
  })

  it('shows the files when a bundle with source files follows one without', async () => {
    const { rerender } = render(SupportBundleViewerLayout, baseProps())
    await rerender({ sourceFiles: [program, udf] })
    await expect.poll(() => capture.sourceTab?.file).toEqual(program)
  })

  it('selects node ranges only in the SQL program', async () => {
    render(SupportBundleViewerLayout, {
      ...baseProps(),
      profile: someProfile,
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
      profile: someProfile,
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
      profile: someProfile,
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

describe('SupportBundleViewerLayout Connectors tab', () => {
  const connectorStatus = {
    inputs: [
      {
        endpoint_name: 't.in1',
        config: { stream: 't' },
        metrics: {
          buffered_bytes: 0,
          buffered_records: 0,
          end_of_input: false,
          num_parse_errors: 2,
          num_transport_errors: 1,
          total_bytes: 0,
          total_records: 0
        },
        paused: false,
        barrier: false,
        parse_errors: [
          { index: 0, tag: 'parse', message: 'bad row one', timestamp: '2026-05-01T12:00:00Z' }
        ]
      }
    ],
    outputs: []
  }
  // The props that `processProfileFiles` makes from a `stats.json` with these connectors.
  const connectorProps = (status: ConnectorStatus) => ({
    connectorStatus: status,
    connectorMetrics: aggregateConnectorMetrics(status)
  })

  it('shows the error count on the tab label only when there are errors', async () => {
    render(SupportBundleViewerLayout, { ...baseProps(), ...connectorProps(connectorStatus) })
    await expect.element(page.getByRole('radio', { name: 'Connectors 3' })).toBeInTheDocument()
  })

  it('adds the errors of inputs and outputs on the tab label', async () => {
    const both: ConnectorStatus = structuredClone(connectorStatus)
    both.outputs = [
      {
        endpoint_name: 'v.out1',
        config: { stream: 'v' },
        metrics: {
          buffered_batches: 0,
          buffered_records: 0,
          memory: 0,
          num_encode_errors: 4,
          num_transport_errors: 5,
          queued_batches: 0,
          queued_records: 0,
          total_processed_input_records: 0,
          total_processed_steps: 0,
          transmitted_bytes: 0,
          transmitted_records: 0
        }
      }
    ]
    render(SupportBundleViewerLayout, { ...baseProps(), ...connectorProps(both) })
    await expect.element(page.getByRole('radio', { name: 'Connectors 12' })).toBeInTheDocument()
  })

  it('has no error count on the tab label when the bundle has no connector statistics', async () => {
    render(SupportBundleViewerLayout, baseProps())
    await expect
      .element(page.getByRole('radio', { name: 'Connectors', exact: true }))
      .toBeInTheDocument()
  })

  it('has no error count on the tab label without errors', async () => {
    const quiet = structuredClone(connectorStatus)
    quiet.inputs[0].metrics.num_parse_errors = 0
    quiet.inputs[0].metrics.num_transport_errors = 0
    render(SupportBundleViewerLayout, { ...baseProps(), ...connectorProps(quiet) })
    await expect
      .element(page.getByRole('radio', { name: 'Connectors', exact: true }))
      .toBeInTheDocument()
  })

  it('opens the errors of a connector from its error icon', async () => {
    render(SupportBundleViewerLayout, { ...baseProps(), ...connectorProps(connectorStatus) })
    ;(page.getByText('Connectors', { exact: false }).first().element() as HTMLElement).click()
    await expect.element(page.getByTestId('btn-icon-input-errors').first()).toBeInTheDocument()
    ;(page.getByTestId('btn-icon-input-errors').first().element() as HTMLElement).click()
    await expect.element(page.getByText('bad row one').first()).toBeInTheDocument()
  })
})

describe('SupportBundleViewerLayout without parts of the bundle', () => {
  const program = { name: 'program.sql', language: 'sql', code: 'select 1' } as const
  const emptyProgram = { name: 'program.sql', language: 'sql', code: ' \n' } as const
  // `stats.json` with one known global metric.
  const someGlobalMetrics = { state: 'Running' }

  // Only the code panel has `role="tab"`. The analysis tabs are radios.
  const fileTabCount = () => document.querySelectorAll('[role="tab"]').length
  const codePaneResizer = (container: HTMLElement) =>
    container.querySelector('.pane-divider-vertical[data-pane-resizer]')
  const analysisTab = (container: HTMLElement, id: string) => {
    const tab = container.querySelector<HTMLInputElement>(`input[value="${id}"]`)
    if (!tab) {
      throw new Error(`${id} tab not found`)
    }
    return tab
  }
  const selectedAnalysisTab = (container: HTMLElement) =>
    container.querySelector<HTMLInputElement>('input[type="radio"]:checked')?.value
  // All header notices start with "This bundle has", so a count above one means that the layout
  // joined two notices.
  const noticeCount = () => (document.body.textContent ?? '').split('This bundle has').length - 1

  describe('code panel', () => {
    it.each([
      ['the normal layout', false],
      ['the SQL full-height layout', true]
    ])('is not shown in %s when the bundle has no source files', async (_, sqlPanelFullHeight) => {
      const { container } = render(SupportBundleViewerLayout, {
        ...baseProps(),
        profile: someProfile,
        sqlPanelFullHeight
      })
      // Throws if the layout did not render the analysis tabs.
      analysisTab(container, 'Logs')
      expect(fileTabCount()).toBe(0)
      expect(codePaneResizer(container)).toBeNull()
    })

    it('is not shown when all source files are empty', async () => {
      const { container } = render(SupportBundleViewerLayout, {
        ...baseProps(),
        sourceFiles: [emptyProgram]
      })
      analysisTab(container, 'Logs')
      expect(fileTabCount()).toBe(0)
      expect(codePaneResizer(container)).toBeNull()
    })

    it('is shown with a resizer when the bundle has code', async () => {
      const { container } = render(SupportBundleViewerLayout, {
        ...baseProps(),
        sourceFiles: [emptyProgram, { ...program, name: 'udf.rs', language: 'rust' }]
      })
      await expect.poll(fileTabCount).toBe(2)
      expect(codePaneResizer(container)).not.toBeNull()
    })

    it('appears when a bundle with code follows one without', async () => {
      const { container, rerender } = render(SupportBundleViewerLayout, baseProps())
      expect(codePaneResizer(container)).toBeNull()
      await rerender({ sourceFiles: [program] })
      await expect.poll(fileTabCount).toBe(1)
      expect(codePaneResizer(container)).not.toBeNull()
    })
  })

  describe('Metrics tab', () => {
    it('is disabled when the bundle has no profile and no global metrics', () => {
      const { container } = render(SupportBundleViewerLayout, baseProps())
      expect(analysisTab(container, 'Metrics').disabled).toBe(true)
    })

    it.each([
      ['a profile', { profile: someProfile }],
      ['only global metrics', { globalMetrics: someGlobalMetrics }]
    ])('is enabled when the bundle has %s', (_, props) => {
      const { container } = render(SupportBundleViewerLayout, { ...baseProps(), ...props })
      expect(analysisTab(container, 'Metrics').disabled).toBe(false)
    })

    it('is not selected when disabled, and the first enabled tab is selected', async () => {
      // Connectors, Health and Config have no data either, so Logs is the first enabled tab.
      const { container } = render(SupportBundleViewerLayout, baseProps())
      await expect.poll(() => selectedAnalysisTab(container)).toBe('Logs')
    })

    it('is selected by default when it is enabled', async () => {
      const { container } = render(SupportBundleViewerLayout, {
        ...baseProps(),
        profile: someProfile
      })
      await expect.poll(() => selectedAnalysisTab(container)).toBe('Metrics')
    })
  })

  describe('header notice', () => {
    it.each([
      [
        'no profile, no metrics, no code',
        {},
        'This bundle has no circuit profile and no pipeline code, so the dataflow graph, metrics and code are unavailable.'
      ],
      [
        'no profile, no code, but global metrics',
        { globalMetrics: someGlobalMetrics },
        'This bundle has no circuit profile and no pipeline code, so the dataflow graph and code are unavailable.'
      ],
      [
        'no profile, no metrics, but code',
        { sourceFiles: [program] },
        'This bundle has no circuit profile, so the dataflow graph and metrics are unavailable.'
      ],
      [
        'no profile, but global metrics and code',
        { globalMetrics: someGlobalMetrics, sourceFiles: [program] },
        'This bundle has no circuit profile, so the dataflow graph is unavailable.'
      ],
      [
        'a profile, but no code',
        { profile: someProfile },
        'This bundle has no pipeline code, so the code panel is unavailable.'
      ],
      [
        'a profile and only empty code',
        { profile: someProfile, sourceFiles: [emptyProgram] },
        'This bundle has no pipeline code, so the code panel is unavailable.'
      ]
    ])('for a bundle with %s is a single message', async (_, props, notice) => {
      render(SupportBundleViewerLayout, { ...baseProps(), ...props })
      await expect.element(page.getByText(notice, { exact: true })).toBeInTheDocument()
      expect(noticeCount()).toBe(1)
    })

    it('for an unreadable profile collapses the graph panel and gives the reason on hover', async () => {
      const profileError = 'Failed to read the circuit profile: unknown version'
      const { container } = render(SupportBundleViewerLayout, {
        ...baseProps(),
        profileError,
        sourceFiles: [program]
      })
      const notice = page.getByText(
        'This bundle has an unreadable circuit profile, so the dataflow graph and metrics are unavailable.',
        { exact: true }
      )
      await expect.element(notice).toHaveAttribute('title', profileError)
      expect(noticeCount()).toBe(1)
      // Only a shown graph panel has the resizer above the analysis panel.
      expect(container.querySelector('.pane-divider-horizontal')).toBeNull()
      expect(document.body.textContent).not.toContain(profileError)
    })

    it('is not shown when the bundle has a profile and code', async () => {
      render(SupportBundleViewerLayout, {
        ...baseProps(),
        profile: someProfile,
        sourceFiles: [program]
      })
      await expect.poll(fileTabCount).toBe(1)
      expect(noticeCount()).toBe(0)
    })
  })
})
