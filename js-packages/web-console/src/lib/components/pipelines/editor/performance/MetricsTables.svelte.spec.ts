import { describe, expect, it, vi } from 'vitest'

import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type {
  AggregatedInputEndpointMetrics,
  AggregatedOutputEndpointMetrics,
  PipelineMetrics
} from '$lib/functions/pipelineMetrics'
import { emptyPipelineMetrics } from '$lib/functions/pipelineMetrics'
import type { InputEndpointMetrics, OutputEndpointMetrics } from '$lib/services/manager'
import MetricsTables from './MetricsTables.svelte'

// --- Mock factories ---

function makeInputMetrics(overrides?: Partial<InputEndpointMetrics>): InputEndpointMetrics {
  return {
    total_bytes: 0,
    total_records: 0,
    buffered_bytes: 0,
    buffered_records: 0,
    num_transport_errors: 0,
    num_parse_errors: 0,
    end_of_input: false,
    ...overrides
  }
}

function makeOutputMetrics(overrides?: Partial<OutputEndpointMetrics>): OutputEndpointMetrics {
  return {
    buffered_batches: 0,
    buffered_records: 0,
    memory: 0,
    num_encode_errors: 0,
    num_transport_errors: 0,
    queued_batches: 0,
    queued_records: 0,
    total_processed_input_records: 0,
    transmitted_bytes: 0,
    transmitted_records: 0,
    total_processed_steps: 0,
    ...overrides
  }
}

function makeInputConnector(
  overrides?: Partial<AggregatedInputEndpointMetrics['connectors'][0]>
): AggregatedInputEndpointMetrics['connectors'][0] {
  return {
    endpointName: 'input-connector-1',
    metrics: makeInputMetrics(),
    io_active: false,
    paused: false,
    barrier: false,
    health: null,
    ...overrides
  }
}

function makeOutputConnector(
  overrides?: Partial<AggregatedOutputEndpointMetrics['connectors'][0]>
): AggregatedOutputEndpointMetrics['connectors'][0] {
  return {
    endpointName: 'output-connector-1',
    metrics: makeOutputMetrics(),
    io_active: false,
    health: null,
    ...overrides
  }
}

function makeAggregatedInput(
  connectors: AggregatedInputEndpointMetrics['connectors']
): AggregatedInputEndpointMetrics {
  const aggregate = connectors.reduce(
    (acc, c) => ({
      total_bytes: acc.total_bytes + c.metrics.total_bytes,
      total_records: acc.total_records + c.metrics.total_records,
      buffered_bytes: acc.buffered_bytes + c.metrics.buffered_bytes,
      buffered_records: acc.buffered_records + c.metrics.buffered_records,
      num_transport_errors: acc.num_transport_errors + c.metrics.num_transport_errors,
      num_parse_errors: acc.num_parse_errors + c.metrics.num_parse_errors,
      end_of_input: acc.end_of_input && c.metrics.end_of_input
    }),
    {
      total_bytes: 0,
      total_records: 0,
      buffered_bytes: 0,
      buffered_records: 0,
      num_transport_errors: 0,
      num_parse_errors: 0,
      end_of_input: true
    }
  )
  return { connectors, aggregate: { metrics: aggregate } }
}

function makeAggregatedOutput(
  connectors: AggregatedOutputEndpointMetrics['connectors']
): AggregatedOutputEndpointMetrics {
  const aggregate = connectors.reduce(
    (acc, c) => ({
      buffered_batches: acc.buffered_batches + c.metrics.buffered_batches,
      buffered_records: acc.buffered_records + c.metrics.buffered_records,
      memory: acc.memory + c.metrics.memory,
      num_encode_errors: acc.num_encode_errors + c.metrics.num_encode_errors,
      num_transport_errors: acc.num_transport_errors + c.metrics.num_transport_errors,
      queued_batches: acc.queued_batches + c.metrics.queued_batches,
      queued_records: acc.queued_records + c.metrics.queued_records,
      total_processed_input_records:
        acc.total_processed_input_records + c.metrics.total_processed_input_records,
      transmitted_bytes: acc.transmitted_bytes + c.metrics.transmitted_bytes,
      transmitted_records: acc.transmitted_records + c.metrics.transmitted_records,
      total_processed_steps: acc.total_processed_steps + c.metrics.total_processed_steps
    }),
    {
      buffered_batches: 0,
      buffered_records: 0,
      memory: 0,
      num_encode_errors: 0,
      num_transport_errors: 0,
      queued_batches: 0,
      queued_records: 0,
      total_processed_input_records: 0,
      transmitted_bytes: 0,
      transmitted_records: 0,
      total_processed_steps: 0
    }
  )
  return { connectors, aggregate: { metrics: aggregate } }
}

function makeMetricsProp(
  tables?: Map<string, AggregatedInputEndpointMetrics>,
  views?: Map<string, AggregatedOutputEndpointMetrics>
): { current: PipelineMetrics } {
  return {
    current: {
      ...emptyPipelineMetrics,
      tables: tables ?? new Map(),
      views: views ?? new Map()
    }
  }
}

async function renderComponent(
  metrics: { current: PipelineMetrics },
  onConnectorSelect?: (...args: any[]) => void
) {
  return render(MetricsTables, {
    metrics,
    onConnectorSelect: onConnectorSelect ?? vi.fn()
  })
}

async function clickHealthFilter(value: string) {
  await page.getByTestId(`btn-select-${value}`).click()
}

// --- Tests ---

describe('MetricsTables.svelte', () => {
  describe('A. Basic rendering', () => {
    it('renders nothing when tables and views are empty', async () => {
      await renderComponent(makeMetricsProp())
      await expect.element(page.getByTestId('box-input-tables')).not.toBeInTheDocument()
      await expect.element(page.getByTestId('box-output-views')).not.toBeInTheDocument()
    })

    it('renders sections with correct headers based on data presence', async () => {
      const tables = new Map([['my_table', makeAggregatedInput([makeInputConnector()])]])
      const views = new Map([['my_view', makeAggregatedOutput([makeOutputConnector()])]])
      await renderComponent(makeMetricsProp(tables, views))

      // Input tables section with headers
      const inputSection = page.getByTestId('box-input-tables')
      await expect.element(inputSection).toBeInTheDocument()
      const inputHtml = inputSection.element().innerHTML
      expect(inputHtml).toContain('Table')
      expect(inputHtml).toContain('Connectors')
      expect(inputHtml).toContain('Ingested')
      expect(inputHtml).toContain('Parse errors')
      expect(inputHtml).toContain('Transport errors')

      // Output views section with headers
      const outputSection = page.getByTestId('box-output-views')
      await expect.element(outputSection).toBeInTheDocument()
      const outputHtml = outputSection.element().innerHTML
      expect(outputHtml).toContain('View')
      expect(outputHtml).toContain('Transmitted')
      expect(outputHtml).toContain('Encode errors')
    })
  })

  describe('B. Metric value display', () => {
    it('displays formatted input metrics, zero values, and relation name', async () => {
      const tables = new Map([
        [
          'ORDERS',
          makeAggregatedInput([
            makeInputConnector({
              metrics: makeInputMetrics({
                total_records: 1234567,
                total_bytes: 1048576,
                buffered_records: 42,
                buffered_bytes: 2048
              })
            })
          ])
        ],
        ['EMPTY_TABLE', makeAggregatedInput([makeInputConnector()])]
      ])
      await renderComponent(makeMetricsProp(tables))

      // Formatted metric values
      await expect.element(page.getByText('1,234,567')).toBeInTheDocument()
      await expect.element(page.getByText('1.0 MiB')).toBeInTheDocument()
      await expect.element(page.getByText('42')).toBeInTheDocument()
      await expect.element(page.getByText('2.0 KiB')).toBeInTheDocument()

      // Zero metrics display correctly
      await expect.element(page.getByText('0 B').first()).toBeInTheDocument()

      // Relation name rendered in row
      await expect.element(page.getByTestId('box-relation-row-ORDERS')).toBeInTheDocument()
      await expect.element(page.getByText('ORDERS')).toBeInTheDocument()
    })

    it('displays formatted output metrics', async () => {
      const views = new Map([
        [
          'out_view',
          makeAggregatedOutput([
            makeOutputConnector({
              metrics: makeOutputMetrics({
                transmitted_records: 5000,
                transmitted_bytes: 512000,
                buffered_records: 10,
                queued_records: 3
              })
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(undefined, views))
      await expect.element(page.getByText('5,000')).toBeInTheDocument()
      await expect.element(page.getByText('500.0 KiB')).toBeInTheDocument()
    })
  })

  describe('C. io_active indicator', () => {
    it('applies green text class on active records for both input and output', async () => {
      const tables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              io_active: true,
              metrics: makeInputMetrics({ total_records: 100 })
            })
          ])
        ]
      ])
      const views = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({
              io_active: true,
              metrics: makeOutputMetrics({ transmitted_records: 200 })
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables, views))

      const inputEl = page.getByText('100')
      await expect.element(inputEl).toBeInTheDocument()
      await expect.element(inputEl).toHaveClass('text-success-600-400')

      const outputEl = page.getByText('200')
      await expect.element(outputEl).toBeInTheDocument()
      await expect.element(outputEl).toHaveClass('text-success-600-400')
    })
  })

  describe('D. Connector status icons', () => {
    it('shows correct status icon for each input connector state', async () => {
      const cases: {
        state: Partial<AggregatedInputEndpointMetrics['connectors'][0]>
        testId: string
      }[] = [
        { state: { barrier: true }, testId: 'box-icon-barrier' },
        {
          state: { metrics: makeInputMetrics({ num_parse_errors: 5 }) },
          testId: 'btn-icon-input-errors'
        },
        { state: { transaction_phase: 'started' }, testId: 'box-icon-transaction-started' },
        { state: { transaction_phase: 'committed' }, testId: 'box-icon-transaction-committed' },
        {
          state: { metrics: makeInputMetrics({ end_of_input: true }) },
          testId: 'box-icon-end-of-input'
        },
        { state: { paused: true }, testId: 'box-icon-paused' },
        { state: { fatal_error: 'err' }, testId: 'btn-icon-input-fatal-error' },
        { state: {}, testId: 'box-icon-running' }
      ]

      for (const { state, testId } of cases) {
        const tables = new Map([['t1', makeAggregatedInput([makeInputConnector(state)])]])
        const { unmount } = await renderComponent(makeMetricsProp(tables))
        await expect.element(page.getByTestId(testId)).toBeInTheDocument()
        unmount()
      }
    })

    it('shows error icon for output connector with errors', async () => {
      const views = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({
              metrics: makeOutputMetrics({ num_encode_errors: 3 })
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(undefined, views))
      await expect.element(page.getByTestId('btn-icon-output-errors')).toBeInTheDocument()
    })

    it('shows no error icon for output connector without errors', async () => {
      const views = new Map([['v1', makeAggregatedOutput([makeOutputConnector()])]])
      await renderComponent(makeMetricsProp(undefined, views))
      await expect.element(page.getByTestId('btn-icon-output-errors')).not.toBeInTheDocument()
    })
  })

  describe('E. Multi-connector rows', () => {
    it('shows summary text and chevron for multi-connector input table', async () => {
      const tables = new Map([
        [
          'multi',
          makeAggregatedInput([
            makeInputConnector({ endpointName: 'c1', paused: false }),
            makeInputConnector({ endpointName: 'c2', paused: true })
          ])
        ],
        ['single', makeAggregatedInput([makeInputConnector({ endpointName: 'c3' })])]
      ])
      await renderComponent(makeMetricsProp(tables))

      // Multi-connector summary with running count
      await expect.element(page.getByTestId('box-multi-connector-summary')).toBeInTheDocument()
      await expect.element(page.getByText('1 / 2 running')).toBeInTheDocument()

      // Chevron on multi-connector row only
      const multiRowEl = page.getByTestId('box-relation-row-multi').element()
      expect(multiRowEl.querySelector('.fd-chevron-down')).not.toBeNull()
    })

    it('expands to show individual connectors and collapses on second click', async () => {
      const tables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({ endpointName: 'alpha' }),
            makeInputConnector({ endpointName: 'beta' })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables))

      // Initially collapsed
      await expect.element(page.getByTestId('box-connector-row-alpha')).not.toBeInTheDocument()

      // Click to expand
      await page.getByTestId('box-relation-row-t1').click({ position: { x: 1, y: 0 } })
      await expect.element(page.getByTestId('box-connector-row-alpha')).toBeInTheDocument()
      await expect.element(page.getByTestId('box-connector-row-beta')).toBeInTheDocument()

      // Click again to collapse
      await page.getByTestId('box-relation-row-t1').click({ position: { x: 1, y: 0 } })
      await expect.element(page.getByTestId('box-connector-row-alpha')).not.toBeInTheDocument()
    })

    it('shows "N connectors" text for multi-connector output views', async () => {
      const views = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({ endpointName: 'o1' }),
            makeOutputConnector({ endpointName: 'o2' }),
            makeOutputConnector({ endpointName: 'o3' })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(undefined, views))
      await expect.element(page.getByText('3 connectors')).toBeInTheDocument()
    })
  })

  describe('F. Error count buttons & callbacks', () => {
    it('clicking error counts calls onConnectorSelect with correct arguments', async () => {
      const onConnectorSelect = vi.fn()
      const tables = new Map([
        [
          'tbl',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'ep1',
              metrics: makeInputMetrics({ num_parse_errors: 7, num_transport_errors: 3 })
            })
          ])
        ]
      ])
      const views = new Map([
        [
          'vw',
          makeAggregatedOutput([
            makeOutputConnector({
              endpointName: 'oep',
              metrics: makeOutputMetrics({ num_encode_errors: 2, num_transport_errors: 4 })
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables, views), onConnectorSelect)

      // Input parse errors
      await page.getByTestId('btn-parse-errors').click()
      expect(onConnectorSelect).toHaveBeenCalledWith('tbl', 'ep1', 'input', 'parse')

      // Input transport errors
      await page.getByTestId('btn-input-transport-errors').click()
      expect(onConnectorSelect).toHaveBeenCalledWith('tbl', 'ep1', 'input', 'transport')

      // Output encode errors
      await page.getByTestId('btn-encode-errors').click()
      expect(onConnectorSelect).toHaveBeenCalledWith('vw', 'oep', 'output', 'encode')

      // Output transport errors
      await page.getByTestId('btn-output-transport-errors').click()
      expect(onConnectorSelect).toHaveBeenCalledWith('vw', 'oep', 'output', 'transport')
    })

    it('error counts are plain text on aggregate rows (multi-connector)', async () => {
      const tables = new Map([
        [
          'tbl',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'c1',
              metrics: makeInputMetrics({ num_parse_errors: 5 })
            }),
            makeInputConnector({
              endpointName: 'c2',
              metrics: makeInputMetrics({ num_parse_errors: 3 })
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables))
      // Aggregate row should NOT have a clickable parse error button
      await expect.element(page.getByTestId('btn-parse-errors')).not.toBeInTheDocument()
      // But should still display the aggregated count as text
      await expect.element(page.getByText('8')).toBeInTheDocument()
    })
  })

  describe('G. Error icon click', () => {
    it('clicking error icons calls onConnectorSelect with all filter', async () => {
      const onConnectorSelect = vi.fn()
      const tables = new Map([
        [
          'tbl',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'ep1',
              metrics: makeInputMetrics({ num_parse_errors: 1 })
            })
          ])
        ]
      ])
      const views = new Map([
        [
          'vw',
          makeAggregatedOutput([
            makeOutputConnector({
              endpointName: 'oep',
              metrics: makeOutputMetrics({ num_encode_errors: 1 })
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables, views), onConnectorSelect)

      // Input error icon
      const inputIcon = page.getByTestId('btn-icon-input-errors')
      await expect.element(inputIcon).toBeInTheDocument()
      ;(inputIcon.element() as HTMLElement).click()
      expect(onConnectorSelect).toHaveBeenCalledWith('tbl', 'ep1', 'input', 'all')

      // Output error icon
      const outputIcon = page.getByTestId('btn-icon-output-errors')
      await expect.element(outputIcon).toBeInTheDocument()
      ;(outputIcon.element() as HTMLElement).click()
      expect(onConnectorSelect).toHaveBeenCalledWith('vw', 'oep', 'output', 'all')
    })
  })

  describe('H. Health status', () => {
    it('shows unhealthy chip for single unhealthy connector', async () => {
      const tables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              health: { status: 'Unhealthy', description: 'Connection lost' }
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables))
      await expect.element(page.getByTestId('box-unhealthy-chip')).toBeInTheDocument()
    })

    it('shows unhealthy chip in summary when collapsed, moves to connector rows when expanded', async () => {
      const tables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'c1',
              health: { status: 'Unhealthy', description: 'bad' }
            }),
            makeInputConnector({ endpointName: 'c2', health: { status: 'Healthy' } })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables))

      // Collapsed: summary shows unhealthy chip
      await expect.element(page.getByTestId('box-unhealthy-chip')).toBeInTheDocument()

      // Expand
      await page.getByTestId('box-relation-row-t1').click({ position: { x: 1, y: 0 } })

      // Chip still exists somewhere (in the connector row)
      await expect.element(page.getByTestId('box-unhealthy-chip')).toBeInTheDocument()
      // But NOT inside the summary row
      const summary = page.getByTestId('box-multi-connector-summary')
      await expect.element(summary.getByTestId('box-unhealthy-chip')).not.toBeInTheDocument()
    })

    it('shows unhealthy chip per connector when expanded', async () => {
      const tables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'c1',
              health: { status: 'Unhealthy', description: 'bad1' }
            }),
            makeInputConnector({
              endpointName: 'c2',
              health: { status: 'Unhealthy', description: 'bad2' }
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables))
      await page.getByTestId('box-relation-row-t1').click({ position: { x: 1, y: 0 } })
      await expect.element(page.getByTestId('box-connector-row-c1')).toBeInTheDocument()
      await expect.element(page.getByTestId('box-connector-row-c2')).toBeInTheDocument()
      const allChips = page.getByTestId('box-unhealthy-chip').all()
      expect(allChips.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('J. Fatal error icon', () => {
    it('input single connector: fatal_error shows fd-circle-x alongside fd-circle-alert when both errors exist', async () => {
      const withFatal = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              fatal_error: 'Connection refused',
              metrics: makeInputMetrics({ num_transport_errors: 1 })
            })
          ])
        ]
      ])
      const { unmount } = await renderComponent(makeMetricsProp(withFatal))
      // Alert icon for transport errors
      const alertIcon = page.getByTestId('btn-icon-input-errors')
      await expect.element(alertIcon).toBeInTheDocument()
      expect(alertIcon.element().classList.contains('fd-circle-alert')).toBe(true)
      // Fatal error icon replaces running
      const fatalIcon = page.getByTestId('btn-icon-input-fatal-error')
      await expect.element(fatalIcon).toBeInTheDocument()
      expect(fatalIcon.element().classList.contains('fd-circle-x')).toBe(true)
      unmount()

      const withoutFatal = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({ metrics: makeInputMetrics({ num_transport_errors: 1 }) })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(withoutFatal))
      const icon = page.getByTestId('btn-icon-input-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-x')).toBe(false)
      await expect.element(page.getByTestId('btn-icon-input-fatal-error')).not.toBeInTheDocument()
      await expect.element(page.getByTestId('box-icon-running')).toBeInTheDocument()
    })

    it('output single connector: fatal_error shows fd-circle-x, absent shows fd-circle-alert', async () => {
      const withFatal = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({
              fatal_error: 'Write failed',
              metrics: makeOutputMetrics({ num_encode_errors: 1 })
            })
          ])
        ]
      ])
      const { unmount } = await renderComponent(makeMetricsProp(undefined, withFatal))
      let icon = page.getByTestId('btn-icon-output-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-x')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(false)
      unmount()

      const withoutFatal = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({ metrics: makeOutputMetrics({ num_encode_errors: 1 }) })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(undefined, withoutFatal))
      icon = page.getByTestId('btn-icon-output-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-x')).toBe(false)
    })

    it('single input connector error icon is interactive (role=button), composite is not', async () => {
      const singleTables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({ metrics: makeInputMetrics({ num_transport_errors: 1 }) })
          ])
        ]
      ])
      const { unmount } = await renderComponent(makeMetricsProp(singleTables))
      const singleIcon = page.getByTestId('btn-icon-input-errors').element()
      expect(singleIcon.getAttribute('role')).toBe('button')
      expect(singleIcon.getAttribute('tabindex')).toBe('0')
      unmount()

      const multiTables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'c1',
              metrics: makeInputMetrics({ num_transport_errors: 1 })
            }),
            makeInputConnector({ endpointName: 'c2' })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(multiTables))
      const compositeIcon = page.getByTestId('btn-icon-input-errors').element()
      expect(compositeIcon.getAttribute('role')).toBeNull()
    })

    it('single output connector error icon is interactive (role=button)', async () => {
      const singleViews = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({ metrics: makeOutputMetrics({ num_encode_errors: 1 }) })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(undefined, singleViews))
      const singleIcon = page.getByTestId('btn-icon-output-errors').element()
      expect(singleIcon.getAttribute('role')).toBe('button')
      expect(singleIcon.getAttribute('tabindex')).toBe('0')
    })

    it('multi-connector output summary error icon is not interactive', async () => {
      const multiViews = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({
              endpointName: 'o1',
              metrics: makeOutputMetrics({ num_encode_errors: 1 })
            }),
            makeOutputConnector({ endpointName: 'o2' })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(undefined, multiViews))
      const compositeIcon = page.getByTestId('btn-icon-output-errors').element()
      expect(compositeIcon.getAttribute('role')).toBeNull()
    })

    it('multi-connector input row: fatal_error on any connector shows fd-circle-x alongside fd-circle-alert in collapsed summary', async () => {
      const withFatal = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'c1',
              fatal_error: 'Disk full',
              metrics: makeInputMetrics({ num_transport_errors: 1 })
            }),
            makeInputConnector({ endpointName: 'c2' })
          ])
        ]
      ])
      const { unmount } = await renderComponent(makeMetricsProp(withFatal))
      // Alert icon for transport errors
      const alertIcon = page.getByTestId('btn-icon-input-errors')
      await expect.element(alertIcon).toBeInTheDocument()
      expect(alertIcon.element().classList.contains('fd-circle-alert')).toBe(true)
      // Fatal error icon replaces running
      const fatalIcon = page.getByTestId('btn-icon-input-fatal-error')
      await expect.element(fatalIcon).toBeInTheDocument()
      expect(fatalIcon.element().classList.contains('fd-circle-x')).toBe(true)
      unmount()

      const withoutFatal = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'c1',
              metrics: makeInputMetrics({ num_transport_errors: 1 })
            }),
            makeInputConnector({ endpointName: 'c2' })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(withoutFatal))
      const icon = page.getByTestId('btn-icon-input-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-x')).toBe(false)
      await expect.element(page.getByTestId('btn-icon-input-fatal-error')).not.toBeInTheDocument()
    })

    it('multi-connector output row: fatal_error on any connector shows fd-circle-x in collapsed summary', async () => {
      const withFatal = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({
              endpointName: 'o1',
              fatal_error: 'Pipe broken',
              metrics: makeOutputMetrics({ num_transport_errors: 1 })
            }),
            makeOutputConnector({ endpointName: 'o2' })
          ])
        ]
      ])
      const { unmount } = await renderComponent(makeMetricsProp(undefined, withFatal))
      let icon = page.getByTestId('btn-icon-output-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-x')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(false)
      unmount()

      const withoutFatal = new Map([
        [
          'v1',
          makeAggregatedOutput([
            makeOutputConnector({
              endpointName: 'o1',
              metrics: makeOutputMetrics({ num_transport_errors: 1 })
            }),
            makeOutputConnector({ endpointName: 'o2' })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(undefined, withoutFatal))
      icon = page.getByTestId('btn-icon-output-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-x')).toBe(false)
    })
  })

  describe('I. Health filter', () => {
    it('defaults to all, filters to unhealthy, then restores on switch back', async () => {
      const tables = new Map([
        ['healthy_t', makeAggregatedInput([makeInputConnector({ health: { status: 'Healthy' } })])],
        [
          'unhealthy_t',
          makeAggregatedInput([
            makeInputConnector({
              health: { status: 'Unhealthy', description: 'err' }
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables))

      // Default: both visible
      await expect.element(page.getByTestId('box-relation-row-healthy_t')).toBeInTheDocument()
      await expect.element(page.getByTestId('box-relation-row-unhealthy_t')).toBeInTheDocument()

      // Filter to unhealthy: only unhealthy visible
      await clickHealthFilter('unhealthy')
      await expect.element(page.getByTestId('box-relation-row-healthy_t')).not.toBeInTheDocument()
      await expect.element(page.getByTestId('box-relation-row-unhealthy_t')).toBeInTheDocument()

      // Switch back to all: both visible again
      await clickHealthFilter('all')
      await expect.element(page.getByTestId('box-relation-row-healthy_t')).toBeInTheDocument()
      await expect.element(page.getByTestId('box-relation-row-unhealthy_t')).toBeInTheDocument()
    })

    it('filtering to unhealthy filters connectors within a table', async () => {
      const tables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'healthy_c',
              health: { status: 'Healthy' }
            }),
            makeInputConnector({
              endpointName: 'unhealthy_c',
              health: { status: 'Unhealthy', description: 'err' }
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables))
      await clickHealthFilter('unhealthy')
      // The table should still show, but with only the unhealthy connector
      await expect.element(page.getByTestId('box-relation-row-t1')).toBeInTheDocument()
      // Since filtered to 1 connector, it should be shown directly (no multi-connector summary)
      await expect.element(page.getByTestId('box-multi-connector-summary')).not.toBeInTheDocument()
    })

    it('unhealthy count badge shows correct number', async () => {
      const tables = new Map([
        [
          't1',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'c1',
              health: { status: 'Unhealthy', description: 'err' }
            })
          ])
        ],
        [
          't2',
          makeAggregatedInput([
            makeInputConnector({
              endpointName: 'c2',
              health: { status: 'Unhealthy', description: 'err2' }
            })
          ])
        ]
      ])
      await renderComponent(makeMetricsProp(tables))
      const badge = page.getByText('2').elements()
      expect(badge.length).toBeGreaterThanOrEqual(1)
    })
  })
})
