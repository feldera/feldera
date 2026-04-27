import { describe, expect, it, vi } from 'vitest'

import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
import { accumulatePipelineMetrics } from '$lib/functions/pipelineMetrics'
import type {
  ControllerStatus,
  GlobalControllerMetrics,
  InputEndpointMetrics,
  InputEndpointStatus,
  OutputEndpointMetrics,
  OutputEndpointStatus
} from '$lib/services/manager'
import MetricsTables from './MetricsTables.svelte'

// --- Factories ---

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

function makeInputStatus(
  stream: string,
  overrides?: Partial<Omit<InputEndpointStatus, 'config'>>
): InputEndpointStatus {
  return {
    config: { stream },
    endpoint_name: 'input-connector-1',
    metrics: makeInputMetrics(),
    paused: false,
    barrier: false,
    health: null,
    fatal_error: null,
    ...overrides
  }
}

function makeOutputStatus(
  stream: string,
  overrides?: Partial<Omit<OutputEndpointStatus, 'config'>>
): OutputEndpointStatus {
  return {
    config: { stream },
    endpoint_name: 'output-connector-1',
    metrics: makeOutputMetrics(),
    health: null,
    fatal_error: null,
    ...overrides
  }
}

function makeStatus(
  outputs: OutputEndpointStatus[] = [],
  inputs: InputEndpointStatus[] = [],
  initiatedByConnectors: Record<string, { phase: 'Started' | 'Committed' }> = {}
): ControllerStatus {
  return {
    global_metrics: {
      transaction_initiators: { initiated_by_connectors: initiatedByConnectors }
    } as GlobalControllerMetrics,
    inputs,
    outputs
  }
}

// Builds PipelineMetrics by running the production accumulator. Pass `prev` to
// trigger io_active detection (requires a previous snapshot with lower counters).
function buildMetrics(
  status: ControllerStatus,
  prev?: ControllerStatus
): { current: PipelineMetrics } {
  const prevMetrics = prev ? accumulatePipelineMetrics(0)(undefined, { status: prev }) : undefined
  return { current: accumulatePipelineMetrics(1)(prevMetrics, { status })! }
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
      await renderComponent(buildMetrics(makeStatus()))
      await expect.element(page.getByTestId('box-input-tables')).not.toBeInTheDocument()
      await expect.element(page.getByTestId('box-output-views')).not.toBeInTheDocument()
    })

    it('renders sections with correct headers based on data presence', async () => {
      const status = makeStatus([makeOutputStatus('my_view')], [makeInputStatus('my_table')])
      await renderComponent(buildMetrics(status))

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
      expect(outputHtml).toContain('batch')
      expect(outputHtml).toContain('Encode errors')
    })
  })

  describe('B. Metric value display', () => {
    it('displays formatted input metrics, zero values, and relation name', async () => {
      const status = makeStatus(
        [],
        [
          makeInputStatus('orders', {
            metrics: makeInputMetrics({
              total_records: 1234567,
              total_bytes: 1048576,
              buffered_records: 42,
              buffered_bytes: 2048
            })
          }),
          makeInputStatus('empty_table')
        ]
      )
      await renderComponent(buildMetrics(status))

      // Formatted metric values
      await expect.element(page.getByText('1,234,567')).toBeInTheDocument()
      await expect.element(page.getByText('1.0 MiB')).toBeInTheDocument()
      await expect.element(page.getByText('42')).toBeInTheDocument()
      await expect.element(page.getByText('2.0 KiB')).toBeInTheDocument()

      // Zero metrics display correctly
      await expect.element(page.getByText('0 B').first()).toBeInTheDocument()

      // Relation name rendered in row (stream names are normalized to lowercase)
      await expect.element(page.getByTestId('box-relation-row-orders')).toBeInTheDocument()
      await expect.element(page.getByText('orders')).toBeInTheDocument()
    })

    it('displays formatted output metrics', async () => {
      const status = makeStatus([
        makeOutputStatus('out_view', {
          metrics: makeOutputMetrics({
            transmitted_records: 5000,
            transmitted_bytes: 512000,
            buffered_records: 10,
            queued_records: 3
          })
        })
      ])
      await renderComponent(buildMetrics(status))
      await expect.element(page.getByText('5,000')).toBeInTheDocument()
      await expect.element(page.getByText('500.0 KiB')).toBeInTheDocument()
    })

    it('displays batch_records_written as a formatted number when set, and em-dash when null', async () => {
      const status = makeStatus([
        makeOutputStatus('v_with_batch', {
          metrics: makeOutputMetrics({ batch_records_written: 7500 })
        }),
        makeOutputStatus('v_null_batch', {
          metrics: makeOutputMetrics({ batch_records_written: null })
        })
      ])
      await renderComponent(buildMetrics(status))
      await expect.element(page.getByText('7,500')).toBeInTheDocument()
      const emDashes = page.getByText('—').all()
      expect(emDashes.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('C. io_active indicator', () => {
    it('applies green text class on active records for both input and output', async () => {
      const prev = makeStatus(
        [
          makeOutputStatus('v1', {
            endpoint_name: 'o1',
            metrics: makeOutputMetrics({ transmitted_records: 0 })
          })
        ],
        [
          makeInputStatus('t1', {
            endpoint_name: 'i1',
            metrics: makeInputMetrics({ total_records: 0 })
          })
        ]
      )
      const cur = makeStatus(
        [
          makeOutputStatus('v1', {
            endpoint_name: 'o1',
            metrics: makeOutputMetrics({ transmitted_records: 200 })
          })
        ],
        [
          makeInputStatus('t1', {
            endpoint_name: 'i1',
            metrics: makeInputMetrics({ total_records: 100 })
          })
        ]
      )
      await renderComponent(buildMetrics(cur, prev))

      const inputEl = page.getByText('100')
      await expect.element(inputEl).toBeInTheDocument()
      await expect.element(inputEl).toHaveClass('text-success-600-400')

      const outputEl = page.getByText('200')
      await expect.element(outputEl).toBeInTheDocument()
      await expect.element(outputEl).toHaveClass('text-success-600-400')
    })

    it('applies green text class to batch_records_written cell when io_active, and not when inactive', async () => {
      const prevActive = makeStatus([
        makeOutputStatus('v_active', {
          endpoint_name: 'o1',
          metrics: makeOutputMetrics({ batch_records_written: 1000 })
        })
      ])
      const curActive = makeStatus([
        makeOutputStatus('v_active', {
          endpoint_name: 'o1',
          metrics: makeOutputMetrics({ batch_records_written: 3333 })
        })
      ])
      const { unmount } = await renderComponent(buildMetrics(curActive, prevActive))
      const activeEl = page.getByText('3,333')
      await expect.element(activeEl).toBeInTheDocument()
      await expect.element(activeEl).toHaveClass('text-success-600-400')
      unmount()

      // No prev snapshot → io_active is always false on first call
      const curInactive = makeStatus([
        makeOutputStatus('v_inactive', {
          metrics: makeOutputMetrics({ batch_records_written: 4444 })
        })
      ])
      await renderComponent(buildMetrics(curInactive))
      const inactiveEl = page.getByText('4,444')
      await expect.element(inactiveEl).toBeInTheDocument()
      await expect.element(inactiveEl).not.toHaveClass('text-success-600-400')
    })
  })

  describe('D. Connector status icons', () => {
    it('shows correct status icon for each input connector state', async () => {
      const cases: {
        statusFn: () => ControllerStatus
        testId: string
        text?: string
      }[] = [
        {
          statusFn: () => makeStatus([], [makeInputStatus('t1', { barrier: true })]),
          testId: 'box-icon-barrier'
        },
        {
          statusFn: () =>
            makeStatus(
              [],
              [makeInputStatus('t1', { metrics: makeInputMetrics({ num_parse_errors: 5 }) })]
            ),
          testId: 'btn-icon-input-errors'
        },
        {
          statusFn: () =>
            makeStatus([], [makeInputStatus('t1', { endpoint_name: 'ep1' })], {
              ep1: { phase: 'Started' }
            }),
          testId: 'box-icon-transaction-started',
          text: 'Started'
        },
        {
          statusFn: () =>
            makeStatus([], [makeInputStatus('t1', { endpoint_name: 'ep1' })], {
              ep1: { phase: 'Committed' }
            }),
          testId: 'box-icon-transaction-committed',
          text: 'Ready to commit'
        },
        {
          statusFn: () =>
            makeStatus(
              [],
              [makeInputStatus('t1', { metrics: makeInputMetrics({ end_of_input: true }) })]
            ),
          testId: 'box-icon-end-of-input'
        },
        {
          statusFn: () => makeStatus([], [makeInputStatus('t1', { paused: true })]),
          testId: 'box-icon-paused'
        },
        {
          statusFn: () => makeStatus([], [makeInputStatus('t1', { fatal_error: 'err' })]),
          testId: 'btn-icon-input-fatal-error'
        },
        {
          statusFn: () => makeStatus([], [makeInputStatus('t1')]),
          testId: 'box-icon-running'
        }
      ]

      for (const { statusFn, testId, text } of cases) {
        const { unmount } = await renderComponent(buildMetrics(statusFn()))
        const el = page.getByTestId(testId)
        await expect.element(el).toBeInTheDocument()
        if (text) {
          expect(el.element().textContent?.trim()).toBe(text)
        }
        unmount()
      }
    })

    it('shows error icon for output connector with errors', async () => {
      const status = makeStatus([
        makeOutputStatus('v1', { metrics: makeOutputMetrics({ num_encode_errors: 3 }) })
      ])
      await renderComponent(buildMetrics(status))
      await expect.element(page.getByTestId('btn-icon-output-errors')).toBeInTheDocument()
    })

    it('shows no error icon for output connector without errors', async () => {
      await renderComponent(buildMetrics(makeStatus([makeOutputStatus('v1')])))
      await expect.element(page.getByTestId('btn-icon-output-errors')).not.toBeInTheDocument()
    })
  })

  describe('E. Multi-connector rows', () => {
    it('shows summary text and chevron for multi-connector input table', async () => {
      const status = makeStatus(
        [],
        [
          makeInputStatus('multi', { endpoint_name: 'c1', paused: false }),
          makeInputStatus('multi', { endpoint_name: 'c2', paused: true }),
          makeInputStatus('single', { endpoint_name: 'c3' })
        ]
      )
      await renderComponent(buildMetrics(status))

      // Multi-connector summary with running count
      await expect.element(page.getByTestId('box-multi-connector-summary')).toBeInTheDocument()
      await expect.element(page.getByText('1 / 2 running')).toBeInTheDocument()

      // Chevron on multi-connector row only
      const multiRowEl = page.getByTestId('box-relation-row-multi').element()
      expect(multiRowEl.querySelector('.fd-chevron-down')).not.toBeNull()
    })

    it('expands to show individual connectors and collapses on second click', async () => {
      const status = makeStatus(
        [],
        [
          makeInputStatus('t1', { endpoint_name: 'alpha' }),
          makeInputStatus('t1', { endpoint_name: 'beta' })
        ]
      )
      await renderComponent(buildMetrics(status))

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
      const status = makeStatus([
        makeOutputStatus('v1', { endpoint_name: 'o1' }),
        makeOutputStatus('v1', { endpoint_name: 'o2' }),
        makeOutputStatus('v1', { endpoint_name: 'o3' })
      ])
      await renderComponent(buildMetrics(status))
      await expect.element(page.getByText('3 connectors')).toBeInTheDocument()
    })
  })

  describe('F. Error count buttons & callbacks', () => {
    it('clicking error counts calls onConnectorSelect with correct arguments', async () => {
      const onConnectorSelect = vi.fn()
      const status = makeStatus(
        [
          makeOutputStatus('vw', {
            endpoint_name: 'oep',
            metrics: makeOutputMetrics({ num_encode_errors: 2, num_transport_errors: 4 })
          })
        ],
        [
          makeInputStatus('tbl', {
            endpoint_name: 'ep1',
            metrics: makeInputMetrics({ num_parse_errors: 7, num_transport_errors: 3 })
          })
        ]
      )
      await renderComponent(buildMetrics(status), onConnectorSelect)

      await page.getByTestId('btn-parse-errors').click()
      expect(onConnectorSelect).toHaveBeenCalledWith('tbl', 'ep1', 'input', 'parse')

      await page.getByTestId('btn-input-transport-errors').click()
      expect(onConnectorSelect).toHaveBeenCalledWith('tbl', 'ep1', 'input', 'transport')

      await page.getByTestId('btn-encode-errors').click()
      expect(onConnectorSelect).toHaveBeenCalledWith('vw', 'oep', 'output', 'encode')

      await page.getByTestId('btn-output-transport-errors').click()
      expect(onConnectorSelect).toHaveBeenCalledWith('vw', 'oep', 'output', 'transport')
    })

    it('error counts are plain text on aggregate rows (multi-connector)', async () => {
      const status = makeStatus(
        [],
        [
          makeInputStatus('tbl', {
            endpoint_name: 'c1',
            metrics: makeInputMetrics({ num_parse_errors: 5 })
          }),
          makeInputStatus('tbl', {
            endpoint_name: 'c2',
            metrics: makeInputMetrics({ num_parse_errors: 3 })
          })
        ]
      )
      await renderComponent(buildMetrics(status))
      // Aggregate row should NOT have a clickable parse error button
      await expect.element(page.getByTestId('btn-parse-errors')).not.toBeInTheDocument()
      // But should still display the aggregated count as text
      await expect.element(page.getByText('8')).toBeInTheDocument()
    })
  })

  describe('G. Error icon click', () => {
    it('clicking error icons calls onConnectorSelect with all filter', async () => {
      const onConnectorSelect = vi.fn()
      const status = makeStatus(
        [
          makeOutputStatus('vw', {
            endpoint_name: 'oep',
            metrics: makeOutputMetrics({ num_encode_errors: 1 })
          })
        ],
        [
          makeInputStatus('tbl', {
            endpoint_name: 'ep1',
            metrics: makeInputMetrics({ num_parse_errors: 1 })
          })
        ]
      )
      await renderComponent(buildMetrics(status), onConnectorSelect)

      const inputIcon = page.getByTestId('btn-icon-input-errors')
      await expect.element(inputIcon).toBeInTheDocument()
      ;(inputIcon.element() as HTMLElement).click()
      expect(onConnectorSelect).toHaveBeenCalledWith('tbl', 'ep1', 'input', 'all')

      const outputIcon = page.getByTestId('btn-icon-output-errors')
      await expect.element(outputIcon).toBeInTheDocument()
      ;(outputIcon.element() as HTMLElement).click()
      expect(onConnectorSelect).toHaveBeenCalledWith('vw', 'oep', 'output', 'all')
    })
  })

  describe('H. Health status', () => {
    it('shows unhealthy chip for single unhealthy connector', async () => {
      const status = makeStatus(
        [],
        [makeInputStatus('t1', { health: { status: 'Unhealthy', description: 'Connection lost' } })]
      )
      await renderComponent(buildMetrics(status))
      await expect.element(page.getByTestId('box-unhealthy-chip')).toBeInTheDocument()
    })

    it('shows unhealthy chip in summary when collapsed, moves to connector rows when expanded', async () => {
      const status = makeStatus(
        [],
        [
          makeInputStatus('t1', {
            endpoint_name: 'c1',
            health: { status: 'Unhealthy', description: 'bad' }
          }),
          makeInputStatus('t1', { endpoint_name: 'c2', health: { status: 'Healthy' } })
        ]
      )
      await renderComponent(buildMetrics(status))

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
      const status = makeStatus(
        [],
        [
          makeInputStatus('t1', {
            endpoint_name: 'c1',
            health: { status: 'Unhealthy', description: 'bad1' }
          }),
          makeInputStatus('t1', {
            endpoint_name: 'c2',
            health: { status: 'Unhealthy', description: 'bad2' }
          })
        ]
      )
      await renderComponent(buildMetrics(status))
      await page.getByTestId('box-relation-row-t1').click({ position: { x: 1, y: 0 } })
      await expect.element(page.getByTestId('box-connector-row-c1')).toBeInTheDocument()
      await expect.element(page.getByTestId('box-connector-row-c2')).toBeInTheDocument()
      const allChips = page.getByTestId('box-unhealthy-chip').all()
      expect(allChips.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('J. Fatal error icon', () => {
    it('input single connector: fatal_error shows fd-circle-x alongside fd-circle-alert when both errors exist', async () => {
      const withFatal = makeStatus(
        [],
        [
          makeInputStatus('t1', {
            fatal_error: 'Connection refused',
            metrics: makeInputMetrics({ num_transport_errors: 1 })
          })
        ]
      )
      const { unmount } = await renderComponent(buildMetrics(withFatal))
      const alertIcon = page.getByTestId('btn-icon-input-errors')
      await expect.element(alertIcon).toBeInTheDocument()
      expect(alertIcon.element().classList.contains('fd-circle-alert')).toBe(true)
      const fatalIcon = page.getByTestId('btn-icon-input-fatal-error')
      await expect.element(fatalIcon).toBeInTheDocument()
      expect(fatalIcon.element().classList.contains('fd-circle-x')).toBe(true)
      unmount()

      const withoutFatal = makeStatus(
        [],
        [makeInputStatus('t1', { metrics: makeInputMetrics({ num_transport_errors: 1 }) })]
      )
      await renderComponent(buildMetrics(withoutFatal))
      const icon = page.getByTestId('btn-icon-input-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-x')).toBe(false)
      await expect.element(page.getByTestId('btn-icon-input-fatal-error')).not.toBeInTheDocument()
      await expect.element(page.getByTestId('box-icon-running')).toBeInTheDocument()
    })

    it('output single connector: fatal_error shows fd-circle-x, absent shows fd-circle-alert', async () => {
      const withFatal = makeStatus([
        makeOutputStatus('v1', {
          fatal_error: 'Write failed',
          metrics: makeOutputMetrics({ num_encode_errors: 1 })
        })
      ])
      const { unmount } = await renderComponent(buildMetrics(withFatal))
      let icon = page.getByTestId('btn-icon-output-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-x')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(false)
      unmount()

      const withoutFatal = makeStatus([
        makeOutputStatus('v1', { metrics: makeOutputMetrics({ num_encode_errors: 1 }) })
      ])
      await renderComponent(buildMetrics(withoutFatal))
      icon = page.getByTestId('btn-icon-output-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-x')).toBe(false)
    })

    it('single input connector error icon is interactive (role=button), composite is not', async () => {
      const singleStatus = makeStatus(
        [],
        [makeInputStatus('t1', { metrics: makeInputMetrics({ num_transport_errors: 1 }) })]
      )
      const { unmount } = await renderComponent(buildMetrics(singleStatus))
      const singleIcon = page.getByTestId('btn-icon-input-errors').element()
      expect(singleIcon.getAttribute('role')).toBe('button')
      expect(singleIcon.getAttribute('tabindex')).toBe('0')
      unmount()

      const multiStatus = makeStatus(
        [],
        [
          makeInputStatus('t1', {
            endpoint_name: 'c1',
            metrics: makeInputMetrics({ num_transport_errors: 1 })
          }),
          makeInputStatus('t1', { endpoint_name: 'c2' })
        ]
      )
      await renderComponent(buildMetrics(multiStatus))
      const compositeIcon = page.getByTestId('btn-icon-input-errors').element()
      expect(compositeIcon.getAttribute('role')).toBeNull()
    })

    it('single output connector error icon is interactive (role=button)', async () => {
      const status = makeStatus([
        makeOutputStatus('v1', { metrics: makeOutputMetrics({ num_encode_errors: 1 }) })
      ])
      await renderComponent(buildMetrics(status))
      const singleIcon = page.getByTestId('btn-icon-output-errors').element()
      expect(singleIcon.getAttribute('role')).toBe('button')
      expect(singleIcon.getAttribute('tabindex')).toBe('0')
    })

    it('multi-connector output summary error icon is not interactive', async () => {
      const status = makeStatus([
        makeOutputStatus('v1', {
          endpoint_name: 'o1',
          metrics: makeOutputMetrics({ num_encode_errors: 1 })
        }),
        makeOutputStatus('v1', { endpoint_name: 'o2' })
      ])
      await renderComponent(buildMetrics(status))
      const compositeIcon = page.getByTestId('btn-icon-output-errors').element()
      expect(compositeIcon.getAttribute('role')).toBeNull()
    })

    it('multi-connector input row: fatal_error on any connector shows fd-circle-x alongside fd-circle-alert in collapsed summary', async () => {
      const withFatal = makeStatus(
        [],
        [
          makeInputStatus('t1', {
            endpoint_name: 'c1',
            fatal_error: 'Disk full',
            metrics: makeInputMetrics({ num_transport_errors: 1 })
          }),
          makeInputStatus('t1', { endpoint_name: 'c2' })
        ]
      )
      const { unmount } = await renderComponent(buildMetrics(withFatal))
      const alertIcon = page.getByTestId('btn-icon-input-errors')
      await expect.element(alertIcon).toBeInTheDocument()
      expect(alertIcon.element().classList.contains('fd-circle-alert')).toBe(true)
      const fatalIcon = page.getByTestId('btn-icon-input-fatal-error')
      await expect.element(fatalIcon).toBeInTheDocument()
      expect(fatalIcon.element().classList.contains('fd-circle-x')).toBe(true)
      unmount()

      const withoutFatal = makeStatus(
        [],
        [
          makeInputStatus('t1', {
            endpoint_name: 'c1',
            metrics: makeInputMetrics({ num_transport_errors: 1 })
          }),
          makeInputStatus('t1', { endpoint_name: 'c2' })
        ]
      )
      await renderComponent(buildMetrics(withoutFatal))
      const icon = page.getByTestId('btn-icon-input-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-x')).toBe(false)
      await expect.element(page.getByTestId('btn-icon-input-fatal-error')).not.toBeInTheDocument()
    })

    it('multi-connector output row: fatal_error on any connector shows fd-circle-x in collapsed summary', async () => {
      const withFatal = makeStatus([
        makeOutputStatus('v1', {
          endpoint_name: 'o1',
          fatal_error: 'Pipe broken',
          metrics: makeOutputMetrics({ num_transport_errors: 1 })
        }),
        makeOutputStatus('v1', { endpoint_name: 'o2' })
      ])
      const { unmount } = await renderComponent(buildMetrics(withFatal))
      let icon = page.getByTestId('btn-icon-output-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-x')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(false)
      unmount()

      const withoutFatal = makeStatus([
        makeOutputStatus('v1', {
          endpoint_name: 'o1',
          metrics: makeOutputMetrics({ num_transport_errors: 1 })
        }),
        makeOutputStatus('v1', { endpoint_name: 'o2' })
      ])
      await renderComponent(buildMetrics(withoutFatal))
      icon = page.getByTestId('btn-icon-output-errors')
      await expect.element(icon).toBeInTheDocument()
      expect(icon.element().classList.contains('fd-circle-alert')).toBe(true)
      expect(icon.element().classList.contains('fd-circle-x')).toBe(false)
    })
  })

  describe('I. Health filter', () => {
    it('defaults to all, filters to unhealthy, then restores on switch back', async () => {
      const status = makeStatus(
        [],
        [
          makeInputStatus('healthy_t', { health: { status: 'Healthy' } }),
          makeInputStatus('unhealthy_t', { health: { status: 'Unhealthy', description: 'err' } })
        ]
      )
      await renderComponent(buildMetrics(status))

      await expect.element(page.getByTestId('box-relation-row-healthy_t')).toBeInTheDocument()
      await expect.element(page.getByTestId('box-relation-row-unhealthy_t')).toBeInTheDocument()

      await clickHealthFilter('unhealthy')
      await expect.element(page.getByTestId('box-relation-row-healthy_t')).not.toBeInTheDocument()
      await expect.element(page.getByTestId('box-relation-row-unhealthy_t')).toBeInTheDocument()

      await clickHealthFilter('all')
      await expect.element(page.getByTestId('box-relation-row-healthy_t')).toBeInTheDocument()
      await expect.element(page.getByTestId('box-relation-row-unhealthy_t')).toBeInTheDocument()
    })

    it('filtering to unhealthy filters connectors within a table', async () => {
      const status = makeStatus(
        [],
        [
          makeInputStatus('t1', { endpoint_name: 'healthy_c', health: { status: 'Healthy' } }),
          makeInputStatus('t1', {
            endpoint_name: 'unhealthy_c',
            health: { status: 'Unhealthy', description: 'err' }
          })
        ]
      )
      await renderComponent(buildMetrics(status))
      await clickHealthFilter('unhealthy')
      await expect.element(page.getByTestId('box-relation-row-t1')).toBeInTheDocument()
      // Since filtered to 1 connector, it should be shown directly (no multi-connector summary)
      await expect.element(page.getByTestId('box-multi-connector-summary')).not.toBeInTheDocument()
    })

    it('unhealthy count badge shows correct number', async () => {
      const status = makeStatus(
        [],
        [
          makeInputStatus('t1', {
            endpoint_name: 'c1',
            health: { status: 'Unhealthy', description: 'err' }
          }),
          makeInputStatus('t2', {
            endpoint_name: 'c2',
            health: { status: 'Unhealthy', description: 'err2' }
          })
        ]
      )
      await renderComponent(buildMetrics(status))
      const badge = page.getByText('2').elements()
      expect(badge.length).toBeGreaterThanOrEqual(1)
    })
  })
})
