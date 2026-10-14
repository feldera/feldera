/** The Connectors tab opens the errors of a connector from its statistics, in a drawer. */

import {
  aggregateConnectorMetrics,
  type ConnectorStatus,
  type InputEndpointStatus,
  type OutputEndpointStatus
} from 'common-ui/connectorMetrics'
import { describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import ConnectorsTab from './ConnectorsTab.svelte'
import type { AnalysisTabProps } from './MetricsTab.svelte'

const timestamp = '2026-05-01T12:00:00Z'

const input = (extra: Partial<InputEndpointStatus> = {}): InputEndpointStatus => ({
  endpoint_name: 't.in1',
  config: { stream: 't' },
  metrics: {
    buffered_bytes: 0,
    buffered_records: 0,
    end_of_input: false,
    num_parse_errors: 1,
    num_transport_errors: 0,
    total_bytes: 0,
    total_records: 0
  },
  paused: false,
  barrier: false,
  parse_errors: [{ index: 0, tag: 'parse', message: 'bad input row', timestamp }],
  ...extra
})

const output = (extra: Partial<OutputEndpointStatus> = {}): OutputEndpointStatus => ({
  endpoint_name: 'v.out1',
  config: { stream: 'v' },
  metrics: {
    buffered_batches: 0,
    buffered_records: 0,
    memory: 0,
    num_encode_errors: 1,
    num_transport_errors: 0,
    queued_batches: 0,
    queued_records: 0,
    total_processed_input_records: 0,
    total_processed_steps: 0,
    transmitted_bytes: 0,
    transmitted_records: 0
  },
  encode_errors: [{ index: 0, tag: 'encode', message: 'bad output row', timestamp }],
  ...extra
})

// The tab reads only these two props.
const props = (status: ConnectorStatus) =>
  ({
    connectorMetrics: aggregateConnectorMetrics(status),
    connectorStatus: status
  }) as unknown as AnalysisTabProps

const bothStatus = (): ConnectorStatus => ({ inputs: [input()], outputs: [output()] })

const click = (testId: string) =>
  (page.getByTestId(testId).first().element() as HTMLElement).click()
const closeButton = () => page.getByRole('button', { name: 'Close' })

describe('ConnectorsTab errors drawer', () => {
  it('is closed at first', async () => {
    render(ConnectorsTab, props(bothStatus()))
    await expect.element(page.getByTestId('btn-icon-input-errors').first()).toBeInTheDocument()
    await expect.element(closeButton()).not.toBeInTheDocument()
  })

  it('opens the parse errors of an input from its error icon', async () => {
    render(ConnectorsTab, props(bothStatus()))
    await expect.element(page.getByTestId('btn-icon-input-errors').first()).toBeInTheDocument()
    click('btn-icon-input-errors')
    await expect.element(page.getByText('bad input row').first()).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('bad output row')
  })

  it('opens the encode errors of an output from its error icon', async () => {
    render(ConnectorsTab, props(bothStatus()))
    await expect.element(page.getByTestId('btn-icon-output-errors').first()).toBeInTheDocument()
    click('btn-icon-output-errors')
    await expect.element(page.getByText('bad output row').first()).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('bad input row')
  })

  it('opens the list of the kind of error that the user clicked', async () => {
    const status = {
      inputs: [
        input({
          metrics: { ...input().metrics, num_transport_errors: 1 },
          transport_errors: [{ index: 0, tag: 'transport', message: 'broker down', timestamp }]
        })
      ],
      outputs: []
    }
    render(ConnectorsTab, props(status))
    await expect.element(page.getByTestId('btn-parse-errors').first()).toBeInTheDocument()
    click('btn-input-transport-errors')
    await expect.element(page.getByText('broker down').first()).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('bad input row')
  })

  it('closes from the close button', async () => {
    render(ConnectorsTab, props(bothStatus()))
    await expect.element(page.getByTestId('btn-icon-input-errors').first()).toBeInTheDocument()
    click('btn-icon-input-errors')
    await expect.element(closeButton()).toBeInTheDocument()
    ;(closeButton().element() as HTMLElement).click()
    await expect.element(closeButton()).not.toBeInTheDocument()
  })

  it('closes when the bundle changes', async () => {
    const view = render(ConnectorsTab, props(bothStatus()))
    await expect.element(page.getByTestId('btn-icon-input-errors').first()).toBeInTheDocument()
    click('btn-icon-input-errors')
    await expect.element(closeButton()).toBeInTheDocument()
    await view.rerender(props(bothStatus()))
    await expect.element(closeButton()).not.toBeInTheDocument()
  })

  it('says there are no errors for a connector missing from the bundle', async () => {
    // The statistics name a connector that the status list does not have.
    render(ConnectorsTab, {
      ...props(bothStatus()),
      connectorStatus: { inputs: [], outputs: [] }
    })
    await expect.element(page.getByTestId('btn-icon-input-errors').first()).toBeInTheDocument()
    click('btn-icon-input-errors')
    await expect.element(page.getByText('No errors', { exact: true })).toBeInTheDocument()
  })

  it('says there are no errors for a bundle collected without connector errors', async () => {
    const status = {
      inputs: [input({ parse_errors: undefined })],
      outputs: [output({ encode_errors: undefined })]
    }
    render(ConnectorsTab, props(status))
    await expect.element(page.getByTestId('btn-icon-input-errors').first()).toBeInTheDocument()
    click('btn-icon-input-errors')
    await expect.element(page.getByText('No errors', { exact: true })).toBeInTheDocument()
  })

  it('shows the errors of the connector that the user clicked, not of another one', async () => {
    const other = (message: string) => [{ index: 0, tag: 'parse', message, timestamp }]
    const status = {
      inputs: [
        input({ parse_errors: other('from in1') }),
        input({
          endpoint_name: 'u.in2',
          config: { stream: 'u' },
          parse_errors: other('from in2')
        })
      ],
      outputs: []
    }
    render(ConnectorsTab, props(status))
    await expect.element(page.getByTestId('btn-icon-input-errors').nth(1)).toBeInTheDocument()
    ;(page.getByTestId('btn-icon-input-errors').nth(1).element() as HTMLElement).click()
    await expect.element(page.getByText('from in2').first()).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('from in1')
  })

  it('says that the bundle has no connector statistics without them', async () => {
    render(ConnectorsTab, { connectorMetrics: undefined, connectorStatus: undefined } as never)
    await expect
      .element(page.getByText('No connector statistics available in this bundle'))
      .toBeInTheDocument()
  })
})
