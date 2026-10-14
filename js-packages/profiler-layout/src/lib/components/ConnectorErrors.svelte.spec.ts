/** The error list of one connector, as the support bundle viewer and the web console show it. */

import { ConnectorErrors } from 'common-ui'
import type {
  ConnectorError,
  InputEndpointStatus,
  OutputEndpointStatus
} from 'common-ui/connectorMetrics'
import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const error = (index: number, message: string, tag: string): ConnectorError => ({
  index,
  message,
  tag,
  timestamp: '2026-05-01T12:00:00Z'
})

const inputStatus = (extra: Partial<InputEndpointStatus> = {}): InputEndpointStatus => ({
  endpoint_name: 't.in1',
  config: { stream: 't' },
  metrics: {
    buffered_bytes: 0,
    buffered_records: 0,
    end_of_input: false,
    num_parse_errors: 0,
    num_transport_errors: 0,
    total_bytes: 0,
    total_records: 0
  },
  paused: false,
  barrier: false,
  ...extra
})

const outputStatus = (extra: Partial<OutputEndpointStatus> = {}): OutputEndpointStatus => ({
  endpoint_name: 'v.out1',
  config: { stream: 'v' },
  metrics: {
    buffered_batches: 0,
    buffered_records: 0,
    memory: 0,
    num_encode_errors: 0,
    num_transport_errors: 0,
    queued_batches: 0,
    queued_records: 0,
    total_processed_input_records: 0,
    total_processed_steps: 0,
    transmitted_bytes: 0,
    transmitted_records: 0
  },
  ...extra
})

const baseProps = {
  relationName: 't',
  connectorName: 't.in1',
  direction: 'input' as const,
  onClose: () => {}
}

const text = (message: string) => page.getByText(message, { exact: true }).first()
const filterButton = (name: string) => page.getByRole('radio', { name, exact: true })
// The segment labels are not stable for Playwright, so the test clicks them through the DOM.
const pickFilter = (name: string) =>
  (page.getByText(name, { exact: true }).element() as HTMLElement).click()

describe('ConnectorErrors', () => {
  it('shows the relation and the connector name without the relation prefix', async () => {
    render(ConnectorErrors, { ...baseProps, status: inputStatus() })
    await expect.element(page.getByText('t · in1')).toBeInTheDocument()
  })

  it('lists the parse and transport errors of an input', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({
        parse_errors: [error(0, 'bad row', 'parse')],
        transport_errors: [error(0, 'broker down', 'transport')]
      })
    })
    await expect.element(text('bad row')).toBeInTheDocument()
    await expect.element(text('broker down')).toBeInTheDocument()
  })

  it('lists the encode and transport errors of an output', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      relationName: 'v',
      connectorName: 'v.out1',
      direction: 'output',
      status: outputStatus({
        encode_errors: [error(0, 'cannot encode', 'encode')],
        transport_errors: [error(0, 'sink down', 'transport')]
      })
    })
    await expect.element(text('cannot encode')).toBeInTheDocument()
    await expect.element(text('sink down')).toBeInTheDocument()
  })

  it('ignores the error list of the other direction', async () => {
    // An output status has no parse list, and an input status has no encode list.
    render(ConnectorErrors, {
      ...baseProps,
      status: { ...inputStatus(), encode_errors: [error(0, 'stray encode', 'encode')] } as never
    })
    await expect.element(page.getByText('No errors')).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('stray encode')
  })

  it('offers the filters of the direction', async () => {
    const view = render(ConnectorErrors, { ...baseProps, status: inputStatus() })
    await expect.element(filterButton('Parse')).toBeInTheDocument()
    await expect.element(filterButton('Encode')).not.toBeInTheDocument()
    await view.unmount()

    render(ConnectorErrors, {
      ...baseProps,
      direction: 'output',
      status: outputStatus()
    })
    await expect.element(filterButton('Encode')).toBeInTheDocument()
    await expect.element(filterButton('Parse')).not.toBeInTheDocument()
  })

  it('says there are no errors when the status has no error lists', async () => {
    // A bundle collected before the pipeline manager included the connector errors.
    render(ConnectorErrors, { ...baseProps, status: inputStatus() })
    await expect.element(page.getByText('No errors', { exact: true })).toBeInTheDocument()
  })

  it('says there are no errors when the error lists are null', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({ parse_errors: null, transport_errors: null })
    })
    await expect.element(page.getByText('No errors', { exact: true })).toBeInTheDocument()
  })

  it('says there are no errors when the status is missing', async () => {
    render(ConnectorErrors, { ...baseProps, status: null })
    await expect.element(page.getByText('No errors', { exact: true })).toBeInTheDocument()
  })

  it('shows loading instead of the list while loading', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      loading: true,
      status: inputStatus({ parse_errors: [error(0, 'bad row', 'parse')] })
    })
    await expect.element(page.getByText('Loading…')).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('bad row')
  })

  it('shows the gap between two errors of one tag', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({
        parse_errors: [error(0, 'first', 'parse'), error(4, 'fifth', 'parse')]
      })
    })
    await expect.element(page.getByText('3 other errors skipped')).toBeInTheDocument()
  })

  it('uses the singular for a gap of one error', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({
        parse_errors: [error(0, 'first', 'parse'), error(2, 'third', 'parse')]
      })
    })
    await expect.element(page.getByText('1 other error skipped')).toBeInTheDocument()
  })

  it('shows no gap between consecutive indexes', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({
        parse_errors: [error(5, 'a', 'parse'), error(6, 'b', 'parse')]
      })
    })
    await expect.element(text('b')).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('skipped')
  })

  it('does not count the indexes of other tags as a gap', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({
        parse_errors: [error(0, 'a', 'parse')],
        transport_errors: [error(9, 'b', 'transport')]
      })
    })
    await expect.element(text('b')).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('skipped')
  })

  it('shows the newest error first', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({
        parse_errors: [error(0, 'older', 'parse'), error(1, 'newer', 'parse')]
      })
    })
    await expect.element(text('newer')).toBeInTheDocument()
    const body = document.body.textContent ?? ''
    expect(body.indexOf('newer')).toBeLessThan(body.indexOf('older'))
  })

  it('starts with the filter from the props', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      filter: 'transport',
      status: inputStatus({
        parse_errors: [error(0, 'bad row', 'parse')],
        transport_errors: [error(0, 'broker down', 'transport')]
      })
    })
    await expect.element(text('broker down')).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('bad row')
  })

  it('narrows the list when the user picks a filter', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({
        parse_errors: [error(0, 'bad row', 'parse')],
        transport_errors: [error(0, 'broker down', 'transport')]
      })
    })
    await expect.element(text('bad row')).toBeInTheDocument()
    pickFilter('Parse')
    await expect.element(text('bad row')).toBeInTheDocument()
    await expect.poll(() => document.body.textContent).not.toContain('broker down')
  })

  it('says no errors of this type when the filtered list is empty', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      filter: 'parse',
      status: inputStatus({ transport_errors: [error(0, 'broker down', 'transport')] })
    })
    await expect.element(page.getByText('No errors of this type')).toBeInTheDocument()
  })

  it('follows a new filter from the props', async () => {
    const view = render(ConnectorErrors, {
      ...baseProps,
      filter: 'parse',
      status: inputStatus({
        parse_errors: [error(0, 'bad row', 'parse')],
        transport_errors: [error(0, 'broker down', 'transport')]
      })
    })
    await expect.element(text('bad row')).toBeInTheDocument()
    await view.rerender({ filter: 'transport' })
    await expect.element(text('broker down')).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('bad row')
  })

  it('marks the error that is the fatal error of the connector', async () => {
    render(ConnectorErrors, {
      ...baseProps,
      status: inputStatus({
        fatal_error: 'broker down',
        transport_errors: [error(0, 'broker down', 'transport'), error(1, 'other', 'transport')]
      })
    })
    await expect.element(text('other')).toBeInTheDocument()
    expect(document.querySelectorAll('.fd-circle-x')).toHaveLength(1)
  })

  it('calls onClose from the close button', async () => {
    const onClose = vi.fn()
    render(ConnectorErrors, { ...baseProps, status: inputStatus(), onClose })
    ;(page.getByRole('button', { name: 'Close' }).element() as HTMLElement).click()
    expect(onClose).toHaveBeenCalledOnce()
  })
})
