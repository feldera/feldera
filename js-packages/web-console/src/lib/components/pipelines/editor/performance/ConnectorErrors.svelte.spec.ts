/** The web console wrapper fetches the connector status from the API and shows its errors. */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const api = vi.hoisted(() => ({
  getInputConnectorStatus: vi.fn(),
  getOutputConnectorStatus: vi.fn()
}))

vi.mock('$lib/services/pipelineManager', () => api)

// Imported AFTER vi.mock so the mock takes effect.
import ConnectorErrors from './ConnectorErrors.svelte'

const timestamp = '2026-05-01T12:00:00Z'

afterEach(() => {
  vi.resetAllMocks()
})

describe('ConnectorErrors wrapper', () => {
  it('fetches the input status by the connector name without the relation prefix', async () => {
    api.getInputConnectorStatus.mockResolvedValue({
      parse_errors: [{ index: 0, tag: 'parse', message: 'bad row', timestamp }],
      transport_errors: []
    })
    render(ConnectorErrors, {
      pipelineName: 'p1',
      relationName: 't',
      connectorName: 't.in1',
      direction: 'input',
      onClose: () => {}
    })
    await expect.element(page.getByText('bad row').first()).toBeInTheDocument()
    expect(api.getInputConnectorStatus).toHaveBeenCalledWith('p1', 't', 'in1')
    expect(api.getOutputConnectorStatus).not.toHaveBeenCalled()
  })

  it('fetches the output status for an output', async () => {
    api.getOutputConnectorStatus.mockResolvedValue({
      encode_errors: [{ index: 0, tag: 'encode', message: 'bad encode', timestamp }],
      transport_errors: []
    })
    render(ConnectorErrors, {
      pipelineName: 'p1',
      relationName: 'v',
      connectorName: 'v.out1',
      direction: 'output',
      onClose: () => {}
    })
    await expect.element(page.getByText('bad encode').first()).toBeInTheDocument()
    expect(api.getOutputConnectorStatus).toHaveBeenCalledWith('p1', 'v', 'out1')
    expect(api.getInputConnectorStatus).not.toHaveBeenCalled()
  })

  it('shows loading until the response arrives', async () => {
    let respond: (status: unknown) => void = () => {}
    api.getInputConnectorStatus.mockReturnValue(new Promise((resolve) => (respond = resolve)))
    render(ConnectorErrors, {
      pipelineName: 'p1',
      relationName: 't',
      connectorName: 't.in1',
      direction: 'input',
      onClose: () => {}
    })
    await expect.element(page.getByText('Loading…')).toBeInTheDocument()
    respond({ parse_errors: [], transport_errors: [] })
    await expect.element(page.getByText('No errors', { exact: true })).toBeInTheDocument()
  })

  it('fetches again for another connector', async () => {
    api.getInputConnectorStatus.mockResolvedValue({ parse_errors: [], transport_errors: [] })
    const view = render(ConnectorErrors, {
      pipelineName: 'p1',
      relationName: 't',
      connectorName: 't.in1',
      direction: 'input',
      onClose: () => {}
    })
    await expect.poll(() => api.getInputConnectorStatus.mock.calls.length).toBe(1)
    await view.rerender({ connectorName: 't.in2' })
    await expect.poll(() => api.getInputConnectorStatus.mock.calls.length).toBe(2)
    expect(api.getInputConnectorStatus).toHaveBeenLastCalledWith('p1', 't', 'in2')
  })

  it('calls onClose from the close button', async () => {
    api.getInputConnectorStatus.mockResolvedValue({ parse_errors: [], transport_errors: [] })
    const onClose = vi.fn()
    render(ConnectorErrors, {
      pipelineName: 'p1',
      relationName: 't',
      connectorName: 't.in1',
      direction: 'input',
      onClose
    })
    await expect.element(page.getByText('No errors', { exact: true })).toBeInTheDocument()
    ;(page.getByRole('button', { name: 'Close' }).element() as HTMLElement).click()
    expect(onClose).toHaveBeenCalledOnce()
  })
})
