import { tick } from 'svelte'
import { describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineThumb } from '$lib/services/pipelineManager'
import List from './List.svelte'

// List only reads `name` (for the label/filter) and `status` (for the status
// dot) off each thumb, so a minimal stub is enough to exercise the search.
const thumb = (name: string): PipelineThumb =>
  ({ name, status: 'Stopped' }) as unknown as PipelineThumb

const pipelines = [thumb('orders'), thumb('payments'), thumb('fraud-detection')]

// Each rendered pipeline is an <a>; the only other interactive elements are the
// search box and the close button, so counting links counts visible pipelines.
const visibleNames = () =>
  page
    .getByRole('link')
    .elements()
    .map((el) => el.textContent?.trim())

const setSearch = async (value: string) => {
  const search = page.getByPlaceholder('Search pipelines...')
  await expect.element(search).toBeInTheDocument()
  const input = search.element() as HTMLInputElement
  input.value = value
  input.dispatchEvent(new Event('input', { bubbles: true }))
  await tick()
}

describe('pipeline list search', () => {
  it('lists every pipeline before any search term is entered', async () => {
    await render(List, { pipelineName: '', pipelines })

    await expect.element(page.getByText('orders')).toBeInTheDocument()
    await expect.element(page.getByText('payments')).toBeInTheDocument()
    await expect.element(page.getByText('fraud-detection')).toBeInTheDocument()
    expect(visibleNames()).toHaveLength(3)
  })

  it('filters to pipelines whose name contains the search term', async () => {
    await render(List, { pipelineName: '', pipelines })

    await setSearch('pay')

    await expect.element(page.getByText('payments')).toBeInTheDocument()
    await expect.element(page.getByText('orders')).not.toBeInTheDocument()
    await expect.element(page.getByText('fraud-detection')).not.toBeInTheDocument()
    expect(visibleNames()).toEqual(['payments'])
  })

  it('matches case-insensitively', async () => {
    await render(List, { pipelineName: '', pipelines })

    await setSearch('PAY')

    expect(visibleNames()).toEqual(['payments'])
  })

  it('matches a substring anywhere in the name, not only a prefix', async () => {
    await render(List, { pipelineName: '', pipelines })

    await setSearch('detect')

    expect(visibleNames()).toEqual(['fraud-detection'])
  })

  it('shows no pipelines when the term matches nothing', async () => {
    await render(List, { pipelineName: '', pipelines })

    await setSearch('no-such-pipeline')

    expect(visibleNames()).toHaveLength(0)
  })

  it('restores the full list when the search term is cleared', async () => {
    await render(List, { pipelineName: '', pipelines })

    await setSearch('pay')
    expect(visibleNames()).toEqual(['payments'])

    await setSearch('')
    expect(visibleNames()).toHaveLength(3)
  })
})
