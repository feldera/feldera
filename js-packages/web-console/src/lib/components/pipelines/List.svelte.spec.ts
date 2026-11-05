import { tick } from 'svelte'
import { describe, expect, it } from 'vitest'
import { page, userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineThumb } from '#lib/services/pipelineManager.ts'
import List from './List.svelte'

// List reads only `name` and `tags` (for the search) and `status` (for the status
// dot) from each thumb, so a small stub is enough to test the search.
const thumb = (name: string): PipelineThumb =>
  ({ name, tags: [], status: 'Stopped' }) as unknown as PipelineThumb

const pipelines = [thumb('orders'), thumb('payments'), thumb('fraud-detection')]

// Each rendered pipeline is an <a>; the only other interactive elements are the
// search box and the close button, so counting links counts visible pipelines.
const visibleNames = () =>
  page
    .getByRole('link')
    .elements()
    .map((el) => el.textContent?.trim())

const setSearch = async (value: string) => {
  const search = page.getByPlaceholder('Search pipelines and tags...')
  await expect.element(search).toBeInTheDocument()
  // The search ignores typing while the input has no focus. `fill` focuses the input first.
  await search.fill(value)
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

describe('pipeline list search by tag', () => {
  const tagged = (name: string, tags: string[]): PipelineThumb =>
    ({ ...thumb(name), tags }) as PipelineThumb

  const taggedPipelines = [
    tagged('alpha', ['prod|ef4444', 'team-a|22c55e']),
    tagged('prod-ingest', []),
    tagged('beta', ['prod|22c55e', 'team-b|3b82f6']),
    tagged('gamma', ['team-a|22c55e']),
    tagged('prod-export', ['team-b|3b82f6'])
  ]

  const input = () => page.getByTestId('input-pipeline-search')
  const suggestion = (name: string) => page.getByTestId(`btn-tag-suggestion-${name}`)
  const removeTag = (name: string) =>
    // The remove button shows only an icon. Tests load no icon font, so the button has
    // zero size and a pointer click fails. Click the element directly.
    (page.getByTestId(`btn-remove-tag-filter-${name}`).element() as HTMLElement).click()

  /** The suggested tag names in screen order. */
  const suggestedNames = () =>
    Array.from(document.querySelectorAll('[data-testid^="btn-tag-suggestion-"]')).map((el) =>
      el.getAttribute('data-testid')!.slice('btn-tag-suggestion-'.length)
    )

  it('suggests a tag name with two colors once, and its pick keeps both pipelines', async () => {
    const { container } = await render(List, { pipelineName: '', pipelines: taggedPipelines })
    container.style.width = '300px'

    // `alpha` has `prod|ef4444` and `beta` has `prod|22c55e`.
    await input().fill('prod')
    await expect.poll(suggestedNames).toEqual(['prod'])

    await suggestion('prod').click()
    await expect.poll(visibleNames).toEqual(['alpha', 'beta'])
  })

  it('narrows the list by each picked tag and by the typed text', async () => {
    const { container } = await render(List, { pipelineName: '', pipelines: taggedPipelines })
    // Leave room on screen for the tag list to the right of the input.
    container.style.width = '300px'

    await input().fill('prod')
    // Name matches come first.
    await expect.poll(visibleNames).toEqual(['prod-ingest', 'prod-export', 'alpha', 'beta'])

    await suggestion('prod').click()
    await expect.poll(visibleNames).toEqual(['alpha', 'beta'])

    await userEvent.keyboard('team')
    await suggestion('team-a').click()
    await expect.poll(visibleNames).toEqual(['alpha'])

    await userEvent.keyboard('gam')
    await expect.poll(visibleNames).toEqual([])

    removeTag('prod')
    await expect.poll(visibleNames).toEqual(['gamma'])

    await input().fill('')
    await expect.poll(visibleNames).toEqual(['alpha', 'gamma'])

    await userEvent.keyboard('pro')
    await expect.element(suggestion('prod')).toBeInTheDocument()
  })
})
