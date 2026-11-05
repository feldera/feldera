import { describe, expect, it } from 'vitest'
import { page, userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import PipelineSearch from './PipelineSearch.svelte'

const pipelines = [
  { name: 'orders', tags: ['team-b|3b82f6', 'prod|22c55e'] },
  { name: 'payments', tags: ['team-a|ef4444'] },
  { name: 'fraud', tags: ['team-c|a855f7', 'other'] }
]

type Layout = 'above' | 'right' | 'below'

/** Renders the search with `query` and `tagFilters` bound to a state the test reads. */
const renderSearch = async (tagsLayout: Layout) => {
  const bound = $state({ query: '', tagFilters: [] as string[] })
  const { container } = await render(PipelineSearch, {
    pipelines,
    tagsLayout,
    get query() {
      return bound.query
    },
    set query(value) {
      bound.query = value
    },
    get tagFilters() {
      return bound.tagFilters
    },
    set tagFilters(value) {
      bound.tagFilters = value
    }
  })
  // Leave room on screen for the popup above or to the right of the input.
  container.style.paddingTop = '200px'
  container.style.width = '160px'
  return bound
}

const input = () => page.getByTestId('input-pipeline-search')
const tagList = () => page.getByTestId('box-tag-suggestions')
const suggestion = (name: string) => page.getByTestId(`btn-tag-suggestion-${name}`)
const removeButton = (name: string) => page.getByTestId(`btn-remove-tag-filter-${name}`)

/** The suggested tag names in screen order. */
const suggestedNames = () =>
  Array.from(document.querySelectorAll('[data-testid^="btn-tag-suggestion-"]')).map((el) =>
    el.getAttribute('data-testid')!.slice('btn-tag-suggestion-'.length)
  )

/** The picked tag names in screen order. */
const pickedNames = () =>
  Array.from(document.querySelectorAll('[data-testid^="btn-remove-tag-filter-"]')).map((el) =>
    el.getAttribute('data-testid')!.slice('btn-remove-tag-filter-'.length)
  )

describe('PipelineSearch', () => {
  describe.each(['above', 'right', 'below'] as const)('with the tags %s the input', (layout) => {
    it('shows the tag list again when the user types after a click on a tag', async () => {
      const bound = await renderSearch(layout)

      await input().fill('team')
      await expect.poll(suggestedNames).toEqual(['team-a', 'team-b', 'team-c'])

      await suggestion('team-b').click()
      await expect.poll(pickedNames).toEqual(['team-b'])
      expect(bound.tagFilters).toEqual(['team-b|3b82f6'])
      expect(bound.query).toBe('')
      await expect.element(tagList()).not.toBeInTheDocument()

      // Type without a click on the input, because the click on the tag must leave the
      // input ready for typing.
      await userEvent.keyboard('team')
      await expect.element(input()).toHaveValue('team')
      await expect.poll(suggestedNames).toEqual(['team-a', 'team-c'])
    })
  })

  it('picks tags by click and by name on Enter, and removes tags', async () => {
    const bound = await renderSearch('right')

    await input().fill('tea')
    await expect.poll(suggestedNames).toEqual(['team-a', 'team-b', 'team-c'])
    await suggestion('team-a').click()
    await userEvent.keyboard('team')
    await suggestion('team-b').click()
    await expect.poll(pickedNames).toEqual(['team-a', 'team-b'])

    await userEvent.keyboard('team-c{Enter}')
    await expect.poll(pickedNames).toEqual(['team-a', 'team-b', 'team-c'])
    expect(bound.tagFilters).toEqual(['team-a|ef4444', 'team-b|3b82f6', 'team-c|a855f7'])
    expect(bound.query).toBe('')

    // The remove button shows only an icon. Tests load no icon font, so the button has
    // zero size and a pointer click fails. Click the element directly.
    ;(removeButton('team-a').element() as HTMLElement).click()
    await expect.poll(pickedNames).toEqual(['team-b', 'team-c'])
    expect(bound.tagFilters).toEqual(['team-b|3b82f6', 'team-c|a855f7'])

    await input().fill('team')
    await expect.poll(suggestedNames).toEqual(['team-a'])

    // A click away from the input closes the tag list.
    await userEvent.click(document.body, { position: { x: 5, y: 5 } })
    await expect.element(tagList()).not.toBeInTheDocument()
  })

  it('keeps text that is not the name of a tag as the query on Enter', async () => {
    const bound = await renderSearch('right')

    // `pro` matches the tag `prod` only in part.
    await input().fill('pro')
    await userEvent.keyboard('{Enter}')

    await expect.element(input()).toHaveValue('pro')
    expect(bound.query).toBe('pro')
    expect(bound.tagFilters).toEqual([])
  })

  it('keeps a comma in the query', async () => {
    const bound = await renderSearch('right')

    await input().fill('team')
    await userEvent.keyboard(',a')

    await expect.element(input()).toHaveValue('team,a')
    expect(bound.query).toBe('team,a')
    expect(bound.tagFilters).toEqual([])
  })

  it('moves the highlight with Tab and Shift+Tab and picks the highlighted tag on Enter', async () => {
    const bound = await renderSearch('right')
    const highlightedNames = () =>
      suggestedNames().filter((name) => suggestion(name).element().hasAttribute('data-highlighted'))

    await input().fill('team')
    await expect.poll(suggestedNames).toEqual(['team-a', 'team-b', 'team-c'])
    expect(highlightedNames()).toEqual([])

    await userEvent.keyboard('{Tab}')
    expect(highlightedNames()).toEqual(['team-a'])
    await userEvent.keyboard('{Tab}{Tab}{Tab}')
    expect(highlightedNames()).toEqual(['team-a'])
    await userEvent.keyboard('{Shift>}{Tab}{/Shift}')
    expect(highlightedNames()).toEqual(['team-c'])
    await expect.element(input()).toHaveFocus()

    // Typing removes the highlight.
    await userEvent.keyboard('-{Backspace}')
    expect(highlightedNames()).toEqual([])

    await userEvent.keyboard('{Shift>}{Tab}{/Shift}{Shift>}{Tab}{/Shift}{Enter}')
    expect(bound.tagFilters).toEqual(['team-b|3b82f6'])
    expect(bound.query).toBe('')
  })

  it('suggests no tags for a query that matches only a pipeline name', async () => {
    await renderSearch('below')

    await input().fill('orders')

    await expect.element(input()).toHaveValue('orders')
    await expect.element(tagList()).not.toBeInTheDocument()
  })
})
