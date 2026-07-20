/**
 * Component tests for the shared SearchBar (common-ui). Runs in the browser project: the
 * component renders real elements and we drive real keyboard + click events. Collapsed it is
 * just a search-icon button; open it shows the query input, an "x of X" counter, and nav
 * buttons.
 */

import { SearchBar } from 'common-ui'
import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

function renderBar(props?: {
  value?: string
  open?: boolean
  results?: { current: number; total: number } | null
  onnext?: () => void
  onprevious?: () => void
  onclear?: () => void
}) {
  const handlers = {
    onnext: props?.onnext ?? vi.fn(),
    onprevious: props?.onprevious ?? vi.fn(),
    onclear: props?.onclear ?? vi.fn()
  }
  render(SearchBar, {
    value: props?.value ?? 'query',
    placeholder: 'Search logs',
    open: props?.open ?? true,
    // Distinguish "not provided" (default to one match) from an explicit `null` (no search).
    results: props && 'results' in props ? props.results : { current: 1, total: 1 },
    ...handlers
  })
  return handlers
}

const searchButton = () =>
  page.getByRole('button', { name: 'Search', exact: true }).element() as HTMLButtonElement
const input = () => page.getByPlaceholder('Search logs').element() as HTMLInputElement
const nextButton = () =>
  page.getByRole('button', { name: 'Next match' }).element() as HTMLButtonElement
const prevButton = () =>
  page.getByRole('button', { name: 'Previous match' }).element() as HTMLButtonElement
const closeButton = () =>
  page.getByRole('button', { name: 'Close search' }).element() as HTMLButtonElement

const pressKey = (key: string, opts: { shiftKey?: boolean } = {}) => {
  input().dispatchEvent(
    new KeyboardEvent('keydown', { key, bubbles: true, cancelable: true, ...opts })
  )
}

// Type into the input the way a user would: set the value and fire an `input` event so both
// the `bind:value` and the component's edit handler run.
const typeInto = (text: string) => {
  const el = input()
  el.value = text
  el.dispatchEvent(new Event('input', { bubbles: true }))
}

describe('SearchBar.svelte', () => {
  it('is collapsed to just the search button until opened', async () => {
    renderBar({ open: false })
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()
    searchButton().click()
    await expect.element(page.getByPlaceholder('Search logs')).toBeInTheDocument()
  })

  it('Enter advances to the next match', () => {
    const { onnext, onprevious } = renderBar()
    pressKey('Enter')
    expect(onnext).toHaveBeenCalledTimes(1)
    expect(onprevious).not.toHaveBeenCalled()
  })

  it('Shift+Enter steps back to the previous match', () => {
    const { onnext, onprevious } = renderBar()
    pressKey('Enter', { shiftKey: true })
    expect(onprevious).toHaveBeenCalledTimes(1)
    expect(onnext).not.toHaveBeenCalled()
  })

  it('Escape closes the popup and clears the search', async () => {
    const { onclear } = renderBar()
    pressKey('Escape')
    expect(onclear).toHaveBeenCalledTimes(1)
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()
  })

  it('the down button advances, the up button steps back', () => {
    const { onnext, onprevious } = renderBar()
    nextButton().click()
    prevButton().click()
    expect(onnext).toHaveBeenCalledTimes(1)
    expect(onprevious).toHaveBeenCalledTimes(1)
  })

  it('derives counter + nav state from `results` — one source of truth', async () => {
    // total 0 → "No results", nav disabled.
    renderBar({ results: { current: 0, total: 0 } })
    await expect.element(page.getByText('No results')).toBeInTheDocument()
    expect(nextButton().disabled).toBe(true)
    expect(prevButton().disabled).toBe(true)
  })

  it('shows "x of X" and enables nav when there are matches', async () => {
    renderBar({ results: { current: 2, total: 5 } })
    await expect.element(page.getByText('2 of 5')).toBeInTheDocument()
    expect(nextButton().disabled).toBe(false)
    expect(prevButton().disabled).toBe(false)
  })

  it('shows no counter and disables nav when results is null (no active search)', async () => {
    renderBar({ results: null })
    await expect.element(page.getByText('No results')).not.toBeInTheDocument()
    expect(nextButton().disabled).toBe(true)
    expect(prevButton().disabled).toBe(true)
  })

  it('editing the query while results are shown calls onclear (host resets results)', () => {
    const { onclear } = renderBar({ value: 'q', results: { current: 2, total: 5 } })
    typeInto('qq')
    expect(onclear).toHaveBeenCalled()
  })

  it('Escape closes the popup and calls onclear', async () => {
    const { onclear } = renderBar({ value: 'q', results: { current: 2, total: 5 } })
    pressKey('Escape')
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()
    expect(onclear).toHaveBeenCalled()
  })

  it('closing the popup clears the field, hides it, and calls onclear', async () => {
    const { onclear } = renderBar({ value: 'q', results: { current: 2, total: 5 } })
    searchButton().click() // toggle closed
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()
    expect(onclear).toHaveBeenCalled()

    searchButton().click() // reopen — field starts empty
    await expect.element(page.getByPlaceholder('Search logs')).toBeInTheDocument()
    await expect.poll(() => input().value).toBe('')
  })

  it('the close (x) button hides the popup and calls onclear', async () => {
    const { onclear } = renderBar({ value: 'q', results: { current: 2, total: 5 } })
    closeButton().click()
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()
    expect(onclear).toHaveBeenCalled()
  })

  it('blurring an empty field closes the popup', async () => {
    renderBar({ value: '', results: null })
    input().dispatchEvent(new FocusEvent('blur', { bubbles: true }))
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()
  })

  it('blurring a non-empty field keeps the popup open', async () => {
    renderBar({ value: 'q', results: { current: 2, total: 5 } })
    input().dispatchEvent(new FocusEvent('blur', { bubbles: true }))
    await expect.element(page.getByPlaceholder('Search logs')).toBeInTheDocument()
  })
})
