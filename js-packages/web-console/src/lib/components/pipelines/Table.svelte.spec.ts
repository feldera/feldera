/**
 * Tests for the pipelines `Table`: the left-to-right column order, mouse
 * interaction with column sorting, the sort affordances each header paints, the
 * borders that close off the last row, the sticky header, the search, column resizing,
 * and a clean teardown.
 *
 * The real pipelines `Table` is mounted with a handful of pipeline thumbs whose name order
 * and "status changed" order deliberately disagree, so every assertion about row
 * order can only pass if the click actually re-sorted the rows. The default sort
 * (name ascending) and its persistence both live in `useLayoutSettings`, so the
 * test also checks the localStorage key the component writes through.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { useLayoutSettings } from '#lib/compositions/layout/useLayoutSettings.svelte.ts'
import type { PipelineThumb } from '#lib/services/pipelineManager.ts'

const SORT_KEY = 'layout/pipelines/table/sort'

// `vi.mock` factories are hoisted above the imports and run before ordinary
// module-scope consts initialize, so a factory can't read them. `vi.hoisted` is
// hoisted too, so its result is available to both the factory and the fixtures.
const { PLATFORM_VERSION } = vi.hoisted(() => ({ PLATFORM_VERSION: '1.0.0' }))

// Table and its PipelineVersion child both read `page.data.feldera`. A `version`
// equal to every thumb's platformVersion keeps the runtime column in its
// "latest" state, so no update Popover is rendered.
vi.mock('$app/state', () => ({
  page: { data: { feldera: { version: PLATFORM_VERSION, unstableFeatures: [] } } }
}))

import Table from './Table.svelte'

// `lastStatusSince` is derived from these timestamps. The date order
// (bravo < delta < charlie < alpha) is a rotation of the name order
// (alpha < bravo < charlie < delta), so sorting by either column yields a
// distinct, unambiguous row sequence.
const lastChange: Record<string, string> = {
  alpha: '2024-01-04T00:00:00Z',
  bravo: '2024-01-01T00:00:00Z',
  charlie: '2024-01-03T00:00:00Z',
  delta: '2024-01-02T00:00:00Z'
}

// Only the fields the Table template reads are populated; the rest of
// PipelineThumb is irrelevant to sorting, hence the cast.
const thumb = (name: string): PipelineThumb =>
  ({
    name,
    description: '',
    tags: [],
    status: 'Stopped',
    storageStatus: 'Cleared',
    deploymentStatusSince: lastChange[name],
    programStatusSince: lastChange[name],
    deploymentError: undefined,
    platformVersion: PLATFORM_VERSION,
    programConfig: { runtime_version: null },
    deploymentResourcesStatus: 'Stopped',
    deploymentResourcesStatusSince: new Date(lastChange[name]),
    deploymentRuntimeStatusDetails: { connector_stats: { num_errors: 0 } },
    connectors: { numErrors: 0 }
  }) as unknown as PipelineThumb

// Fed deliberately unsorted so a passing default-order assertion proves the
// table sorted them rather than echoing the input order.
const pipelines = [thumb('charlie'), thumb('alpha'), thumb('delta'), thumb('bravo')]

// Each rendered row carries data-testid="box-row-<name>"; reading them back in
// DOM order gives the visible row sequence.
const rowOrder = () =>
  Array.from(document.querySelectorAll('tbody tr[data-testid^="box-row-"]')).map((tr) =>
    tr.getAttribute('data-testid')!.slice('box-row-'.length)
  )

// The sortable headers render their label inside the clickable <th>.
const header = (label: string) => page.getByText(label, { exact: true })
const headerCell = (label: string) => header(label).element().closest('th')!

// ThSort groups the label and the sort arrow in a pill, the second <div> of the
// header cell; the arrow is that pill's last child. The sorted column shows its
// arrow unconditionally, every other column only while the pointer is on it, so
// a header must be left unhovered for its arrow to mark the sort.
const sortPill = (label: string) => headerCell(label).querySelectorAll('div')[1] as HTMLElement
const sortArrow = (label: string) => sortPill(label).lastElementChild as HTMLElement
const showsSortArrow = (label: string) =>
  getComputedStyle(sortArrow(label)).visibility === 'visible'
const arrowDirection = (label: string) =>
  sortArrow(label).classList.contains('fd-arrow-down') ? 'desc' : 'asc'

// The visible column order, read left to right off the header row. Each cell's
// text is whitespace-collapsed so responsive label variants (e.g. the short and
// long "Runtime errors" spans, both present in the DOM) compare deterministically.
const columnHeaders = () =>
  Array.from(document.querySelectorAll('thead th')).map((th) =>
    th.textContent!.replace(/\s+/g, ' ').trim()
  )

const persistedSort = () => JSON.parse(localStorage.getItem(SORT_KEY)!)

const mountTable = () => render(Table, { props: { pipelines, selectedPipelines: [] } } as any)

describe('Table — column sorting', () => {
  beforeEach(() => {
    // The persisted sort is a process-wide singleton (useLocalStorage caches by
    // key), so reset both the backing store and the cached value before each test
    // to keep them independent.
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
  })

  afterEach(() => {
    localStorage.clear()
  })

  it('sorts by pipeline name ascending by default', async () => {
    mountTable()

    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    expect(showsSortArrow('Pipeline name')).toBe(true)
  })

  it('clicking the name header toggles ascending → descending → ascending', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    await header('Pipeline name').click()
    await expect.poll(rowOrder).toEqual(['delta', 'charlie', 'bravo', 'alpha'])

    await header('Pipeline name').click()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
  })

  it('clicking a different header sorts by that column and moves the active marker', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    // "Status changed" sorts by lastStatusSince, ascending on first click.
    await header('Status changed').click()
    await expect.poll(rowOrder).toEqual(['bravo', 'delta', 'charlie', 'alpha'])

    // The click leaves the pointer on "Status changed", whose arrow a hover alone
    // would reveal, so park it on a third header before reading the marker.
    await header('Deployed on').hover()
    expect(showsSortArrow('Status changed')).toBe(true)
    expect(showsSortArrow('Pipeline name')).toBe(false)
  })

  it('persists the active sort to localStorage', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    await header('Pipeline name').click()
    await expect.poll(persistedSort).toEqual({ column: 'name', direction: 'desc' })

    await header('Status changed').click()
    await expect.poll(persistedSort).toEqual({ column: 'lastStatusSince', direction: 'asc' })
  })

  it('restores a persisted non-default sort on mount', async () => {
    // Simulate a returning user whose last sort was name descending.
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'desc' }

    mountTable()

    await expect.poll(rowOrder).toEqual(['delta', 'charlie', 'bravo', 'alpha'])
    expect(showsSortArrow('Pipeline name')).toBe(true)
  })
})

describe('Table — column order', () => {
  beforeEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
  })

  afterEach(() => {
    localStorage.clear()
  })

  it('renders the columns left to right in the expected order', async () => {
    mountTable()

    // The leading cell is the select-all checkbox and carries no label.
    await expect
      .poll(columnHeaders)
      .toEqual([
        '',
        'Pipeline name',
        'Storage',
        'Status',
        'Message',
        'Tags',
        'Runtime',
        'Errors',
        'Status changed',
        'Deployed on'
      ])
  })
})

describe('Table — row borders', () => {
  beforeEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
  })

  afterEach(() => {
    localStorage.clear()
  })

  // Every cell draws the line above it, and the table's own border closes off the last
  // row, so no row draws a bottom border. Widths are read off the rendered CSS.
  const bottomBorderWidths = () =>
    Array.from(document.querySelectorAll('tbody tr')).flatMap((tr) =>
      Array.from(tr.querySelectorAll('td')).map((td) => getComputedStyle(td).borderBottomWidth)
    )

  it('closes off the table with its own border rather than a last-row border', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    expect(bottomBorderWidths().every((width) => parseFloat(width) === 0)).toBe(true)
    expect(
      parseFloat(getComputedStyle(document.querySelector('table')!).borderBottomWidth)
    ).toBeGreaterThan(0)
  })

  it('closes off the placeholder row the same way', async () => {
    render(Table, { props: { pipelines: [], selectedPipelines: [] } } as any)
    await expect.element(page.getByText('No pipelines found')).toBeVisible()

    expect(bottomBorderWidths().length).toBeGreaterThan(0)
    expect(bottomBorderWidths().every((width) => parseFloat(width) === 0)).toBe(true)
    expect(
      parseFloat(getComputedStyle(document.querySelector('table')!).borderBottomWidth)
    ).toBeGreaterThan(0)
  })
})

describe('Table — sort affordances', () => {
  beforeEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
  })

  afterEach(() => {
    localStorage.clear()
  })

  // ThSort paints the pill through a `group-hover/sort:` variant, so it is read
  // back off the rendered CSS under a real hover.
  const painted = (el: HTMLElement) => getComputedStyle(el).backgroundColor !== 'rgba(0, 0, 0, 0)'

  it('reveals the arrow and the pill on an unsorted column only while it is hovered', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    expect(showsSortArrow('Deployed on')).toBe(false)
    expect(painted(sortPill('Deployed on'))).toBe(false)

    await header('Deployed on').hover()

    await expect.poll(() => showsSortArrow('Deployed on')).toBe(true)
    expect(painted(sortPill('Deployed on'))).toBe(true)
  })

  it('shows only the arrow on the sorted column without a hover', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    expect(showsSortArrow('Pipeline name')).toBe(true)
    expect(painted(sortPill('Pipeline name'))).toBe(false)
  })

  it('points the arrow the way the column is sorted', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    expect(arrowDirection('Pipeline name')).toBe('asc')

    await header('Pipeline name').click()
    await expect.poll(rowOrder).toEqual(['delta', 'charlie', 'bravo', 'alpha'])
    expect(arrowDirection('Pipeline name')).toBe('desc')

    await header('Pipeline name').click()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    expect(arrowDirection('Pipeline name')).toBe('asc')
  })
})

describe('Table — header alignment', () => {
  beforeEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
  })

  afterEach(() => {
    localStorage.clear()
  })

  // A sortable header lays its label out in a flex row, so the `justify-*` the
  // column passes to ThSort only takes effect if it reaches that row. Headers are
  // matched on their whitespace-collapsed text, as in the column-order suite.
  const justifyOf = (text: string) => {
    const th = Array.from(document.querySelectorAll('thead th')).find(
      (cell) => cell.textContent!.replace(/\s+/g, ' ').trim() === text
    )!
    return getComputedStyle(th.querySelector('div')!).justifyContent
  }

  it('passes each column justify-* down to the row holding the label', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    // Status and Errors are left-aligned so their values line up with the labels above them.
    expect(justifyOf('Status')).toBe('normal')
    expect(justifyOf('Errors')).toBe('normal')
    expect(justifyOf('Pipeline name')).toBe('normal')
  })
})

describe('Table — sticky header', () => {
  beforeEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
  })

  afterEach(() => {
    localStorage.clear()
  })

  it('keeps the column headers on screen after scrolling down', async () => {
    const manyPipelines = Array.from({ length: 60 }, (_, i) => ({
      ...thumb('alpha'),
      name: `pipeline-${String(i).padStart(2, '0')}`
    }))
    const { container } = render(Table, {
      props: { pipelines: manyPipelines, selectedPipelines: [] }
    } as any)
    // The container stands in for the page: the one scroll container above the table.
    container.style.height = '400px'
    container.style.overflowY = 'auto'
    await expect.poll(rowOrder).toHaveLength(manyPipelines.length)
    expect(container.scrollHeight).toBeGreaterThan(container.clientHeight)

    container.scrollTop = container.scrollHeight
    await new Promise((painted) => requestAnimationFrame(() => requestAnimationFrame(painted)))

    const viewport = container.getBoundingClientRect()
    const firstRow = document.querySelector('tbody tr')!.getBoundingClientRect()
    // The first row has scrolled out of view, so the headers are not simply where
    // they started.
    expect(firstRow.bottom).toBeLessThan(viewport.top)
    // Visible means the header, not a row scrolled underneath it, is the topmost
    // element at its position.
    const nameHeader = headerCell('Pipeline name')
    const box = nameHeader.getBoundingClientRect()
    expect(box.top).toBeGreaterThanOrEqual(viewport.top)
    expect(box.bottom).toBeLessThanOrEqual(viewport.bottom)
    const hit = document.elementFromPoint(box.left + box.width / 2, box.top + box.height / 2)
    expect(nameHeader.contains(hit)).toBe(true)
  })
})

describe('Table — search', () => {
  beforeEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
  })

  afterEach(() => {
    localStorage.clear()
  })

  it('matches a pipeline by its name or by any of its tags', async () => {
    render(Table, {
      props: {
        pipelines: [
          { ...thumb('alpha'), tags: ['Finance'] },
          { ...thumb('bravo'), tags: ['ops', 'finance-eu'] },
          thumb('charlie')
        ],
        selectedPipelines: []
      }
    } as any)
    const search = page.getByTestId('input-pipeline-search')

    await search.fill('finance')
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo'])

    await search.fill('char')
    await expect.poll(rowOrder).toEqual(['charlie'])
  })
})

describe('Table — teardown', () => {
  it('leaves no timer that writes to the table after unmount', async () => {
    const errors: string[] = []
    const recordError = (event: ErrorEvent) => errors.push(event.message)
    window.addEventListener('error', recordError)
    try {
      const { rerender, unmount } = mountTable()
      await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

      // New rows go through `TableHandler.setRows`, and unmounting straight after
      // leaves any timer it scheduled to fire on a destroyed table.
      await rerender({ pipelines: [...pipelines, thumb('echo')], selectedPipelines: [] })
      unmount()
      await new Promise((resolve) => setTimeout(resolve, 20))

      expect(errors).toEqual([])
    } finally {
      window.removeEventListener('error', recordError)
    }
  })
})

describe('Table — column resizing', () => {
  const WIDTHS_KEY = 'layout/pipelines/table/columnWidths'

  beforeEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
    useLayoutSettings().pipelinesTableColumnWidths.value = {}
  })

  afterEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableColumnWidths.value = {}
  })

  const handle = (column: string) =>
    page.getByRole('separator', { name: `Resize ${column} column` }).element() as HTMLElement
  const widthOf = (column: string) => headerCell(column).getBoundingClientRect().width

  // A mouse drag of the handle by `dx` px: down, one move, up.
  const drag = (column: string, dx: number) => {
    const target = handle(column)
    const { left } = target.getBoundingClientRect()
    const pointer = { pointerId: 1, button: 0, bubbles: true }
    target.dispatchEvent(new PointerEvent('pointerdown', { ...pointer, clientX: left }))
    target.dispatchEvent(new PointerEvent('pointermove', { ...pointer, clientX: left + dx }))
    target.dispatchEvent(new PointerEvent('pointerup', { ...pointer, clientX: left + dx }))
    target.dispatchEvent(new MouseEvent('click', { bubbles: true }))
  }

  it('widens a column by the distance dragged, without sorting it', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    const before = widthOf('Status changed')

    drag('Status changed', 60)

    await expect.poll(() => widthOf('Status changed')).toBeCloseTo(before + 60, 0)
    expect(rowOrder()).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    expect(persistedSort()).toEqual({ column: 'name', direction: 'asc' })
  })

  it('keeps a resized width across a remount', async () => {
    const { unmount } = mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    const resized = widthOf('Pipeline name') + 40
    drag('Pipeline name', 40)
    await expect.poll(() => widthOf('Pipeline name')).toBeCloseTo(resized, 0)
    expect(JSON.parse(localStorage.getItem(WIDTHS_KEY)!)).toEqual({ name: resized })

    unmount()
    mountTable()
    await expect.poll(() => widthOf('Pipeline name')).toBeCloseTo(resized, 0)
  })

  it('stops at the minimum width', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    drag('Tags', -1000)

    await expect.poll(() => widthOf('Tags')).toBeCloseTo(64, 0)
  })

  it('returns a column to its default width on double-click', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    const initial = widthOf('Runtime')
    drag('Runtime', 50)
    await expect.poll(() => widthOf('Runtime')).toBeCloseTo(initial + 50, 0)

    handle('Runtime').dispatchEvent(new MouseEvent('dblclick', { bubbles: true }))

    await expect.poll(() => widthOf('Runtime')).toBeCloseTo(initial, 0)
    expect(JSON.parse(localStorage.getItem(WIDTHS_KEY)!)).toEqual({})
  })

  it('gives every column but the checkbox one a resize handle', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    expect(
      Array.from(document.querySelectorAll('[role="separator"]')).map((el) =>
        el.getAttribute('aria-label')
      )
    ).toEqual(
      [
        'Pipeline name',
        'Storage',
        'Status',
        'Message',
        'Tags',
        'Runtime',
        'Errors',
        'Status changed',
        'Deployed on'
      ].map((column) => `Resize ${column} column`)
    )
  })

  it('lets Message fill the leftover room until it is resized, then keeps its width', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    const table = document.querySelector('table')!
    const fillingTable = table.getBoundingClientRect().width
    const filled = widthOf('Message')
    const others = () =>
      Array.from(document.querySelectorAll('thead th'))
        .map((th) => th.getBoundingClientRect().width)
        .filter((_, i) => i !== 4)
    const othersBefore = others()

    // Narrower than the "Message" label, which then truncates rather than holding the column open.
    drag('Message', -50)

    await expect.poll(() => widthOf('Message')).toBeCloseTo(filled - 50, 0)
    // The other columns keep their widths; the room Message gave up is left empty.
    expect(others()).toEqual(othersBefore)
    expect(table.getBoundingClientRect().width).toBeCloseTo(fillingTable - 50, 0)

    handle('Message').dispatchEvent(new MouseEvent('dblclick', { bubbles: true }))
    await expect.poll(() => widthOf('Message')).toBeCloseTo(filled, 0)
  })

  it('resizes with the arrow keys', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])
    const initial = widthOf('Errors')
    const separator = handle('Errors')
    separator.focus()

    separator.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowRight', bubbles: true }))
    await expect.poll(() => widthOf('Errors')).toBeCloseTo(initial + 16, 0)
    separator.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowLeft', bubbles: true }))
    await expect.poll(() => widthOf('Errors')).toBeCloseTo(initial, 0)
    expect(separator.getAttribute('aria-valuenow')).toBe(String(initial))
  })
})
