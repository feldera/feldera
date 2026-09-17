/**
 * Tests for the pipelines `Table`: the left-to-right column order, mouse
 * interaction with column sorting, the sort affordances each header paints, and
 * the borders that close off the last row.
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
import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
import type { PipelineThumb } from '$lib/services/pipelineManager'

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

  afterEach(async () => {
    // @vincjo/datatables' setRows() defers a scroll-position restore via
    // setTimeout(..., 2) that dereferences table.element. On unmount Svelte nulls
    // that binding, so a timer still in flight throws "Cannot set properties of
    // null (setting 'scrollTop')". vitest-browser-svelte unmounts after afterEach
    // runs, so waiting out the 2 ms window here lets the timer fire while the
    // component — and its element — is still alive.
    await new Promise((resolve) => setTimeout(resolve, 10))
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

  afterEach(async () => {
    // See the sorting suite's afterEach: wait out @vincjo/datatables' 2 ms
    // scroll-restore timer so it fires while the component is still mounted.
    await new Promise((resolve) => setTimeout(resolve, 10))
    localStorage.clear()
  })

  it('renders the columns left to right in the expected order', async () => {
    mountTable()

    // The leading cell is the select-all checkbox and carries no label. "Runtime
    // errors" holds both the short ("Errors") and long ("Runtime errors") span,
    // so its collapsed text is the concatenation of the two.
    await expect
      .poll(columnHeaders)
      .toEqual([
        '',
        'Pipeline name',
        'Storage',
        'Status',
        'Message',
        'Tags',
        'Runtime version',
        'Errors Runtime errors',
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

  afterEach(async () => {
    // See the sorting suite's afterEach: wait out @vincjo/datatables' 2 ms
    // scroll-restore timer so it fires while the component is still mounted.
    await new Promise((resolve) => setTimeout(resolve, 10))
    localStorage.clear()
  })

  // Every cell draws a top border; only the last row draws a bottom one, so the
  // widths below are read off the rendered CSS rather than off the class strings.
  const bottomBorderWidths = () =>
    Array.from(document.querySelectorAll('tbody tr[data-testid^="box-row-"]')).map((tr) =>
      Array.from(tr.querySelectorAll('td')).map((td) => getComputedStyle(td).borderBottomWidth)
    )

  it('closes off the table with a bottom border on the last row only', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    const widths = bottomBorderWidths()
    expect(
      widths
        .slice(0, -1)
        .flat()
        .every((width) => parseFloat(width) === 0)
    ).toBe(true)
    expect(widths.at(-1)!.every((width) => parseFloat(width) > 0)).toBe(true)
  })

  // The placeholder row is no `group`, so the `group-last:` variant that closes
  // off a populated table cannot reach it and Table spells its border out.
  it('closes off the placeholder row with a bottom border', async () => {
    render(Table, { props: { pipelines: [], selectedPipelines: [] } } as any)
    await expect.element(page.getByText('No pipelines found')).toBeVisible()

    const cells = Array.from(document.querySelectorAll('tbody tr td'))
    expect(cells.length).toBeGreaterThan(0)
    expect(cells.every((td) => parseFloat(getComputedStyle(td).borderBottomWidth) > 0)).toBe(true)
  })
})

describe('Table — sort affordances', () => {
  beforeEach(() => {
    localStorage.clear()
    useLayoutSettings().pipelinesTableSort.value = { column: 'name', direction: 'asc' }
  })

  afterEach(async () => {
    // See the sorting suite's afterEach: wait out @vincjo/datatables' 2 ms
    // scroll-restore timer so it fires while the component is still mounted.
    await new Promise((resolve) => setTimeout(resolve, 10))
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

  it('shows the arrow and the pill on the sorted column without a hover', async () => {
    mountTable()
    await expect.poll(rowOrder).toEqual(['alpha', 'bravo', 'charlie', 'delta'])

    expect(showsSortArrow('Pipeline name')).toBe(true)
    expect(painted(sortPill('Pipeline name'))).toBe(true)
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

  afterEach(async () => {
    // See the sorting suite's afterEach: wait out @vincjo/datatables' 2 ms
    // scroll-restore timer so it fires while the component is still mounted.
    await new Promise((resolve) => setTimeout(resolve, 10))
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

    expect(justifyOf('Status')).toBe('center')
    expect(justifyOf('Errors Runtime errors')).toBe('flex-end')
    expect(justifyOf('Pipeline name')).toBe('normal')
  })
})
