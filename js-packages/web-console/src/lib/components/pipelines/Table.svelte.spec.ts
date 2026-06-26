/**
 * Mouse interaction tests for column sorting in the pipelines `Table`.
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
    status: 'Stopped',
    storageStatus: 'Cleared',
    deploymentStatusSince: lastChange[name],
    programStatusSince: lastChange[name],
    deploymentError: undefined,
    platformVersion: PLATFORM_VERSION,
    programConfig: { runtime_version: null },
    deploymentResourcesStatus: 'Stopped',
    deploymentResourcesStatusSince: new Date(lastChange[name]),
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

// The sortable headers render their label inside the clickable <th>; the <th>
// gains the `active` class while it is the column the table is sorted by.
const header = (label: string) => page.getByText(label, { exact: true })
const headerCell = (label: string) => header(label).element().closest('th')!

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
    expect(headerCell('Pipeline name').classList.contains('active')).toBe(true)
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

    expect(headerCell('Status changed').classList.contains('active')).toBe(true)
    expect(headerCell('Pipeline name').classList.contains('active')).toBe(false)
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
    expect(headerCell('Pipeline name').classList.contains('active')).toBe(true)
  })
})
