/**
 * Real-wiring tests for the Logs-tab search experience in MonitoringPanel:
 *
 *   <input> (search bar in the Logs tab bar)
 *        ↳ logSearch ────▶ TabLogs ▶ LogsStreamList ▶ LogList (virtualised)
 *        ↳ onLogSearchShortcut ◀── Ctrl/Cmd-F handler in LogList
 *
 * The test mounts the production `MonitoringPanel` and feeds it 1 000 log lines (each line
 * is just its own 1-based line number — so a search for "42" deterministically hits lines
 * 42, 142, 242, ...) through a mocked `pipelineLogsStream`. Every component in the
 * search-input → LogList chain is the real one — nothing is re-wired in the test
 * file itself.
 */

import { afterEach, describe, expect, it, vi } from 'vitest'
import { page, userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '$lib/services/rbac'

// The Ad-Hoc Queries and Changes Stream tabs are gated on `exec:pipeline_data`.
// Default the mocked role to `write` so the log-search tests (which already hide
// those tabs) are unaffected; the gating tests below flip it to `read`. The
// `feldera` getter is read at render time, so setting `roleState.current` before
// each render selects the role under test.
const roleState = vi.hoisted(() => ({ current: 'write' as 'read' | 'write' | 'admin' | 'owner' }))
vi.mock('$app/state', () => ({
  page: {
    data: {
      get feldera() {
        return { role: roleState.current, permissions: permissionsOf(roleState.current) }
      }
    }
  }
}))

// --- Mock the pipeline manager's log-stream fetch ----------------------------
// Each call returns a fresh ReadableStream that emits all 1 000 lines as a single
// chunk and closes — exactly the shape the production code expects (it consumes
// the stream via `parseCancellable` and `SplitNewlineTransformStream`).

type FakeLogsStream = { stream: ReadableStream<Uint8Array>; cancel: () => void }

const pipelineLogsStreamMock = vi.fn<(...args: unknown[]) => Promise<FakeLogsStream>>()

vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({ pipelineLogsStream: pipelineLogsStreamMock })
}))

// Imported AFTER vi.mock so the mock takes effect.
import MonitoringPanel from './MonitoringPanel.svelte'

// --- Fixtures ----------------------------------------------------------------

const LOG_TEXT = Array.from({ length: 1000 }, (_, i) => `${i + 1}\n`).join('')
const encoder = new TextEncoder()

const buildFakeLogsStream = (): FakeLogsStream => ({
  stream: new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(LOG_TEXT))
      controller.close()
    }
  }),
  cancel: () => {}
})

// `streams` inside TabLogs is module-level and keyed by pipeline name. A unique
// name per test avoids state leaking from one render into the next.
let testCounter = 0
const nextPipelineName = () => `logsearch-test-${++testCounter}`

// The minimal `pipeline` prop MonitoringPanel needs to render the Logs tab: a name (used as
// the localStorage / log-stream key), a status, and an empty `compilerOutput` so the error
// extraction it runs on mount finds nothing to report.
const pipelineProp = (name: string) =>
  ({
    current: {
      name,
      status: 'Stopped',
      compilerOutput: {}
    }
  }) as any

const metricsProp = () => ({ current: {} }) as any

const HIDDEN_TABS = [
  'Errors',
  'Performance',
  'Ad-Hoc Queries',
  'Changes Stream',
  'Samply',
  'Health'
]

const ROW_MOUNT_TIMEOUT_MS = 2000

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

async function mountLogsTab() {
  pipelineLogsStreamMock.mockImplementation(async () => buildFakeLogsStream())

  // MonitoringPanel's elements size themselves to their parent (TailwindCSS `h-full`, i.e.
  // height: 100%); without a sized ancestor the LogList's scroll container collapses to
  // clientHeight=0 and virtua never mounts any rows. A flex column of fixed height gives the
  // same shape the real app provides via the page layout.
  mountTarget = document.createElement('div')
  mountTarget.style.cssText = 'height: 800px; width: 1200px; display: flex; flex-direction: column;'
  document.body.appendChild(mountTarget)

  mounted = render(MonitoringPanel, {
    target: mountTarget,
    props: {
      pipeline: pipelineProp(nextPipelineName()),
      metrics: metricsProp(),
      deleted: false,
      hiddenTabs: HIDDEN_TABS,
      currentTab: 'Logs'
    }
  } as any)

  // Wait until the first log row has been mounted by the virtualiser — proves the
  // streaming pipeline parsed → pushed → rendered the lines we enqueued.
  await expect
    .poll(() => document.querySelector('[data-rowindex]'), { timeout: ROW_MOUNT_TIMEOUT_MS })
    .toBeTruthy()
}

// data-rowindex on each line is its position in the rows array (zero-based).
// Line "N" is at row-index N-1.
async function expectRowMounted(rowIndex: number) {
  await expect
    .poll(() => document.querySelector(`[data-rowindex="${rowIndex}"]`), {
      timeout: ROW_MOUNT_TIMEOUT_MS
    })
    .toBeTruthy()
}

// --- Tests -------------------------------------------------------------------

describe('MonitoringPanel — log-search wiring', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    vi.clearAllMocks()
  })

  it('Enter on the search input scrolls to each "42" occurrence in order', async () => {
    await mountLogsTab()

    const input = page.getByPlaceholder('Search logs')
    await input.fill('42')
    // `fill` leaves the input focused; `userEvent.keyboard('{Enter}')` then sends Enter
    // to it, which fires the onkeydown handler that calls `submitLogSearch`.
    await userEvent.keyboard('{Enter}')
    // First substring match for "42" is line "42" → rows[41].
    await expectRowMounted(41)

    // Same pattern → advanceSearch bumps occurrenceIndex to 1 → line "142" → rows[141].
    await userEvent.keyboard('{Enter}')
    await expectRowMounted(141)

    // …and again → line "242" → rows[241].
    await userEvent.keyboard('{Enter}')
    await expectRowMounted(241)
  })

  it('Escape clears the input and removes the highlight', async () => {
    await mountLogsTab()

    const input = page.getByPlaceholder('Search logs')
    await input.fill('42')
    await userEvent.keyboard('{Enter}')
    await expectRowMounted(41)
    // The match is painted via the CSS Custom Highlight API under LogList's fixed name.
    await expect
      .poll(() => CSS.highlights.has('feldera-log-list-search'), { timeout: ROW_MOUNT_TIMEOUT_MS })
      .toBe(true)

    await userEvent.keyboard('{Escape}')
    expect((input.element() as HTMLInputElement).value).toBe('')
    await expect
      .poll(() => CSS.highlights.has('feldera-log-list-search'), { timeout: ROW_MOUNT_TIMEOUT_MS })
      .toBe(false)
  })

  it('Ctrl+F from the log list focuses the search input; typing + Enter searches', async () => {
    await mountLogsTab()

    const scrollContainer = document.querySelector<HTMLElement>('.log-list-scroll')
    expect(scrollContainer).toBeTruthy()
    scrollContainer!.focus()

    await userEvent.keyboard('{Control>}f{/Control}')

    // Focus is the user-visible cue: the handler `.focus()`es the input (browser focus ring)
    // and `.select()`s its text, so the user sees where their keystrokes will land.
    const input = page.getByPlaceholder('Search logs')
    expect(document.activeElement).toBe(input.element())

    // Type immediately after the shortcut — the now-focused input receives the keys.
    await userEvent.keyboard('100')
    await userEvent.keyboard('{Enter}')

    // First substring match for "100" is line "100" → rows[99].
    await expectRowMounted(99)
  })

  it('Cmd+F (Meta+F) from the log list also focuses the search input', async () => {
    await mountLogsTab()

    const scrollContainer = document.querySelector<HTMLElement>('.log-list-scroll')
    expect(scrollContainer).toBeTruthy()
    scrollContainer!.focus()

    await userEvent.keyboard('{Meta>}f{/Meta}')

    const input = page.getByPlaceholder('Search logs')
    expect(document.activeElement).toBe(input.element())

    await userEvent.keyboard('500')
    await userEvent.keyboard('{Enter}')

    // First substring match for "500" is line "500" → rows[499].
    await expectRowMounted(499)
  })
})

// --- exec:pipeline_data tab gating ------------------------------------------
//
// Ad-Hoc Queries and Changes Stream read/stream live pipeline data and must not
// be reachable without `exec:pipeline_data`. Their tab triggers are hidden, and
// a saved selection pointing at one of them is dropped to the first visible tab
// on init (so a `read` caller never lands on a blank panel).

// Mount MonitoringPanel with an explicit role and, optionally, a pre-seeded
// saved-tab in localStorage. Returns once the Logs tab has rendered a row, which
// proves a visible tab is active.
async function mountGatingPanel(opts: {
  role: 'read' | 'write'
  hiddenTabs: string[]
  currentTab: 'Logs' | null
  seedTab?: string
}) {
  roleState.current = opts.role
  pipelineLogsStreamMock.mockImplementation(async () => buildFakeLogsStream())

  const name = nextPipelineName()
  if (opts.seedTab) {
    localStorage.setItem(`pipelines/${name}/currentMonitoringTab`, JSON.stringify(opts.seedTab))
  }

  mountTarget = document.createElement('div')
  mountTarget.style.cssText = 'height: 800px; width: 1200px; display: flex; flex-direction: column;'
  document.body.appendChild(mountTarget)

  mounted = render(MonitoringPanel, {
    target: mountTarget,
    props: {
      pipeline: pipelineProp(name),
      // The Performance tab label counts connector problems, so `inputs`/`outputs`
      // must be iterable even when empty.
      metrics: { current: { inputs: [], outputs: [] } } as any,
      deleted: false,
      hiddenTabs: opts.hiddenTabs,
      currentTab: opts.currentTab
    }
  } as any)
}

const tabTexts = () =>
  Array.from(document.querySelectorAll('[role="tab"]')).map((t) => t.textContent ?? '')

describe('MonitoringPanel — exec:pipeline_data tab gating', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    localStorage.clear()
    roleState.current = 'write'
    vi.clearAllMocks()
  })

  it('shows the Ad-Hoc Queries and Changes Stream tabs for a write caller', async () => {
    await mountGatingPanel({ role: 'write', hiddenTabs: [], currentTab: 'Logs' })
    await expectRowMounted(0)

    const labels = tabTexts()
    expect(labels.some((t) => t.includes('Ad-Hoc'))).toBe(true)
    expect(labels.some((t) => t.includes('Change'))).toBe(true)
  })

  it('hides both tabs for a read-only caller', async () => {
    await mountGatingPanel({ role: 'read', hiddenTabs: [], currentTab: 'Logs' })
    await expectRowMounted(0)

    // Reverting the gate (dropping the exec:pipeline_data check in the tabs
    // filter) renders these triggers and fails this test.
    const labels = tabTexts()
    expect(labels.some((t) => t.includes('Ad-Hoc'))).toBe(false)
    expect(labels.some((t) => t.includes('Change'))).toBe(false)
  })

  it('switches away from a saved forbidden tab to the first visible tab', async () => {
    // A caller who once had access saved 'Ad-Hoc Queries'; on init as `read`, all
    // other tabs but Logs are hidden, so the panel must land on Logs, not blank.
    await mountGatingPanel({
      role: 'read',
      hiddenTabs: ['Errors', 'Performance', 'Samply', 'Health'],
      currentTab: null,
      seedTab: 'Ad-Hoc Queries'
    })

    // Logs rows render → the saved 'Ad-Hoc Queries' was replaced by a visible tab.
    await expectRowMounted(0)
    expect(tabTexts().some((t) => t.includes('Ad-Hoc'))).toBe(false)
  })
})
