/**
 * Real-wiring tests for the Logs-tab search experience in MonitoringPanel:
 *
 *   SearchBar (popup overlay, owned by TabLogs)
 *        ↳ logSearch ────▶ LogsStreamList ▶ LogList (virtualised)
 *        ↳ focusInput() ◀── Ctrl/Cmd-F handler in LogList
 *
 * The test mounts the production `MonitoringPanel` and feeds it 1 000 log lines (each line
 * is just its own 1-based line number — so a search for "42" deterministically hits lines
 * 42, 142, 242, ...) through a mocked `pipelineLogsStream`. Every component in the
 * SearchBar → LogList chain is the real one — nothing is re-wired in the test file itself.
 *
 * The search is a popup: collapsed it is just a search-icon button, so tests open it first
 * (click the button, or trigger Ctrl/Cmd-F which opens + focuses it).
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

type FakeLogsStream = {
  response: Response
  stream: ReadableStream<Uint8Array>
  cancel: () => void
}

const pipelineLogsStreamMock = vi.fn<(...args: unknown[]) => Promise<FakeLogsStream>>()

vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({ pipelineLogsStream: pipelineLogsStreamMock })
}))

// Imported AFTER vi.mock so the mock takes effect.
import MonitoringPanel from './MonitoringPanel.svelte'

// --- Fixtures ----------------------------------------------------------------

const LOG_TEXT = Array.from({ length: 1000 }, (_, i) => `${i + 1}\n`).join('')
const encoder = new TextEncoder()

// The response headers report where the server picked us up, which is what TabLogs reads
// to decide whether to keep or clear the rows already on screen. These describe a first
// connection: the start of the stream, with nothing lost along the way.
const buildFakeLogsStream = (): FakeLogsStream => ({
  response: new Response(null, {
    headers: {
      'feldera-logs-epoch': '0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08',
      'feldera-logs-seq': '0',
      'feldera-logs-gap': '0'
    }
  }),
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

// The search bar is collapsed to an icon button by default; open its popup and wait for the
// query input to appear.
async function openSearch() {
  ;(page.getByRole('button', { name: 'Search' }).element() as HTMLButtonElement).click()
  await expect.element(page.getByPlaceholder('Search logs')).toBeInTheDocument()
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
    await openSearch()

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

  it('editing the query after a search removes the highlight and disables nav', async () => {
    await mountLogsTab()
    await openSearch()

    const input = page.getByPlaceholder('Search logs')
    await input.fill('42')
    await userEvent.keyboard('{Enter}')
    await expectRowMounted(41)
    await expect.poll(() => CSS.highlights.has('feldera-log-list-search')).toBe(true)
    const next = page.getByRole('button', { name: 'Next match' })
    expect((next.element() as HTMLButtonElement).disabled).toBe(false)
    await expect.element(page.getByText(/\d+ of \d+/)).toBeInTheDocument()

    // Type more — the submitted results must be dropped as one: highlight gone, counter gone,
    // and nav disabled (single source of truth).
    await userEvent.keyboard('7')
    await expect.poll(() => CSS.highlights.has('feldera-log-list-search')).toBe(false)
    await expect.element(page.getByText(/\d+ of \d+/)).not.toBeInTheDocument()
    await expect.poll(() => (next.element() as HTMLButtonElement).disabled).toBe(true)
  })

  it('Escape closes the popup and removes the highlight', async () => {
    await mountLogsTab()
    await openSearch()

    const input = page.getByPlaceholder('Search logs')
    await input.fill('42')
    await userEvent.keyboard('{Enter}')
    await expectRowMounted(41)
    // The match is painted via the CSS Custom Highlight API under LogList's fixed name.
    await expect
      .poll(() => CSS.highlights.has('feldera-log-list-search'), { timeout: ROW_MOUNT_TIMEOUT_MS })
      .toBe(true)

    await userEvent.keyboard('{Escape}')
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()
    await expect.poll(() => CSS.highlights.has('feldera-log-list-search')).toBe(false)
  })

  it('Ctrl+F opens, Esc closes and returns focus to the container, Ctrl+F reopens', async () => {
    await mountLogsTab()

    // The reported flow: focus the log container, then drive the search entirely by keyboard.
    const scrollContainer = document.querySelector<HTMLElement>('.log-list-scroll')!
    scrollContainer.focus()

    // Ctrl+F opens the search (handled at the window level, so it works regardless of focus).
    await userEvent.keyboard('{Control>}f{/Control}')
    const input = page.getByPlaceholder('Search logs')
    expect(document.activeElement).toBe(input.element())

    await input.fill('42')
    await userEvent.keyboard('{Enter}')
    await expectRowMounted(41)

    // Esc closes the popup and hands focus back to the log container (so Arrow keys scroll it).
    await userEvent.keyboard('{Escape}')
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()
    expect(document.activeElement).toBe(scrollContainer)

    // Ctrl+F reopens — the whole point of the fix.
    await userEvent.keyboard('{Control>}f{/Control}')
    await expect.element(page.getByPlaceholder('Search logs')).toBeInTheDocument()
  })

  it('Ctrl+F reopens the search even when focus is not on the log container', async () => {
    // Regression guard for the Chrome failure: reopening must not depend on where focus sits. The
    // window-level shortcut handler makes Ctrl+F reach the search bar wherever focus landed (e.g.
    // on the toolbar trigger button after a mouse-open, which Chrome focuses on click).
    await mountLogsTab()

    // Open via the toolbar search button (real mouse click, so Chrome's focus-on-click applies).
    await userEvent.click(page.getByRole('button', { name: 'Search', exact: true }))
    const input = page.getByPlaceholder('Search logs')
    await expect.element(input).toBeInTheDocument()

    await input.fill('42')
    await userEvent.keyboard('{Enter}')
    await expectRowMounted(41)

    await userEvent.keyboard('{Escape}')
    await expect.element(page.getByPlaceholder('Search logs')).not.toBeInTheDocument()

    // Regardless of what holds focus now, Ctrl+F reopens the search.
    await userEvent.keyboard('{Control>}f{/Control}')
    await expect.element(page.getByPlaceholder('Search logs')).toBeInTheDocument()
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

// --- Log stream resume --------------------------------------------------------

const EPOCH = '0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08'

/**
 * A log stream that supports resuming: the position in the headers, the lines in the body.
 * The body holds log lines and nothing else, which is what lets the viewer take its next
 * position by counting what it received.
 */
const buildCursorLogsStream = (
  position: { epoch: string; seq: number; gap: number },
  lines: string[]
): FakeLogsStream => ({
  response: new Response(null, {
    headers: {
      'feldera-logs-epoch': position.epoch,
      'feldera-logs-seq': String(position.seq),
      'feldera-logs-gap': String(position.gap)
    }
  }),
  stream: new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(lines.map((l) => `${l}\n`).join('')))
      controller.close()
    }
  }),
  cancel: () => {}
})

/** A log stream from an older server: no position headers, just the lines. */
const buildPlainLogsStream = (lines: string[]): FakeLogsStream => ({
  response: new Response(null, { headers: { 'content-type': 'text/plain' } }),
  stream: new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(lines.map((l) => `${l}\n`).join('')))
      controller.close()
    }
  }),
  cancel: () => {}
})

/** Mounts the Logs tab for a specific pipeline, so a remount reuses its stream state. */
async function mountLogsTabFor(name: string) {
  mountTarget = document.createElement('div')
  mountTarget.style.cssText = 'height: 800px; width: 1200px; display: flex; flex-direction: column;'
  document.body.appendChild(mountTarget)

  mounted = render(MonitoringPanel, {
    target: mountTarget,
    props: {
      pipeline: pipelineProp(name),
      metrics: metricsProp(),
      deleted: false,
      hiddenTabs: HIDDEN_TABS,
      currentTab: 'Logs'
    }
  } as any)
}

async function unmountLogsTab() {
  await mounted?.unmount()
  mounted = undefined
  mountTarget?.remove()
  mountTarget = undefined
}

/** The `cursor` argument of the nth call to the log stream endpoint. */
const cursorOfCall = (n: number) => pipelineLogsStreamMock.mock.calls[n]?.[1]

const rowText = (index: number) =>
  document.querySelector(`[data-rowindex="${index}"]`)?.textContent?.trim()

/**
 * Waits for a row to show `text`. We have to poll rather than assert straight away: on a
 * remount the rows from before render first, and are only replaced once the new
 * connection delivers something. That delay is deliberate, so the view does not go blank
 * while a reconnect is in flight.
 */
const expectRowText = (index: number, text: string) =>
  expect.poll(() => rowText(index), { timeout: ROW_MOUNT_TIMEOUT_MS }).toBe(text)

/**
 * TabLogs keeps its streams in module state keyed by pipeline name, so they outlive the
 * component. Unmounting and remounting therefore stands in for a dropped connection, and
 * saves us waiting out the five second retry timer.
 */
describe('MonitoringPanel — log stream resume', () => {
  afterEach(async () => {
    await unmountLogsTab()
    vi.clearAllMocks()
  })

  it('asks to resume from where the previous connection stopped', async () => {
    const name = nextPipelineName()
    pipelineLogsStreamMock.mockImplementation(async () =>
      buildCursorLogsStream({ epoch: EPOCH, seq: 0, gap: 0 }, ['one', 'two', 'three'])
    )
    await mountLogsTabFor(name)
    await expectRowMounted(2)
    // Nothing has been read yet, so there is no position to send. The empty cursor is
    // still what asks the server to report where it picked us up.
    expect(cursorOfCall(0)).toBe('')

    await unmountLogsTab()
    pipelineLogsStreamMock.mockImplementation(async () =>
      buildCursorLogsStream({ epoch: EPOCH, seq: 3, gap: 0 }, ['four'])
    )
    await mountLogsTabFor(name)
    await expectRowMounted(3)

    // Three lines arrived, so we ask to carry on from three. Everything in the body is a
    // log line, so the count is simply how many rows we received.
    expect(cursorOfCall(1)).toBe(`${EPOCH}:3`)
    // The rows already on screen were kept and the new line appended after them.
    expect(rowText(0)).toBe('one')
    expect(rowText(3)).toBe('four')
  })

  it('starts over when the server restarted its log buffer', async () => {
    const name = nextPipelineName()
    pipelineLogsStreamMock.mockImplementation(async () =>
      buildCursorLogsStream({ epoch: EPOCH, seq: 0, gap: 0 }, ['one', 'two'])
    )
    await mountLogsTabFor(name)
    await expectRowMounted(1)

    await unmountLogsTab()
    // A new epoch means the counts refer to different lines. Keeping the old rows would
    // show two unrelated stretches of log as though they ran together.
    pipelineLogsStreamMock.mockImplementation(async () =>
      buildCursorLogsStream({ epoch: '0199d000-0000-7000-8000-000000000000', seq: 0, gap: 0 }, [
        'fresh'
      ])
    )
    await mountLogsTabFor(name)
    await expectRowText(0, 'fresh')

    expect(document.querySelector('[data-rowindex="1"]')).toBeNull()
  })

  it('reports discarded lines and starts over when the resume point is gone', async () => {
    const name = nextPipelineName()
    pipelineLogsStreamMock.mockImplementation(async () =>
      buildCursorLogsStream({ epoch: EPOCH, seq: 0, gap: 0 }, ['one', 'two'])
    )
    await mountLogsTabFor(name)
    await expectRowMounted(1)

    await unmountLogsTab()
    pipelineLogsStreamMock.mockImplementation(async () =>
      buildCursorLogsStream({ epoch: EPOCH, seq: 900, gap: 898 }, ['late'])
    )
    await mountLogsTabFor(name)
    await expectRowText(0, 'late')

    // The server used to print this as a log line. Now it only says so in a header, so
    // the console has to show it.
    await expect
      .poll(() => document.body.textContent, { timeout: ROW_MOUNT_TIMEOUT_MS })
      .toContain('898 earlier log lines are no longer available')
  })

  it('starts over after the server rejects the cursor', async () => {
    const name = nextPipelineName()
    pipelineLogsStreamMock.mockImplementation(async () =>
      buildCursorLogsStream({ epoch: EPOCH, seq: 0, gap: 0 }, ['one', 'two'])
    )
    await mountLogsTabFor(name)
    await expectRowMounted(1)

    // The server refuses the cursor. Keeping it would send the same rejected value on
    // every retry, so the viewer would never show another log line.
    await unmountLogsTab()
    pipelineLogsStreamMock.mockImplementation(
      async () =>
        new Error("Invalid log cursor 'x:1'", {
          cause: { response: { status: 400 } }
        }) as unknown as FakeLogsStream
    )
    await mountLogsTabFor(name)
    await expect
      .poll(() => document.body.textContent, { timeout: ROW_MOUNT_TIMEOUT_MS })
      .toContain('Invalid log cursor')

    await unmountLogsTab()
    pipelineLogsStreamMock.mockImplementation(async () =>
      buildCursorLogsStream({ epoch: EPOCH, seq: 0, gap: 0 }, ['fresh'])
    )
    await mountLogsTabFor(name)
    await expectRowText(0, 'fresh')

    expect(cursorOfCall(1)).toBe(`${EPOCH}:2`)
    expect(cursorOfCall(2)).toBe('')
  })

  it('replays from the beginning against a server that reports no position', async () => {
    const name = nextPipelineName()
    // An older server ignores the parameter and starts sending logs straight away, with no
    // headers saying where we are, so there is no cursor to keep.
    pipelineLogsStreamMock.mockImplementation(async () => buildPlainLogsStream(['a', 'b']))
    await mountLogsTabFor(name)
    await expectRowText(0, 'a')

    await unmountLogsTab()
    pipelineLogsStreamMock.mockImplementation(async () => buildPlainLogsStream(['c', 'd']))
    await mountLogsTabFor(name)
    await expectRowText(0, 'c')

    // With no position reported there is no cursor to send, and the new lines replace the
    // old rows rather than being added after them.
    expect(cursorOfCall(1)).toBe('')
    expect(document.querySelector('[data-rowindex="2"]')).toBeNull()
  })
})
