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
  await expect.poll(() => document.querySelector('[data-rowindex]')).toBeTruthy()
}

// data-rowindex on each line is its position in the rows array (zero-based).
// Line "N" is at row-index N-1.
async function expectRowMounted(rowIndex: number) {
  await expect.poll(() => document.querySelector(`[data-rowindex="${rowIndex}"]`)).toBeTruthy()
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
    await expect.poll(() => CSS.highlights.has('feldera-log-list-search')).toBe(true)

    await userEvent.keyboard('{Escape}')
    expect((input.element() as HTMLInputElement).value).toBe('')
    await expect.poll(() => CSS.highlights.has('feldera-log-list-search')).toBe(false)
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
