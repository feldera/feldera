import { beforeAll, describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineMetrics } from '#lib/functions/pipelineMetrics.ts'
import type {
  CommitProgressSummary,
  ConcurrentBootstrapPhase,
  TransactionStatus
} from '#lib/services/manager/index.ts'
import CommitProgressIndicator from './CommitProgressIndicator.svelte'

const progress = (completed: number, in_progress: number, remaining: number) =>
  ({
    completed,
    in_progress,
    remaining,
    in_progress_processed_records: 0,
    in_progress_total_records: 0
  }) satisfies CommitProgressSummary

function makeMetrics(global: {
  transaction_status?: TransactionStatus
  transaction_id?: number
  commit_progress?: CommitProgressSummary | null
  concurrent_bootstrap_phase?: ConcurrentBootstrapPhase
  concurrent_bootstrap_progress?: CommitProgressSummary | null
}): { current: PipelineMetrics } {
  return {
    current: {
      global: {
        transaction_status: 'NoTransaction',
        concurrent_bootstrap_phase: 'Inactive',
        ...global
      }
    } as PipelineMetrics
  }
}

/** The tile's rows, in document order. */
const rows = (container: HTMLElement) => [...container.firstElementChild!.children] as HTMLElement[]

/** Renders both rows inside a container of exactly `width`. */
const renderBothRows = (width: string) => {
  const result = render(CommitProgressIndicator, {
    metrics: makeMetrics({
      transaction_status: 'TransactionInProgress',
      transaction_id: 1,
      commit_progress: progress(3, 2, 5),
      concurrent_bootstrap_phase: 'ConcurrentBootstrapping',
      concurrent_bootstrap_progress: progress(1, 1, 2)
    })
  })
  result.container.style.width = width
  return rows(result.container)
}

const rem = () => parseFloat(getComputedStyle(document.documentElement).fontSize)

/** A row's natural width, from its `basis-150` and `max-w-150`. */
const rowMaxWidth = () => 37.5 * rem()

/** The gap between side-by-side rows, from the tile's `gap-x-8`. */
const rowGap = () => 2 * rem()

const rowTitle = (label: string) => page.getByText(label, { exact: true })

const commitBar = () => page.getByTestId('commit-progress-bar')

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const running = (transaction_id: number) =>
  makeMetrics({ transaction_status: 'TransactionInProgress', transaction_id })

const committing = (transaction_id: number, commit_progress: CommitProgressSummary) =>
  makeMetrics({ transaction_status: 'CommitInProgress', transaction_id, commit_progress })

describe('CommitProgressIndicator.svelte', () => {
  it('reports no transaction and no bootstrapping row while neither is in progress', async () => {
    const { container } = render(CommitProgressIndicator, { metrics: makeMetrics({}) })
    await expect.element(rowTitle('Transaction')).toBeInTheDocument()
    await expect.element(page.getByText('None')).toBeInTheDocument()
    await expect.element(rowTitle('Bootstrapping')).not.toBeInTheDocument()
    // The transaction row holds its place; only the bootstrapping row comes and goes.
    expect(rows(container)).toHaveLength(1)
  })

  describe('transaction only', () => {
    const transacting = () =>
      render(CommitProgressIndicator, { metrics: committing(7, progress(3, 2, 5)) })

    it('shows the transaction status and its ID', async () => {
      await transacting()
      await expect.element(rowTitle('Transaction')).toBeInTheDocument()
      await expect.element(page.getByText('Committing')).toBeInTheDocument()
      await expect.element(page.getByText('ID:7')).toBeInTheDocument()
    })

    it('counts the ID up to the new transaction', async () => {
      const { rerender } = await transacting()
      await rerender({ metrics: running(9) })
      await expect.element(page.getByText('ID:8'), { timeout: 3000 }).toBeInTheDocument()
      await expect.element(page.getByText('ID:9'), { timeout: 3000 }).toBeInTheDocument()
    })

    it('hides the ID when no transaction runs', async () => {
      const { rerender } = await transacting()
      await rerender({ metrics: makeMetrics({ transaction_id: 8 }) })
      await expect.element(page.getByText('None')).toBeInTheDocument()
      await expect.element(page.getByText('ID:', { exact: false })).not.toBeInTheDocument()
    })

    it('does not show the commit bar after one poll', async () => {
      await transacting()
      await expect.element(page.getByText('ID:7')).toBeInTheDocument()
      expect(commitBar().elements()).toHaveLength(0)
      expect(page.getByRole('progressbar').elements()).toHaveLength(0)
    })

    it('shows the commit bar and its counts for a long commit', async () => {
      const { rerender } = await transacting()
      // 1 of 10 operators in one poll period: the estimate is 20 s.
      await rerender({ metrics: committing(7, progress(4, 2, 4)) })
      await expect.element(commitBar()).toBeInTheDocument()
      await expect.element(page.getByText(/Completed ops\s*4\s*out of\s*10/)).toBeInTheDocument()
      expect(page.getByRole('progressbar').elements()).toHaveLength(2)
    })

    it('does not show the commit bar for a short commit', async () => {
      const { rerender } = await transacting()
      // 5 of 10 operators in one poll period: the estimate is 4 s.
      await rerender({ metrics: committing(7, progress(8, 0, 2)) })
      await expect.element(page.getByText('ID:7')).toBeInTheDocument()
      expect(commitBar().elements()).toHaveLength(0)
    })

    it('does not show the commit bar when the transaction changes between polls', async () => {
      const { rerender } = await transacting()
      await rerender({ metrics: committing(8, progress(3, 2, 5)) })
      await expect.element(page.getByText('ID:8')).toBeInTheDocument()
      expect(commitBar().elements()).toHaveLength(0)
    })

    it('keeps the commit bar until it is full, then hides it', async () => {
      const { rerender } = await transacting()
      await rerender({ metrics: committing(7, progress(4, 2, 4)) })
      await expect.element(commitBar()).toBeInTheDocument()
      await rerender({ metrics: makeMetrics({ transaction_id: 0 }) })
      // The bar of the commit that ended moves to 100% first.
      await expect.element(page.getByText('Committing')).toBeInTheDocument()
      await expect.element(commitBar()).toBeInTheDocument()

      await expect.element(page.getByText('None'), { timeout: 3000 }).toBeInTheDocument()
      await expect.element(commitBar(), { timeout: 3000 }).not.toBeInTheDocument()
    })

    it('keeps the height of the tile when the commit bar shows', async () => {
      const { container, rerender } = await transacting()
      // The tile slides in when it mounts.
      await new Promise((resolve) => setTimeout(resolve, 600))
      const tile = container.firstElementChild!
      const heightWithoutBar = tile.getBoundingClientRect().height
      await rerender({ metrics: committing(7, progress(4, 2, 4)) })
      await expect.element(commitBar()).toBeInTheDocument()
      // While the bar slides in, and after.
      expect(tile.getBoundingClientRect().height).toBe(heightWithoutBar)
      await new Promise((resolve) => setTimeout(resolve, 600))
      expect(tile.getBoundingClientRect().height).toBe(heightWithoutBar)
    })

    it('hides the bootstrapping row', async () => {
      await transacting()
      await expect.element(rowTitle('Bootstrapping')).not.toBeInTheDocument()
    })
  })

  describe('bootstrap only', () => {
    const bootstrapping = () =>
      render(CommitProgressIndicator, {
        metrics: makeMetrics({
          concurrent_bootstrap_phase: 'ConcurrentBootstrapping',
          concurrent_bootstrap_progress: progress(1, 1, 2)
        })
      })

    it('shows the bootstrapping status and its operator counts', async () => {
      await bootstrapping()
      await expect.element(rowTitle('Bootstrapping')).toBeInTheDocument()
      await expect.element(page.getByText('Backfilling')).toBeInTheDocument()
      await expect.element(page.getByText(/Completed ops\s*1\s*out of\s*4/)).toBeInTheDocument()
    })

    it('keeps the transaction row in place, reporting no transaction', async () => {
      await bootstrapping()
      await expect.element(rowTitle('Transaction')).toBeInTheDocument()
      await expect.element(page.getByText('None')).toBeInTheDocument()
      await expect.element(page.getByText('ID:')).not.toBeInTheDocument()
      // Only the bootstrapping row shows a pair of bars and operator counts.
      await expect.poll(() => page.getByRole('progressbar').elements()).toHaveLength(2)
      expect(page.getByTestId('box-label-completed').elements()).toHaveLength(1)
    })
  })

  it('flags the cutover pause while synchronizing', async () => {
    await render(CommitProgressIndicator, {
      metrics: makeMetrics({ concurrent_bootstrap_phase: 'Synchronizing' })
    })
    await expect.element(page.getByText('Synchronizing')).toBeInTheDocument()
  })

  it('shows both rows while a transaction and a bootstrap overlap', async () => {
    await render(CommitProgressIndicator, {
      metrics: makeMetrics({
        transaction_status: 'TransactionInProgress',
        transaction_id: 1,
        concurrent_bootstrap_phase: 'ConcurrentBootstrapping'
      })
    })
    await expect.element(page.getByText('Started')).toBeInTheDocument()
    await expect.element(page.getByText('Backfilling')).toBeInTheDocument()
  })

  describe('screenshots', () => {
    /** A short poll period, so that the bars get to their final widths quickly. */
    const PERIOD_MS = 400
    /** Waits until the bars have moved to the latest poll. */
    const settleBars = () => sleep(2 * PERIOD_MS + 100)
    // The browser is slow while the other test files run. Thus, the screenshot can take longer.
    const screenshotOptions = { timeout: 15_000 }
    // A desktop viewport, so that each row shows its label and its bar side by
    // side. The theme is the one of `src/app.html`. The style stops the CSS
    // animations, for example the endless spinner and the indeterminate bar,
    // and makes the bars jump to their widths. Thus, each screenshot is stable.
    beforeAll(async () => {
      await page.viewport(1280, 720)
      document.documentElement.dataset.theme = 'feldera-modern-theme'
      const style = document.createElement('style')
      style.textContent =
        '*, *::before, *::after { animation: none !important; transition: none !important; }'
      document.head.append(style)
      return () => {
        delete document.documentElement.dataset.theme
        style.remove()
      }
    })

    /** Operator counts where the in-progress operators have flushed half of their records. */
    const flushing = (completed: number, in_progress: number, remaining: number) => ({
      ...progress(completed, in_progress, remaining),
      in_progress_processed_records: 50,
      in_progress_total_records: 100
    })

    /** Renders the tile wide enough to show both rows side by side. */
    const renderTile = (metrics: { current: PipelineMetrics }) => {
      const result = render(CommitProgressIndicator, { metrics, pollPeriodMs: PERIOD_MS })
      result.container.style.width = `${2 * rowMaxWidth() + rowGap()}px`
      return result
    }
    const tile = (container: HTMLElement) => page.elementLocator(container.firstElementChild!)
    const row = (container: HTMLElement, index: number) =>
      page.elementLocator(rows(container)[index])

    it('shows an idle transaction row', async () => {
      const { container } = await renderTile(makeMetrics({}))
      await settleBars()
      await expect
        .element(row(container, 0))
        .toMatchScreenshot('transaction-idle', screenshotOptions)
    })

    it('shows a running transaction without the commit bar', async () => {
      const { container } = await renderTile(running(7))
      await settleBars()
      await expect
        .element(row(container, 0))
        .toMatchScreenshot('transaction-running', screenshotOptions)
    })

    it('shows the commit bar of a long commit', async () => {
      const { container, rerender } = await renderTile(committing(7, flushing(6, 2, 12)))
      // 1 of 20 operators in one poll period: the estimate is 8 s.
      await rerender({ metrics: committing(7, flushing(7, 2, 11)) })
      await expect.element(commitBar()).toBeInTheDocument()
      await settleBars()
      await expect
        .element(row(container, 0))
        .toMatchScreenshot('transaction-long-commit', screenshotOptions)
    })

    it('shows the bar of a bootstrap', async () => {
      const { container } = await renderTile(
        makeMetrics({
          concurrent_bootstrap_phase: 'ConcurrentBootstrapping',
          concurrent_bootstrap_progress: flushing(3, 2, 5)
        })
      )
      await settleBars()
      await expect
        .element(row(container, 1))
        .toMatchScreenshot('bootstrap-backfilling', screenshotOptions)
    })

    it('shows the bar of a bootstrap without counts while synchronizing', async () => {
      const { container } = await renderTile(
        makeMetrics({ concurrent_bootstrap_phase: 'Synchronizing' })
      )
      await settleBars()
      await expect
        .element(row(container, 1))
        .toMatchScreenshot('bootstrap-synchronizing', screenshotOptions)
    })

    it('shows the commit bar and the bootstrap bar side by side', async () => {
      const { container, rerender } = await renderTile(
        makeMetrics({
          transaction_status: 'CommitInProgress',
          transaction_id: 7,
          commit_progress: flushing(6, 2, 12),
          concurrent_bootstrap_phase: 'ConcurrentBootstrapping',
          concurrent_bootstrap_progress: flushing(3, 2, 5)
        })
      )
      await rerender({
        metrics: makeMetrics({
          transaction_status: 'CommitInProgress',
          transaction_id: 7,
          commit_progress: flushing(7, 2, 11),
          concurrent_bootstrap_phase: 'ConcurrentBootstrapping',
          concurrent_bootstrap_progress: flushing(3, 2, 5)
        })
      })
      // Both rows use the same test ID for their bars.
      await expect.poll(() => commitBar().elements()).toHaveLength(2)
      await settleBars()
      await expect.element(tile(container)).toMatchScreenshot('both-bars', screenshotOptions)
    })
  })

  describe('row layout', () => {
    it('puts both rows on one line when there is room for both', () => {
      const [transaction, bootstrap] = renderBothRows(`${2 * rowMaxWidth() + 4 * rem()}px`)
      expect(bootstrap.getBoundingClientRect().top).toBe(transaction.getBoundingClientRect().top)
    })

    it('separates side-by-side rows by gap-x-8', () => {
      const [transaction, bootstrap] = renderBothRows(`${2 * rowMaxWidth() + 8 * rem()}px`)
      const gap = bootstrap.getBoundingClientRect().left - transaction.getBoundingClientRect().right
      expect(gap).toBeCloseTo(rowGap(), 1)
    })

    it('wraps the second row when both no longer fit', () => {
      // One row short of the pair's combined width, so only the first fits.
      const [transaction, bootstrap] = renderBothRows(`${1.5 * rowMaxWidth()}px`)
      expect(bootstrap.getBoundingClientRect().top).toBeGreaterThanOrEqual(
        transaction.getBoundingClientRect().bottom
      )
    })

    it('keeps each row at its own max width rather than stretching', () => {
      const [transaction, bootstrap] = renderBothRows('4000px')
      expect(transaction.getBoundingClientRect().width).toBeCloseTo(rowMaxWidth(), 1)
      expect(bootstrap.getBoundingClientRect().width).toBeCloseTo(rowMaxWidth(), 1)
    })

    it('shrinks a row below its max width in a narrow container', () => {
      // The row only shrinks down to its own min-content width, since the
      // operator counts do not wrap; below that it overflows rather than clip.
      const [transaction] = renderBothRows(`${0.6 * rowMaxWidth()}px`)
      expect(transaction.getBoundingClientRect().width).toBeLessThan(rowMaxWidth())
    })
  })
})
