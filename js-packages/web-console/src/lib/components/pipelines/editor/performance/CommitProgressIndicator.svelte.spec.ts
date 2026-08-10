import { describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
import type {
  CommitProgressSummary,
  ConcurrentBootstrapPhase,
  TransactionStatus
} from '$lib/services/manager'
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

/**
 * Height of the progress column - operator counts plus bar - of the transaction
 * row, which is the first row of the tile.
 */
const transactionProgressHeight = (container: HTMLElement) => {
  const transactionRow = container.firstElementChild!.firstElementChild!
  return transactionRow.children[1].getBoundingClientRect().height
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

/**
 * The row title, which also carries the detail below the `sm` breakpoint, so the
 * transaction row reads "Transaction ID:7" rather than "Transaction" there.
 */
const rowTitle = (label: string) => page.getByText(new RegExp(`^${label}\\b`))

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
      render(CommitProgressIndicator, {
        metrics: makeMetrics({
          transaction_status: 'CommitInProgress',
          transaction_id: 7,
          commit_progress: progress(3, 2, 5)
        })
      })

    it('shows the transaction status, its ID and its operator counts', async () => {
      await transacting()
      await expect.element(rowTitle('Transaction')).toBeInTheDocument()
      await expect.element(page.getByText('Committing')).toBeInTheDocument()
      await expect.element(page.getByText('ID:7')).toBeInTheDocument()
      await expect.element(page.getByText(/Completed\s*3\s*out of\s*10/)).toBeInTheDocument()
    })

    it('hides the bootstrapping row', async () => {
      await transacting()
      await expect.element(rowTitle('Bootstrapping')).not.toBeInTheDocument()
      // Only the transaction row's overlaid pair of bars is rendered.
      expect(page.getByRole('progressbar').elements()).toHaveLength(2)
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
      await expect.element(page.getByText(/Completed\s*1\s*out of\s*4/)).toBeInTheDocument()
    })

    it('keeps the transaction row in place, reporting no transaction', async () => {
      await bootstrapping()
      await expect.element(rowTitle('Transaction')).toBeInTheDocument()
      await expect.element(page.getByText('None')).toBeInTheDocument()
      await expect.element(page.getByText('ID:')).not.toBeInTheDocument()
      // Both rows keep their pair of bars, so neither shifts vertically.
      expect(page.getByRole('progressbar').elements()).toHaveLength(4)
      // Only the bootstrapping row reports operator counts.
      expect(page.getByTestId('box-label-completed').elements()).toHaveLength(1)
    })

    it('keeps the transaction bar in place when it reports no operator counts', async () => {
      const transacting = render(CommitProgressIndicator, {
        metrics: makeMetrics({
          transaction_status: 'CommitInProgress',
          transaction_id: 7,
          commit_progress: progress(3, 2, 5)
        })
      })
      const heightWithCounts = transactionProgressHeight(transacting.container)
      transacting.unmount()

      const idle = render(CommitProgressIndicator, {
        metrics: makeMetrics({ concurrent_bootstrap_phase: 'ConcurrentBootstrapping' })
      })
      // Dropping the placeholder for the hidden counts shortens the column,
      // shifting the bar up.
      expect(transactionProgressHeight(idle.container)).toBe(heightWithCounts)
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
