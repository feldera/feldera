import { CountValue, type NodeAttributes, type TooltipRow } from 'profiler-lib'
import { tick } from 'svelte'
import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import { createLookupCoordinator } from '../functions/lookup'
import { setCollapsed } from './metrics/collapsedBlocks.svelte'

afterEach(() => {
  delete document.documentElement.dataset.theme
})
import MetricsView from './MetricsView.svelte'

const row = (metric: string): TooltipRow => ({
  metric,
  isCurrentMetric: false,
  cells: [{ value: new CountValue(1), percentile: 50 }]
})

// One node with two metrics of one category, so the view renders a single block.
const nodeAttributes: NodeAttributes = {
  title: 'n op',
  isRegion: false,
  nodeId: 'n',
  columns: [],
  rows: [row('alpha_count'), row('beta_count')],
  attributes: new Map()
}

// Legacy metric names, whose categories `profiler-lib` knows without loaded descriptions, so the
// view renders one block per category: CPU, memory, storage, cache in that order. The match below
// sits in the third, with a fourth block beneath it, so the panel can bring it to the very top.
const manyCategories: NodeAttributes = {
  ...nodeAttributes,
  nodeId: 'many',
  rows: [
    row('time'),
    row('invocations'),
    row('total size'),
    row('merging size'),
    row('foreground cache hit')
  ]
}

describe('MetricsView search', () => {
  it('expands a collapsed block that holds the match', async () => {
    const lookup = createLookupCoordinator()
    const { container } = render(MetricsView, {
      props: {
        mode: 'node',
        tooltipData: { nodeAttributes },
        rootNodeId: 'root',
        showAdvanced: true,
        lookup
      }
    })
    await tick()
    const block = container.querySelector<HTMLElement>('.metrics-block')!
    setCollapsed(block.dataset.blockId!, true)
    await expect.poll(() => block.querySelectorAll('.value-cell').length).toBe(0)

    // The match is a metric label; scrolling to a block that hides the row would show nothing.
    expect(lookup.execute('Metrics', 'Alpha count').total).toBe(1)
    await expect.poll(() => block.querySelectorAll('.value-cell').length).toBe(8)
  })

  it('scrolls the expanded block to the top of the panel', async () => {
    // A panel shorter than its content, so reaching the last block takes a real scroll.
    await import('../../routes/layout.css')
    await import('feldera-theme/feldera-modern.css')
    document.documentElement.dataset.theme = 'feldera-modern-theme'
    const lookup = createLookupCoordinator()
    const { container } = render(MetricsView, {
      props: {
        mode: 'node',
        tooltipData: { nodeAttributes: manyCategories },
        rootNodeId: 'root',
        showAdvanced: true,
        lookup
      }
    })
    container.style.position = 'relative'
    container.style.height = '100px'
    await tick()
    const panel = container.querySelector<HTMLElement>('.overflow-auto')!
    const blocks = Array.from(container.querySelectorAll<HTMLElement>('.metrics-block'))
    for (const block of blocks) {
      setCollapsed(block.dataset.blockId!, true)
    }
    const match = blocks[2]!
    await expect.poll(() => match.querySelectorAll('.value-cell').length).toBe(0)

    expect(lookup.execute('Metrics', 'Merging size').total).toBe(1)
    // Scrolling while the block was still collapsed would stop at the shorter content's end,
    // leaving the match below the fold.
    await expect
      .poll(() => Math.abs(match.getBoundingClientRect().top - panel.getBoundingClientRect().top))
      .toBeLessThan(4)
  })
})

describe('MetricsView global stats', () => {
  const globalMetrics = {
    state: 'Running',
    rss_bytes: 2048,
    transaction_msecs: null,
    transaction_initiators: { initiated_by_connectors: { kafka_input: { phase: 'Started' } } },
    future_metric: true
  }

  const rows = (container: HTMLElement) => {
    const tile = container.querySelector<HTMLElement>('[data-block-id="global-metrics"]')
    return Array.from(tile?.querySelectorAll('dt') ?? []).map((dt) => [
      dt.textContent?.trim(),
      dt.nextElementSibling?.textContent?.trim()
    ])
  }

  it('shows text, nested and unknown fields in the overview tile', async () => {
    const { container } = render(MetricsView, {
      props: {
        mode: 'overview',
        tooltipData: null,
        rootNodeId: 'root',
        globalMetrics,
        showAdvanced: false
      }
    })
    await tick()
    expect(rows(container)).toEqual([
      ['State', 'Running'],
      ['Memory (RSS)', '2KiB'],
      ['Transaction initiators', 'kafka_input (Started)'],
      ['future_metric', 'true']
    ])
  })

  it('finds a non-numeric field by its label', async () => {
    const lookup = createLookupCoordinator()
    render(MetricsView, {
      props: {
        mode: 'overview',
        tooltipData: null,
        rootNodeId: 'root',
        globalMetrics,
        showAdvanced: false,
        lookup
      }
    })
    await tick()
    expect(lookup.execute('Metrics', 'Transaction initiators').total).toBe(1)
  })

  it('hides the tile outside the overview', async () => {
    const { container } = render(MetricsView, {
      props: {
        mode: 'node',
        tooltipData: { nodeAttributes },
        rootNodeId: 'root',
        globalMetrics,
        showAdvanced: false
      }
    })
    await tick()
    expect(rows(container)).toEqual([])
  })
})
