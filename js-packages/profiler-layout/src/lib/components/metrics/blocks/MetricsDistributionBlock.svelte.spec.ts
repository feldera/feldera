/**
 * The Total column shows a metric's readings added up across the node's workers, and only for
 * metrics that add up. `profiler-lib` decides that: it fills `TooltipRow.total` for counts, byte
 * sizes and durations and leaves it absent for rates, reported minima and maxima, flags and
 * settings, where a total states nothing. This test pins both halves of the contract - the value
 * appears when the row carries one, the cell stays blank when it does not - and the header's
 * column count, which has to stay in step with the grid template.
 */

import { CountValue, PercentValue, type TooltipRow } from 'profiler-lib'
import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import { setCollapsed } from '../collapsedBlocks.svelte'
import type { RenderableMetric } from '../dispatch'
import MetricsDistributionBlock from './MetricsDistributionBlock.svelte'

/** Load the real stylesheets, for the tests that read the colors and sizes the browser resolved.
 * The tests that read a declared style run without them: a loaded theme reports a transparent
 * background as `oklab(0 0 0 / 0)` rather than `rgba(0, 0, 0, 0)`. */
const loadTheme = async (theme: 'light' | 'dark' = 'light') => {
  await import('../../../../routes/layout.css')
  await import('feldera-theme/feldera-modern.css')
  document.documentElement.dataset.theme = 'feldera-modern-theme'
  document.documentElement.classList.toggle('dark', theme === 'dark')
}

afterEach(() => {
  document.documentElement.classList.remove('dark')
  delete document.documentElement.dataset.theme
})

const cells = (values: Array<CountValue | PercentValue>) =>
  values.map((value) => ({ value, percentile: 50 }))

const entry = (
  label: string,
  row: Partial<TooltipRow> & Pick<TooltipRow, 'cells'>
): RenderableMetric => ({
  label,
  row: { metric: label, isCurrentMetric: false, ...row }
})

describe('MetricsDistributionBlock total column', () => {
  const adds = entry('records', {
    cells: cells([new CountValue(10), new CountValue(30)]),
    // Two workers holding 10 and 30 records: the node holds 40, and that is the most any node
    // holds, so its cell saturates in the per-node view.
    total: { value: new CountValue(40), percentile: 100 }
  })
  const doesNotAdd = entry('hit_rate', {
    cells: cells([new PercentValue(1, 2), new PercentValue(1, 4)])
  })

  const texts = (container: HTMLElement) =>
    Array.from(container.querySelectorAll('.value-cell')).map((c) => c.textContent?.trim())

  it('heads the four statistics columns', async () => {
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'State', entries: [adds] }
    })
    const headers = Array.from(container.querySelectorAll('.sticky')).map((h) =>
      h.textContent?.trim()
    )
    expect(headers).toContain('Avg')
    expect(headers).toContain('Min')
    expect(headers).toContain('Max')
    expect(headers).toContain('Total')
  })

  it('shows the total for a metric that adds up', async () => {
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'State', entries: [adds] }
    })
    // Avg, Min, Max, Total.
    expect(texts(container)).toEqual(['20', '10', '30', '40'])
  })

  it('leaves the total blank for a metric that does not add up', async () => {
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'Cache', entries: [doesNotAdd] }
    })
    const shown = texts(container)
    expect(shown).toHaveLength(4)
    // Adding two rates would print an impossible 75%; the cell stays empty instead.
    expect(shown[3]).toBe('')
  })

  // The fourth cell of each row is the total. Read the declared background rather than the
  // resolved color: the mix is expressed in theme tokens, which this page does not load.
  const backgrounds = (container: HTMLElement) =>
    Array.from(container.querySelectorAll('.value-cell'))
      .filter((_, i) => i % 4 === 3)
      .map((c) => (c as HTMLElement).style.backgroundColor)

  // Four totals spanning orders of magnitude, as a block routinely holds: a byte count in the
  // billions beside a batch count in the hundreds.
  const spread = [
    entry('bytes', {
      cells: cells([new CountValue(1)]),
      total: { value: new CountValue(13_260_000_000), percentile: 100 }
    }),
    entry('records', {
      cells: cells([new CountValue(1)]),
      total: { value: new CountValue(5_930_000), percentile: 4 }
    }),
    entry('batches', {
      cells: cells([new CountValue(1)]),
      total: { value: new CountValue(4810), percentile: 1 }
    }),
    entry('hits', {
      cells: cells([new CountValue(1)]),
      total: { value: new CountValue(366), percentile: 0.2 }
    }),
    doesNotAdd
  ]

  // The standing itself is `profiler-lib`'s business (see `totalShare` and `categoryShares`
  // there); the block's job is to paint it and to leave a row without a total unpainted.
  it('paints each total at the standing it was given', async () => {
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'State', entries: spread }
    })
    const shown = backgrounds(container)
    expect(shown[0]).toContain('--bar-high) 100.00%')
    expect(shown[1]).toContain('--bar-high) 4.00%')
    expect(shown[3]).toContain('--bar-high) 0.20%')
    // Distinct standings stay distinguishable rather than collapsing to one shade.
    expect(new Set(shown.slice(0, 4)).size).toBe(4)
    expect(shown[4]).toBe('transparent')
  })

  it('paints nothing behind a total of no standing', async () => {
    // The fill starts at transparent rather than at a floor color: a grey cell would read as a
    // value where there is none.
    const none = entry('idle', {
      cells: cells([new CountValue(0)]),
      total: { value: new CountValue(0), percentile: 0 }
    })
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'State', entries: [none] }
    })
    const cell = container.querySelectorAll('.value-cell')[3] as HTMLElement
    expect(cell.style.backgroundColor).toContain('transparent 100.00%')
    expect(cell.style.backgroundColor).toContain('--bar-high) 0.00%')
    expect(getComputedStyle(cell).backgroundColor).toBe('rgba(0, 0, 0, 0)')
  })

  const textColors = (container: HTMLElement) =>
    Array.from(container.querySelectorAll('.value-cell'))
      .filter((_, i) => i % 4 === 3)
      .map((c) => (c as HTMLElement).style.color)

  // Past 70% of the way to `--bar-high` the fill is dark enough that the default dark text
  // disappears into it, so the text turns white. Below that the cell keeps its inherited color.
  it('turns a deeply colored total white', async () => {
    const straddling = [
      entry('above', {
        cells: cells([new CountValue(1)]),
        total: { value: new CountValue(9), percentile: 71 }
      }),
      entry('at', {
        cells: cells([new CountValue(1)]),
        total: { value: new CountValue(7), percentile: 70 }
      }),
      entry('below', {
        cells: cells([new CountValue(1)]),
        total: { value: new CountValue(5), percentile: 69 }
      }),
      doesNotAdd
    ]
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'State', entries: straddling }
    })
    const shown = textColors(container)
    expect(shown[0]).toBe('white')
    // The threshold is exclusive: 70% is still light enough to read dark text against.
    expect(shown[1]).toBe('')
    expect(shown[2]).toBe('')
    // A row with no total has no fill to read against, so it is left alone.
    expect(shown[3]).toBe('')
  })

  it('keeps each row independent of the others', async () => {
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'Mixed', entries: [adds, doesNotAdd] }
    })
    // The rate's Avg is the pooled 2/6, not the mean of the two rates.
    expect(texts(container)).toEqual(['20', '10', '30', '40', '33.3%', '25.0%', '50.0%', ''])
  })

  // Issue 6993: a category the user collapsed stays collapsed from node to node, so the metrics
  // of interest need no scrolling to reach.
  describe('collapsing', () => {
    const titleButton = (container: HTMLElement) =>
      container.querySelector<HTMLButtonElement>('h3 button')!
    const valueCells = (container: HTMLElement) => container.querySelectorAll('.value-cell').length

    it('collapses to its title on a click and expands on the next', async () => {
      const { container } = render(MetricsDistributionBlock, {
        props: { id: 'collapse-toggle', title: 'State', entries: [adds, doesNotAdd] }
      })
      titleButton(container).click()
      await expect.poll(() => valueCells(container)).toBe(0)
      expect(container.textContent).toContain('State')
      expect(container.textContent).not.toContain('Avg')
      expect(titleButton(container).getAttribute('aria-expanded')).toBe('false')

      titleButton(container).click()
      await expect.poll(() => valueCells(container)).toBe(8)
      expect(titleButton(container).getAttribute('aria-expanded')).toBe('true')
    })

    it('remembers the choice for the category, not for the block instance', async () => {
      // Switching nodes rebuilds the blocks with new entries.
      const first = render(MetricsDistributionBlock, {
        props: { id: 'collapse-remembered', title: 'Time', entries: [adds] }
      })
      titleButton(first.container).click()
      await expect.poll(() => valueCells(first.container)).toBe(0)
      await first.unmount()

      const again = render(MetricsDistributionBlock, {
        props: { id: 'collapse-remembered', title: 'Time', entries: [doesNotAdd] }
      })
      expect(valueCells(again.container)).toBe(0)
      const other = render(MetricsDistributionBlock, {
        props: { id: 'collapse-other', title: 'Memory', entries: [adds] }
      })
      expect(valueCells(other.container)).toBe(4)
    })

    // A collapsed block is a title and the padding around it, nothing more: the point of
    // collapsing is to fit the categories a reader ignores into as little height as possible.
    it('shrinks to its title, with no room left for the rows', async () => {
      // Real padding and radius come from the theme, so the size is only meaningful with it.
      await loadTheme()
      const { container } = render(MetricsDistributionBlock, {
        props: { id: 'collapse-height', title: 'State', entries: [adds, doesNotAdd] }
      })
      // A pane narrower than the table's 30rem minimum, where the expanded block scrolls.
      container.style.width = '360px'
      const card = container.querySelector<HTMLElement>('.metrics-block')!
      const expanded = card.getBoundingClientRect().height
      setCollapsed('collapse-height', true)
      await expect.poll(() => card.querySelectorAll('.value-cell').length).toBe(0)

      const title = card.querySelector<HTMLElement>('h3')!.getBoundingClientRect().height
      const height = card.getBoundingClientRect().height
      // The card's own padding, top and bottom, is all that surrounds the title.
      expect(height - title).toBeLessThanOrEqual(12)
      expect(height).toBeLessThan(expanded / 2)
      // Nothing of the table is left to widen the card, so a title-only card cannot scroll
      // sideways and take its title out of view.
      const scrolls = (el: HTMLElement) => el.scrollWidth > el.clientWidth + 1
      expect([card, ...card.querySelectorAll<HTMLElement>('*')].filter(scrolls)).toEqual([])
    })

    it('stays open while it holds the current metric, keeping the choice for later', async () => {
      const current = entry('records', {
        cells: cells([new CountValue(1)]),
        isCurrentMetric: true
      })
      const plain = render(MetricsDistributionBlock, {
        props: { id: 'collapse-current', title: 'State', entries: [adds] }
      })
      titleButton(plain.container).click()
      await expect.poll(() => valueCells(plain.container)).toBe(0)
      await plain.unmount()

      // The panel leads with the current metric's value, so its block cannot hide it.
      const holding = render(MetricsDistributionBlock, {
        props: { id: 'collapse-current', title: 'State', entries: [current, adds] }
      })
      expect(valueCells(holding.container)).toBe(8)
      const pinned = titleButton(holding.container)
      expect(pinned.getAttribute('aria-expanded')).toBe('true')
      // Marked rather than `disabled`: a disabled button leaves the tab order and stops firing
      // pointer events, hiding the tooltip that explains why it will not move.
      expect(pinned.getAttribute('aria-disabled')).toBe('true')
      expect(pinned.title).toContain('current metric')
      pinned.focus()
      expect(document.activeElement).toBe(pinned)
      pinned.click()
      await expect.poll(() => valueCells(holding.container)).toBe(8)
      await holding.unmount()

      // Once the current metric lives in another category, the choice applies again.
      const after = render(MetricsDistributionBlock, {
        props: { id: 'collapse-current', title: 'State', entries: [adds] }
      })
      expect(valueCells(after.container)).toBe(0)
    })
  })
})

// Issue 6990: the diagram is colored by one metric, and reading its value meant hunting for the
// row. That row is banded so it stands out; `dispatch` also floats it to the top.
//
// The band's color comes from a theme token through a custom property. Asserting the class
// string cannot catch a typo in the property name or a token leaving the theme, so these tests
// load the real stylesheets and read the color the browser resolved.
describe('MetricsDistributionBlock selected metric', () => {
  const rows = [
    entry('records', { cells: cells([new CountValue(10)]), isCurrentMetric: true }),
    entry('bytes', { cells: cells([new CountValue(20)]) })
  ]

  const themed = async (theme: 'light' | 'dark') => {
    await loadTheme(theme)
    return render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'State', entries: rows }
    })
  }

  // One subgrid element per row, marked for assistive technology as the current one.
  const rowElements = (container: HTMLElement) =>
    Array.from(container.querySelectorAll('.metrics-block > div > div > div')).filter((el) =>
      el.className.includes('grid-cols-subgrid')
    ) as HTMLElement[]

  it('marks the current metric and no other', async () => {
    const { container } = await themed('light')
    expect(
      rowElements(container).map((row) => [
        row.textContent?.trim().startsWith('records') ? 'records' : 'bytes',
        row.getAttribute('aria-current')
      ])
    ).toEqual([
      ['records', 'true'],
      ['bytes', null]
    ])
  })

  it('paints the current row in the theme blue and leaves the rest unpainted', async () => {
    const { container } = await themed('light')
    const [selected, other] = rowElements(container)
    // tertiary-50, the palette's palest blue; primary is pink in this theme.
    expect(getComputedStyle(selected!).backgroundColor).toBe('oklch(0.9535 0.02 274.08)')
    expect(getComputedStyle(other!).backgroundColor).toBe('rgba(0, 0, 0, 0)')
  })

  it('paints it a dark blue in dark mode', async () => {
    const { container } = await themed('dark')
    const [selected] = rowElements(container)
    expect(getComputedStyle(selected!).backgroundColor).toBe('oklch(0.3449 0.13 266.38)')
  })

  it('bands the row in one piece, its columns lining up with the header', async () => {
    const { container } = await themed('light')
    const [selected] = rowElements(container)
    // A subgrid row borrows the block's columns, so one background covers the cells and the gaps
    // between them, and the cells stay under their headers.
    expect(getComputedStyle(selected!).display).toBe('grid')
    const header = Array.from(container.querySelectorAll('.sticky')).find(
      (h) => h.textContent?.trim() === 'Total'
    ) as HTMLElement
    const totalCell = selected!.querySelectorAll('.value-cell')[3] as HTMLElement
    expect(totalCell.getBoundingClientRect().left).toBeCloseTo(
      header.getBoundingClientRect().left,
      0
    )
  })

  it('leaves the histogram under the current row uncolored', async () => {
    const { container } = await themed('light')
    // The bars carry their own scale; a band behind them would compete with it.
    for (const chart of container.querySelectorAll('.bar-chart')) {
      expect(getComputedStyle(chart).backgroundColor).toBe('rgba(0, 0, 0, 0)')
    }
  })

  it('paints nothing when no metric is current', async () => {
    document.documentElement.dataset.theme = 'feldera-modern-theme'
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'State', entries: [rows[1]!] }
    })
    const [only] = rowElements(container)
    expect(only!.getAttribute('aria-current')).toBeNull()
    expect(getComputedStyle(only!).backgroundColor).toBe('rgba(0, 0, 0, 0)')
  })
})
