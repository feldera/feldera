/**
 * The Total column shows a metric's readings added up across the node's workers, and only for
 * metrics that add up. `profiler-lib` decides that: it fills `TooltipRow.total` for counts, byte
 * sizes and durations and leaves it absent for rates, reported minima and maxima, flags and
 * settings, where a total states nothing. This test pins both halves of the contract - the value
 * appears when the row carries one, the cell stays blank when it does not - and the header's
 * column count, which has to stay in step with the grid template.
 */

import { CountValue, PercentValue, type TooltipRow } from 'profiler-lib'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import type { RenderableMetric } from '../dispatch'
import MetricsDistributionBlock from './MetricsDistributionBlock.svelte'

const cells = (values: Array<CountValue | PercentValue>) =>
  values.map((value) => ({ value, percentile: 50 }))

const entry = (label: string, row: Partial<TooltipRow> & Pick<TooltipRow, 'cells'>):
  RenderableMetric => ({
  label,
  row: { metric: label, isCurrentMetric: false, ...row }
})

describe('MetricsDistributionBlock total column', () => {
  const adds = entry('records', {
    cells: cells([new CountValue(10), new CountValue(30)]),
    // Two workers holding 10 and 30 records: the node holds 40.
    total: new CountValue(40)
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

  it('keeps each row independent of the others', async () => {
    const { container } = render(MetricsDistributionBlock, {
      props: { id: 'b', title: 'Mixed', entries: [adds, doesNotAdd] }
    })
    // The rate's Avg is the pooled 2/6, not the mean of the two rates.
    expect(texts(container)).toEqual(['20', '10', '30', '40', '33.3%', '25.0%', '50.0%', ''])
  })
})
