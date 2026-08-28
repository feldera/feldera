/**
 * The monospace wrap model behind the `LogView` size cache.
 *
 * The last suite here is the one that matters most: it renders a corpus and checks the model
 * against what the browser actually laid out. A prediction that disagrees with the renderer is
 * worse than no prediction, because the virtualiser is seeded with it and every offset is derived
 * from it. That is what puts the whole file in the browser.
 *
 * The first suites pin the individual rules, so a regression names which one broke.
 */

import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import {
  isPredictable,
  measureRowMetrics,
  predictedLineHeight,
  wrappedRowCount
} from '$lib/logRowMetrics'
import LogViewFixture from './fixtures/LogViewFixture.svelte'

/** The grid the expectations below were measured on. */
const COLUMNS = 78
const LINE_HEIGHT = 24

const rows = (text: string) => wrappedRowCount(text, COLUMNS)

describe('wrappedRowCount', () => {
  it('keeps a line that fits on one row', () => {
    expect(rows('a')).toBe(1)
    expect(rows(' ')).toBe(1)
    expect(rows('x'.repeat(10))).toBe(1)
    expect(rows('x'.repeat(COLUMNS))).toBe(1)
  })

  it('breaks an unbroken run at the edge', () => {
    expect(rows('x'.repeat(COLUMNS + 1))).toBe(2)
    expect(rows('x'.repeat(100))).toBe(2)
    expect(rows('x'.repeat(200))).toBe(3)
    expect(rows('x'.repeat(2000))).toBe(26)
  })

  it('wraps at spaces, leaving the tail of the row unused', () => {
    // Fifteen five-character words fit in 78 columns and the sixteenth moves down. Counting
    // characters alone gives the right answer here by luck; the second case is the one that bites,
    // since eleven-character words pack seven to a row rather than the 7.09 division suggests.
    expect(rows('word '.repeat(40))).toBe(3)
    expect(rows('aaaaaaaaaa '.repeat(20))).toBe(3)
  })

  it('hangs trailing whitespace instead of wrapping on it', () => {
    expect(rows('trailing spaces' + ' '.repeat(200))).toBe(1)
    // A run in the middle hangs too, and only the word after it moves down.
    expect(rows('a'.repeat(10) + ' '.repeat(100) + 'b'.repeat(10))).toBe(2)
  })

  it('advances tabs to the next tab stop', () => {
    expect(rows('\tindented')).toBe(1)
    expect(rows('\t\t\tdeep\tindent\there')).toBe(1)
    // Twenty tab stops overshoot 78 columns, so the tail lands on the next row.
    expect(rows('\t'.repeat(20) + 'tail')).toBe(2)
  })

  it('moves an overlong word to its own row before breaking it', () => {
    // 300 z's cannot share the first row with "short then ", so they start on the second and span
    // four. Breaking in place would give four rows in total rather than five.
    expect(rows('short then ' + 'z'.repeat(300))).toBe(5)
    expect(rows('a'.repeat(95) + ' ' + 'b'.repeat(95))).toBe(4)
    // ...unless the tail still fits beside the remainder of the broken word.
    expect(rows('a'.repeat(99) + ' ' + 'b'.repeat(5))).toBe(2)
  })

  it('allows a break after a hyphen and nowhere else inside a token', () => {
    expect(rows('h'.repeat(40) + '-' + 't'.repeat(90))).toBe(3)
    expect(rows('x'.repeat(40) + '--' + 'y'.repeat(90))).toBe(3)
    // "ends-exactly" plus 88 q's is one 100-character token to a naive reading, which fits in two
    // rows. The browser breaks after the hyphen and needs three.
    expect(rows('ends-exactly' + 'q'.repeat(88))).toBe(3)
    // No other punctuation offers a break: the same shape around a slash stays at two rows.
    expect(rows('h'.repeat(40) + '/' + 't'.repeat(90))).toBe(2)
    expect(rows('h'.repeat(40) + '_' + 't'.repeat(90))).toBe(2)
    expect(rows('h'.repeat(40) + '.' + 't'.repeat(90))).toBe(2)
  })

  it('never reports fewer than one row, whatever the grid', () => {
    expect(wrappedRowCount('anything', 0)).toBe(1)
    expect(wrappedRowCount('', COLUMNS)).toBe(1)
  })

  it('declines a line that steps off the grid rather than guessing', () => {
    expect(rows('漢'.repeat(60))).toBeUndefined()
    expect(rows('café')).toBeUndefined()
  })
})

describe('isPredictable', () => {
  it('accepts printable ASCII and tabs, which is what log output is made of', () => {
    expect(isPredictable('2026-08-24T12:00:00Z INFO\tready in 1.4s')).toBe(true)
    expect(isPredictable('~!@#$%^&*()_+{}|:"<>?')).toBe(true)
    expect(isPredictable('')).toBe(true)
  })

  it('declines anything the monospace cell does not describe', () => {
    // A fallback font's advance bears no fixed relation to the cell, so there is no cell count
    // that is right at every width. Declining leaves the chunk to the virtualiser to measure.
    expect(isPredictable('漢字')).toBe(false)
    expect(isPredictable('🚀')).toBe(false)
    expect(isPredictable('café')).toBe(false)
    // Control characters other than tab render unpredictably too.
    expect(isPredictable('carriage\rreturn')).toBe(false)
  })
})

describe('predictedLineHeight', () => {
  const metrics = { columns: COLUMNS, lineHeight: LINE_HEIGHT }

  it('gives an empty line no height at all', () => {
    // An empty line renders no content, so it has no line box. Charging it a row would put
    // everything below it out by one line height per blank line.
    expect(predictedLineHeight('', metrics)).toBe(0)
  })

  it('scales the row count by the line height', () => {
    expect(predictedLineHeight('a', metrics)).toBe(LINE_HEIGHT)
    expect(predictedLineHeight('x'.repeat(200), metrics)).toBe(3 * LINE_HEIGHT)
  })

  it('ignores ANSI escapes, which are markup by the time they render', () => {
    const plain = 'x'.repeat(100)
    expect(predictedLineHeight(`[31m${plain}[0m`, metrics)).toBe(
      predictedLineHeight(plain, metrics)
    )
    // A line of nothing but escapes still renders a box, so it keeps one row.
    expect(predictedLineHeight('[31m[0m', metrics)).toBe(LINE_HEIGHT)
  })

  it('returns undefined for a line outside the grid rather than guessing', () => {
    expect(predictedLineHeight('漢'.repeat(60), metrics)).toBeUndefined()
  })
})

/**
 * The model against the renderer.
 *
 * One chunk's worth of lines, so the virtualiser mounts every one of them and each row can be
 * compared with its prediction. Repeated across widths, because the grid is what the prediction is
 * a function of and an off-by-one in the column count only shows up at some of them.
 */
describe('the model agrees with the browser', () => {
  let mounted: { unmount: () => Promise<void> } | undefined
  let mountTarget: HTMLDivElement | undefined

  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
  })

  const corpus = [
    '',
    ' ',
    'a',
    'short line',
    'x'.repeat(60),
    'x'.repeat(100),
    'x'.repeat(2000),
    'word '.repeat(40),
    'aaaaaaaaaa '.repeat(20),
    '\tindented',
    '\t\t\tdeep\tindent\there',
    '\t'.repeat(20) + 'tail',
    'a'.repeat(95) + ' ' + 'b'.repeat(95),
    'short then ' + 'z'.repeat(300),
    'trailing spaces' + ' '.repeat(200),
    'ends-exactly' + 'q'.repeat(88),
    'x'.repeat(40) + '--' + 'y'.repeat(90),
    '2026-08-24T12:00:00Z ERROR pipeline=supply-chain step=join-3 elapsed=1.42s',
    'a'.repeat(10) + ' '.repeat(100) + 'b'.repeat(10),
    '漢'.repeat(60),
    '🚀'.repeat(60),
    '[31mred[0m ' + 'e'.repeat(200),
    '/very/long/path/without/spaces/that/has/to/be/broken/somewhere/deep/in/the/tree/file.log'
  ]

  /** The CJK and emoji lines above, which the model declines by design. */
  const DECLINED_LINES = 2

  for (const width of [800, 620, 400, 260]) {
    it(`predicts every row height at ${width}px`, async () => {
      mountTarget = document.createElement('div')
      document.body.appendChild(mountTarget)
      mounted = render(LogViewFixture, {
        target: mountTarget,
        props: { initialLines: corpus, initialWidth: width, initialHeight: 400 }
      } as any)
      for (let frame = 0; frame < 8; frame++) {
        await new Promise((resolve) => requestAnimationFrame(resolve))
      }

      const scroll = mountTarget.querySelector<HTMLDivElement>('.log-view-scroll')!
      const metrics = measureRowMetrics(scroll, false)
      expect(metrics, 'nothing measurable at this width').toBeDefined()

      const mismatches: string[] = []
      let predictedRows = 0
      for (const row of scroll.querySelectorAll<HTMLElement>('[data-line]')) {
        const line = corpus[Number(row.dataset.line)]
        const predicted = predictedLineHeight(line, metrics!)
        if (predicted === undefined) {
          // Declined on purpose. The corpus carries a few of these so the caller's handling of
          // them is exercised; what matters is that nothing else is declined.
          expect(isPredictable(line), `line ${row.dataset.line} declined unexpectedly`).toBe(false)
          continue
        }
        predictedRows++
        const actual = row.getBoundingClientRect().height
        if (predicted !== actual) {
          mismatches.push(
            `line ${row.dataset.line} (${line.length} chars): predicted ${predicted}, rendered ${actual}`
          )
        }
      }
      // Guards the test itself: with nothing mounted every comparison is vacuous.
      expect(scroll.querySelectorAll('[data-line]').length).toBe(corpus.length)
      expect(predictedRows).toBe(corpus.length - DECLINED_LINES)
      expect(mismatches, `at ${metrics!.columns} columns`).toEqual([])
    })
  }
})
