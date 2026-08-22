// Navigating from a diagram node to its SQL crosses a convention boundary: the compiler names the
// last character of a source range, while Monaco's range end is the caret position after it. Getting
// that wrong is invisible in every unit test of the plumbing - the ranges match, the selection is
// applied - and shows up only as a selection one character short on screen. So this mounts the real
// editor over a real program and asks it what it selected.

import * as monaco from 'monaco-editor'
import { SourcePositionRange } from 'profiler-lib'
import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import SqlCodeView from './SqlCodeView.svelte'

const CODE = [
  'CREATE TABLE fact_1 (id BIGINT);',
  'CREATE VIEW v AS',
  '  SELECT id FROM fact_1;'
].join('\n')

/** A range in the compiler's own terms: 1-based, and `endColumn` is the last character of it. */
const range = (startLine: number, startColumn: number, endLine: number, endColumn: number) =>
  new SourcePositionRange({
    start_line_number: startLine,
    start_column: startColumn,
    end_line_number: endLine,
    end_column: endColumn
  })

/** The text of each selection the mounted editor holds, once it has one. */
const selectedText = async () =>
  await vi.waitFor(() => {
    const editor = monaco.editor.getEditors().at(-1)
    const model = editor?.getModel()
    const selections = editor?.getSelections() ?? []
    // A fresh editor starts with an empty selection at 1:1, which is not an answer; `waitFor` retries
    // on a throw, so keep asking until the effect has applied the ranges.
    if (!model || !selections.some((s) => !s.isEmpty())) {
      throw new Error('the editor has not applied a selection yet')
    }
    return selections.map((s) => model.getValueInRange(s))
  })

describe('sql source highlighting', () => {
  it('selects the whole range the compiler pointed at, last character included', async () => {
    // `fact_1` spans columns 14 to 19 of line 1 - 19 being the `1`, the shape a dataflow graph
    // actually carries. Built from `end` rather than `endExclusive`, this selects `fact_`.
    const view = render(SqlCodeView, { code: CODE, highlightRanges: [range(1, 14, 1, 19)] })
    expect(await selectedText()).toEqual(['fact_1'])
    view.unmount()
  })

  it('selects every range of a node, across lines', async () => {
    const view = render(SqlCodeView, {
      code: CODE,
      // The head of the view definition, and the table it reads. Disjoint on purpose: Monaco merges
      // selections that overlap, so a range nested inside another would not come back as its own.
      highlightRanges: [range(2, 1, 3, 8), range(3, 18, 3, 23)]
    })
    expect(await selectedText()).toEqual(['CREATE VIEW v AS\n  SELECT', 'fact_1'])
    view.unmount()
  })
})
