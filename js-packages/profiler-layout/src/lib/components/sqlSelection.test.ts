/**
 * Clicking a profile node highlights the SQL that produced it.
 * The test drives a real Monaco editor and reads back the text it selected.
 */

import { setSelections } from 'common-ui'
import * as monaco from 'monaco-editor'
import { SourcePositionRange } from 'profiler-lib'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

import SqlCodeView from './SqlCodeView.svelte'

const CODE = [
  'SELECT *',
  'FROM events e',
  "WHERE e.site_id = 'boomtrain'",
  '  AND e.ts > NOW() - INTERVAL 5 DAYS'
].join('\n')

let editor: monaco.editor.IStandaloneCodeEditor | undefined
let container: HTMLElement | undefined

const open = () => {
  container = document.createElement('div')
  container.style.width = '600px'
  container.style.height = '200px'
  document.body.appendChild(container)
  editor = monaco.editor.create(container, { value: CODE, language: 'sql' })
  return editor
}

afterEach(() => {
  editor?.getModel()?.dispose()
  editor?.dispose()
  container?.remove()
  editor = undefined
  container = undefined
})

/** The text Monaco ends up selecting, which is what the user sees highlighted. */
const selected = (editorRef: monaco.editor.IStandaloneCodeEditor) => {
  const selection = editorRef.getSelection()!
  return editorRef.getModel()!.getValueInRange(selection)
}

describe('setSelections over compiler source positions', () => {
  it('includes the last character of a range', () => {
    const editorRef = open()
    // `'boomtrain'` occupies columns 19 to 29 of line 3; the compiler reports 29, the column of
    // the closing quote.
    setSelections(editorRef, [{ start: { line: 3, column: 19 }, end: { line: 3, column: 29 } }])
    expect(selected(editorRef)).toBe("'boomtrain'")
  })

  it('includes the last character of a keyword at the end of a line', () => {
    const editorRef = open()
    // `NOW() - INTERVAL 5 DAYS`, columns 14 to 36 of line 4. Dropping the last character left
    // `DAY`, a different interval unit.
    setSelections(editorRef, [{ start: { line: 4, column: 14 }, end: { line: 4, column: 36 } }])
    expect(selected(editorRef)).toBe('NOW() - INTERVAL 5 DAYS')
  })

  it('includes the last character of a range spanning lines', () => {
    const editorRef = open()
    setSelections(editorRef, [{ start: { line: 1, column: 1 }, end: { line: 2, column: 13 } }])
    expect(selected(editorRef)).toBe('SELECT *\nFROM events e')
  })

  it('selects a single character', () => {
    const editorRef = open()
    // A one-character range reports the same column at both ends.
    setSelections(editorRef, [{ start: { line: 1, column: 8 }, end: { line: 1, column: 8 } }])
    expect(selected(editorRef)).toBe('*')
  })

  it('applies every range it is given', () => {
    const editorRef = open()
    setSelections(editorRef, [
      { start: { line: 3, column: 19 }, end: { line: 3, column: 29 } },
      { start: { line: 1, column: 8 }, end: { line: 1, column: 8 } }
    ])
    const model = editorRef.getModel()!
    expect(editorRef.getSelections()!.map((s) => model.getValueInRange(s))).toEqual([
      "'boomtrain'",
      '*'
    ])
  })
})

// `setSelections` owning the conversion only helps if the ranges reach it untouched. These mount the
// component the diagram actually navigates through, over a program shaped like a real one, and ask
// the editor what it ended up selecting.
describe("SqlCodeView over a node's source ranges", () => {
  const PROGRAM = [
    'CREATE TABLE fact_1 (id BIGINT);',
    'CREATE VIEW v AS',
    '  SELECT id FROM fact_1;'
  ].join('\n')

  /** A range in the compiler's terms: 1-based, `end_column` being the last character of it. */
  const range = (startLine: number, startColumn: number, endLine: number, endColumn: number) =>
    new SourcePositionRange({
      start_line_number: startLine,
      start_column: startColumn,
      end_line_number: endLine,
      end_column: endColumn
    })

  /** The text of every selection the mounted editor holds, once it has one. */
  const selectedTexts = () =>
    vi.waitFor(() => {
      const editorRef = monaco.editor.getEditors().at(-1)
      const model = editorRef?.getModel()
      const selections = editorRef?.getSelections() ?? []
      // A fresh editor starts with an empty selection at 1:1, which is not an answer; `waitFor`
      // retries on a throw, so keep asking until the effect has applied the ranges.
      if (!model || !selections.some((s) => !s.isEmpty())) {
        throw new Error('the editor has not applied a selection yet')
      }
      return selections.map((s) => model.getValueInRange(s))
    })

  it('selects the whole range the compiler pointed at, last character included', async () => {
    // `fact_1` spans columns 14 to 19 of line 1, 19 being the `1`. One character short selects
    // `fact_`; one character long swallows the space after it.
    const view = render(SqlCodeView, { code: PROGRAM, highlightRanges: [range(1, 14, 1, 19)] })
    expect(await selectedTexts()).toEqual(['fact_1'])
    view.unmount()
  })

  it('selects every range of a node, across lines', async () => {
    const view = render(SqlCodeView, {
      code: PROGRAM,
      // The head of the view definition, and the table it reads. Disjoint on purpose: Monaco merges
      // selections that overlap, so a range nested inside another would not come back as its own.
      highlightRanges: [range(2, 1, 3, 8), range(3, 18, 3, 23)]
    })
    expect(await selectedTexts()).toEqual(['CREATE VIEW v AS\n  SELECT', 'fact_1'])
    view.unmount()
  })
})
