/**
 * Clicking a profile node highlights the SQL that produced it.
 * The test drives a real Monaco editor and reads back the text it selected.
 */

import { setSelections } from 'common-ui'
import * as monaco from 'monaco-editor'
import { afterEach, describe, expect, it } from 'vitest'

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
