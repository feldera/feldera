import type { editor } from 'monaco-editor'

export type CodePosition = { line: number; column: number }
export type CodeRange = { start: CodePosition; end: CodePosition }

/**
 * Apply one or more selections to a Monaco editor and reveal the first range
 * in the viewport center. No-op when the editor is undefined or no ranges are
 * provided.
 */
export function setSelections(
  editorRef: editor.IStandaloneCodeEditor | undefined,
  ranges: CodeRange[]
) {
  if (!editorRef || ranges.length === 0) {
    return
  }
  editorRef.setSelections(
    ranges.map((range) => ({
      selectionStartLineNumber: range.start.line,
      selectionStartColumn: range.start.column,
      positionLineNumber: range.end.line,
      positionColumn: range.end.column
    }))
  )
  editorRef.revealRangeInCenter({
    startLineNumber: ranges[0].start.line,
    startColumn: ranges[0].start.column,
    endLineNumber: ranges[0].end.line,
    endColumn: ranges[0].end.column
  })
}
