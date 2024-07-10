import type Monaco from 'svelte-monaco'
import { MarkerSeverity, type editor, type Range } from 'monaco-editor'
import invariant from 'tiny-invariant'
import type { SystemError } from '$lib/compositions/health/systemErrors'
import type { SqlCompilerMessage } from '$lib/services/manager'

const getDefaultErrorMarker = (monaco: Monaco, error: { message: string }) => ({
  startLineNumber: 0,
  endLineNumber: 0,
  startColumn: 0,
  endColumn: 1,
  message: error.message,
  severity: monaco.MarkerSeverity.Error
})

const getRangeErrorMarker = (monaco: Monaco, error: { message: string }, range: Range) => ({
  startLineNumber: range.startLineNumber,
  endLineNumber: range.endLineNumber,
  startColumn: range.startColumn,
  endColumn: range.endColumn,
  message: error.message,
  severity: monaco.MarkerSeverity.Error
})

const handleValueError = (editor: editor.IStandaloneCodeEditor, monaco: Monaco, e: unknown) => {
  invariant(e instanceof Error)
  const errorMarkers = [e].map((error) => {
    const defaultErr = getDefaultErrorMarker(monaco, error)
    if (!(e instanceof SyntaxError)) {
      return defaultErr
    }
    {
      const offender = error.message.match(/"(.*)(\r\n|\r|\n|.*)*"\.\.\. is not valid JSON/)?.[1]
      if (offender) {
        const offenderPos = editor
          .getModel()!
          .findNextMatch(offender, { lineNumber: 0, column: 0 }, false, true, null, false)?.range
        if (!offenderPos) {
          return defaultErr
        }
        return getRangeErrorMarker(monaco, error, offenderPos)
      }
    }
    {
      const [line, col] = (([_, line, col]) => [parseInt(line), parseInt(col)])(
        Array.from(error.message.match(/line (\d+) column (\d+)/) ?? [])
      )
      if (line >= 0 && col >= 0) {
        return {
          startLineNumber: line,
          endLineNumber: line,
          startColumn: col,
          endColumn: col + 1,
          message: error.message,
          severity: monaco.MarkerSeverity.Error
        }
      }
    }
    return defaultErr
  })
  monaco.editor.setModelMarkers(editor.getModel()!, 'parse-errors', errorMarkers)
}

export const getFormErrorsMarkers = (
  errors: Record<string, { message: string }>,
  editor: editor.IStandaloneCodeEditor,
  monaco: Monaco
) => {
  return Object.entries(errors).map(([field, error]) => {
    const offenderPos = ((model) =>
      model.findNextMatch(`"${field}":`, { lineNumber: 0, column: 0 }, false, true, null, false) ??
      model.findNextMatch(
        `"${field.replaceAll('_', '.')}":`,
        { lineNumber: 0, column: 0 },
        false,
        true,
        null,
        false
      ))(editor.getModel()!)?.range

    const defaultErr = getDefaultErrorMarker(monaco, error)
    if (!offenderPos) {
      return defaultErr
    }
    return getRangeErrorMarker(monaco, error, offenderPos)
  })
}

export const extractSQLCompilerErrorMarkers = (
  errors: SystemError<string | SqlCompilerMessage>[]
) => {
  return errors.map(({ cause: { body: error } }) => {
    if (typeof error === 'string') {
      // Just make sure error highlights all of the program
      return {
        startLineNumber: 0,
        endLineNumber: 999,
        startColumn: 0,
        endColumn: 9999,
        message: error,
        severity: MarkerSeverity.Error
      }
    }
    return {
      startLineNumber: error.startLineNumber,
      endLineNumber: error.endLineNumber,
      startColumn: error.startColumn,
      endColumn: error.endColumn + 1,
      message: error.message,
      severity: error.warning ? MarkerSeverity.Warning : MarkerSeverity.Error
    }
  })
}
