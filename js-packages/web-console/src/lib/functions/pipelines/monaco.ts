import { editor, MarkerSeverity, type Range } from 'monaco-editor/esm/vs/editor/editor.api.js'
import invariant from 'tiny-invariant'
import { type SystemError, showSqlCompilerMessage } from '$lib/compositions/health/systemErrors'
import { nonNull } from '$lib/functions/common/function'
import type { SqlCompilerMessage } from '$lib/services/pipelineManager'

const getDefaultErrorMarker = (error: { message: string }) => ({
  startLineNumber: 0,
  endLineNumber: 0,
  startColumn: 0,
  endColumn: 1,
  message: error.message,
  severity: MarkerSeverity.Error
})

const getRangeErrorMarker = (error: { message: string }, range: Range) => ({
  startLineNumber: range.startLineNumber,
  endLineNumber: range.endLineNumber,
  startColumn: range.startColumn,
  endColumn: range.endColumn,
  message: error.message,
  severity: MarkerSeverity.Error
})

const handleValueError = (editorRef: editor.IStandaloneCodeEditor, e: unknown) => {
  invariant(e instanceof Error)
  const errorMarkers = [e].map((error) => {
    const defaultErr = getDefaultErrorMarker(error)
    if (!(e instanceof SyntaxError)) {
      return defaultErr
    }
    {
      const offender = error.message.match(/"(.*)(\r\n|\r|\n|.*)*"\.\.\. is not valid JSON/)?.[1]
      if (offender) {
        const offenderPos = editorRef
          .getModel()!
          .findNextMatch(offender, { lineNumber: 0, column: 0 }, false, true, null, false)?.range
        if (!offenderPos) {
          return defaultErr
        }
        return getRangeErrorMarker(error, offenderPos)
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
          severity: MarkerSeverity.Error
        }
      }
    }
    return defaultErr
  })
  editor.setModelMarkers(editorRef.getModel()!, 'parse-errors', errorMarkers)
}

export const getFormErrorsMarkers = (
  errors: Record<string, { message: string }>,
  editor: editor.IStandaloneCodeEditor
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

    const defaultErr = getDefaultErrorMarker(error)
    if (!offenderPos) {
      return defaultErr
    }
    return getRangeErrorMarker(error, offenderPos)
  })
}

type ErrorRange = {
  startLineNumber: number
  endLineNumber: number
  startColumn: number
  endColumn: number
  message: string
}

export const felderaCompilerMarkerSource = 'feldera compiler'

export const extractErrorMarkers = (
  errors: SystemError<string | SqlCompilerMessage | ErrorRange>[]
) => {
  return errors
    .map(({ cause: { body: error, warning } }) => {
      if (typeof error === 'string') {
        return null
      }
      if ('startLineNumber' in error) {
        return {
          startLineNumber: error.startLineNumber,
          endLineNumber: error.endLineNumber,
          startColumn: error.startColumn,
          endColumn: error.endColumn + 1,
          message: error.message,
          severity: warning ? MarkerSeverity.Warning : MarkerSeverity.Error
        }
      }
      return {
        startLineNumber: error.start_line_number,
        endLineNumber: error.end_line_number,
        startColumn: error.start_column,
        endColumn: error.end_column + 1,
        message: showSqlCompilerMessage(error),
        severity: warning ? MarkerSeverity.Warning : MarkerSeverity.Error
      }
    })
    .filter(nonNull)
}
