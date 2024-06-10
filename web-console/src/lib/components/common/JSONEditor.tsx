'use client'

import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
import { editor, Range } from 'monaco-editor'
import { Dispatch, SetStateAction, useEffect, useRef } from 'react'
import invariant from 'tiny-invariant'
import { useDebouncedCallback } from 'use-debounce'

import { Editor, Monaco, useMonaco } from '@monaco-editor/react'
import { useTheme } from '@mui/material'

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
  const errorMarkers = [e].map(error => {
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

/**
 * @param props.setEditorDirty Editor is dirty if its content has changed but it hasn't been parsed to yield a value or an error
 * @returns
 */
export function JSONEditor<T>(props: {
  disabled?: boolean
  valueFromText: (t: string) => T
  valueToText: (config: T) => string
  errors: Record<string, { message: string }>
  value: T
  setValue: (value: T) => void
  'data-testid'?: string
  setEditorDirty?: Dispatch<SetStateAction<'dirty' | 'clean' | 'error'>>
}) {
  const theme = useTheme()
  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'
  const editorRef = useRef<editor.IStandaloneCodeEditor>()
  const monaco = useMonaco()!

  useEffect(() => {
    if (!editorRef.current) {
      return
    }
    const errorMarkers = Object.entries(props.errors).map(([field, error]) => {
      const offenderPos = (model =>
        model.findNextMatch(`"${field}":`, { lineNumber: 0, column: 0 }, false, true, null, false) ??
        model.findNextMatch(
          `"${field.replaceAll('_', '.')}":`,
          { lineNumber: 0, column: 0 },
          false,
          true,
          null,
          false
        ))(editorRef.current!.getModel()!)?.range

      const defaultErr = getDefaultErrorMarker(monaco, error)
      if (!offenderPos) {
        return defaultErr
      }
      return getRangeErrorMarker(monaco, error, offenderPos)
    })
    monaco.editor.setModelMarkers(editorRef.current.getModel()!, 'config-errors', errorMarkers)
  }, [props.errors, editorRef, monaco])

  const handleContentUpdate = (editor: editor.IStandaloneCodeEditor, monaco: Monaco) => {
    try {
      props.setValue(props.valueFromText(editor.getValue()))
      monaco.editor.setModelMarkers(editorRef.current!.getModel()!, 'parse-errors', [])
      props.setEditorDirty?.('clean')
    } catch (e) {
      handleValueError(editor, monaco, e)
      props.setEditorDirty?.('error')
    }
  }

  const debouncedContentUpdate = useDebouncedCallback(handleContentUpdate, 1000)

  function handleEditorDidMount(editor: editor.IStandaloneCodeEditor, monaco: Monaco) {
    editorRef.current = editor
    // Only process input when we have finished typing
    editor.onDidBlurEditorText(() => debouncedContentUpdate(editor, monaco))
    editor.onDidChangeModelContent(() => {
      props.setEditorDirty?.('dirty')
      debouncedContentUpdate(editor, monaco)
    })
  }
  return (
    <Editor
      onMount={handleEditorDidMount}
      theme={vscodeTheme}
      defaultLanguage='yaml'
      options={{
        ...isMonacoEditorDisabled(props.disabled),
        scrollbar: {
          vertical: 'hidden'
        }
      }}
      value={props.valueToText(props.value)}
      data-testid={props['data-testid']}
    />
  )
}
