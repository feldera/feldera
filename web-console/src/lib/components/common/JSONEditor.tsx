'use client'

import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
import { editor } from 'monaco-editor'
import { useEffect, useRef } from 'react'
import invariant from 'tiny-invariant'

import { Editor, Monaco, useMonaco } from '@monaco-editor/react'
import { useTheme } from '@mui/material'

export function JSONEditor<T>(props: {
  disabled?: boolean
  valueFromText: (t: string) => T
  valueToText: (config: T) => string
  errors: Record<string, { message: string }>
  value: T
  setValue: (value: T) => void
  'data-testid'?: string
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

      const defaultErr = {
        startLineNumber: 0,
        endLineNumber: 0,
        startColumn: 0,
        endColumn: 1,
        message: error.message,
        severity: monaco.MarkerSeverity.Error
      }
      if (!offenderPos) {
        return defaultErr
      }
      return {
        startLineNumber: offenderPos.startLineNumber,
        endLineNumber: offenderPos.endLineNumber,
        startColumn: offenderPos.startColumn,
        endColumn: offenderPos.endColumn,
        message: error.message,
        severity: monaco.MarkerSeverity.Error
      }
    })
    monaco.editor.setModelMarkers(editorRef.current.getModel()!, 'config-errors', errorMarkers)
  }, [props.errors, editorRef, monaco.MarkerSeverity.Error, monaco.editor])
  function handleEditorDidMount(editor: editor.IStandaloneCodeEditor, monaco: Monaco) {
    editorRef.current = editor
    // Only process input when we have finished typing
    editor.onDidBlurEditorText(() => {
      try {
        props.setValue(props.valueFromText(editor.getValue()))
        monaco.editor.setModelMarkers(editorRef.current!.getModel()!, 'parse-errors', [])
      } catch (e) {
        invariant(e instanceof Error)
        const errorMarkers = [e].map(error => {
          const defaultErr = {
            startLineNumber: 0,
            endLineNumber: 0,
            startColumn: 0,
            endColumn: 1,
            message: error.message,
            severity: monaco.MarkerSeverity.Error
          }
          if (e instanceof SyntaxError) {
            const offender = error.message.match(/"(.*)(\r\n|\r|\n|.*)*"\.\.\. is not valid JSON/)?.[1]
            if (offender) {
              const offenderPos = editor
                .getModel()!
                .findNextMatch(offender, { lineNumber: 0, column: 0 }, false, true, null, false)?.range
              if (!offenderPos) {
                return defaultErr
              }

              return {
                startLineNumber: offenderPos.startLineNumber,
                endLineNumber: offenderPos.endLineNumber,
                startColumn: offenderPos.startColumn,
                endColumn: offenderPos.endColumn,
                message: error.message,
                severity: monaco.MarkerSeverity.Error
              }
            }
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
            return defaultErr
          }

          return defaultErr
        })
        monaco.editor.setModelMarkers(editorRef.current!.getModel()!, 'parse-errors', errorMarkers)
      }
    })
  }
  return (
    <Editor
      onMount={handleEditorDidMount}
      theme={vscodeTheme}
      defaultLanguage='yaml'
      options={isMonacoEditorDisabled(props.disabled)}
      value={props.valueToText(props.value)}
      data-testid={props['data-testid']}
    />
  )
}
