'use client'

import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { Direction } from '$lib/types/connectors'
import { editor } from 'monaco-editor'
import { useEffect, useRef } from 'react'
import { TextFieldElement, useFormContext, useFormState } from 'react-hook-form-mui'
import invariant from 'tiny-invariant'

import { Editor, Monaco, useMonaco } from '@monaco-editor/react'
import { useTheme } from '@mui/material'
import Box from '@mui/material/Box'
import Grid from '@mui/material/Grid'
import TextField from '@mui/material/TextField'

export const GenericEditorForm = (props: {
  disabled?: boolean
  direction: Direction
  configFromText: (t: string) => unknown
  configToText: (c: unknown) => string
}) => {
  return (
    <Grid container spacing={4}>
      <Grid item sm={4} xs={12}>
        <TextFieldElement
          name='name'
          label={props.direction === Direction.OUTPUT ? 'Data Sink Name' : 'Data Source Name'}
          size='small'
          fullWidth
          placeholder={PLACEHOLDER_VALUES['connector_name']}
          aria-describedby='validation-name'
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-datasource-name'
          }}
        />
      </Grid>
      <Grid item sm={8} xs={12}>
        <TextField
          name='description'
          label='Description'
          size='small'
          fullWidth
          placeholder={PLACEHOLDER_VALUES['connector_description']}
          aria-describedby='validation-description'
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-datasource-description'
          }}
        />
      </Grid>
      <Grid item sm={12} xs={12}>
        <Box sx={{ height: { xs: '50vh', md: '40vh' } }}>
          <JSONConfigEditor {...props}></JSONConfigEditor>
        </Box>
      </Grid>
    </Grid>
  )
}

const JSONConfigEditor = (props: {
  disabled?: boolean
  configFromText: (t: string) => unknown
  configToText: (c: unknown) => string
}) => {
  const theme = useTheme()
  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'
  const ctx = useFormContext()
  const configText: string = ctx.watch('config')
  const { errors } = useFormState({ control: ctx.control })
  const editorRef = useRef<editor.IStandaloneCodeEditor>()
  const monaco = useMonaco()!
  useEffect(() => {
    if (!editorRef.current) {
      return
    }
    const errorMarkers = Object.entries(errors?.config ?? {}).map(([field, error]) => {
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
  }, [errors, editorRef, monaco.MarkerSeverity.Error, monaco.editor])
  function handleEditorDidMount(editor: editor.IStandaloneCodeEditor, monaco: Monaco) {
    editorRef.current = editor
    // Only process input when we have finished typing
    editor.onDidBlurEditorText(() => {
      try {
        const v = props.configFromText(editor.getValue() ?? '')
        ctx.setValue('config', v)
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
      {...{
        value: props.configToText(configText)
      }}
      data-testid='input-config'
    />
  )
}
