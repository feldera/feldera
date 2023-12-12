'use client'

import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { Direction } from '$lib/types/connectors'
import { Controller } from 'react-hook-form'
import { TextFieldElement, useFormContext } from 'react-hook-form-mui'

import { Editor } from '@monaco-editor/react'
import { useTheme } from '@mui/material'
import Box from '@mui/material/Box'
import FormControl from '@mui/material/FormControl'
import FormHelperText from '@mui/material/FormHelperText'
import Grid from '@mui/material/Grid'
import TextField from '@mui/material/TextField'

export const GenericEditorForm = (props: {
  disabled?: boolean
  direction: Direction
  configFromText: (t: string) => unknown
  configToText: (c: unknown) => string
}) => {
  const theme = useTheme()
  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'
  const ctx = useFormContext()
  return (
    <Grid container spacing={4}>
      <Grid item sm={4} xs={12}>
        <TextFieldElement
          name='name'
          label={props.direction === Direction.OUTPUT ? 'Data Sink Name' : 'Data Source Name'}
          size='small'
          fullWidth
          id='connector-name' // referenced by webui-tester
          placeholder={PLACEHOLDER_VALUES['connector_name']}
          aria-describedby='validation-name'
          disabled={props.disabled}
        />
      </Grid>
      <Grid item sm={8} xs={12}>
        <TextField
          name='description'
          label='Description'
          size='small'
          fullWidth
          id='connector-description' // referenced by webui-tester
          placeholder={PLACEHOLDER_VALUES['connector_description']}
          aria-describedby='validation-description'
          disabled={props.disabled}
        />
      </Grid>
      <Grid item sm={12} xs={12}>
        <FormControl fullWidth disabled={props.disabled}>
          <Controller
            name='config'
            control={ctx.control}
            render={({ field: { ref, value, onChange, ...field } }) => (
              void ref,
              (
                <Box sx={{ height: { xs: '50vh', md: '40vh' } }}>
                  <Editor
                    theme={vscodeTheme}
                    defaultLanguage='yaml'
                    options={{ domReadOnly: props.disabled, readOnly: props.disabled }}
                    {...{
                      ...field,
                      onChange: e => {
                        try {
                          // Ignore parsing errors when haven't finished typing configuration
                          onChange(props.configFromText(e ?? ''))
                        } catch {}
                      },
                      value: props.configToText(value)
                    }}
                  />
                </Box>
              )
            )}
            disabled={props.disabled}
          />
          {(e =>
            e && (
              <FormHelperText sx={{ color: 'error.main' }} id='validation-config'>
                {e.message}
              </FormHelperText>
            ))(ctx.getFieldState('config').error)}
        </FormControl>
      </Grid>
    </Grid>
  )
}
