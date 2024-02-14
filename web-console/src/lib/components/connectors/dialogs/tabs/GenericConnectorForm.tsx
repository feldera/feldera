'use client'

import { JSONEditor } from '$lib/components/common/JSONEditor'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { Direction } from '$lib/types/connectors'
import { TextFieldElement, useFormContext, useFormState } from 'react-hook-form-mui'

import Box from '@mui/material/Box'
import Grid from '@mui/material/Grid'

export type JSONConfigEditorFields = {
  transport: Record<string, any>
  format: Record<string, any>
}

export const GenericEditorForm = (props: {
  disabled?: boolean
  direction: Direction
  configFromText: (t: string) => JSONConfigEditorFields
  configToText: (c: JSONConfigEditorFields) => string
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
        <TextFieldElement
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
          <JSONConfigEditorElement {...props}></JSONConfigEditorElement>
        </Box>
      </Grid>
    </Grid>
  )
}

export const JSONConfigEditorElement = (props: {
  disabled?: boolean
  configFromText: (text: string) => JSONConfigEditorFields
  configToText: (config: JSONConfigEditorFields) => string
}) => {
  const ctx = useFormContext()
  const transport: Record<string, unknown> = ctx.watch('transport')
  const format: Record<string, unknown> = ctx.watch('format')
  const { errors } = useFormState({ control: ctx.control })
  return (
    <JSONEditor
      disabled={props.disabled}
      valueFromText={props.configFromText}
      valueToText={props.configToText}
      errors={(errors?.config as any) ?? {}}
      value={{ transport, format }}
      setValue={v => {
        ctx.setValue('transport', v.transport)
        ctx.setValue('format', v.format)
      }}
      data-testid='input-config'
    ></JSONEditor>
  )
}
