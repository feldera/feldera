'use client'

import { JSONEditor } from '$lib/components/common/JSONEditor'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { Direction } from '$lib/types/connectors'
import { Dispatch, SetStateAction } from 'react'
import { FieldValues, Path, TextFieldElement, useFormContext, useFormState } from 'react-hook-form-mui'

import Box from '@mui/material/Box'
import Grid from '@mui/material/Grid'

export const GenericEditorForm = <T extends FieldValues>(props: {
  disabled?: boolean
  direction: Direction
  configFromText: (t: string) => T
  configToText: (c: T) => string
  setEditorDirty?: Dispatch<SetStateAction<'dirty' | 'clean' | 'error'>>
}) => {
  return (
    <>
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
            <JSONEditorElement {...props}></JSONEditorElement>
          </Box>
        </Grid>
      </Grid>
      <Box sx={{ mt: 'auto' }}></Box>
    </>
  )
}

export const JSONEditorElement = <T extends FieldValues>(props: {
  disabled?: boolean
  configFromText: (text: string) => T
  configToText: (config: T) => string
  setEditorDirty?: Dispatch<SetStateAction<'dirty' | 'clean' | 'error'>>
}) => {
  const ctx = useFormContext<T>()
  const value = ctx.watch()
  const { errors } = useFormState({ control: ctx.control })
  return (
    <JSONEditor
      disabled={props.disabled}
      valueFromText={props.configFromText}
      valueToText={props.configToText}
      errors={(errors?.config as any) ?? {}}
      value={value}
      setValue={v => {
        Object.entries(v).forEach(([key, value]) => {
          ctx.setValue(key as Path<T>, value)
        })
      }}
      setEditorDirty={props.setEditorDirty}
      data-testid='input-config'
    ></JSONEditor>
  )
}
