import { FormTooltip } from '$lib/components/forms/Tooltip'
import { useFormContext, useWatch } from 'react-hook-form-mui'
import { FormFieldOptions } from 'src/lib/functions/forms'

import { Grid, IconButton, Typography } from '@mui/material'
import { Box } from '@mui/system'

import { FormFieldElement } from './FormFieldElement'

export const FormField = (props: {
  field: string
  fieldOptions: FormFieldOptions
  disabled?: boolean
  parentName: string
  optional?: boolean
}) => {
  const ctx = useFormContext()
  const value = useWatch({ name: props.parentName + '.' + props.field })

  return (
    <>
      <Grid item xs={12} sm={6} display='flex' alignItems='start'>
        <FormTooltip title={props.fieldOptions.tooltip}>
          <Typography sx={{ pt: '0.5em' }}>{props.fieldOptions?.label ?? props.field}</Typography>
        </FormTooltip>
      </Grid>
      <Grid item xs={12} sm={6}>
        <Box sx={{ display: 'flex' }}>
          <FormFieldElement
            fieldOptions={props.fieldOptions}
            field={props.field}
            disabled={props.disabled}
            parentName={props.parentName}
            helperText={props.fieldOptions.getHelperText?.(value as never)}
          ></FormFieldElement>
          {props.optional ? (
            <IconButton
              size='small'
              sx={{ mr: '-2.5rem', ml: '0.5rem' }}
              onClick={() => ctx.unregister(props.parentName + '.' + props.field)}
              data-testid={`button-remove-${props.field}`}
            >
              <i className='bx bx-x' />
            </IconButton>
          ) : (
            <></>
          )}
        </Box>
      </Grid>
    </>
  )
}
