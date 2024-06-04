import { useFormContext } from 'react-hook-form-mui'
import Markdown from 'react-markdown'
import { FormFieldOptions } from 'src/lib/functions/forms'

import { Grid, IconButton, Tooltip, Typography, useTheme } from '@mui/material'
import { Box } from '@mui/system'

import { FormFieldElement } from './FormFieldElement'

export const FormField = (props: {
  field: string
  fieldOptions: FormFieldOptions
  disabled?: boolean
  parentName: string
  optional?: boolean
}) => {
  const theme = useTheme()
  const ctx = useFormContext()
  return (
    <>
      <Grid item xs={12} sm={6} display='flex' alignItems='start'>
        <Tooltip
          slotProps={{
            tooltip: {
              sx: {
                backgroundColor: theme.palette.background.default,
                color: theme.palette.text.primary,
                fontSize: 14
              }
            }
          }}
          title={props.fieldOptions.tooltip ? <Markdown>{props.fieldOptions.tooltip}</Markdown> : undefined}
          disableInteractive
        >
          <Typography sx={{ pt: '0.5em' }}>{props.fieldOptions?.label ?? props.field}</Typography>
        </Tooltip>
      </Grid>
      <Grid item xs={12} sm={6}>
        <Box sx={{ display: 'flex' }}>
          <FormFieldElement
            fieldOptions={props.fieldOptions}
            field={props.field}
            disabled={props.disabled}
            parentName={props.parentName}
            helperText={props.fieldOptions.getHelperText?.(
              ctx.getValues(props.parentName + '.' + props.field) as never
            )}
          ></FormFieldElement>
          {props.optional ? (
            <IconButton
              size='small'
              sx={{ mr: '-2.5rem', ml: '0.5rem' }}
              onClick={() => ctx.unregister(props.parentName + '.' + props.field)}
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
