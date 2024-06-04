import { FormField } from '$lib/components/forms/FormField'
import { match } from 'ts-pattern'

import { Grid } from '@mui/material'

export const DeltaLakeWriteModeElement = (props: { parentName: string }) => {
  const extraOptions = {
    mode: (range => ({
      type: 'enum' as const,
      label: 'Write mode',
      range,
      default: 'append',
      getHelperText: (mode: (typeof range)[number]) =>
        match(mode)
          .with('append', () => 'New updates will be appended to the existing table at the target location.')
          .with(
            'truncate',
            () =>
              'Existing table at the specified location will get truncated by outputing delete actions for all files.'
          )
          .with('error_if_exists', () => 'If a table exists at the specified location, the operation must fail.')
          .exhaustive()
    }))(['append', 'truncate', 'error_if_exists'] as const)
  }
  return (
    <Grid container spacing={4} sx={{ height: 'auto', pr: '2rem', pb: 4 }}>
      {Object.entries(extraOptions).map(([field, fieldOptions]) => (
        <FormField
          key={field}
          field={field}
          fieldOptions={fieldOptions}
          parentName={props.parentName}
          optional={false}
        ></FormField>
      ))}
    </Grid>
  )
}
