import { FormFieldElement } from '$lib/components/forms/FormFieldElement'
import { outputBufferOptions } from '$lib/functions/connectors/outputBuffer'

import { Grid, Typography } from '@mui/material'

export const TabOutputBufferOptions = (props: { disabled?: boolean }) => {
  const fieldOptions = outputBufferOptions
  return (
    <Grid container spacing={4} sx={{ alignItems: 'center' }}>
      {(['enable_output_buffer', 'max_output_buffer_time_millis', 'max_output_buffer_size_records'] as const).map(
        field => (
          <>
            <Grid item xs={12} sm={6}>
              <Typography>{field}</Typography>
            </Grid>

            <Grid item xs={12} sm={6}>
              <FormFieldElement
                key={field}
                fieldOptions={fieldOptions[field]}
                field={field}
                disabled={props.disabled}
                parentName={undefined}
              ></FormFieldElement>
            </Grid>
          </>
        )
      )}
    </Grid>
  )
}
