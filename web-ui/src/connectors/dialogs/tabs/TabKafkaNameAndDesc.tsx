// Contains the name and description form elements for kafka input and output
// connectors.

import TextField from '@mui/material/TextField'
import { Control, Controller, FieldErrors } from 'react-hook-form'
import { FormControl, FormHelperText, Grid } from '@mui/material'
import { KafkaInputConfig, KafkaOutputConfig } from 'src/types/manager'
import { PLACEHOLDER_VALUES } from 'src/utils'

const TabKafkaNameAndDesc = (props: {
  control: Control<KafkaInputConfig | KafkaOutputConfig>
  errors: Partial<FieldErrors<KafkaInputConfig | KafkaOutputConfig>>
}) => {
  return (
    <Grid container spacing={6}>
      <Grid item xs={12}>
        <FormControl>
          <Controller
            name='name'
            control={props.control}
            render={({ field }) => (
              <TextField
                label='Datasource Name'
                placeholder={PLACEHOLDER_VALUES['connector_name']}
                error={Boolean(props.errors.name)}
                aria-describedby='validation-name'
                {...field}
              />
            )}
          />
          {props.errors.name && (
            <FormHelperText sx={{ color: 'error.main' }} id='validation-name'>
              {props.errors.name.message}
            </FormHelperText>
          )}
        </FormControl>
      </Grid>
      <Grid item xs={12}>
        <FormControl fullWidth>
          <Controller
            name='description'
            control={props.control}
            render={({ field }) => (
              <TextField
                fullWidth
                label='Description'
                placeholder={PLACEHOLDER_VALUES['connector_description']}
                error={Boolean(props.errors.description)}
                aria-describedby='validation-description'
                {...field}
              />
            )}
          />
          {props.errors.description && (
            <FormHelperText sx={{ color: 'error.main' }} id='validation-description'>
              {props.errors.description.message}
            </FormHelperText>
          )}
        </FormControl>
      </Grid>
    </Grid>
  )
}

export default TabKafkaNameAndDesc
