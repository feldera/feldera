// ** MUI Imports
import TextField from '@mui/material/TextField'
import { RedpandaSource } from 'src/data/DialogCreateRedpanda'
import { Control, Controller, FieldErrors } from 'react-hook-form'
import { FormControl, FormHelperText, Grid } from '@mui/material'

const TabDetails = (props: { control: Control<RedpandaSource>; errors: Partial<FieldErrors<RedpandaSource>> }) => {
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
                placeholder='AAPL'
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
                placeholder='5 min Stock Ticker Data'
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

export default TabDetails
