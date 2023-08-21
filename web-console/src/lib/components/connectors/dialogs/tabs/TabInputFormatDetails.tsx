import { KafkaInputSchema } from '$lib/components/connectors/dialogs'
import { Control, Controller, FieldErrors, UseFormWatch } from 'react-hook-form'
import FormControl from '@mui/material/FormControl'
import FormHelperText from '@mui/material/FormHelperText'
import Grid from '@mui/material/Grid'
import InputLabel from '@mui/material/InputLabel'
import MenuItem from '@mui/material/MenuItem'
import Select from '@mui/material/Select'
import { FormControlLabel, Switch } from '@mui/material'
import { UrlSchema } from '../UrlConnector'

const TabInputFormatDetails = (props: {
  control: Control<KafkaInputSchema | UrlSchema>
  errors: Partial<FieldErrors<KafkaInputSchema | UrlSchema>>
  watch: UseFormWatch<KafkaInputSchema | UrlSchema>
}) => {
  const { watch } = props
  const selectedFormat = watch('format_name')

  return (
    <Grid container spacing={6}>
      <Grid item xs={12}>
        <FormControl>
          <InputLabel id='format_name'>Data Format</InputLabel>
          <Controller
            name='format_name'
            control={props.control}
            render={({ field }) => (
              <Select label='Format' id='format_name' {...field}>
                <MenuItem value='json'>JSON</MenuItem>
                <MenuItem value='csv'>CSV</MenuItem>
              </Select>
            )}
          />
          {props.errors.format_name && (
            <FormHelperText sx={{ color: 'error.main' }} id='validation-data-format'>
              {props.errors.format_name.message}
            </FormHelperText>
          )}
        </FormControl>
      </Grid>

      {selectedFormat === 'json' && (
        <>
          <Grid item xs={12}>
            <FormControl>
              <InputLabel id='json_update_format'>Update Format</InputLabel>
              <Controller
                name='json_update_format'
                control={props.control}
                render={({ field }) => (
                  <Select label='Format' {...field}>
                    <MenuItem value='raw'>Raw</MenuItem>
                    <MenuItem value='insert_delete'>Insert & Delete</MenuItem>
                  </Select>
                )}
              />
              {props.errors.json_update_format && (
                <FormHelperText sx={{ color: 'error.main' }} id='validation-update-format'>
                  {props.errors.json_update_format.message}
                </FormHelperText>
              )}
            </FormControl>
          </Grid>

          <Grid item xs={12}>
            <FormControl fullWidth>
              <Controller
                name='json_array'
                control={props.control}
                defaultValue={false}
                render={({ field: { value, ref, ...field } }) => (
                  <FormControlLabel
                    label='Records are encapsulated in an array'
                    control={<Switch {...field} inputRef={ref} checked={!!value} />}
                  />
                )}
              />
              {props.errors.json_array && (
                <FormHelperText sx={{ color: 'error.main' }} id='validation-json-array'>
                  {props.errors.json_array.message}
                </FormHelperText>
              )}
            </FormControl>
          </Grid>
        </>
      )}
    </Grid>
  )
}

export default TabInputFormatDetails
