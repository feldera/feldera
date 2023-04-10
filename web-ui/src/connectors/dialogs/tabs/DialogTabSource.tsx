import TextField from '@mui/material/TextField'
import Select from '@mui/material/Select'
import MenuItem from '@mui/material/MenuItem'
import InputLabel from '@mui/material/InputLabel'
import FormControl from '@mui/material/FormControl'
import FormHelperText from '@mui/material/FormHelperText'
import Grid from '@mui/material/Grid'
import { Control, Controller, FieldErrors } from 'react-hook-form'
import { KafkaInputSchema } from 'src/connectors/dialogs/KafkaInputConnector'
import Autocomplete from '@mui/material/Autocomplete'

const TabSource = (props: { control: Control<KafkaInputSchema>; errors: Partial<FieldErrors<KafkaInputSchema>> }) => {
  return (
    <Grid container spacing={6}>
      <Grid item xs={12}>
        <FormControl>
          <Controller
            name='host'
            control={props.control}
            render={({ field }) => (
              <TextField
                fullWidth
                label='Bootstrap Server Hostname'
                placeholder='localhost'
                error={Boolean(props.errors.host)}
                aria-describedby='validation-host'
                {...field}
              />
            )}
          />
          {props.errors.host && (
            <FormHelperText sx={{ color: 'error.main' }} id='validation-host'>
              {props.errors.host.message}
            </FormHelperText>
          )}
        </FormControl>
      </Grid>

      <Grid item xs={12}>
        <FormControl>
          <InputLabel id='auto-offset'>Auto Offset Reset</InputLabel>
          <Controller
            name='auto_offset'
            control={props.control}
            render={({ field }) => (
              <Select label='Age' id='reset' {...field}>
                <MenuItem value='earliest'>Earliest</MenuItem>
                <MenuItem value='latest'>Latest</MenuItem>
              </Select>
            )}
          />
          <FormHelperText>From when to consume the topics.</FormHelperText>
        </FormControl>
      </Grid>

      <Grid item xs={12}>
        <FormControl fullWidth>
          <Controller
            name='topics'
            control={props.control}
            render={({ field: { onChange } }) => (
              <Autocomplete
                fullWidth
                multiple
                freeSolo
                options={[]}
                onChange={(event, item) => {
                  onChange(item)
                }}
                renderInput={params => (
                  <TextField {...params} label='Topics' placeholder='Add topic, press Enter to add...' />
                )}
              />
            )}
          />
          <FormHelperText>What topics to consume from.</FormHelperText>
        </FormControl>
      </Grid>
    </Grid>
  )
}

export default TabSource
