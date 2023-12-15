import { GridItems } from '$lib/components/common/GridItems'
import { Controller } from 'react-hook-form'
import { SelectElement, TextFieldElement, useFormContext } from 'react-hook-form-mui'

import Autocomplete from '@mui/material/Autocomplete'
import FormControl from '@mui/material/FormControl'
import FormHelperText from '@mui/material/FormHelperText'
import Grid from '@mui/material/Grid'
import TextField from '@mui/material/TextField'

const TabKafkaInputDetails = (props: { disabled?: boolean }) => {
  const ctx = useFormContext()
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <TextFieldElement
          name='config.bootstrap_servers'
          label='bootstrap.servers'
          size='small'
          helperText='Bootstrap Server Hostname'
          placeholder='kafka.example.com'
          aria-describedby='validation-host'
          fullWidth
          disabled={props.disabled}
        />

        <Grid item xs={12}>
          <SelectElement
            name='config.auto_offset_reset'
            label='auto.offset.reset'
            size='small'
            id='reset'
            options={[
              {
                id: 'earliest',
                label: 'Earliest'
              },
              {
                id: 'latest',
                label: 'Latest'
              }
            ]}
            helperText='From when to consume the topics.'
            disabled={props.disabled}
          ></SelectElement>
        </Grid>

        <Grid item xs={12}>
          <TextFieldElement
            name='config.group_id'
            label='group.id'
            size='small'
            fullWidth
            placeholder='my-group-id'
            aria-describedby='validation-group-id'
            disabled={props.disabled}
          />
        </Grid>

        <FormControl fullWidth>
          <Controller
            name='config.topics'
            control={ctx.control}
            render={({ field: { ref, onChange, ...field } }) => (
              <Autocomplete
                {...field}
                size='small'
                fullWidth
                autoSelect
                multiple
                freeSolo
                options={[]}
                onChange={(event, item) => {
                  onChange(item)
                }}
                ChipProps={{
                  sx: {
                    fontSize: '1rem',
                    '& .MuiChip-label': { textTransform: 'none' }
                  }
                }}
                renderInput={params => (
                  <TextField {...params} inputRef={ref} label='topics' placeholder='Add topic, press Enter to add…' />
                )}
              />
            )}
            disabled={props.disabled}
          />
          {(e =>
            e ? (
              <FormHelperText sx={{ color: 'error.main' }} id='validation-topics'>
                {e.message}
              </FormHelperText>
            ) : (
              <FormHelperText>A list of Kafka topics to consume from.</FormHelperText>
            ))(ctx.getFieldState('config.topics').error)}
        </FormControl>
      </GridItems>
    </Grid>
  )
}

export default TabKafkaInputDetails
