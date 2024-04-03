import { Controller, useFormContext } from 'react-hook-form-mui'

import { Autocomplete, FormControl, FormHelperText, TextField } from '@mui/material'

export const KafkaTopicsElement = (props: { disabled?: boolean; parentName: string }) => {
  const ctx = useFormContext()
  return (
    <FormControl fullWidth>
      <Controller
        name={props.parentName + '.topics'}
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
            data-testid='input-topics'
            renderInput={params => (
              <TextField {...params} inputRef={ref} label='topics' placeholder='Add topic, press Enter to add…' />
            )}
          />
        )}
        disabled={props.disabled}
      />
      {(e =>
        e ? (
          <FormHelperText sx={{ color: 'error.main' }}>{e.message}</FormHelperText>
        ) : (
          <FormHelperText>A list of Kafka topics to consume from.</FormHelperText>
        ))(ctx.getFieldState(props.parentName + '.topics').error)}
    </FormControl>
  )
}
