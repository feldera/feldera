import { Controller, SelectElement, TextFieldElement, useFormContext } from 'react-hook-form-mui'

import { Autocomplete, FormControl, FormHelperText, TextField } from '@mui/material'

export const KafkaAutoOffsetResetElement = (props: { disabled?: boolean; parentName: string }) => {
  return (
    <SelectElement
      name={props.parentName + '.auto_offset_reset'}
      label='auto.offset.reset'
      size='small'
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
      inputProps={{
        'data-testid': 'input-auto-offset-reset'
      }}
    ></SelectElement>
  )
}

export const KafkaGroupIdElement = (props: { disabled?: boolean; parentName: string }) => {
  return (
    <TextFieldElement
      name={props.parentName + '.group_id'}
      label='group.id'
      size='small'
      fullWidth
      placeholder='my-group-id'
      aria-describedby='validation-group-id'
      disabled={props.disabled}
      inputProps={{
        'data-testid': 'input-group-id'
      }}
    />
  )
}

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
            data-testid='input-wrapper-topics'
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
