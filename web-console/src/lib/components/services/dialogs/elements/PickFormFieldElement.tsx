import { nonNull } from '$lib/functions/common/function'
import BigNumber from 'bignumber.js/bignumber.js'
import { useFormContext } from 'react-hook-form-mui'
import invariant from 'tiny-invariant'

import { Autocomplete, createFilterOptions, FilterOptionsState, TextField } from '@mui/material'

type FieldOption = { value: string; label: string }

const filter = createFilterOptions<FieldOption | string>()

export function PickFormFieldElement(props: {
  parentName: string
  getDefault: (field: string) => string | number | BigNumber | boolean | string[]
  options: string[]
  allowCustom?: boolean
}) {
  const ctx = useFormContext()
  const filterOptions = props.allowCustom
    ? (options: (FieldOption | string)[], params: FilterOptionsState<FieldOption | string>) => {
        const filtered = filter(options, params)
        const { inputValue } = params
        const isExisting = options.some(option => inputValue === option)

        if (inputValue !== '' && !isExisting) {
          filtered.push({
            value: inputValue,
            label: `Add "${inputValue}"`
          })
        }

        return filtered
      }
    : undefined

  const addValue = ({ value }: { value: string }) => {
    const field = value.replaceAll('.', '_')
    setTimeout(() => ctx.setFocus(props.parentName + '.' + field), 0)
    if (ctx.getValues(props.parentName + '.' + field)) {
      return
    }
    const defaultValue = props.getDefault(field)
    invariant(nonNull(defaultValue))
    ctx.setValue(props.parentName + '.' + field, defaultValue)
  }
  return (
    <Autocomplete
      inputValue={undefined}
      blurOnSelect={true}
      freeSolo={props.allowCustom}
      clearOnBlur
      clearOnEscape
      filterOptions={filterOptions}
      onChange={(e, newValue) => {
        if (!newValue) {
          return
        }
        if (typeof newValue === 'string') {
          addValue({
            value: newValue
          })
          return
        }
        addValue({
          value: newValue.value
        })
      }}
      getOptionLabel={o => (typeof o === 'string' ? o : o.label)}
      options={props.options}
      size='small'
      renderInput={params => {
        // Hack to force clear the input after option is selected
        const inputProps = { ...params.inputProps, value: params.inputProps.value === '' ? '' : undefined }
        return (
          <TextField
            placeholder={props.options.length ? 'Select option to add' : 'Enter option name, press Enter to add it'}
            {...params}
            inputProps={{ ...inputProps, 'data-testid': 'input-select-field' }}
          />
        )
      }}
    />
  )
}
