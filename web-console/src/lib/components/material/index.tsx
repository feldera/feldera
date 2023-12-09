import { TextField, TextFieldProps, TextFieldVariants } from '@mui/material'

export const ParsedTextField = <Variant extends TextFieldVariants = TextFieldVariants>({
  parseValue = v => v,
  ...props
}: { parseValue: (value: any) => any } & TextFieldProps<Variant>) => (
  <TextField
    {...props}
    onChange={e => {
      if (!props.onChange) {
        return
      }
      const value = parseValue(e.target.value)
      props.onChange({ ...e, target: { ...e.target, value }, currentTarget: { ...e.currentTarget, value } })
    }}
  />
)
