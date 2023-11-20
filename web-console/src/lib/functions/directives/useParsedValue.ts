import { ParsedTextField } from 'src/lib/components/material'

/**
 * To be used with TextFieldElement from react-hook-form-mui
 * Allows to transform input value before setting it in the form
 * @param parseValue
 * @returns
 */
export const useParsedValue = (parseValue: (value: any) => any) => ({
  component: ParsedTextField as any,
  parseValue
})
