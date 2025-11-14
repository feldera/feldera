import { BigNumber } from 'bignumber.js'
import { match } from 'ts-pattern'

type EnumFormField<TEnum extends string = any> = {
  type: 'enum'
  range: readonly TEnum[]
  default: TEnum
  getHelperText?: (value: TEnum) => string
}

export type FormFieldOptions = {
  label?: string
  tooltip?: string
} & (
  | {
      type: 'string'
      default?: string
      getHelperText?: (value: string) => string
    }
  | {
      type: 'boolean'
      default?: string
      getHelperText?: (value: boolean) => string
    }
  | {
      type: 'list' | 'array'
      default?: string
      getHelperText?: (value: string[]) => string
    }
  | {
      type: 'secret_string'
      getHelperText?: (value: string) => string
    }
  | EnumFormField<any>
  | {
      type: 'number'
      range: { min: number; max: number }
      default?: string
      getHelperText?: (value: number) => string
    }
  | {
      type: 'bignumber'
      range: { min: BigNumber; max: BigNumber }
      default?: string
      getHelperText?: (value: BigNumber) => string
    }
)

export type FormFields = Record<string, FormFieldOptions>

export const formFieldDefaultValue = (option: FormFieldOptions) =>
  match(option)
    .with({ type: 'boolean' }, (o) => o.default === 'true')
    .with({ type: 'number' }, (o) => (o.default ? parseInt(o.default) : 0))
    .with({ type: 'bignumber' }, (o) => (o.default ? BigNumber(o.default) : 0))
    .with({ type: 'enum' }, (o) => o.default)
    .with({ type: 'string' }, (o) => o.default ?? '')
    .with({ type: 'list' }, (o) => (o.default ?? '').split(', '))
    .with({ type: 'array' }, (o) => (o.default ?? '').split(', '))
    .with({ type: 'secret_string' }, () => '')
    .exhaustive()
