import BigNumber from 'bignumber.js'
import { match } from 'ts-pattern'

export type FormFieldOptions =
  | {
      type: 'string' | 'boolean' | 'list' | 'array'
      default?: string
    }
  | {
      type: 'enum'
      range: string[]
      default: string
    }
  | {
      type: 'number'
      range: { min: number; max: number }
      default?: string
    }
  | {
      type: 'bignumber'
      range: { min: BigNumber; max: BigNumber }
      default?: string
    }

export const formFieldDefaultValue = (option: FormFieldOptions) =>
  match(option)
    .with({ type: 'boolean' }, o => o.default === 'true')
    .with({ type: 'number' }, o => (o.default ? parseInt(o.default) : 0))
    .with({ type: 'bignumber' }, o => (o.default ? BigNumber(o.default) : 0))
    .with({ type: 'enum' }, o => o.default)
    .with({ type: 'string' }, o => o.default ?? '')
    .with({ type: 'list' }, o => (o.default ?? '').split(', '))
    .with({ type: 'array' }, o => (o.default ?? '').split(', '))
    .exhaustive()
