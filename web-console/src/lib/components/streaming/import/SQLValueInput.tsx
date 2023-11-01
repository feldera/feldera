import { BigNumberInput } from '$lib/components/input/BigNumberInput'
import { numericRange } from '$lib/functions/ddl'
import { ColumnType } from '$lib/services/manager'
import BigNumber from 'bignumber.js'
import { match } from 'ts-pattern'

import { TextField, TextFieldProps } from '@mui/material'

/**
 * Input for a value representable by given SQL type accounting for precision, nullability etc.
 * @param param0
 * @returns
 */
export const SQLValueInput = ({
  columnType,
  ...props
}: { columnType: ColumnType; value: any; onChange: (value: any) => void } & Omit<
  TextFieldProps,
  'type' | 'value' | 'onChange'
>) =>
  match(columnType.type)
    .with('BIGINT', 'DECIMAL', () => {
      return (
        <BigNumberInput
          {...{ ...props, defaultValue: props.defaultValue as BigNumber | undefined }}
          precision={columnType.precision}
          scale={columnType.scale}
        ></BigNumberInput>
      )
    })
    .otherwise(() => (
      <TextField
        {...match(columnType.type)
          .with('BOOLEAN', 'BOOL', () => ({
            type: 'checkbox'
          }))
          .with('TINYINT', () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with('SMALLINT', () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with('INTEGER', () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with('FLOAT', () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with('DOUBLE', () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with('VARCHAR', () => ({
            type: 'string',
            inputProps: {
              maxLength: columnType.precision ?? 0 > 0 ? columnType.precision : undefined
            }
          }))
          .with('CHAR', () => ({
            type: 'string',
            inputProps: {
              maxLength: 1
            }
          }))
          .with('TIME', () => ({
            type: 'string'
          }))
          .with('TIMESTAMP', () => ({
            type: 'datetime-local'
          }))
          .with('DATE', () => ({
            type: 'date'
          }))
          .with('GEOMETRY', () => ({
            type: 'string'
          }))
          .with('ARRAY', () => ({
            type: 'string'
          }))
          .otherwise(() => ({
            type: 'string'
          }))}
        {...props}
      />
    ))
