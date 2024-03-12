import { BigNumberInput } from '$lib/components/input/BigNumberInput'
import { numericRange } from '$lib/functions/ddl'
import { ColumnType, SqlType } from '$lib/services/manager'
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
    .with(SqlType.BIGINT, SqlType.DECIMAL, () => {
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
          .with(SqlType.BOOLEAN, () => ({
            type: 'checkbox'
          }))
          .with(SqlType.TINYINT, () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with(SqlType.SMALLINT, () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with(SqlType.INTEGER, () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with(SqlType.REAL, () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with(SqlType.DOUBLE, () => ({
            type: 'number',
            inputProps: {
              ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType))
            }
          }))
          .with(SqlType.VARCHAR, () => ({
            type: 'string',
            inputProps: {
              maxLength: columnType.precision ?? 0 > 0 ? columnType.precision : undefined
            }
          }))
          .with(SqlType.CHAR, () => ({
            type: 'string',
            inputProps: {
              maxLength: 1
            }
          }))
          .with(SqlType.TIME, () => ({
            type: 'string'
          }))
          .with(SqlType.TIMESTAMP, () => ({
            type: 'datetime-local'
          }))
          .with(SqlType.DATE, () => ({
            type: 'date'
          }))
          .with(SqlType.ARRAY, () => ({
            type: 'string'
          }))
          .otherwise(() => ({
            type: 'string'
          }))}
        {...props}
      />
    ))
