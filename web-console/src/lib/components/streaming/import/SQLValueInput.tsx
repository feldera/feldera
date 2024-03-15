import { bigNumberInputProps } from '$lib/components/input/BigNumberInput'
import { numericRange } from '$lib/functions/ddl'
import { ColumnType, SqlType } from '$lib/services/manager'
import BigNumber from 'bignumber.js'
import { ChangeEvent } from 'react'
import { match } from 'ts-pattern'
import IconX from '~icons/bx/x'

import { IconButton, TextField, TextFieldProps } from '@mui/material'

/**
 * Input for a value representable by given SQL type accounting for precision, nullability etc.
 * @param param0
 * @returns
 */
export const SQLValueInput = ({
  columnType,
  ...props
}: { columnType: ColumnType; value: any; onChange: (event: ChangeEvent<HTMLInputElement>) => void } & Omit<
  TextFieldProps,
  'type' | 'value' | 'onChange'
>) => {
  const numericOnChange = {
    onChange: (event: any) => {
      if (event.target.value === '' || event.target.value === undefined) {
        return props.onChange({ ...event, target: { ...event.target, value: null as any } })
      }
      return props.onChange(event)
    }
  }
  return (
    <TextField
      InputProps={{
        endAdornment: columnType.nullable ? (
          <IconButton
            size='small'
            sx={{ mr: -3 }}
            onClick={() => props.onChange({ target: { value: null as any } } as any)}
          >
            <IconX></IconX>
          </IconButton>
        ) : undefined
      }}
      {...match(columnType.type)
        .with(SqlType.TINYINT, SqlType.SMALLINT, SqlType.INTEGER, SqlType.REAL, SqlType.DOUBLE, () => ({
          type: 'number',
          inputProps: {
            ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType)),
            value: props.value === null ? '' : props.value, // Enable clearing of the input when setting value to null
            placeholder: props.value === null ? 'null' : ''
          },
          ...props,
          ...numericOnChange
        }))
        .with(SqlType.BIGINT, SqlType.DECIMAL, () => ({
          ...bigNumberInputProps({
            ...props,
            ...numericOnChange,
            defaultValue: props.defaultValue as BigNumber | undefined,
            precision: columnType.precision,
            scale: columnType.scale
          }),
          placeholder: props.value === null ? 'null' : '',
          ...props,
          onChange: undefined
        }))
        .with(SqlType.BOOLEAN, () => ({
          type: 'checkbox',
          ...props,
          inputProps: {
            ...props.inputProps,
            checked: props.value
          },
          onChange: (e: ChangeEvent) =>
            props.onChange({ ...e, target: { ...e.target, value: (e.target as any).checked } } as any)
        }))
        .with(SqlType.CHAR, () => ({
          type: 'string',
          inputProps: {
            maxLength: 1
          },
          ...props
        }))
        .with(SqlType.VARCHAR, () => ({
          type: 'string',
          inputProps: {
            maxLength: columnType.precision ?? 0 > 0 ? columnType.precision : undefined
          },
          ...props,
          value: props.value === null ? '' : props.value,
          placeholder: props.value === null ? 'null' : ''
        }))
        .with(SqlType.TIME, () => ({
          type: 'string',
          ...props
        }))
        .with(SqlType.TIMESTAMP, () => ({
          type: 'datetime-local',
          ...props
        }))
        .with(SqlType.DATE, () => ({
          type: 'date',
          ...props
        }))
        .with(SqlType.ARRAY, () => ({
          type: 'string',
          ...props
        }))
        .with(SqlType.INTERVAL, () => ({
          type: 'string',
          ...props
        }))
        .with(SqlType.BINARY, () => ({
          type: 'string',
          ...props
        }))
        .with(SqlType.VARBINARY, () => ({
          type: 'string',
          ...props
        }))
        .with(SqlType.NULL, () => ({
          type: 'string',
          ...props,
          value: '',
          placeholder: 'null',
          disabled: true
        }))
        .exhaustive()}
    />
  )
}
