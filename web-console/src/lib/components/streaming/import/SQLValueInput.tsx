import { bigNumberInputProps } from '$lib/components/input/BigNumberInput'
import { getValueFormatter, numericRange, SQLValueJS, xgressJSONToSQLValue } from '$lib/functions/ddl'
import { ColumnType, SqlType } from '$lib/services/manager'
import { Arguments } from '$lib/types/common/function'
import BigNumber from 'bignumber.js'
import { ChangeEvent, Dispatch, useReducer } from 'react'
import { nonNull } from 'src/lib/functions/common/function'
import invariant from 'tiny-invariant'
import JSONbig from 'true-json-bigint'
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
}: { columnType: ColumnType; value: SQLValueJS; onChange: (event: ChangeEvent<HTMLInputElement>) => void } & Omit<
  TextFieldProps,
  'type' | 'value' | 'onChange'
>) => {
  const onChangeEmptyNull = (props: { onChange: (event: ChangeEvent<HTMLInputElement>) => void }) => ({
    onChange: (event: any) => {
      if (event.target.value === '' || event.target.value === undefined) {
        return props.onChange({ ...event, target: { ...event.target, value: null as any } })
      }
      return props.onChange(event)
    }
  })
  const onChangeNumber = (props: { onChange: (event: ChangeEvent<HTMLInputElement>) => void }) => ({
    onChange: (event: any) => {
      if (!nonNull(event.target.value)) {
        return props.onChange(event)
      }
      const number = parseFloat(event.target.value)
      return props.onChange({ ...event, target: { ...event.target, value: number } })
    }
  })

  if (columnType.type === SqlType.ARRAY) {
    return <SQLArrayInput {...{ columnType, ...props }}></SQLArrayInput>
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
        .with(SqlType.TINYINT, SqlType.SMALLINT, SqlType.INTEGER, () => ({
          type: 'number',
          ...props,
          inputProps: {
            ...props.inputProps,
            ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType)),
            value: props.value === null ? '' : props.value, // Enable clearing of the input when setting value to null
            placeholder: props.value === null ? 'null' : ''
          },
          ...onChangeNumber(onChangeEmptyNull(props))
        }))
        .with(SqlType.REAL, SqlType.DOUBLE, () => ({
          type: 'number',
          ...props,
          inputProps: {
            ...props.inputProps,
            value: props.value === null ? '' : props.value, // Enable clearing of the input when setting value to null
            placeholder: props.value === null ? 'null' : ''
          },
          ...onChangeNumber(onChangeEmptyNull(props))
        }))
        .with(SqlType.BIGINT, SqlType.DECIMAL, () => ({
          ...bigNumberInputProps({
            ...props,
            value: props.value as BigNumber,
            ...onChangeEmptyNull(props),
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
          ...props,
          value: props.value === null ? '' : props.value,
          placeholder: props.value === null ? 'null' : ''
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
          ...props,
          value: props.value === null ? '' : props.value,
          placeholder: props.value === null ? 'null' : ''
        }))
        .with(SqlType.TIMESTAMP, () => ({
          type: 'datetime-local',
          ...props
        }))
        .with(SqlType.DATE, () => ({
          type: 'date',
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

type ArrayInputState = { valid: SQLValueJS } | { intermediate: string }

const sqlArrayInputReducer =
  (columnType: ColumnType, setValue: Dispatch<SQLValueJS>) =>
  (_state: ArrayInputState, action: string | null): ArrayInputState => {
    try {
      const value = action === null ? null : xgressJSONToSQLValue(columnType, JSONbig.parse(action))
      setValue(value)
      return {
        valid: value
      }
    } catch {
      invariant(action !== null)
      return {
        intermediate: action
      }
    }
  }

export const SQLArrayInput = (props: Arguments<typeof SQLValueInput>[0]) => {
  const [arrayValue, setArrayText] = useReducer(
    sqlArrayInputReducer(props.columnType, value => props.onChange({ target: { value } } as any)),
    { valid: props.value }
  )
  return (
    <TextField
      InputProps={{
        endAdornment: props.columnType.nullable ? (
          <IconButton size='small' sx={{ mr: -3 }} onClick={() => setArrayText(null)}>
            <IconX></IconX>
          </IconButton>
        ) : undefined
      }}
      {...{
        type: 'string',
        ...props,
        error: 'intermediate' in arrayValue,
        onChange: e => setArrayText(e.target.value),
        value:
          'valid' in arrayValue && arrayValue.valid === null
            ? ''
            : 'valid' in arrayValue
              ? getValueFormatter(props.columnType)(arrayValue.valid)
              : arrayValue.intermediate,
        placeholder: props.value === null ? 'null' : ''
      }}
    ></TextField>
  )
}
