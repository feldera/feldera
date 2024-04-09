import { bigNumberInputProps } from '$lib/components/input/BigNumberInput'
import {
  JSONXgressValue,
  numericRange,
  SQLValueJS,
  sqlValueToXgressJSON,
  xgressJSONToSQLValue
} from '$lib/functions/ddl'
import { ColumnType } from '$lib/services/manager'
import BigNumber from 'bignumber.js'
import Dayjs, { isDayjs } from 'dayjs'
import { ChangeEvent, Dispatch, useReducer } from 'react'
import { nonNull } from 'src/lib/functions/common/function'
import invariant from 'tiny-invariant'
import JSONbig from 'true-json-bigint'
import { match, P } from 'ts-pattern'
import IconX from '~icons/bx/x'

import { IconButton, TextField, TextFieldProps, useTheme } from '@mui/material'

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
  invariant(columnType.type)
  if (columnType.type === 'ARRAY') {
    return (
      <IntermediateValueInput
        {...{ columnType, type: 'string', ...props, toText: JSONbig.stringify, fromText: JSONbig.parse }}
      ></IntermediateValueInput>
    )
  }
  if (columnType.type === 'TIME') {
    return (
      <IntermediateValueInput
        {...{
          columnType,
          type: 'string',
          step: 1,
          ...props,
          toText: v => v as string,
          fromText: t => t
        }}
      ></IntermediateValueInput>
    )
  }
  if (columnType.type === 'DATE') {
    return (
      <IntermediateValueInput
        {...{ columnType, type: 'date', ...props, toText: v => v as string, fromText: t => t }}
      ></IntermediateValueInput>
    )
  }
  if (columnType.type === 'STRUCT') {
    return (
      <IntermediateValueInput
        {...{ columnType, type: 'string', ...props, toText: v => v as string, fromText: t => t }}
      ></IntermediateValueInput>
    )
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
        .with('TINYINT', 'SMALLINT', 'INTEGER', () => ({
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
        .with('REAL', 'DOUBLE', () => ({
          type: 'number',
          ...props,
          inputProps: {
            ...props.inputProps,
            value: props.value === null ? '' : props.value, // Enable clearing of the input when setting value to null
            placeholder: props.value === null ? 'null' : ''
          },
          ...onChangeNumber(onChangeEmptyNull(props))
        }))
        .with('BIGINT', 'DECIMAL', () => ({
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
        .with('BOOLEAN', () => ({
          type: 'checkbox',
          ...props,
          inputProps: {
            ...props.inputProps,
            checked: props.value
          },
          onChange: (e: ChangeEvent) =>
            props.onChange({ ...e, target: { ...e.target, value: (e.target as any).checked } } as any)
        }))
        .with('CHAR', () => ({
          type: 'string',
          inputProps: {
            maxLength: 1
          },
          ...props,
          value: props.value === null ? '' : props.value,
          placeholder: props.value === null ? 'null' : ''
        }))
        .with('VARCHAR', () => ({
          type: 'string',
          inputProps: {
            maxLength: columnType.precision ?? 0 > 0 ? columnType.precision : undefined
          },
          ...props,
          value: props.value === null ? '' : props.value,
          placeholder: props.value === null ? 'null' : ''
        }))
        .with('TIMESTAMP', () => ({
          type: 'datetime-local',
          ...props,
          value: (() => {
            invariant(props.value === null || isDayjs(props.value))
            return props.value?.format('YYYY-MM-DDTHH:mm:ss') ?? ''
          })(),
          onChange: (e: ChangeEvent) =>
            props.onChange({ ...e, target: { ...e.target, value: Dayjs((e.target as any).value) } } as any)
        }))
        .with({ Interval: P._ }, () => ({
          type: 'string',
          ...props
        }))
        .with('BINARY', () => ({
          type: 'string',
          ...props
        }))
        .with('VARBINARY', () => ({
          type: 'string',
          ...props
        }))
        .with('NULL', () => ({
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

/**
 * Input component that can be in an invalid state.
 * While in invalid state, changes in input are not reflected on edited value.
 * The valid state is determined by successful serialization of an SQL value.
 */
export function IntermediateValueInput(
  props: {
    value: SQLValueJS
    toText: (v: SQLValueJS) => string
    fromText: (text: string) => JSONXgressValue
    columnType: ColumnType
    onChange: (event: ChangeEvent<HTMLInputElement>) => void
  } & Omit<TextFieldProps, 'value' | 'onChange'>
) {
  const [value, setValueText] = useReducer(
    intermediateInputReducer(
      text => xgressJSONToSQLValue(props.columnType, props.fromText(text)),
      value => props.onChange({ target: { value } } as any)
    ),
    { valid: props.value }
  )
  const theme = useTheme()
  const error = 'intermediate' in value
  return (
    <TextField
      variant='outlined'
      {...props}
      sx={{
        ...props.sx,
        backgroundColor: error ? theme.palette.error.light : 'undefined'
      }}
      error={error}
      onChange={e => setValueText(e.target.value)}
      value={
        'valid' in value && value.valid === null
          ? ''
          : 'valid' in value
            ? props.toText(sqlValueToXgressJSON(props.columnType, value.valid))
            : value.intermediate
      }
      placeholder={props.value === null ? 'null' : ''}
      InputProps={{
        endAdornment: (
          <IconButton size='small' sx={{ mr: -3 }} onClick={() => setValueText(null)}>
            <IconX></IconX>
          </IconButton>
        )
      }}
    ></TextField>
  )
}

type IntermediateInputState<T> = { valid: T | null } | { intermediate: string }

function intermediateInputReducer<T>(textToValue: (text: string) => T, setValue: Dispatch<T | null>) {
  return (_state: IntermediateInputState<T>, action: string | null): IntermediateInputState<T> => {
    try {
      const value = action === null ? null : textToValue(action)
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
}
