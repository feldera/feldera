import { BigNumberInput } from '$lib/components/input/BigNumberInput'
import { useIntermediateInput } from '$lib/components/input/IntermediateInput'
import { NumberInput } from '$lib/components/input/NumberInput'
import {
  JSONXgressValue,
  numericRange,
  SQLValueJS,
  sqlValueToXgressJSON,
  xgressJSONToSQLValue
} from '$lib/functions/sqlValue'
import { ColumnType } from '$lib/services/manager'
import { BigNumber } from 'bignumber.js/bignumber.js'
import Dayjs, { isDayjs } from 'dayjs'
import { ChangeEvent } from 'react'
import invariant from 'tiny-invariant'
import JSONbig from 'true-json-bigint'
import { match, P } from 'ts-pattern'

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

  const extraProps = {
    InputProps: {
      endAdornment: columnType.nullable ? (
        <IconButton
          size='small'
          sx={{ mr: -3 }}
          onClick={() => props.onChange({ target: { value: null as any } } as any)}
        >
          <i className='bx bx-x' />
        </IconButton>
      ) : undefined
    }
  }
  invariant(columnType.type)
  if (columnType.type === 'ARRAY') {
    return (
      <SqlValueTextInput
        {...extraProps}
        {...{
          columnType,
          type: 'string',
          ...props,
          valueToText: JSONbig.stringify,
          fromText: text => (text === null ? null : JSONbig.parse(text))
        }}
      ></SqlValueTextInput>
    )
  }
  if (columnType.type === 'TIME') {
    return (
      <SqlValueTextInput
        {...extraProps}
        {...{
          columnType,
          type: 'string',
          step: 1,
          ...props,
          valueToText: v => v as string,
          fromText: t => t
        }}
      ></SqlValueTextInput>
    )
  }
  if (columnType.type === 'DATE') {
    return (
      <SqlValueTextInput
        {...extraProps}
        {...{ columnType, type: 'date', ...props, valueToText: v => v as string, fromText: t => t }}
      ></SqlValueTextInput>
    )
  }
  if (columnType.type === 'STRUCT') {
    return (
      <SqlValueTextInput
        {...extraProps}
        {...{ columnType, type: 'string', ...props, valueToText: v => v as string, fromText: t => t }}
      ></SqlValueTextInput>
    )
  }
  if (columnType.type === 'TINYINT' || columnType.type === 'SMALLINT' || columnType.type === 'INTEGER') {
    return (
      <NumberInput
        {...{
          ...props,
          ...extraProps,
          ...(({ min, max }) => ({ min: min.toNumber(), max: max.toNumber() }))(numericRange(columnType)),
          value: props.value as null | number,
          placeholder: props.value === null ? 'null' : ''
        }}
      ></NumberInput>
    )
  }
  if (columnType.type === 'REAL' || columnType.type === 'DOUBLE') {
    return (
      <NumberInput
        {...{
          ...props,
          ...extraProps,
          value: props.value as null | number,
          placeholder: props.value === null ? 'null' : ''
        }}
      ></NumberInput>
    )
  }
  if (columnType.type === 'BIGINT' || columnType.type === 'DECIMAL') {
    return (
      <BigNumberInput
        {...{
          ...props,
          ...extraProps,
          value: props.value as BigNumber,
          ...onChangeEmptyNull(props),
          defaultValue: props.defaultValue as BigNumber | undefined,
          precision: columnType.precision,
          scale: columnType.scale,
          placeholder: props.value === null ? 'null' : ''
        }}
      ></BigNumberInput>
    )
  }

  return (
    <TextField
      {...extraProps}
      {...match(columnType.type)
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
 * The input is valid if the serialization of an SQL value is successful.
 */
function SqlValueTextInput(
  props: {
    value: SQLValueJS
    valueToText: (v: SQLValueJS) => string
    fromText: (text: string | null) => JSONXgressValue
    columnType: ColumnType
    onChange: (event: ChangeEvent<HTMLInputElement>) => void
  } & Omit<TextFieldProps, 'value' | 'onChange'>
) {
  const intermediateInputProps = useIntermediateInput({
    ...props,
    textToValue: text => {
      try {
        return { valid: xgressJSONToSQLValue(props.columnType, props.fromText(text)) }
      } catch {
        return 'invalid'
      }
    },
    valueToText: valid => props.valueToText(sqlValueToXgressJSON(props.columnType, valid))
  })
  return (
    <TextField
      variant='outlined'
      {...props}
      {...intermediateInputProps}
      placeholder={props.value === null ? 'null' : ''}
    >
      {props.children}
    </TextField>
  )
}
