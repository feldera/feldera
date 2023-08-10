// Workaround limitation of react-hook-form that can't update the object schema
// dynamically.
//
// See also here for more details:
// https://github.com/orgs/react-hook-form/discussions/3972

import { useCallback, useState } from 'react'
import { FieldValues, useForm, UseFormProps, UseFormReturn } from 'react-hook-form'
import * as yup from 'yup'
import { yupResolver } from '@hookform/resolvers/yup'

type WidenSchema<T> = T extends (...args: any[]) => any
  ? T
  : { [K in keyof T]: T[K] extends Record<string, unknown> ? WidenSchema<T[K]> : any }

export type useCustomFormSchema<TFieldValues> = yup.ObjectSchema<
  object | undefined,
  Partial<WidenSchema<TFieldValues>>,
  object
>

export type UseCustomFormReturn<TFieldValues extends FieldValues = FieldValues, TContext = any> = UseFormReturn<
  TFieldValues,
  TContext
> & {
  updateSchema: (newSchema: useCustomFormSchema<TFieldValues>) => void
}

/**
 * A custom useForm hook that allows dynamic validation schemas
 * @example
 * // Let's say we have a form with fields described by the following interface
 * interface FormValues {
 *  name: string
 *  age: number
 * }
 *
 * // And we setup the form with the following schema (only validating the name)
 * const { updateSchema, ...otherMethods } = useCustomForm<FormValues>({
 *  schema: yup.object().shape({})
 * })
 *
 * // We can then update the schema to additionally validate the age field by calling
 * // the updateSchema method somewhere in the application
 * updateSchema(yup.object().shape({ age: yup.number().required() }))
 *
 * // The final validation schema will be
 * yup.object().shape({
 *   name: yup.string().required(),
 *   age: yup.number().required()
 * })
 */
export const useCustomForm = <TFieldValues extends FieldValues = FieldValues, TContext = any>(
  props?: Omit<UseFormProps<TFieldValues, TContext>, 'resolver'> & {
    schema?: useCustomFormSchema<TFieldValues>
  }
): UseCustomFormReturn<TFieldValues> => {
  const [schema, setSchema] = useState<useCustomFormSchema<TFieldValues> | null>(props?.schema ?? null)

  const updateSchema = useCallback((newSchema: useCustomFormSchema<TFieldValues>) => {
    setSchema(oldSchema => {
      if (oldSchema) {
        return yup.object().shape({
          ...oldSchema.fields,
          ...newSchema.fields
        }) as useCustomFormSchema<TFieldValues>
      }
      return newSchema
    })
  }, [])

  const form = useForm({
    ...props,
    resolver: schema ? yupResolver<useCustomFormSchema<TFieldValues>>(schema) : undefined
  })

  return {
    ...form,
    updateSchema
  }
}
