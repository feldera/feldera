import { useCallback, useState } from 'react'
import { useForm, UseFormProps } from 'react-hook-form'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'

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
 * const { updateSchema, ...otherMethods } = useDynamicValidationForm<FormValues>({
 *  schema: object({})
 * })
 *
 * // We can then update the schema to additionally validate the age field by calling
 * // the updateSchema method somewhere in the application
 * updateSchema(object({ age: nonOptional(number()) }))
 *
 * // The final validation schema will be
 * object({
 *   name: nonOptional(string()),
 *   age: nonOptional(number())
 * })
 */
export const useDynamicValidationForm = <TContext = any,>(
  props?: Omit<UseFormProps<va.ObjectShape, TContext>, 'resolver'> & {
    schema?: va.ObjectSchema<va.ObjectShape>
  }
) => {
  const [schema, setSchema] = useState<va.ObjectSchema<va.ObjectShape> | null>(props?.schema ?? null)

  const updateSchema = useCallback((newSchema: va.ObjectSchema<va.ObjectShape>) => {
    setSchema(oldSchema => {
      if (oldSchema) {
        return va.merge([oldSchema, newSchema])
      }
      return newSchema
    })
  }, [])

  const form = useForm({
    ...props,
    resolver: schema ? valibotResolver(schema) : undefined
  })

  return {
    ...form,
    setSchema,
    updateSchema
  }
}
