/**
 * Helpers for `openapi-ts.config.ts` that replace a generated type with a
 * hand-written one.
 *
 * The generator maps every schema to a structural TypeScript type, so a value
 * that carries a unit or an identity in the domain arrives as a bare `number`
 * or `string`. Overriding takes two steps: mark the schema with a custom
 * `format`, then resolve that format to an imported type.
 */

import type { OpenApiSchemaObject, UserConfig } from '@hey-api/openapi-ts'

/** Hand-written type substituted for a marked schema. */
export type CustomType = {
  /** Module the type is imported from, as written in the generated file. */
  module: string
  /** Exported name of the type. */
  name: string
}

/** Custom types keyed by the `format` that marks a schema as using them. */
export type CustomTypes = Readonly<Record<string, CustomType>>

type Plugins = NonNullable<UserConfig['plugins']>[number]
type TypeScriptPlugin = Extract<Plugins, { name: '@hey-api/typescript' }>

/** The schema a `parser.patch.schemas` callback receives, across spec versions. */
type PatchedSchema =
  | OpenApiSchemaObject.V2_0_X
  | OpenApiSchemaObject.V3_0_X
  | OpenApiSchemaObject.V3_1_X

/**
 * Marks `property` of `schema` with a custom `format`, so that
 * [`typescriptPluginWithCustomTypes`] substitutes the matching type.
 *
 * Throws when the property is absent, which is how a renamed field surfaces:
 * silently generating `number` again would drop the override without a trace.
 */
export const overrideOpenapiType = (schema: PatchedSchema, property: string, format: string) => {
  const properties = (schema as { properties?: Record<string, { format?: string }> }).properties
  const target = properties?.[property]
  if (!target) {
    throw new Error(`Cannot mark '${property}' as '${format}': the property is missing`)
  }
  target.format = format
}

/**
 * The TypeScript plugin, resolving every schema marked with a custom `format`
 * to the corresponding type instead of the structural default. The generated
 * file imports the type from its module.
 *
 * Only numeric schemas are covered; extend with the `string` resolver when a
 * custom type is needed there.
 */
export const customTypesPlugin = (customTypes: CustomTypes): TypeScriptPlugin => ({
  name: '@hey-api/typescript',
  $resolvers: {
    number: (ctx) => {
      const customType = customTypes[ctx.schema.format as string]
      if (!customType) {
        return undefined
      }
      const symbol = ctx.plugin.symbolFactory.register(customType.name, {
        external: customType.module,
        importKind: 'named',
        kind: 'type'
      })
      return ctx.$.type(symbol)
    }
  }
})
