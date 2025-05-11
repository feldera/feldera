import openapi from '../../../../openapi.json'

const schemaFileAssociations: Record<string, string[]> = {
  RuntimeConfig: ['**/RuntimeConfig.json'],
  ProgramConfig: ['**ProgramConfig**'],
  ConnectorConfig: ['**ConnectorConfig**']
}

/**
 * Updates all fields with a specific name in a nested object using an updater function.
 * @param obj The input object to process (can be nested).
 * @param fieldName The name of the fields to update.
 * @param updater A function that takes the current field value and returns the updated value.
 * @returns A new object with the specified fields updated.
 */
function updateFieldsByName<T extends object>(
  obj: T,
  fieldName: string,
  updater: (value: any) => any
): T {
  // Handle null or non-object inputs
  if (obj === null || typeof obj !== 'object') {
    return obj
  }

  // Handle arrays
  if (Array.isArray(obj)) {
    return obj.map((item) => updateFieldsByName(item, fieldName, updater)) as unknown as T
  }

  // Handle objects
  const result: Record<string, any> = {}
  for (const [key, value] of Object.entries(obj)) {
    if (key === fieldName) {
      // Apply updater to matching field
      result[key] = updater(value)
    } else if (typeof value === 'object' && value !== null) {
      // Recursively process nested objects or arrays
      result[key] = updateFieldsByName(value, fieldName, updater)
    } else {
      // Copy non-matching, non-object values as-is
      result[key] = value
    }
  }

  return result as T
}

export const felderaApiJsonSchemas = Object.entries(openapi.components.schemas).map(
  ([name, schema]) => ({
    uri: `file://feldera/components/schemas/${name}`,
    fileMatch: schemaFileAssociations[name],
    schema: updateFieldsByName(schema, '$ref', (val: string) => val.slice(1)) as any
  })
)
