import openapi from '../../../../../openapi.json'

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

/**
 * Recursively updates nested objects using an updater function.
 * @param obj The input object to process (can be nested).
 * @param updater A function that takes an object and returns the same or updated object.
 * @returns A new object with updates applied.
 */
function updateNestedObjects<T extends object>(
  obj: T,
  updater: (obj: Record<string, any>) => Record<string, any>
): T {
  // Handle non-object inputs (e.g., null, primitives)
  if (obj === null || typeof obj !== 'object') {
    return obj
  }

  // Handle arrays
  if (Array.isArray(obj)) {
    return obj.map((item) => updateNestedObjects(item, updater)) as unknown as T
  }

  // Apply updater to the current object
  const updatedObj = updater({ ...obj })

  // Recursively process nested fields
  const result: Record<string, any> = {}
  for (const [key, value] of Object.entries(updatedObj)) {
    if (typeof value === 'object' && value !== null) {
      result[key] = updateNestedObjects(value, updater)
    } else {
      result[key] = value
    }
  }

  return result as T
}

export const felderaApiJsonSchemas = Object.entries(openapi.components.schemas).map(
  ([name, schema]) => ({
    uri: `file://feldera/components/schemas/${name}`,
    fileMatch: schemaFileAssociations[name],
    schema: updateNestedObjects(schema, (obj) => {
      if ('$ref' in obj) {
        return {
          $ref: obj['$ref'].slice(1)
        }
      }
      if (obj.nullable === true && 'type' in obj) {
        return {
          ...obj,
          type: [obj.type, 'null']
        }
      }
      if (obj.nullable === true && 'allOf' in obj) {
        return {
          ...obj,
          allOf: undefined,
          oneOf: [...obj.allOf, { type: 'null' }]
        }
      }
      return obj
    })
  })
)
