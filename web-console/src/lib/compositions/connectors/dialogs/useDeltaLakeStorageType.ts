import { DeltaLakeStorageType, inferDeltaLakeStorageConfig } from '$lib/functions/deltalake/inferConfig'
import { useEffect, useReducer } from 'react'
import { useFormContext, useWatch } from 'react-hook-form'

const getInferred = (uri: string) => {
  const inferred = inferDeltaLakeStorageConfig(uri)

  return { inferred, uri }
}
const inferredFields = (
  inferred:
    | {
        config: Record<string, string | number | boolean>
      }
    | undefined
) => (inferred ? Object.keys(inferred.config) : [])

export const useDeltaLakeStorageType = ({ parentName }: { parentName: string }) => {
  const ctx = useFormContext()
  const uri = useWatch<Record<string, string>>({ name: parentName + '.uri' })

  const handleAutofill = ({
    removeFields,
    inferred
  }: {
    removeFields: string[]
    inferred:
      | {
          config: Record<string, string | number | boolean>
          type: DeltaLakeStorageType
        }
      | undefined
  }) => {
    removeFields.forEach(key => {
      ctx.unregister(parentName + '.' + key)
    })

    if (!inferred) {
      return
    }
    Object.entries(inferred.config).forEach(([key, value]) => {
      ctx.setValue(parentName + '.' + key, value)
    })
  }

  const [{ inferred }, inferStorageType] = useReducer(
    (
      state: {
        inferred: { type: DeltaLakeStorageType; config: Record<string, string | number | boolean> } | undefined
        uri: string
      },
      uri: string
    ) => {
      if (uri === state.uri) {
        return state
      }
      const inferred = inferDeltaLakeStorageConfig(uri)
      // TODO: this enables autofilling inferred options from URI. Enable if needed.
      // Otherwise `useDeltaLakeStorageType` Hook can be simplified
      // handleAutofill({ inferred, removeFields: inferredFields(state.inferred) })
      void handleAutofill
      void inferredFields
      return { uri, inferred }
    },
    getInferred(uri)
  )

  useEffect(() => inferStorageType(uri), [uri])

  return {
    storageType: inferred?.type
  }
}
