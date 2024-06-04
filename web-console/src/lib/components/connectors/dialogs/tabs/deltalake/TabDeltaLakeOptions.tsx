import { FormField } from '$lib/components/forms/FormField'
import { PickFormFieldElement } from '$lib/components/services/dialogs/elements/PickFormFieldElement'
import { useDeltaLakeStorageType } from '$lib/compositions/connectors/dialogs/useDeltaLakeStorageType'
import {
  deltaLakeAwsOptions,
  deltaLakeAzureOptions,
  deltaLakeFileSystemOptions,
  deltaLakeGenericHttpOptions,
  deltaLakeGoogleOptions,
  deltaLakeNoOptions
} from '$lib/functions/deltalake/configSchema'
import { formFieldDefaultValue } from '$lib/functions/forms'
import { useWatch } from 'react-hook-form-mui'
import { match } from 'ts-pattern'

import { Box, Grid } from '@mui/material'

// Essentially, all named fields in DeltaTableReaderConfig
const omitFields = new Set(['uri', 'timestamp_column', 'mode', 'snapshot_filter', 'version', 'datetime'])

export const TabDeltaLakeOptions = (props: { parentName: string; disabled?: boolean }) => {
  const { storageType } = useDeltaLakeStorageType(props)
  const fieldOptions = match(storageType)
    .with('aws_s3', () => deltaLakeAwsOptions)
    .with('azure_blob', () => deltaLakeAzureOptions)
    .with('google_cloud_storage', () => deltaLakeGoogleOptions)
    .with('file_system', () => deltaLakeFileSystemOptions)
    .with('generic_http', () => deltaLakeGenericHttpOptions)
    .with(undefined, () => deltaLakeNoOptions)
    .exhaustive()

  const formValues = useWatch({ name: props.parentName })
  const usedFields = Object.keys(formValues).filter(field => !omitFields.has(field))

  return (
    <>
      <Box sx={{ height: '100%', overflowY: 'auto', pr: '3rem', mr: '-1rem' }}>
        <Grid container spacing={4} sx={{ height: 'auto', alignItems: 'start' }}>
          {usedFields.map(field => (
            <FormField
              key={field}
              field={field}
              fieldOptions={fieldOptions[field] ?? { type: 'string' }}
              disabled={props.disabled}
              parentName={props.parentName}
              optional={true}
            ></FormField>
          ))}

          <Grid item xs={12} sm={6}>
            <PickFormFieldElement
              parentName={props.parentName}
              options={Object.keys(fieldOptions).filter(option => !usedFields.includes(option))}
              getDefault={(field: string) => formFieldDefaultValue(fieldOptions[field] ?? { type: 'string' })}
              allowCustom
            ></PickFormFieldElement>
          </Grid>
        </Grid>
      </Box>
    </>
  )
}
