import { FormFieldElement } from '$lib/components/services/dialogs/elements/FormFieldElement'
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
import { useFormContext, useWatch } from 'react-hook-form-mui'
import { match } from 'ts-pattern'

import { Box, Grid, IconButton, Typography } from '@mui/material'

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
  const ctx = useFormContext()
  const usedFields = Object.keys(formValues).filter(field => field !== 'uri') // .filter(field => fieldOptions[field])

  return (
    <>
      <Box sx={{ height: '100%', overflowY: 'auto', pr: '3rem', mr: '-1rem' }}>
        <Grid container spacing={4} sx={{ height: 'auto', alignItems: 'center' }}>
          {usedFields.map(field => (
            <>
              <Grid item xs={12} sm={6}>
                <Typography>{field}</Typography>
              </Grid>
              <Grid item xs={12} sm={6}>
                <Box sx={{ display: 'flex' }}>
                  <FormFieldElement
                    fieldOptions={fieldOptions[field] ?? { type: 'string' }}
                    field={field}
                    disabled={props.disabled}
                    parentName={props.parentName}
                  ></FormFieldElement>
                  <IconButton
                    size='small'
                    sx={{ mr: '-2.5rem', ml: '0.5rem' }}
                    onClick={() => ctx.unregister(props.parentName + '.' + field)}
                  >
                    <i className='bx bx-x' />
                  </IconButton>
                </Box>
              </Grid>
            </>
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
