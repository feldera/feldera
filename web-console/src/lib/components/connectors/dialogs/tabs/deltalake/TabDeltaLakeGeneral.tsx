import { GridItems } from '$lib/components/common/GridItems'
import { FormTooltip } from '$lib/components/forms/Tooltip'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { Direction } from '$lib/types/connectors'
import { TextFieldElement } from 'react-hook-form-mui'
import { useDeltaLakeStorageType } from 'src/lib/compositions/connectors/dialogs/useDeltaLakeStorageType'
import { match } from 'ts-pattern'

import { Chip, Grid } from '@mui/material'

/**
 * Contains the name and description form elements for kafka input and output connectors.
 * @param props
 * @returns
 */
export const TabDeltaLakeGeneral = (props: { direction: Direction; disabled?: boolean; parentName: string }) => {
  const { storageType } = useDeltaLakeStorageType(props)

  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <TextFieldElement
          name='name'
          label={props.direction === Direction.OUTPUT ? 'DeltaLake Sink Name' : 'DeltaLake Source Name'}
          size='small'
          fullWidth
          placeholder={PLACEHOLDER_VALUES['connector_name']}
          aria-describedby='validation-name'
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-datasource-name'
          }}
        />
        <TextFieldElement
          name='description'
          label='Description'
          size='small'
          fullWidth
          placeholder={PLACEHOLDER_VALUES['connector_description']}
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-datasource-description'
          }}
        />
        <FormTooltip
          title={`Delta Table storage URI.

The following URI types are recognized:
* AWS S3 - compatible URL
* Google Cloud Storage URL
* Microsoft Azure Storage URL
* HTTP(S) URL of a generic resource
* File System URI
`}
        >
          <TextFieldElement
            name={props.parentName + '.' + 'uri'}
            size='small'
            label='DeltaLake storage URI'
            placeholder='E.g. s3://bucket/path, abfs://container.dfs.core.windows.net/path ...'
            fullWidth
            inputProps={{
              'data-testid': 'input-storage-uri'
            }}
          ></TextFieldElement>
        </FormTooltip>
        {match(storageType)
          .with(undefined, () => <Chip sx={{ width: 200 }} label='URI not recognized'></Chip>)
          .with('aws_s3', () => (
            <Chip sx={{ width: 200 }} label='AWS S3 compatible URI' color='success' variant='outlined'></Chip>
          ))
          .with('google_cloud_storage', () => (
            <Chip sx={{ width: 200 }} label='Google Cloud URI' color='success' variant='outlined'></Chip>
          ))
          .with('azure_blob', () => (
            <Chip sx={{ width: 200 }} label='Azure blob URI' color='success' variant='outlined'></Chip>
          ))
          .with('generic_http', () => (
            <Chip sx={{ width: 200 }} label='Generic HTTP URI' color='success' variant='outlined'></Chip>
          ))
          .with('file_system', () => (
            <Chip sx={{ width: 200 }} label='File system URI' color='success' variant='outlined'></Chip>
          ))
          .exhaustive()}
      </GridItems>
    </Grid>
  )
}
