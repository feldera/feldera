import { GridItems } from '$lib/components/common/GridItems'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { Direction } from '$lib/types/connectors'
import { TextFieldElement } from 'react-hook-form-mui'

import { Grid } from '@mui/material'

/**
 * Contains the name and description form elements for kafka input and output connectors.
 * @param props
 * @returns
 */
export const TabKafkaNameAndDesc = (props: { direction: Direction; disabled?: boolean; parentName: string }) => {
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <TextFieldElement
          name='name'
          label={props.direction === Direction.OUTPUT ? 'Data Sink Name' : 'Data Source Name'}
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
          aria-describedby='validation-description'
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-datasource-description'
          }}
        />
      </GridItems>
    </Grid>
  )
}
