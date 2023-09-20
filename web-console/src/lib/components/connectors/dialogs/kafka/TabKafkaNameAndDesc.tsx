// Contains the name and description form elements for kafka input and output
// connectors.

import { GridItems } from '$lib/components/common/GridItems'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { TextFieldElement } from 'react-hook-form-mui'

import { Grid } from '@mui/material'

const TabKafkaNameAndDesc = () => {
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <TextFieldElement
          name='name'
          label='Datasource Name'
          size='small'
          fullWidth
          placeholder={PLACEHOLDER_VALUES['connector_name']}
          aria-describedby='validation-name'
        />
        <TextFieldElement
          name='description'
          label='Description'
          size='small'
          fullWidth
          placeholder={PLACEHOLDER_VALUES['connector_description']}
          aria-describedby='validation-description'
        />
      </GridItems>
    </Grid>
  )
}

export default TabKafkaNameAndDesc
