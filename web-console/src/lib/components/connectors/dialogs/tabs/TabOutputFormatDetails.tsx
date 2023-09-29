import { GridItems } from '$lib/components/common/GridItems'
import { KafkaOutputSchema } from '$lib/components/connectors/dialogs'
import { SelectElement, SwitchElement, useFormContext } from 'react-hook-form-mui'

import Grid from '@mui/material/Grid'

const TabOutputFormatDetails = () => {
  const selectedFormat = useFormContext<KafkaOutputSchema>().watch('format_name')

  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <SelectElement
          name='format_name'
          label='Data Format'
          size='small'
          options={[
            {
              id: 'json',
              label: 'JSON'
            },
            {
              id: 'csv',
              label: 'CSV'
            }
          ]}
        ></SelectElement>

        {selectedFormat === 'json' && (
          <SwitchElement name='json_array' label='Wrap records in an array' defaultValue='false' />
        )}
      </GridItems>
    </Grid>
  )
}

export default TabOutputFormatDetails
