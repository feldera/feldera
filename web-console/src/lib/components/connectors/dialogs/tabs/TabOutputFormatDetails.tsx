import { GridItems } from '$lib/components/common/GridItems'
import { KafkaOutputSchema } from '$lib/components/connectors/dialogs'
import { SelectElement, SwitchElement, useFormContext } from 'react-hook-form-mui'

import Grid from '@mui/material/Grid'

const TabOutputFormatDetails = (props: { disabled?: boolean }) => {
  const selectedFormat = useFormContext<KafkaOutputSchema>().watch('config.format_name')

  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <SelectElement
          name='config.format_name'
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
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-data-format'
          }}
        ></SelectElement>

        {selectedFormat === 'json' && (
          <SwitchElement
            name='config.json_array'
            label='Wrap records in an array'
            defaultValue='false'
            disabled={props.disabled}
            data-testid='input-is-wrapped'
          />
        )}
      </GridItems>
    </Grid>
  )
}

export default TabOutputFormatDetails
