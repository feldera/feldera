import { GridItems } from '$lib/components/common/GridItems'
import { SelectElement } from 'react-hook-form-mui'

import Grid from '@mui/material/Grid'

export const SnowflakeOutputFormatDetails = (props: { disabled?: boolean }) => {
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <SelectElement
          name='format.format_name'
          label='Data Format'
          size='small'
          options={[
            {
              id: 'json',
              label: 'JSON'
            },
            {
              id: 'avro',
              label: 'AVRO',
              disabled: true
            }
          ]}
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-data-format'
          }}
        ></SelectElement>
      </GridItems>
    </Grid>
  )
}
