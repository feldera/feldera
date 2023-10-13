import { GridItems } from '$lib/components/common/GridItems'
import { SelectElement } from 'react-hook-form-mui'

import Grid from '@mui/material/Grid'

export const SnowflakeOutputFormatDetails = () => {
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <SelectElement
          name='format_name'
          label='Data Format'
          size='small'
          id='format_name'
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
        ></SelectElement>
      </GridItems>
    </Grid>
  )
}
