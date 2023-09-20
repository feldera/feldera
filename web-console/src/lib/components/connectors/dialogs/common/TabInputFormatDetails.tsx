import { GridItems } from '$lib/components/common/GridItems'
import { SelectElement, SwitchElement, useWatch } from 'react-hook-form-mui'

import Grid from '@mui/material/Grid'

const TabInputFormatDetails = () => {
  const watch = useWatch()
  const selectedFormat = watch['format_name']

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
              id: 'csv',
              label: 'CSV'
            }
          ]}
        ></SelectElement>

        {selectedFormat === 'json' && (
          <>
            <SelectElement
              name='json_update_format'
              label='Update Format'
              size='small'
              options={[
                {
                  id: 'raw',
                  label: 'Raw'
                },
                {
                  id: 'insert_delete',
                  label: 'Insert & Delete'
                }
              ]}
            ></SelectElement>
            <SwitchElement label='Records wrapped in an array' name='json_array' defaultValue={'false'} />
          </>
        )}
      </GridItems>
    </Grid>
  )
}

export default TabInputFormatDetails
