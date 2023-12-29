import { GridItems } from '$lib/components/common/GridItems'
import { SelectElement, SwitchElement, useWatch } from 'react-hook-form-mui'

import Grid from '@mui/material/Grid'

const TabGenericInputFormatDetails = (props: { disabled?: boolean }) => {
  const watch = useWatch()
  const selectedFormat = watch.config['format_name']

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
          <>
            <SelectElement
              name='config.update_format'
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
              disabled={props.disabled}
              inputProps={{
                'data-testid': 'input-update-format'
              }}
              SelectProps={{
                MenuProps: {
                  MenuListProps: {
                    'data-testid': 'box-update-format-options'
                  } as any
                },
                SelectDisplayProps: {
                  'data-testid': 'input-update-format-display'
                } as any
              }}
            ></SelectElement>
            <SwitchElement
              label='Records wrapped in an array'
              name='config.json_array'
              defaultValue={'false'}
              disabled={props.disabled}
              data-testid='input-is-wrapped'
            />
          </>
        )}
      </GridItems>
    </Grid>
  )
}

export default TabGenericInputFormatDetails
