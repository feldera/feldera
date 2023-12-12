import { GridItems } from '$lib/components/common/GridItems'
import { DebeziumInputSchema } from '$lib/components/connectors/dialogs/DebeziumInputConnector'
import { SelectElement, useWatch } from 'react-hook-form-mui'

import Grid from '@mui/material/Grid'

export const DebeziumInputFormatDetails = (props: { disabled?: boolean }) => {
  const watch = useWatch<DebeziumInputSchema>()
  const selectedFormat = watch.config!['format_name']

  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        {selectedFormat === 'json' && (
          <>
            <SelectElement
              name='config.json_flavor'
              label='Source database'
              size='small'
              options={[
                {
                  id: 'debezium_cassandra',
                  label: 'Cassandra',
                  disabled: true
                },
                {
                  id: 'debezium_mongodb',
                  label: 'MongoDB',
                  disabled: true
                },
                {
                  id: 'debezium_mysql',
                  label: 'MySQL'
                },
                {
                  id: 'debezium_oracle',
                  label: 'Oracle',
                  disabled: true
                },
                {
                  id: 'debezium_postgresql',
                  label: 'PostgreSQL',
                  disabled: true
                }
              ]}
              disabled={props.disabled}
            ></SelectElement>
          </>
        )}
        <SelectElement
          name='config.format_name'
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
          disabled={props.disabled}
        ></SelectElement>
      </GridItems>
    </Grid>
  )
}
