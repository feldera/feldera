import { GridItems } from '$lib/components/common/GridItems'
import { PresetServiceElement } from '$lib/components/connectors/dialogs/elements/PresetServiceElement'
import { tuple } from '$lib/functions/common/tuple'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { Direction } from '$lib/types/connectors'
import { TextFieldElement, useFormContext } from 'react-hook-form-mui'

import { Button, Grid, Link, Typography } from '@mui/material'

/**
 * Contains the name and description form elements for kafka input and output connectors.
 * @param props
 * @returns
 */
export const TabKafkaNameAndDesc = (props: { direction: Direction; disabled?: boolean; parentName: string }) => {
  const ctx = useFormContext()
  const onPresetChange = (preset: string | null) => {
    if (!preset) {
      ;[tuple('bootstrap_servers', [''])].forEach(([field, value]) =>
        ctx.register(props.parentName + '.' + field, { value })
      )
      return
    }
    ;['bootstrap_servers'].forEach(field => {
      const fieldName = props.parentName + '.' + field
      const value = ctx.getValues(fieldName)
      // Remove field if it is an empty string or array
      if (!(Array.isArray(value) ? value.join() : value)) {
        ctx.unregister(fieldName)
      }
    })
  }
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
        <PresetServiceElement
          serviceType='kafka'
          disabled={props.disabled}
          onChange={onPresetChange}
          parentName={props.parentName}
        ></PresetServiceElement>
        <Typography>
          To re-use the configuration in this connector you can register a &nbsp;
          <Button variant='outlined' size='small' href='/services/list#create/kafka' LinkComponent={Link}>
            new Data Service
          </Button>
        </Typography>
      </GridItems>
    </Grid>
  )
}
