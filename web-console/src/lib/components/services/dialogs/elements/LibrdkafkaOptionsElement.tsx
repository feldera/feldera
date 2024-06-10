import { FormField } from '$lib/components/forms/FormField'
import { PickFormFieldElement } from '$lib/components/services/dialogs/elements/PickFormFieldElement'
import { nubLast } from '$lib/functions/common/array'
import { formFieldDefaultValue } from '$lib/functions/forms'
import { LibrdkafkaOptions } from '$lib/functions/kafka/librdkafkaOptions'
import { useWatch } from 'react-hook-form-mui'

import { Box, Grid, Link, Typography } from '@mui/material'

export const LibrdkafkaOptionsElement = (props: {
  disabled?: boolean
  parentName: string

  fieldOptions: Record<string, LibrdkafkaOptions>
  requiredFields: string[]
}) => {
  const formValues = useWatch({ name: props.parentName })
  const usedFields = nubLast(
    props.requiredFields.concat(Object.keys(formValues).filter(field => props.fieldOptions[field]))
  )

  return (
    <>
      <Box sx={{ height: '100%', overflowY: 'auto', pr: '3rem', mr: '-1rem' }}>
        <Grid container spacing={4} sx={{ height: 'auto' }}>
          {usedFields.map(field => (
            <FormField
              key={field}
              field={field}
              fieldOptions={props.fieldOptions[field]}
              disabled={props.disabled}
              parentName={props.parentName}
              optional={!props.requiredFields.includes(field)}
            ></FormField>
          ))}

          <Grid item xs={12} sm={6}>
            <PickFormFieldElement
              parentName={props.parentName}
              options={Object.keys(props.fieldOptions)
                .filter(option => !usedFields.includes(option))
                .map(option => option.replaceAll('_', '.'))}
              getDefault={(field: string) => formFieldDefaultValue(props.fieldOptions[field])}
            ></PickFormFieldElement>
          </Grid>
        </Grid>
      </Box>
      <Typography sx={{ mt: 'auto', pt: 4 }}>
        See{' '}
        <Link href='https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md'>
          librdkafka documentation
        </Link>{' '}
        for reference
      </Typography>
    </>
  )
}
