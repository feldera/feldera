import { LibrdkafkaOptionElement } from '$lib/components/services/dialogs/elements/LibrdkafkaOptionElement'
import { PickLibrdkafkaOptionElement } from '$lib/components/services/dialogs/elements/PickLibrdkafkaOptionElement'
import { nubLast } from '$lib/functions/common/array'
import { LibrdkafkaOptions, librdkafkaOptions } from '$lib/functions/kafka/librdkafkaOptions'
import { useFormContext, useWatch } from 'react-hook-form-mui'
import Markdown from 'react-markdown'
import IconX from '~icons/bx/x'

import { Box, Grid, IconButton, Link, Tooltip, Typography, useTheme } from '@mui/material'

export const LibrdkafkaOptionsElement = (props: {
  disabled?: boolean
  parentName: string

  fieldOptions: Record<string, Omit<LibrdkafkaOptions, 'name'>>
  requiredFields: string[]
}) => {
  const formValues = useWatch({ name: props.parentName })
  const ctx = useFormContext()
  const usedFields = nubLast(
    props.requiredFields.concat(Object.keys(formValues).filter(field => props.fieldOptions[field]))
  )
  const theme = useTheme()

  return (
    <>
      <Box sx={{ height: '100%', overflowY: 'auto', pr: '3rem', mr: '-1rem' }}>
        <Grid container spacing={4} sx={{ height: 'auto' }}>
          {usedFields.map(field => (
            <>
              <Grid item xs={12} sm={6} display='flex' alignItems='center'>
                <Tooltip
                  slotProps={{
                    tooltip: {
                      sx: {
                        backgroundColor: theme.palette.background.default,
                        color: theme.palette.text.primary,
                        fontSize: 14
                      }
                    }
                  }}
                  title={
                    <Markdown>
                      {
                        (optionName => librdkafkaOptions.find(option => option.name === optionName))(
                          field.replaceAll('_', '.')
                        )?.description
                      }
                    </Markdown>
                  }
                  disableInteractive
                >
                  <Typography>{field.replaceAll('_', '.')}</Typography>
                </Tooltip>
              </Grid>
              <Grid item xs={12} sm={6}>
                <Box sx={{ display: 'flex' }}>
                  <LibrdkafkaOptionElement
                    fieldOptions={props.fieldOptions[field]}
                    field={field}
                    disabled={props.disabled}
                    parentName={props.parentName}
                  ></LibrdkafkaOptionElement>
                  {props.requiredFields.includes(field) ? (
                    <></>
                  ) : (
                    <IconButton
                      size='small'
                      sx={{ mr: '-2.5rem', ml: '0.5rem' }}
                      onClick={() => ctx.unregister(props.parentName + '.' + field)}
                    >
                      <IconX></IconX>
                    </IconButton>
                  )}
                </Box>
              </Grid>
            </>
          ))}

          <Grid item xs={12} sm={6}>
            <PickLibrdkafkaOptionElement
              parentName={props.parentName}
              fieldOptions={props.fieldOptions}
              usedFields={usedFields}
            ></PickLibrdkafkaOptionElement>
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
