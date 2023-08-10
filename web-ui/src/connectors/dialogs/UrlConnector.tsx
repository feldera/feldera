// A create/update dialog window for the URL connector.

import Box from '@mui/material/Box'
import Dialog from '@mui/material/Dialog'
import Button from '@mui/material/Button'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'
import DialogContent from '@mui/material/DialogContent'
import Grid from '@mui/material/Grid'
import FormControl from '@mui/material/FormControl'
import TextField from '@mui/material/TextField'
import DialogActions from '@mui/material/DialogActions'
import FormHelperText from '@mui/material/FormHelperText'
import { Icon } from '@iconify/react'
import * as yup from 'yup'
import { yupResolver } from '@hookform/resolvers/yup'
import { Controller, useForm } from 'react-hook-form'

import Transition from './tabs/Transition'
import { ConnectorDescr, ConnectorId, NewConnectorRequest, UpdateConnectorRequest } from 'src/types/manager'
import { ConnectorFormNewRequest, ConnectorFormUpdateRequest } from './SubmitHandler'
import { connectorTypeToConfig, parseUrlSchema, ConnectorType } from 'src/types/connectors'
import { AddConnectorCard } from './AddConnectorCard'
import ConnectorDialogProps from './ConnectorDialogProps'
import { PLACEHOLDER_VALUES } from 'src/utils'
import { useEffect, useState } from 'react'
import { InputLabel, MenuItem, Select } from '@mui/material'

const schema = yup
  .object({
    name: yup.string().required(),
    description: yup.string().default(''),
    url: yup.string().required(),
    format: yup.string().oneOf(['csv', 'json']).default('csv')
  })
  .required()

export type UrlSchema = yup.InferType<typeof schema>

export const UrlConnectorDialog = (props: ConnectorDialogProps) => {
  const [curValues, setCurValues] = useState<UrlSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseUrlSchema(props.connector))
    }
  }, [props.connector])

  const {
    control,
    reset,
    handleSubmit,
    formState: { errors }
  } = useForm<UrlSchema>({
    resolver: yupResolver(schema),
    defaultValues: {
      name: '',
      description: '',
      url: '',
      format: 'csv'
    },
    values: curValues
  })

  const handleClose = () => {
    reset()
    props.setShow(false)
  }
  const onFormSubmitted = (connector: ConnectorDescr | undefined) => {
    handleClose()
    if (connector !== undefined && props.onSuccess !== undefined) {
      props.onSuccess(connector)
    }
  }

  // Define what should happen when the form is submitted
  const genericRequest = (
    data: UrlSchema,
    connector_id?: string
  ): [ConnectorId | undefined, NewConnectorRequest | UpdateConnectorRequest] => {
    return [
      connector_id,
      {
        name: data.name,
        description: data.description,
        config: {
          transport: {
            name: connectorTypeToConfig(ConnectorType.URL),
            config: {
              path: data.url
            }
          },
          format: {
            name: data.format
          }
        }
      }
    ]
  }

  const newRequest = (data: UrlSchema): [undefined, NewConnectorRequest] => {
    return genericRequest(data) as [undefined, NewConnectorRequest]
  }
  const updateRequest = (data: UrlSchema): [ConnectorId, UpdateConnectorRequest] => {
    return genericRequest(data, props.connector?.connector_id) as [ConnectorId, UpdateConnectorRequest]
  }

  const onSubmit =
    props.connector === undefined
      ? ConnectorFormNewRequest<UrlSchema>(onFormSubmitted, newRequest)
      : ConnectorFormUpdateRequest<UrlSchema>(onFormSubmitted, updateRequest)

  return (
    <Dialog
      fullWidth
      open={props.show}
      maxWidth='md'
      scroll='body'
      onClose={() => props.setShow(false)}
      TransitionComponent={Transition}
    >
      <form id='create-url-resource' onSubmit={handleSubmit(onSubmit)}>
        <DialogContent sx={{ pb: 8, px: { xs: 8, sm: 15 }, pt: { xs: 8, sm: 12.5 }, position: 'relative' }}>
          <IconButton
            size='small'
            onClick={() => props.setShow(false)}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {props.connector === undefined ? 'New URL' : 'Update ' + props.connector.name}
            </Typography>
            {props.connector === undefined && <Typography variant='body2'>Provide the URL to a data source</Typography>}
          </Box>
          <Grid container spacing={6}>
            <Grid item sm={4} xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='name'
                  control={control}
                  render={({ field }) => (
                    <TextField
                      label='Datasource Name'
                      placeholder={PLACEHOLDER_VALUES['connector_name']}
                      error={Boolean(errors.name)}
                      aria-describedby='validation-name'
                      {...field}
                    />
                  )}
                />
                {errors.name && (
                  <FormHelperText sx={{ color: 'error.main' }} id='validation-name'>
                    {errors.name.message}
                  </FormHelperText>
                )}
              </FormControl>
            </Grid>
            <Grid item sm={8} xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='description'
                  control={control}
                  render={({ field }) => (
                    <TextField
                      fullWidth
                      label='Description'
                      placeholder={PLACEHOLDER_VALUES['connector_description']}
                      error={Boolean(errors.description)}
                      aria-describedby='validation-description'
                      {...field}
                    />
                  )}
                />
                {errors.description && (
                  <FormHelperText sx={{ color: 'error.main' }} id='validation-description'>
                    {errors.description.message}
                  </FormHelperText>
                )}
              </FormControl>
            </Grid>
            <Grid item sm={12} xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='url'
                  control={control}
                  render={({ field }) => (
                    <TextField
                      fullWidth
                      label='URL'
                      placeholder='https://gist.githubusercontent.com/...'
                      error={Boolean(errors.description)}
                      aria-describedby='validation-description'
                      {...field}
                    />
                  )}
                />
                {errors.url && (
                  <FormHelperText sx={{ color: 'error.main' }} id='validation-description'>
                    {errors.url.message}
                  </FormHelperText>
                )}
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <FormControl>
                <InputLabel id='format-label'>Format</InputLabel>
                <Controller
                  name='format'
                  control={control}
                  render={({ field }) => (
                    <Select label='Format' id='format' {...field}>
                      <MenuItem value='csv'>CSV</MenuItem>
                      <MenuItem value='json'>JSON</MenuItem>
                    </Select>
                  )}
                />
                <FormHelperText>The data format of the resource.</FormHelperText>
              </FormControl>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions sx={{ pb: { xs: 8, sm: 12.5 }, justifyContent: 'center' }}>
          <Button
            variant='contained'
            sx={{ mr: 1 }}
            color='success'
            endIcon={<Icon icon='bx:check' />}
            form='create-url-resource'
            type='submit'
          >
            {props.connector !== undefined ? 'Update' : 'Create'}
          </Button>
          <Button variant='outlined' color='secondary' onClick={() => props.setShow(false)}>
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

export const AddUrlConnectorCard = () => {
  return <AddConnectorCard icon='tabler:http-get' title='Load data from an HTTP URL' dialog={UrlConnectorDialog} />
}
