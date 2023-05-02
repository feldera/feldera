// A create/update dialog window for the CSV file connector.

import Box from '@mui/material/Box'
import Dialog from '@mui/material/Dialog'
import Button from '@mui/material/Button'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'
import DialogContent from '@mui/material/DialogContent'
import Grid from '@mui/material/Grid'
import FormControl from '@mui/material/FormControl'
import TextField from '@mui/material/TextField'
import FormControlLabel from '@mui/material/FormControlLabel'
import Switch from '@mui/material/Switch'
import DialogActions from '@mui/material/DialogActions'
import FormHelperText from '@mui/material/FormHelperText'
import { Icon } from '@iconify/react'
import YAML from 'yaml'
import * as yup from 'yup'
import { yupResolver } from '@hookform/resolvers/yup'
import { Controller, useForm } from 'react-hook-form'

import Transition from './tabs/Transition'
import { ConnectorDescr, ConnectorType, NewConnectorRequest, UpdateConnectorRequest } from 'src/types/manager'
import { ConnectorFormNewRequest, ConnectorFormUpdateRequest } from './SubmitHandler'
import { connectorTypeToConfig, parseCsvFileSchema } from 'src/types/connectors'
import { AddConnectorCard } from './AddConnectorCard'
import ConnectorDialogProps from './ConnectorDialogProps'
import { PLACEHOLDER_VALUES } from 'src/utils'
import { useEffect, useState } from 'react'

const schema = yup
  .object({
    name: yup.string().required(),
    description: yup.string().default(''),
    url: yup.string().required(),
    has_headers: yup.boolean().default(true)
  })
  .required()

export type CsvFileSchema = yup.InferType<typeof schema>

export const CsvFileConnectorDialog = (props: ConnectorDialogProps) => {
  const [curValues, setCurValues] = useState<CsvFileSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseCsvFileSchema(props.connector))
    }
  }, [props.connector])

  const {
    control,
    reset,
    handleSubmit,
    formState: { errors }
  } = useForm<CsvFileSchema>({
    resolver: yupResolver(schema),
    defaultValues: {
      name: '',
      description: '',
      url: '',
      has_headers: true
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
  const genericRequest = (data: CsvFileSchema, connector_id?: number): NewConnectorRequest | UpdateConnectorRequest => {
    return {
      name: data.name,
      description: data.description,
      typ: ConnectorType.FILE,
      config: YAML.stringify({
        transport: {
          name: connectorTypeToConfig(ConnectorType.FILE),
          config: {
            path: data.url
          }
        },
        format: {
          name: 'csv'
        }
      }),
      ...(connector_id && { connector_id: connector_id })
    }
  }
  const newRequest = (data: CsvFileSchema): NewConnectorRequest => {
    return genericRequest(data) as NewConnectorRequest
  }
  const updateRequest = (data: CsvFileSchema): UpdateConnectorRequest => {
    return genericRequest(data, props.connector?.connector_id) as UpdateConnectorRequest
  }
  const onSubmit =
    props.connector === undefined
      ? ConnectorFormNewRequest<CsvFileSchema>(onFormSubmitted, newRequest)
      : ConnectorFormUpdateRequest<CsvFileSchema>(onFormSubmitted, updateRequest)

  return (
    <Dialog
      fullWidth
      open={props.show}
      maxWidth='md'
      scroll='body'
      onClose={() => props.setShow(false)}
      TransitionComponent={Transition}
    >
      <form id='create-csv-file' onSubmit={handleSubmit(onSubmit)}>
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
              {props.connector === undefined ? 'New CSV File' : 'Update ' + props.connector.name}
            </Typography>
            {props.connector === undefined && <Typography variant='body2'>Provide the URL to a CSV file</Typography>}
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
                      label='File Path'
                      placeholder='data.csv'
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
              <FormControl fullWidth>
                <Controller
                  name='has_headers'
                  control={control}
                  render={({ field }) => (
                    <FormControlLabel
                      control={<Switch checked={field.value} />}
                      label='CSV file has headers'
                      aria-describedby='header-description'
                      {...field}
                    />
                  )}
                />
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
            form='create-csv-file'
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

export const AddCsvFileConnectorCard = () => {
  return (
    <AddConnectorCard
      icon='ph:file-csv'
      title='Provide data in the form of CSV files'
      dialog={CsvFileConnectorDialog}
    />
  )
}
