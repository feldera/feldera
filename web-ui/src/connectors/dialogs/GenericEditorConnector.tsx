// A create/update dialog window for a untyped/unknown connector.
//
// It just has an editor for the YAML config.

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
import YAML from 'yaml'
import * as yup from 'yup'
import { yupResolver } from '@hookform/resolvers/yup'
import { Controller, useForm } from 'react-hook-form'
import { useTheme } from '@mui/material'
import { Editor } from '@monaco-editor/react'

import Transition from './tabs/Transition'
import { ConnectorDescr, ConnectorType, NewConnectorRequest, UpdateConnectorRequest } from 'src/types/manager'
import { ConnectorFormNewRequest, ConnectorFormUpdateRequest } from './SubmitHandler'
import { connectorToFormSchema } from 'src/types/connectors'
import { AddConnectorCard } from './AddConnectorCard'
import ConnectorDialogProps from './ConnectorDialogProps'

const schema = yup
  .object({
    name: yup.string().required(),
    description: yup.string().default(''),
    config: yup.string().required()
  })
  .required()

export type EditorSchema = yup.InferType<typeof schema>

export const ConfigEditorDialog = (props: ConnectorDialogProps) => {
  const handleClose = () => {
    props.setShow(false)
  }
  const onFormSubmitted = (connector: ConnectorDescr | undefined) => {
    handleClose()
    if (connector !== undefined && props.onSuccess !== undefined) {
      props.onSuccess(connector)
    }
  }

  const theme = useTheme()
  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'

  // Initialize the form either with default or values from the passed in connector
  const defaultValues = props.connector
    ? connectorToFormSchema(props.connector)
    : {
        name: '',
        description: '',
        config: YAML.stringify({
          transport: {
            name: 'transport-name',
            config: {
              property: 'value'
            }
          },
          format: {
            name: 'csv'
          }
        })
      }
  const {
    control,
    handleSubmit,
    formState: { errors }
  } = useForm<EditorSchema>({
    resolver: yupResolver(schema),
    defaultValues
  })

  // Define what should happen when the form is submitted
  const genericRequest = (data: EditorSchema, connector_id?: number): NewConnectorRequest | UpdateConnectorRequest => {
    return {
      name: data.name,
      description: data.description,
      typ: ConnectorType.FILE, // TODO this will go away
      config: data.config,
      ...(connector_id && { connector_id: connector_id })
    }
  }
  const newRequest = (data: EditorSchema): NewConnectorRequest => {
    return genericRequest(data) as NewConnectorRequest
  }
  const updateRequest = (data: EditorSchema): UpdateConnectorRequest => {
    return genericRequest(data, props.connector?.connector_id) as UpdateConnectorRequest
  }
  const onSubmit =
    props.connector === undefined
      ? ConnectorFormNewRequest<EditorSchema>(onFormSubmitted, newRequest)
      : ConnectorFormUpdateRequest<EditorSchema>(onFormSubmitted, updateRequest)

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
              {props.connector === undefined ? 'Connector Editor' : 'Update ' + props.connector.name}
            </Typography>
            {props.connector === undefined && <Typography variant='body2'>Write a custom connector config</Typography>}
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
                      placeholder='AAPL'
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
                      placeholder='5 min Stock Ticker Data'
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
                  name='config'
                  control={control}
                  render={({ field }) => <Editor height='20vh' theme={vscodeTheme} defaultLanguage='yaml' {...field} />}
                />
                {errors.config && (
                  <FormHelperText sx={{ color: 'error.main' }} id='validation-config'>
                    {errors.config.message}
                  </FormHelperText>
                )}
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

export const AddGenericConnectorCard = () => {
  return <AddConnectorCard icon='file-icons:test-generic' title='A generic connector' dialog={ConfigEditorDialog} />
}
