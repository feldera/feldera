// A create/update dialog window for a untyped/unknown connector.
//
// It just has an editor for the YAML config.

import { connectorTypeToIcon, parseEditorSchema } from '$lib/functions/connectors'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { ConnectorFormNewRequest, ConnectorFormUpdateRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { ConnectorDescr, ConnectorId, NewConnectorRequest, UpdateConnectorRequest } from '$lib/services/manager'
import { ConnectorType } from '$lib/types/connectors'
import ConnectorDialogProps from '$lib/types/connectors/ConnectorDialogProps'
import { useEffect, useState } from 'react'
import { Controller, useForm } from 'react-hook-form'
import * as yup from 'yup'

import { yupResolver } from '@hookform/resolvers/yup'
import { Icon } from '@iconify/react'
import { Editor } from '@monaco-editor/react'
import { useTheme } from '@mui/material'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import FormControl from '@mui/material/FormControl'
import FormHelperText from '@mui/material/FormHelperText'
import Grid from '@mui/material/Grid'
import IconButton from '@mui/material/IconButton'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'

import { AddConnectorCard } from './AddConnectorCard'
import Transition from './tabs/Transition'

const schema = yup
  .object({
    name: yup.string().required(),
    description: yup.string().default(''),
    config: yup.string().required()
  })
  .required()

export type EditorSchema = yup.InferType<typeof schema>

export const ConfigEditorDialog = (props: ConnectorDialogProps) => {
  const theme = useTheme()
  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'
  const [curValues, setCurValues] = useState<EditorSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseEditorSchema(props.connector))
    }
  }, [props.connector])

  const {
    control,
    reset,
    handleSubmit,
    formState: { errors }
  } = useForm<EditorSchema>({
    resolver: yupResolver(schema),
    defaultValues: {
      name: '',
      description: '',
      config: JSON.stringify(
        {
          transport: {
            name: 'transport-name',
            config: {
              property: 'value'
            }
          },
          format: {
            name: 'csv'
          }
        },
        null,
        2
      )
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
    data: EditorSchema,
    connector_id?: string
  ): [ConnectorId | undefined, NewConnectorRequest | UpdateConnectorRequest] => {
    return [
      connector_id,
      {
        name: data.name,
        description: data.description,
        config: JSON.parse(data.config)
      }
    ]
  }
  const newRequest = (data: EditorSchema): [undefined, NewConnectorRequest] => {
    return genericRequest(data) as [undefined, NewConnectorRequest]
  }
  const updateRequest = (data: EditorSchema): [ConnectorId, UpdateConnectorRequest] => {
    return genericRequest(data, props.connector?.connector_id) as [ConnectorId, UpdateConnectorRequest]
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
      {/* id is referenced by webui-tester */}
      <form id='generic-connector-form' onSubmit={handleSubmit(onSubmit)}>
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
                      id='connector-name' // referenced by webui-tester
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
                      id='connector-description' // referenced by webui-tester
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
            form='generic-connector-form'
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
  // id is referenced by webui-tester
  return (
    <AddConnectorCard
      id='generic-connector'
      icon={connectorTypeToIcon(ConnectorType.UNKNOWN)}
      title='A generic connector'
      dialog={ConfigEditorDialog}
    />
  )
}
