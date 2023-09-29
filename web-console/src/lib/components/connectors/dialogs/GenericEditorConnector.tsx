// A create/update dialog window for a untyped/unknown connector.
//
// It just has an editor for the YAML config.
'use client'

import { parseEditorSchema } from '$lib/functions/connectors'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import ConnectorDialogProps from '$lib/types/connectors/ConnectorDialogProps'
import { useEffect, useState } from 'react'
import { Controller } from 'react-hook-form'
import { FormContainer, TextFieldElement, useFormContext } from 'react-hook-form-mui'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
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

import Transition from './tabs/Transition'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(2)])),
  description: va.optional(va.string(), ''),
  config: va.nonOptional(va.string())
})

export type EditorSchema = va.Input<typeof schema>

export const ConfigEditorDialog = (props: ConnectorDialogProps) => {
  const [curValues, setCurValues] = useState<EditorSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseEditorSchema(props.connector))
    }
  }, [props.connector])

  const defaultValues: EditorSchema = {
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
  }

  const handleClose = () => {
    props.setShow(false)
  }
  // Define what should happen when the form is submitted
  const prepareData = (data: EditorSchema) => ({ ...data, config: JSON.parse(data.config) })

  const onSubmit = useConnectorRequest(props.connector, prepareData, props.onSuccess, handleClose)

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
      <FormContainer
        resolver={valibotResolver(schema)}
        values={curValues}
        defaultValues={defaultValues}
        FormProps={{ id: 'generic-connector-form' }}
        onSuccess={onSubmit}
      >
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
          <GenericEditorForm></GenericEditorForm>
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
      </FormContainer>
    </Dialog>
  )
}

const GenericEditorForm = () => {
  const theme = useTheme()
  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'
  const ctx = useFormContext()
  return (
    <Grid container spacing={4}>
      <Grid item sm={4} xs={12}>
        <TextFieldElement
          name='name'
          label='Data Source Name'
          size='small'
          fullWidth
          id='connector-name' // referenced by webui-tester
          placeholder={PLACEHOLDER_VALUES['connector_name']}
          aria-describedby='validation-name'
        />
      </Grid>
      <Grid item sm={8} xs={12}>
        <TextField
          name='description'
          label='Description'
          size='small'
          fullWidth
          id='connector-description' // referenced by webui-tester
          placeholder={PLACEHOLDER_VALUES['connector_description']}
          aria-describedby='validation-description'
        />
      </Grid>
      <Grid item sm={12} xs={12}>
        <FormControl fullWidth>
          <Controller
            name='config'
            control={ctx.control}
            render={({ field: { ref, ...field } }) => (
              void ref, (<Editor height='20vh' theme={vscodeTheme} defaultLanguage='yaml' {...field} />)
            )}
          />
          {(e =>
            e && (
              <FormHelperText sx={{ color: 'error.main' }} id='validation-config'>
                {e.message}
              </FormHelperText>
            ))(ctx.getFieldState('config').error)}
        </FormControl>
      </Grid>
    </Grid>
  )
}
