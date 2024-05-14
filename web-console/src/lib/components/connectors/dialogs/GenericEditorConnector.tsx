// A create/update dialog window for a untyped/unknown connector.
//
// It just has an editor for the YAML config.
'use client'

import { GenericEditorForm } from '$lib/components/connectors/dialogs/tabs/GenericConnectorForm'
import Transition from '$lib/components/connectors/dialogs/tabs/Transition'
import { parseEditorSchema } from '$lib/functions/connectors'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { Direction } from '$lib/types/connectors'
import { ConnectorDialogProps } from '$lib/types/connectors/ConnectorDialogProps'
import { useEffect, useState } from 'react'
import { FormContainer } from 'react-hook-form-mui'
import JSONbig from 'true-json-bigint'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify connector name')])),
  description: va.optional(va.string(), ''),
  transport: va.nonOptional(va.object({}, va.unknown())),
  format: va.nonOptional(va.object({}, va.unknown()))
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
    transport: {
      name: 'transport-name',
      config: {
        property: 'value'
      }
    },
    format: {
      name: 'csv',
      config: {}
    }
  }

  const handleClose = () => {
    props.setShow(false)
  }
  // Define what should happen when the form is submitted
  const prepareData = ({ transport, format, ...data }: EditorSchema) => ({ ...data, config: { transport, format } })

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
      <FormContainer
        resolver={valibotResolver(schema)}
        mode='onChange'
        values={curValues}
        defaultValues={defaultValues}
        FormProps={{}}
        onSuccess={onSubmit}
      >
        <DialogContent sx={{ pb: 8, px: { xs: 8, sm: 15 }, pt: { xs: 8, sm: 12.5 }, position: 'relative' }}>
          <IconButton
            size='small'
            onClick={() => props.setShow(false)}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <i className={`bx bx-x`} style={{}} />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {props.connector === undefined ? 'Connector Editor' : props.existingTitle?.(props.connector.name) ?? ''}
            </Typography>
            {props.connector === undefined && <Typography variant='body2'>Write a custom connector config</Typography>}
          </Box>
          <GenericEditorForm
            disabled={props.disabled}
            direction={Direction.INPUT}
            configFromText={config => JSONbig.parse(config)}
            configToText={config => JSONbig.stringify(config, null, 2)}
          ></GenericEditorForm>
        </DialogContent>
        <DialogActions sx={{ pb: { xs: 8, sm: 12.5 }, justifyContent: 'center' }}>
          {props.submitButton}
          <Button variant='outlined' color='secondary' onClick={() => props.setShow(false)}>
            Cancel
          </Button>
        </DialogActions>
      </FormContainer>
    </Dialog>
  )
}
