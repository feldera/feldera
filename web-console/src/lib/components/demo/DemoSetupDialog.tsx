import { runDemoSetup } from '$lib/functions/demo/runDemoSetup'
import { DemoSetup } from '$lib/types/demo'
import { useState } from 'react'
import { FormContainer, TextFieldElement } from 'react-hook-form-mui'
import { match, P } from 'ts-pattern'

import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  LinearProgress
} from '@mui/material'

const DemoSetupForm = (props: { demo: { name: string; setup: DemoSetup }; onClose: () => void }) => {
  const [progress, setProgress] = useState<{ description: string; ratio: number } | 'done'>()
  const runOperation = async (form: { prefix: string }) => {
    for await (const progress of runDemoSetup({ prefix: form.prefix, steps: props.demo.setup.steps })) {
      setProgress(progress)
    }
    setProgress('done')
    setTimeout(() => props.onClose(), 1000)
  }
  return (
    <FormContainer
      defaultValues={{
        prefix: props.demo.setup.prefix
      }}
      onSuccess={runOperation}
    >
      <DialogTitle>Run {props.demo.name} demo</DialogTitle>
      <DialogContent>
        <DialogContentText>This prefix will be added to the name of every entity in the demo.</DialogContentText>
      </DialogContent>
      <DialogContent>
        <TextFieldElement
          autoFocus
          margin='dense'
          name='prefix'
          label='Demo prefix'
          type='text'
          fullWidth
          size='small'
        />
      </DialogContent>
      <DialogActions>
        {match(progress)
          .with('done', () => <></>)
          .with(undefined, () => <></>)
          .with({ ratio: P._ }, p => (
            <Box sx={{ width: '100%' }}>
              {p.description}
              <LinearProgress variant='determinate' value={p.ratio * 100} />
            </Box>
          ))
          .exhaustive()}

        {match(progress)
          .with('done', () => (
            <Button color='success' variant='contained'>
              Done!
            </Button>
          ))
          .with(undefined, () => (
            <Button type='submit' variant='contained'>
              Run demo
            </Button>
          ))
          .with({ ratio: P._ }, () => (
            <Button disabled variant='contained' sx={{ whiteSpace: 'nowrap' }}>
              Setting up...
            </Button>
          ))
          .exhaustive()}
      </DialogActions>
    </FormContainer>
  )
}

export const DemoSetupDialog = (props: { demo?: { name: string; setup: DemoSetup }; onClose: () => void }) => {
  return (
    <Dialog
      open={!!props.demo}
      disableEscapeKeyDown
      aria-labelledby='alert-dialog-title'
      aria-describedby='alert-dialog-description'
      onClose={(_event, _reason) => {
        props.onClose()
      }}
    >
      {props.demo ? <DemoSetupForm demo={props.demo} onClose={props.onClose}></DemoSetupForm> : <></>}
    </Dialog>
  )
}
