import { demoFormResolver } from '$lib/functions/demo/demoSetupDialog'
import { runDemoCleanup } from '$lib/functions/demo/runDemo'
import { Arguments } from '$lib/types/common/function'
import { DemoSetup } from '$lib/types/demo'
import { Dispatch, Fragment, SetStateAction, useEffect, useState } from 'react'
import { FormContainer, TextFieldElement, useFormContext, useWatch } from 'react-hook-form-mui'
import { match, P } from 'ts-pattern'

import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  LinearProgress,
  Typography
} from '@mui/material'
import { useQuery } from '@tanstack/react-query'

type Progress = { description: string; ratio: number } | 'done'

const DemoCleanupFormContent = ({
  setProgress,
  ...props
}: Arguments<typeof DemoCleanupForm>[0] & {
  progress: Progress | undefined
  setProgress: Dispatch<SetStateAction<Progress | undefined>>
}) => {
  const prefix = useWatch<{ prefix: string }>({ name: 'prefix' })
  useEffect(() => {
    setProgress(undefined)
  }, [prefix, setProgress])
  const cleanupScope = useQuery({
    queryKey: ['demo/cleanup', prefix],
    queryFn: () => runDemoCleanup({ prefix: prefix, steps: props.demo.setup.steps })
  })
  const progressBar = match(props.progress)
    .with(undefined, () => undefined)
    .with({ ratio: P._ }, p => p)
    .with('done', () => ({ description: 'Done', ratio: 1 }))
    .exhaustive()
  const runOperation = async () => {
    const generator = cleanupScope.data?.cleanup()
    if (!generator) {
      return
    }
    for await (const progress of generator) {
      setProgress(progress)
    }
    setProgress('done')
  }
  const ctx = useFormContext<{ prefix: string }>()
  const handle = ctx.handleSubmit(runOperation)
  const toDeleteNumber = !cleanupScope.data
    ? 0
    : cleanupScope.data.relatedPipelines.length +
      cleanupScope.data.relatedConnectors.length +
      cleanupScope.data.relatedPrograms.length
  return (
    <>
      <DialogTitle>Clean up after {props.demo.name} demo</DialogTitle>
      <DialogContent>
        <DialogContentText>Every entity with this prefix will be removed.</DialogContentText>
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
      <DialogContent>
        <DialogContentText>{toDeleteNumber > 0 ? 'This will delete:' : 'Nothing to clean up'}</DialogContentText>
        {cleanupScope.data && (
          <Typography sx={{ maxHeight: 200, overflowY: 'auto' }}>
            {cleanupScope.data.relatedPipelines.map(e => (
              <Fragment key={e.descriptor.pipeline_id}>
                {e.descriptor.name}
                <br />
              </Fragment>
            ))}
            {cleanupScope.data.relatedPrograms.map(e => (
              <Fragment key={e.program_id}>
                {e.name}
                <br />
              </Fragment>
            ))}
            {cleanupScope.data.relatedConnectors.map(e => (
              <Fragment key={e.connector_id}>
                {e.name}
                <br />
              </Fragment>
            ))}
          </Typography>
        )}
      </DialogContent>
      <DialogActions>
        {progressBar && (
          <Box sx={{ width: '100%', display: 'flex', flexDirection: 'column', gap: 2 }}>
            {progressBar.description}
            <LinearProgress
              variant='determinate'
              value={progressBar.ratio * 100}
              color={progressBar.ratio === 1 ? 'success' : 'primary'}
            />
          </Box>
        )}

        {match(props.progress)
          .with('done', () => (
            <Button color='success' variant='outlined' onClick={props.onClose}>
              Done!
            </Button>
          ))
          .with(undefined, () => (
            <Button onClick={handle} variant='contained' disabled={toDeleteNumber === 0}>
              Clean up
            </Button>
          ))
          .with({ ratio: P._ }, () => (
            <Button disabled variant='contained' sx={{ whiteSpace: 'nowrap' }}>
              Cleaning up...
            </Button>
          ))
          .exhaustive()}
      </DialogActions>
    </>
  )
}

const DemoCleanupForm = (props: { demo: { name: string; setup: DemoSetup }; onClose: () => void }) => {
  const [progress, setProgress] = useState<{ description: string; ratio: number } | 'done'>()
  return (
    <FormContainer
      defaultValues={{
        prefix: props.demo.setup.prefix
      }}
      resolver={demoFormResolver}
    >
      <DemoCleanupFormContent {...{ ...props, progress, setProgress }}></DemoCleanupFormContent>
    </FormContainer>
  )
}

export const DemoCleanupDialog = (props: { demo?: { name: string; setup: DemoSetup }; onClose: () => void }) => {
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
      {props.demo ? <DemoCleanupForm demo={props.demo} onClose={props.onClose}></DemoCleanupForm> : <></>}
    </Dialog>
  )
}
