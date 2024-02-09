import { demoFormResolver } from '$lib/functions/demo/demoSetupDialog'
import { DemoSetupProgress, runDemoCleanup } from '$lib/functions/demo/runDemo'
import { Arguments } from '$lib/types/common/function'
import { DemoSetup } from '$lib/types/demo'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
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
  Step,
  StepContent,
  StepLabel,
  Stepper,
  Typography
} from '@mui/material'
import { useQuery } from '@tanstack/react-query'

const stageNumbers = {
  program: 1,
  connector: 2,
  pipeline: 0
}

const getStageNumber = (progress: DemoSetupProgress | undefined) =>
  progress ? (progress === 'done' ? 3 : stageNumbers[progress.stage]) : -1

const DemoCleanupFormContent = ({
  setProgress,
  ...props
}: Arguments<typeof DemoCleanupForm>[0] & {
  progress: DemoSetupProgress | undefined
  setProgress: Dispatch<SetStateAction<DemoSetupProgress | undefined>>
}) => {
  const prefix = useWatch<{ prefix: string }>({ name: 'prefix' })
  useEffect(() => {
    setProgress(undefined)
  }, [prefix, setProgress])
  const { refetch, ...cleanupScope } = useQuery({
    queryKey: ['demo/cleanup'],
    queryFn: () => runDemoCleanup({ prefix: prefix, steps: props.demo.setup.steps })
  })
  useEffect(() => {
    refetch()
  }, [refetch, prefix])
  const progressBar = match(props.progress)
    .with(undefined, () => ({ description: '\xa0', ratio: 0 }))
    .with({ ratio: P._ }, p => p)
    .with('done', () => ({ description: '\xa0', ratio: 1 }))
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
  const handleSubmit = useFormContext().handleSubmit(runOperation)
  const toDeleteNumber = !cleanupScope.data
    ? 0
    : cleanupScope.data.related.pipelines.length +
      cleanupScope.data.related.connectors.length +
      cleanupScope.data.related.programs.length
  return (
    <Box sx={{ width: 550 }}>
      <DialogTitle>Clean up after {props.demo.name} demo</DialogTitle>
      <DialogContent>
        <DialogContentText>Every item with this prefix will be removed.</DialogContentText>
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
        <LinearProgress
          variant='determinate'
          value={progressBar.ratio * 100}
          color={progressBar.ratio === 1 ? 'success' : 'primary'}
        />
      </DialogContent>
      <DialogContent>
        {toDeleteNumber > 0 ? (
          <>
            <Stepper activeStep={getStageNumber(props.progress)} orientation='vertical'>
              {[
                { label: 'Delete Pipelines', related: 'pipelines' as const },
                { label: 'Delete Connectors', related: 'connectors' as const },
                { label: 'Delete SQL Programs', related: 'programs' as const }
              ].map(step => (
                <Step
                  key={step.label}
                  sx={{
                    m: 0,
                    '.Mui-disabled': { color: 'text.primary' },
                    '& .MuiStepLabel-iconContainer .Mui-completed': {
                      color: 'success.main'
                    }
                  }}
                  expanded
                >
                  <StepLabel>
                    <Box sx={{ display: 'flex' }}>{step.label}</Box>
                  </StepLabel>
                  <StepContent>
                    <Typography variant='body2'>
                      {cleanupScope.data?.related[step.related]
                        .map(e => ('name' in e ? e.name : e.descriptor.name))
                        .join(', ')}
                    </Typography>
                  </StepContent>
                </Step>
              ))}
            </Stepper>
          </>
        ) : (
          <DialogContentText>Nothing to clean up</DialogContentText>
        )}
      </DialogContent>
      <DialogActions>
        {match(props.progress)
          .with('done', () => (
            <Button variant='contained' onClick={props.onClose}>
              Close
            </Button>
          ))
          .with(undefined, () =>
            toDeleteNumber > 0 ? (
              <Button onClick={handleSubmit} variant='contained' disabled={toDeleteNumber === 0}>
                Clean up
              </Button>
            ) : (
              <Button variant='contained' onClick={props.onClose}>
                Close
              </Button>
            )
          )
          .with({ ratio: P._ }, () => (
            <Button disabled variant='contained' sx={{ whiteSpace: 'nowrap' }}>
              Clean up
            </Button>
          ))
          .exhaustive()}
      </DialogActions>
    </Box>
  )
}

const DemoCleanupForm = (props: { demo: { name: string; setup: DemoSetup }; onClose: () => void }) => {
  const [progress, setProgress] = useState<DemoSetupProgress>()
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
