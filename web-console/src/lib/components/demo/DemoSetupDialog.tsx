import { intersperse } from '$lib/functions/common/array'
import { demoFormResolver } from '$lib/functions/demo/demoSetupDialog'
import { DemoSetupProgress, runDemoSetup } from '$lib/functions/demo/runDemo'
import { Arguments } from '$lib/types/common/function'
import { DemoSetup } from '$lib/types/demo'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
import { FormContainer, TextFieldElement, useFormContext, useWatch } from 'react-hook-form-mui'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'

import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  LinearProgress,
  Link,
  Step,
  StepContent,
  StepLabel,
  Stepper,
  Typography
} from '@mui/material'
import { useQuery } from '@tanstack/react-query'

const stageNumbers = {
  program: 0,
  connector: 1,
  pipeline: 2
}

const getStageNumber = (progress: DemoSetupProgress | undefined) =>
  progress ? (progress === 'done' ? 3 : stageNumbers[progress.stage]) : -1

const DemoSetupFormContent = ({
  setProgress,
  ...props
}: Arguments<typeof DemoSetupForm>[0] & {
  progress: DemoSetupProgress | undefined
  setProgress: Dispatch<SetStateAction<DemoSetupProgress | undefined>>
}) => {
  const prefix = useWatch<{ prefix: string }>({ name: 'prefix' })
  useEffect(() => {
    setProgress(undefined)
  }, [prefix, setProgress])
  const { refetch, ...setupScope } = useQuery({
    queryKey: ['demo/setup'],
    queryFn: () => runDemoSetup({ prefix: prefix, steps: props.demo.setup.steps })
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
    const generator = setupScope.data?.setup()
    if (!generator) {
      return
    }
    for await (const progress of generator) {
      setProgress(progress)
    }
    setProgress('done')
  }
  const handleSubmit = useFormContext().handleSubmit(runOperation)
  const resultEntities = {
    pipeline: (() => {
      const e = props.demo.setup.steps[0]?.entities.find(e => e.type === 'pipeline')
      if (!e) {
        return e
      }
      invariant(e.type === 'pipeline')
      return {
        ...e,
        name: prefix + e.name
      }
    })(),
    program: (() => {
      const e = props.demo.setup.steps[0]?.entities.find(e => e.type === 'program')
      if (!e) {
        return e
      }
      invariant(e.type === 'program')
      return {
        ...e,
        name: prefix + e.name
      }
    })()
  }
  const hasConflict = (setupScope.data?.entities ?? []).some(e => e.exists)
  return (
    <Box sx={{ width: 550 }}>
      <DialogTitle>Setup {props.demo.name} demo Pipeline</DialogTitle>
      <DialogContent>
        <DialogContentText>This prefix will be added to the name of every item in the demo.</DialogContentText>
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
        {hasConflict && (
          <Alert severity='warning'>
            {props.progress ? <>Some items were overwritten</> : <>With this prefix, some items will be overwritten</>}
          </Alert>
        )}
        <LinearProgress
          variant='determinate'
          value={progressBar.ratio * 100}
          color={progressBar.ratio === 1 ? 'success' : 'primary'}
        />
      </DialogContent>
      <DialogContent>
        <Stepper activeStep={getStageNumber(props.progress)} orientation='vertical'>
          {[
            { label: 'Create SQL Programs', type: 'program' as const },
            { label: 'Create Connectors', type: 'connector' as const },
            { label: 'Create Pipelines', type: 'pipeline' as const }
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
              <StepLabel sx={{ display: 'flex' }}>
                <Box sx={{ display: 'flex' }}>{step.label}</Box>
              </StepLabel>
              <StepContent>
                <Box component='span'>
                  {intersperse(
                    setupScope.data?.entities
                      .filter(e => e.type === step.type)
                      .map((e, i) => (
                        <Typography
                          component='span'
                          key={i * 2}
                          variant='body2'
                          color={e.exists ? 'warning.contrastText' : undefined}
                          sx={{ whiteSpace: 'nowrap' }}
                        >
                          {prefix + e.name}
                        </Typography>
                      )) ?? [],
                    i => (
                      <Typography key={i} component='span' color='text.secondary'>
                        ,{' '}
                      </Typography>
                    )
                  )}
                </Box>
              </StepContent>
            </Step>
          ))}
        </Stepper>
      </DialogContent>
      <DialogActions sx={{ gap: 4 }}>
        {match(props.progress)
          .with(undefined, () => (
            <Button onClick={handleSubmit} type='submit' variant='contained'>
              Setup demo
            </Button>
          ))
          .with('done', () =>
            resultEntities.pipeline ? (
              <Button
                variant='contained'
                href={'/streaming/management/#' + resultEntities.pipeline.name}
                LinkComponent={Link}
              >
                Go to Pipeline
              </Button>
            ) : resultEntities.program ? (
              <Button
                variant='contained'
                href={'/analytics/editor/?program_name=' + resultEntities.program.name}
                LinkComponent={Link}
              >
                See SQL Program
              </Button>
            ) : (
              <Button variant='contained' onClick={props.onClose} LinkComponent={Link}>
                Done
              </Button>
            )
          )
          .with({ ratio: P._ }, () => (
            <Button variant='contained' disabled>
              Setup demo
            </Button>
          ))
          .exhaustive()}
      </DialogActions>
    </Box>
  )
}

const DemoSetupForm = (props: { demo: { name: string; setup: DemoSetup }; onClose: () => void }) => {
  const [progress, setProgress] = useState<DemoSetupProgress>()
  return (
    <FormContainer
      defaultValues={{
        prefix: props.demo.setup.prefix
      }}
      resolver={demoFormResolver}
    >
      <DemoSetupFormContent {...{ ...props, progress, setProgress }}></DemoSetupFormContent>
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
