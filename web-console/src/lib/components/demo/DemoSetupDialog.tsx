import { demoFormResolver } from '$lib/functions/demo/demoSetupDialog'
import { runDemoSetup } from '$lib/functions/demo/runDemo'
import { Arguments } from '$lib/types/common/function'
import { DemoSetup } from '$lib/types/demo'
import { Dispatch, Fragment, SetStateAction, useEffect, useState } from 'react'
import { FormContainer, TextFieldElement, useFormContext, useWatch } from 'react-hook-form-mui'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'

import IconArrowDownward from '@mui/icons-material/ExpandMoreOutlined'
import IconReportProblemOutlined from '@mui/icons-material/ReportProblemOutlined'
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  LinearProgress,
  Link,
  Typography
} from '@mui/material'
import { useQuery } from '@tanstack/react-query'

type Progress = { description: string; ratio: number } | 'done'

const DemoSetupFormContent = ({
  setProgress,
  ...props
}: Arguments<typeof DemoSetupForm>[0] & {
  progress: Progress | undefined
  setProgress: Dispatch<SetStateAction<Progress | undefined>>
}) => {
  const prefix = useWatch<{ prefix: string }>({ name: 'prefix' })
  useEffect(() => {
    setProgress(undefined)
  }, [prefix, setProgress])
  const setupScope = useQuery({
    queryKey: ['demo/setup', prefix],
    queryFn: () => runDemoSetup({ prefix: prefix, steps: props.demo.setup.steps })
  })
  const progressBar = match(props.progress)
    .with(undefined, () => undefined)
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
      return e
    })(),
    program: (() => {
      const e = props.demo.setup.steps[0]?.entities.find(e => e.type === 'program')
      if (!e) {
        return e
      }
      invariant(e.type === 'program')
      return e
    })()
  }
  const hasConflict = (setupScope.data?.entities ?? []).some(e => e.exists)
  return (
    <>
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
      {hasConflict && <Alert severity='warning'>Some entities will be overwritten</Alert>}
      <Accordion
        variant='elevation'
        style={{ boxShadow: 'none' }}
        {...{ disableGutters: true }}
        sx={{
          '&.MuiAccordion-root:before': {
            height: 0
          }
        }}
      >
        <AccordionSummary expandIcon={<IconArrowDownward />}>
          <Typography>Entities in the demo</Typography>
          {hasConflict && <IconReportProblemOutlined color='warning'></IconReportProblemOutlined>}
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', height: '100%', maxHeight: 200, overflowY: 'auto' }}>
            <Box component='div'>
              {(setupScope.data?.entities ?? []).map(entity => (
                <Fragment key={entity.name}>
                  {prefix + entity.name}
                  {entity.exists && (
                    <Chip sx={{ ml: 4 }} color='warning' variant='outlined' label='conflict' size='small' />
                  )}

                  <br />
                </Fragment>
              ))}
            </Box>
          </Box>
        </AccordionDetails>
      </Accordion>
      <DialogActions sx={{ gap: 4 }}>
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
            <Button onClick={handleSubmit} type='submit' variant='contained'>
              Setup demo
            </Button>
          ))
          .with({ ratio: P._ }, () => (
            <Button disabled variant='contained' sx={{ whiteSpace: 'nowrap' }}>
              Setting up...
            </Button>
          ))
          .exhaustive()}
      </DialogActions>
      {props.progress === 'done' && (
        <DialogContent sx={{ display: 'flex', gap: 4, justifyContent: 'space-between' }}>
          {resultEntities.program && (
            <Button
              variant='contained'
              href={'/analytics/editor/?program_name=' + resultEntities.program.name}
              LinkComponent={Link}
            >
              Go to program
            </Button>
          )}
          {resultEntities.pipeline && (
            <Button
              variant='contained'
              href={'/streaming/builder/?pipeline_name=' + resultEntities.pipeline.name}
              LinkComponent={Link}
            >
              Go to pipeline
            </Button>
          )}
        </DialogContent>
      )}
    </>
  )
}

const DemoSetupForm = (props: { demo: { name: string; setup: DemoSetup }; onClose: () => void }) => {
  const [progress, setProgress] = useState<Progress>()
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
