// Displays a button that displays if the pipeline has changes since the last
// revision. If clicked, a dialog is opened that shows the changes.

import {
  Ref,
  useState,
  forwardRef,
  ReactElement,
  useEffect,
  SyntheticEvent,
  Dispatch,
  SetStateAction,
  useRef
} from 'react'

import Badge from '@mui/material/Badge'
import Box from '@mui/material/Box'
import Grid from '@mui/material/Grid'
import Dialog from '@mui/material/Dialog'
import Button from '@mui/material/Button'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'
import Fade, { FadeProps } from '@mui/material/Fade'
import DialogContent from '@mui/material/DialogContent'
import DialogActions from '@mui/material/DialogActions'
import Tab from '@mui/material/Tab'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import TabContext from '@mui/lab/TabContext'
import { Alert, AlertTitle, useTheme } from '@mui/material'

import { Icon } from '@iconify/react'
import { useQuery } from '@tanstack/react-query'
import { DiffEditor, MonacoDiffEditor } from '@monaco-editor/react'
import { diffLines, Change } from 'diff'

import { ApiError, ErrorResponse, PipelineDescr, PipelineRevision, ProgramCodeResponse } from 'src/types/manager'
import useStartPipeline from './hooks/useStartPipeline'
import { ThemeColor } from 'src/@core/layouts/types'

interface ErrorProps {
  error: ErrorResponse | undefined
}

export const ErrorBox = (props: ErrorProps) => {
  if (props.error) {
    // TODO should return/display all errors found, not just the first one
    // (needs also a change in the validate method in the backend)
    return (
      <Alert severity='warning'>
        <AlertTitle>The new configuration has the following problems:</AlertTitle>
        {props.error.message}
      </Alert>
    )
  } else {
    return <></>
  }
}

const Transition = forwardRef(function Transition(
  props: FadeProps & { children?: ReactElement<any, any> },
  ref: Ref<unknown>
) {
  return <Fade ref={ref} {...props} />
})

interface DialogProps {
  pipeline: PipelineDescr
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  origConfig: string
  newConfig: string
  origProgram: string
  newProgram: string
  diffCount: { config: Change[]; program: Change[] }
  validationError: ErrorResponse | undefined
}

const countDiff = (diff: Change[], additions: boolean, removals: boolean): number => {
  return diff
    .filter(line => (additions && line.added) || (removals && line.removed))
    .map(line => line.count || 0)
    .reduce((partialSum, a) => partialSum + a, 0)
}

const TabLabel = (props: { label: string; diff: Change[] }) => {
  const additions = countDiff(props.diff, true, false)
  const deletions = countDiff(props.diff, false, true)
  return (
    <Box sx={{ display: 'flex' }}>
      {props.label}&nbsp;
      <Box>(</Box>
      <Box sx={{ color: 'success.main' }}>+{additions}</Box>
      <Box>&nbsp;/&nbsp;</Box>
      <Box sx={{ color: 'error.main' }}>-{deletions}</Box>
      <Box>)</Box>
    </Box>
  )
}

export const PipelineConfigDiffDialog = (props: DialogProps) => {
  const { pipeline, show, setShow, diffCount, origConfig, newConfig, origProgram, newProgram, validationError } = props

  const theme = useTheme()
  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'
  const diffConfigEditorRef = useRef(null)
  const diffSqlEditorRef = useRef(null)

  // Switch to SQL tab if config doesn't have changes:
  const initialTab = diffCount.config.length == 0 && diffCount.program.length > 0 ? '2' : '1'
  const [value, setValue] = useState<string>(initialTab)

  const startPipelineClick = useStartPipeline()

  const handleStart = () => {
    startPipelineClick(pipeline.pipeline_id)
    setShow(false)
  }
  const handleChange = (event: SyntheticEvent, newValue: string) => {
    setValue(newValue)
  }
  function handleSqlEditorDidMount(editor: MonacoDiffEditor) {
    diffSqlEditorRef.current = editor
  }
  function handleConfigEditorDidMount(editor: MonacoDiffEditor) {
    diffConfigEditorRef.current = editor
  }

  return (
    <Dialog
      fullWidth
      open={show}
      maxWidth='md'
      scroll='body'
      onClose={() => setShow(false)}
      TransitionComponent={Transition}
      onBackdropClick={() => setShow(false)}
    >
      <DialogContent sx={{ pt: { sm: 8.5 }, position: 'relative' }}>
        <IconButton
          size='small'
          onClick={() => {
            setShow(false)
          }}
          sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
        >
          <Icon icon='bx:x' />
        </IconButton>
        <Box sx={{ mb: 4, textAlign: 'center' }}>
          <Typography variant='h5' sx={{ mb: 3 }}>
            Pipeline Changeset
          </Typography>
          <Typography variant='body2'>Review changes since last deployment</Typography>
        </Box>
        <Grid container spacing={6}>
          <Grid item xs={12}>
            <ErrorBox error={validationError} />
          </Grid>
          <Grid item xs={12}>
            <TabContext value={value}>
              <TabList centered onChange={handleChange} aria-label='tabs with diffs'>
                <Tab value='1' label={<TabLabel label='Pipeline Config' diff={diffCount.config} />} />
                <Tab value='2' label={<TabLabel label='SQL Code' diff={diffCount.program} />} />
              </TabList>
              <TabPanel value='1'>
                <DiffEditor
                  height='60vh'
                  theme={vscodeTheme}
                  // switch language to `toml` once supported:
                  // https://github.com/microsoft/monaco-editor/issues/2798
                  language='yaml'
                  original={origConfig}
                  modified={newConfig}
                  options={{ readOnly: true, scrollBeyondLastColumn: 5, scrollBeyondLastLine: false }}
                  onMount={handleConfigEditorDidMount}
                />
              </TabPanel>
              <TabPanel value='2'>
                <DiffEditor
                  height='60vh'
                  theme={vscodeTheme}
                  language='sql'
                  original={origProgram}
                  modified={newProgram}
                  options={{ readOnly: true, scrollBeyondLastColumn: 5, scrollBeyondLastLine: false }}
                  onMount={handleSqlEditorDidMount}
                />
              </TabPanel>
            </TabContext>
          </Grid>
        </Grid>
      </DialogContent>
      <DialogActions sx={{ pb: { xs: 8, sm: 12.5 }, justifyContent: 'center' }}>
        <Button variant='outlined' color='secondary' onClick={() => setShow(false)}>
          Cancel
        </Button>
        <Button variant='contained' sx={{ mr: 1 }} onClick={handleStart} endIcon={<Icon icon='bx:play-circle' />}>
          Start
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export interface Props {
  pipeline: PipelineDescr
}

export const PipelineRevisionStatusChip = (props: Props) => {
  const pipeline = props.pipeline
  // note: for diffCount we only keep the added and removed lines in the
  // Change[] arrays and throw out the unchanged entries, see `diffLines` below.
  const [diffCount, setDiffCount] = useState<{ config: Change[]; program: Change[] } | undefined>(undefined)
  const [label, setLabel] = useState<string | undefined>(undefined)
  const [show, setShow] = useState<boolean>(false)
  const [validationError, setValidationError] = useState<ErrorResponse | undefined>(undefined)
  const [color, setColor] = useState<ThemeColor>('success')

  const pipelineValidateQuery = useQuery<any, ApiError>(['pipelineValidate', { pipeline_id: pipeline.pipeline_id }], {
    retry: false
  })
  useEffect(() => {
    if (!pipelineValidateQuery.isLoading && pipelineValidateQuery.isError) {
      setValidationError(pipelineValidateQuery.error.body as ErrorResponse)
      setColor('warning')
    }
  }, [pipeline.pipeline_id, pipelineValidateQuery])

  const curPipelineConfigQuery = useQuery<string>(['pipelineConfig', { pipeline_id: pipeline.pipeline_id }])
  const curProgramQuery = useQuery<ProgramCodeResponse>(['programCode', { program_id: pipeline.program_id }], {
    enabled: pipeline.program_id != null
  })
  const pipelineRevisionQuery = useQuery<PipelineRevision>([
    'pipelineLastRevision',
    { pipeline_id: pipeline.pipeline_id }
  ])
  useEffect(() => {
    if (
      !pipelineRevisionQuery.isLoading &&
      !pipelineRevisionQuery.isError &&
      pipelineRevisionQuery.data &&
      !curPipelineConfigQuery.isLoading &&
      !curPipelineConfigQuery.isError
    ) {
      const configDiffResult = diffLines(pipelineRevisionQuery.data.config, curPipelineConfigQuery.data).filter(
        line => line.added || line.removed
      )
      // Distinguish the case where the program is not set in the pipeline
      const programDiffResult =
        !curProgramQuery.isLoading && !curProgramQuery.isError && curProgramQuery.data
          ? diffLines(pipelineRevisionQuery.data.code, curProgramQuery.data.code).filter(
              line => line.added || line.removed
            )
          : diffLines(pipelineRevisionQuery.data.code, '')

      setDiffCount({ config: configDiffResult, program: programDiffResult })
      if (configDiffResult.length > 0 || programDiffResult.length > 0) {
        setLabel('Modified')
      }
    }
  }, [
    pipelineRevisionQuery.isLoading,
    pipelineRevisionQuery.isError,
    pipelineRevisionQuery.data,
    curPipelineConfigQuery.isLoading,
    curPipelineConfigQuery.isError,
    curPipelineConfigQuery.data,
    curProgramQuery.isLoading,
    curProgramQuery.isError,
    curProgramQuery.data,
    setDiffCount,
    setLabel
  ])

  return diffCount && diffCount.program.length + diffCount.config.length > 0 ? (
    <Badge badgeContent={countDiff(diffCount.config.concat(diffCount.program), true, true)} color={color}>
      <Button onClick={() => setShow(true)} size='small' variant='outlined' color={color}>
        {label}
      </Button>
      <PipelineConfigDiffDialog
        pipeline={pipeline}
        show={show}
        setShow={setShow}
        diffCount={diffCount}
        origConfig={pipelineRevisionQuery.data?.config || ''}
        newConfig={curPipelineConfigQuery.data || ''}
        origProgram={pipelineRevisionQuery.data?.code || ''}
        newProgram={curProgramQuery.data?.code || ''}
        validationError={validationError}
      />
    </Badge>
  ) : (
    <></>
  )
}
