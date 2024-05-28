// Editor for SQL programs. This is the main component for the editor page.
// It is responsible for loading the program, compiling it, and saving it.

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import { EntitySyncIndicator, EntitySyncIndicatorStatus } from '$lib/components/common/EntitySyncIndicator'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import CompileIndicator from '$lib/components/layouts/analytics/CompileIndicator'
import { useUpdateProgram } from '$lib/compositions/analytics/useUpdateProgram'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
import { invalidateQuery } from '$lib/functions/common/tanstack'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import {
  ApiError,
  CompilationProfile,
  NewProgramRequest,
  NewProgramResponse,
  ProgramDescr,
  SqlCompilerMessage,
  UpdateProgramRequest
} from '$lib/services/manager'
import { ProgramsService } from '$lib/services/manager/services/ProgramsService'
import { PipelineManagerQueryKey, programStatusCacheUpdate } from '$lib/services/pipelineManagerQuery'
import { useRouter } from 'next/navigation'
import { Dispatch, MutableRefObject, SetStateAction, useEffect, useRef, useState } from 'react'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'
import { useDebouncedCallback } from 'use-debounce'

import Editor, { useMonaco } from '@monaco-editor/react'
import { Card, CardContent, CardHeader, FormHelperText, useTheme } from '@mui/material'
import Divider from '@mui/material/Divider'
import FormControl from '@mui/material/FormControl'
import Grid from '@mui/material/Grid'
import TextField from '@mui/material/TextField'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

// How many ms to wait until we save the project.
const SAVE_DELAY = 1000

// The error format for the editor form.
interface FormError {
  name?: { message?: string }
}

// Top level form with Name and Description TextInput elements
const MetadataForm = (props: {
  programName: string | undefined
  errors: FormError
  program: ProgramDescr
  updateProgram: Dispatch<SetStateAction<UpdateProgramRequest>>
  disabled?: boolean
}) => {
  const updateName = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.updateProgram(p => ({ ...p, name: event.target.value }))
  }

  const updateDescription = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.updateProgram(p => ({ ...p, description: event.target.value }))
  }

  return (
    <Grid container spacing={5}>
      <Grid item xs={4}>
        <FormControl fullWidth>
          <TextField
            fullWidth
            type='text'
            label='Name'
            placeholder={PLACEHOLDER_VALUES['program_name']}
            value={props.program.name}
            error={Boolean(props.errors.name)}
            onChange={updateName}
            inputProps={{
              'data-testid': 'input-program-name'
            }}
            disabled={props.disabled}
          />
          {props.errors.name && (
            <FormHelperText data-testid='box-error-name' sx={{ color: 'error.main' }}>
              {props.errors.name.message}
            </FormHelperText>
          )}
        </FormControl>
      </Grid>
      <Grid item xs={8}>
        <TextField
          fullWidth
          type='Description'
          label='Description'
          placeholder={PLACEHOLDER_VALUES['program_description']}
          value={props.program.description}
          onChange={updateDescription}
          inputProps={{
            'data-testid': 'input-program-description'
          }}
          disabled={props.disabled}
        />
      </Grid>
    </Grid>
  )
}

const stateToEditorLabel = (state: EntitySyncIndicatorStatus) =>
  match(state)
    .with('isNew', () => {
      return 'Name the program to save it'
    })
    .with('isModified', () => {
      return 'Modified'
    })
    .with('isSaving', () => {
      return 'Saving …'
    })
    .with('isUpToDate', () => {
      return 'Saved'
    })
    .with('isLoading', () => {
      return 'Loading …'
    })
    .exhaustive()

const useCreateProgramEffect = (
  program: ProgramDescr,
  setStatus: Dispatch<SetStateAction<EntitySyncIndicatorStatus>>,
  setFormError: Dispatch<SetStateAction<FormError>>
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const router = useRouter()

  const { mutate } = useMutation<NewProgramResponse, ApiError, NewProgramRequest>({
    mutationFn: ProgramsService.newProgram
  })
  const createProgram = (program: NewProgramRequest) => {
    invariant(program.name, 'Cannot create a program with an empty name!')
    setStatus('isSaving')
    return mutate(program, {
      onSettled: () => {
        invalidateQuery(queryClient, PipelineManagerQueryKey.programs())
        invalidateQuery(queryClient, PipelineManagerQueryKey.programStatus(program.name))
      },
      onSuccess: (_data: NewProgramResponse) => {
        if (!program.name) {
          setFormError({ name: { message: 'Enter a name for the project.' } })
        }
        setStatus('isUpToDate')
        router.push(`/analytics/editor/?program_name=${program.name}`)
        setFormError({})
      },
      onError: (error: ApiError) => {
        // TODO: would be good to have error codes from the API
        if (error.message.includes('name already exists')) {
          setFormError({ name: { message: 'This name is already in use. Enter a different name.' } })
          setStatus('isNew')
        } else {
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
        }
      }
    })
  }
  const createProgramDebounced = useDebouncedCallback(() => {
    if (!program.name || program.program_id) {
      return
    }
    return createProgram({ ...program, config: undefined, code: program.code ?? '' })
  }, SAVE_DELAY)
  useEffect(() => createProgramDebounced(), [program, createProgramDebounced])
}

// Polls the server during compilation and checks for the status.
const usePollCompilationStatusEffect = (project: ProgramDescr) => {
  const queryClient = useQueryClient()
  const pipelineManagerQuery = usePipelineManagerQuery()
  const compilationStatus = useQuery({
    ...pipelineManagerQuery.programStatus(project.name),
    refetchInterval: 1000,
    enabled: project.program_id !== '' && (project.status === 'Pending' || project.status === 'CompilingSql')
  })

  useEffect(() => {
    if (!compilationStatus.data || compilationStatus.isPending || compilationStatus.isError) {
      return
    }
    if (project.status !== compilationStatus.data.status) {
      programStatusCacheUpdate(queryClient, project.name, compilationStatus.data.status)
    }
  }, [
    compilationStatus,
    compilationStatus.data,
    compilationStatus.isPending,
    compilationStatus.isError,
    project.status,
    project.version,
    project.name,
    queryClient,
    pipelineManagerQuery
  ])

  return compilationStatus
}

const useDisplayCompilerErrorsInEditor = (project: ProgramDescr, editorRef: MutableRefObject<any>) => {
  const monaco = useMonaco()
  useEffect(() => {
    if (monaco !== null && editorRef.current !== null) {
      match(project.status)
        .with({ SqlError: P.select() }, (err: SqlCompilerMessage[]) => {
          const monaco_markers = err.map(item => {
            return {
              startLineNumber: item.startLineNumber,
              endLineNumber: item.endLineNumber,
              startColumn: item.startColumn,
              endColumn: item.endColumn + 1,
              message: item.message,
              severity: item.warning ? monaco.MarkerSeverity.Warning : monaco.MarkerSeverity.Error
            }
          })
          monaco.editor.setModelMarkers(editorRef.current.getModel(), 'sql-errors', monaco_markers)
        })
        .otherwise(() => {
          monaco.editor.setModelMarkers(editorRef.current.getModel(), 'sql-errors', [])
        })
    }
  }, [monaco, project.status, editorRef])
}

export const ProgramEditorImpl = ({
  program,
  updateProgram,
  formError,
  setFormError,
  status,
  setStatus
}: {
  program: ProgramDescr
  updateProgram: Dispatch<SetStateAction<UpdateProgramRequest>>
  formError: FormError
  setFormError: Dispatch<SetStateAction<FormError>>
  status: EntitySyncIndicatorStatus
  setStatus: Dispatch<SetStateAction<EntitySyncIndicatorStatus>>
}) => {
  const theme = useTheme()
  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'

  useCreateProgramEffect(program, setStatus, setFormError)

  usePollCompilationStatusEffect(program)

  // Mounting and callback for when code is edited
  // TODO: The IStandaloneCodeEditor type is not exposed in the react monaco
  // editor package?
  const editorRef = useRef<any /* IStandaloneCodeEditor */>(null)

  function handleEditorDidMount(editor: any) {
    editorRef.current = editor
  }

  const updateCode = (value: string | undefined) => {
    updateProgram(p => ({ ...p, code: value }))
  }
  useDisplayCompilerErrorsInEditor(program, editorRef)

  return (
    <Card>
      <CardHeader title='SQL Code'></CardHeader>
      <CardContent>
        <MetadataForm
          disabled={status === 'isLoading'}
          programName={program.name}
          program={program}
          updateProgram={updateProgram}
          errors={formError}
        />
      </CardContent>
      <CardContent>
        <Grid item xs={12}>
          <EntitySyncIndicator getLabel={stateToEditorLabel} state={status} />
          <CompileIndicator program={program} />
        </Grid>
      </CardContent>
      <CardContent>
        <Editor
          height='60vh'
          theme={vscodeTheme}
          defaultLanguage='sql'
          value={program.code || ''}
          onChange={updateCode}
          onMount={editor => handleEditorDidMount(editor)}
          wrapperProps={{
            'data-testid': 'box-program-code-wrapper'
          }}
          options={isMonacoEditorDisabled(status === 'isLoading')}
        />
      </CardContent>
      <Divider sx={{ m: '0 !important' }} />
    </Card>
  )
}

export const ProgramEditor = ({ programName }: { programName: string }) => {
  const [status, setStatus] = useState<EntitySyncIndicatorStatus>(programName ? 'isLoading' : 'isNew')
  const pipelineManagerQuery = usePipelineManagerQuery()

  const [formError, setFormError] = useState<FormError>({})
  const programQuery = useQuery({
    ...pipelineManagerQuery.programCode(programName!),
    enabled: !!programName,
    initialData: {
      name: '',
      description: '',
      program_id: '',
      status: 'Pending',
      config: { profile: CompilationProfile.OPTIMIZED },
      version: 0
    },
    refetchOnWindowFocus: false
  })
  const program = programQuery.data
  invariant(program, 'Program should be initialized with a default value')
  // Clear loading state when program is fetched
  useEffect(() => {
    if (!program.program_id) {
      return
    }
    setStatus('isUpToDate')
  }, [program.program_id, setStatus])

  const updateProgram = useUpdateProgram(programName, setStatus, setFormError)

  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/analytics/programs`} data-testid='button-breadcrumb-sql-programs'>
          SQL Programs
        </Breadcrumbs.Link>
        <Breadcrumbs.Link href={`/analytics/editor/?program_name=${programName}`}>{programName}</Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <ProgramEditorImpl
        {...{
          program,
          updateProgram
        }}
        {...{ status, setStatus, formError, setFormError }}
      ></ProgramEditorImpl>
    </>
  )
}
