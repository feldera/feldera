// Editor for SQL programs. This is the main component for the editor page.
// It is responsible for loading the program, compiling it, and saving it.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import SaveIndicator, { SaveIndicatorState } from '$lib/components/common/SaveIndicator'
import { usePageHeader } from '$lib/compositions/global/pageHeader'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { programQueryCacheUpdate, programStatusUpdate } from '$lib/services/defaultQueryFn'
import {
  ApiError,
  CompileProgramRequest,
  NewProgramRequest,
  NewProgramResponse,
  ProgramId,
  SqlCompilerMessage,
  UpdateProgramRequest,
  UpdateProgramResponse
} from '$lib/services/manager'
import { ProgramDescr } from '$lib/services/manager/models/ProgramDescr'
import { ProgramsService } from '$lib/services/manager/services/ProgramsService'
import assert from 'assert'
import { Dispatch, MutableRefObject, SetStateAction, useEffect, useRef, useState } from 'react'
import { match, P } from 'ts-pattern'
import { useDebouncedCallback } from 'use-debounce'

import Editor, { useMonaco } from '@monaco-editor/react'
import { Card, CardContent, CardHeader, FormHelperText, useTheme } from '@mui/material'
import Divider from '@mui/material/Divider'
import FormControl from '@mui/material/FormControl'
import Grid from '@mui/material/Grid'
import TextField from '@mui/material/TextField'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import CompileIndicator from './CompileIndicator'

// How many ms to wait until we save the project.
const SAVE_DELAY = 2000

// The error format for the editor form.
interface FormError {
  name?: { message?: string }
}

// Top level form with Name and Description TextInput elements
const MetadataForm = (props: { errors: FormError; project: ProgramDescr; setProject: any; setState: any }) => {
  const debouncedSaveStateUpdate = useDebouncedCallback(() => {
    props.setState('isModified')
  }, SAVE_DELAY)

  const updateName = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.setProject((prevState: ProgramDescr) => ({ ...prevState, name: event.target.value }))
    props.setState('isDebouncing')
    debouncedSaveStateUpdate()
  }

  const updateDescription = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.setProject((prevState: ProgramDescr) => ({ ...prevState, description: event.target.value }))
    props.setState('isDebouncing')
    debouncedSaveStateUpdate()
  }

  return (
    <Grid container spacing={5}>
      <Grid item xs={4}>
        <FormControl fullWidth>
          <TextField
            id='program-name' // Referenced by webui-tester
            fullWidth
            type='text'
            label='Name'
            placeholder={PLACEHOLDER_VALUES['program_name']}
            value={props.project.name}
            error={Boolean(props.errors.name)}
            onChange={updateName}
          />
          {props.errors.name && (
            <FormHelperText sx={{ color: 'error.main' }} id='validation-schema-first-name'>
              {props.errors.name.message}
            </FormHelperText>
          )}
        </FormControl>
      </Grid>
      <Grid item xs={8}>
        <TextField
          fullWidth
          id='program-description' // Referenced by webui-tester
          type='Description'
          label='Description'
          placeholder={PLACEHOLDER_VALUES['program_description']}
          value={props.project.description}
          onChange={updateDescription}
        />
      </Grid>
    </Grid>
  )
}

const stateToEditorLabel = (state: SaveIndicatorState): string =>
  match(state)
    .with('isNew' as const, () => {
      return 'New Project'
    })
    .with('isDebouncing' as const, () => {
      return 'Saving ...'
    })
    .with('isModified' as const, () => {
      return 'Saving ...'
    })
    .with('isSaving' as const, () => {
      return 'Saving ...'
    })
    .with('isUpToDate' as const, () => {
      // If you change this string, adjust the webui-tester too
      return 'Saved'
    })
    .exhaustive()

// Watches for changes to the form and saves them as a new project (if we don't
// have a program_id yet).
const useCreateProjectIfNew = (
  state: SaveIndicatorState,
  project: ProgramDescr,
  setProject: Dispatch<SetStateAction<ProgramDescr>>,
  setState: Dispatch<SetStateAction<SaveIndicatorState>>,
  setFormError: Dispatch<SetStateAction<FormError>>
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate } = useMutation<NewProgramResponse, ApiError, NewProgramRequest>(ProgramsService.newProgram)
  useEffect(() => {
    if (project.program_id == '') {
      if (state === 'isModified') {
        mutate(
          {
            name: project.name,
            description: project.description,
            code: project.code || ''
          },
          {
            onSettled: () => {
              queryClient.invalidateQueries(['program'])
              queryClient.invalidateQueries(['programStatus', { program_id: project.program_id }])
            },
            onSuccess: (data: NewProgramResponse) => {
              setProject((prevState: ProgramDescr) => ({
                ...prevState,
                version: data.version,
                program_id: data.program_id
              }))
              if (project.name === '') {
                setFormError({ name: { message: 'Enter a name for the project.' } })
              }
              setState('isUpToDate')
              setFormError({})
            },
            onError: (error: ApiError) => {
              // TODO: would be good to have error codes from the API
              if (error.message.includes('name already exists')) {
                setFormError({ name: { message: 'This name already exists. Enter a different name.' } })
                // This won't try to save again, but set the save indicator to
                // Saving... until the user changes something:
                setState('isDebouncing')
              } else {
                pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
              }
            }
          }
        )
      }
    }
  }, [
    project.program_id,
    mutate,
    project.code,
    project.description,
    project.name,
    state,
    pushMessage,
    setFormError,
    setProject,
    setState,
    queryClient
  ])
}

// Fetches the data for an existing project (if we have a program_id).
const useFetchExistingProject = (
  programId: string | null,
  setProject: Dispatch<SetStateAction<ProgramDescr>>,
  setState: Dispatch<SetStateAction<SaveIndicatorState>>,
  lastCompiledVersion: number,
  setLastCompiledVersion: Dispatch<SetStateAction<number>>,
  loaded: boolean,
  setLoaded: Dispatch<SetStateAction<boolean>>
) => {
  const codeQuery = useQuery<number, ApiError, ProgramDescr>(['programCode', { program_id: programId }], {
    enabled: programId != null && !loaded
  })
  useEffect(() => {
    if (!loaded && codeQuery.data && !codeQuery.isLoading && !codeQuery.isError) {
      setProject({
        program_id: codeQuery.data.program_id,
        name: codeQuery.data.name,
        description: codeQuery.data.description,
        status: codeQuery.data.status,
        version: codeQuery.data.version,
        code: codeQuery.data.code || ''
      })
      if (codeQuery.data.version > lastCompiledVersion && codeQuery.data.status !== 'None') {
        setLastCompiledVersion(codeQuery.data.version)
      }
      setState('isUpToDate')
      setLoaded(true)
    }
  }, [
    codeQuery.isLoading,
    codeQuery.isError,
    codeQuery.data,
    lastCompiledVersion,
    setProject,
    setState,
    setLastCompiledVersion,
    setLoaded,
    loaded
  ])
}

// Updates the project if it has changed and we have a program_id.
const useUpdateProjectIfChanged = (
  state: SaveIndicatorState,
  project: ProgramDescr,
  setProject: Dispatch<SetStateAction<ProgramDescr>>,
  setState: Dispatch<SetStateAction<SaveIndicatorState>>,
  setFormError: Dispatch<SetStateAction<FormError>>
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate, isLoading } = useMutation<
    UpdateProgramResponse,
    ApiError,
    { program_id: ProgramId; update_request: UpdateProgramRequest }
  >({
    mutationFn: (args: { program_id: ProgramId; update_request: UpdateProgramRequest }) => {
      return ProgramsService.updateProgram(args.program_id, args.update_request)
    }
  })
  useEffect(() => {
    if (project.program_id !== '' && state === 'isModified' && !isLoading) {
      const updateRequest = {
        name: project.name,
        description: project.description,
        code: project.code
      }
      mutate(
        { program_id: project.program_id, update_request: updateRequest },
        {
          onSettled: () => {
            queryClient.invalidateQueries(['program'])
            queryClient.invalidateQueries(['programCode', { program_id: project.program_id }])
            queryClient.invalidateQueries(['programStatus', { program_id: project.program_id }])
          },
          onSuccess: (data: UpdateProgramResponse) => {
            assert(project.program_id)
            programQueryCacheUpdate(queryClient, project.program_id, updateRequest)
            setProject((prevState: ProgramDescr) => ({ ...prevState, version: data.version }))
            setState('isUpToDate')
            setFormError({})
          },
          onError: (error: ApiError) => {
            // TODO: would be good to have error codes from the API
            if (error.message.includes('name already exists')) {
              setFormError({ name: { message: 'This name already exists. Enter a different name.' } })
              // This won't try to save again, but set the save indicator to
              // Saving... until the user changes something:
              setState('isDebouncing')
            } else {
              pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            }
          }
        }
      )
    }
  }, [
    mutate,
    state,
    project.program_id,
    project.description,
    project.name,
    project.code,
    setState,
    isLoading,
    queryClient,
    pushMessage,
    setFormError,
    setProject
  ])
}

// Send a compile request if the project changes (e.g., we got a new version and
// we're not already compiling)
const useCompileProjectIfChanged = (
  state: SaveIndicatorState,
  project: ProgramDescr,
  setProject: Dispatch<SetStateAction<ProgramDescr>>,
  lastCompiledVersion: number
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate, isLoading, isError } = useMutation<
    CompileProgramRequest,
    ApiError,
    { program_id: ProgramId; request: CompileProgramRequest }
  >({
    mutationFn: args => {
      return ProgramsService.compileProgram(args.program_id, args.request)
    }
  })
  useEffect(() => {
    if (
      !isLoading &&
      !isError &&
      state == 'isUpToDate' &&
      project.program_id !== '' &&
      lastCompiledVersion !== undefined &&
      project.version > lastCompiledVersion &&
      project.status !== 'Pending' &&
      project.status !== 'CompilingSql'
    ) {
      //console.log('compileProject ' + project.version)
      setProject((prevState: ProgramDescr) => ({ ...prevState, status: 'Pending' }))
      mutate(
        { program_id: project.program_id, request: { version: project.version } },
        {
          onSettled: () => {
            queryClient.invalidateQueries(['program'])
            queryClient.invalidateQueries(['programStatus', { program_id: project.program_id }])
          },
          onError: (error: ApiError) => {
            setProject((prevState: ProgramDescr) => ({ ...prevState, status: 'None' }))
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          }
        }
      )
    }
  }, [
    mutate,
    isLoading,
    isError,
    state,
    project.program_id,
    project.version,
    project.status,
    lastCompiledVersion,
    queryClient,
    pushMessage,
    setProject
  ])
}

// Polls the server during compilation and checks for the status.
const usePollCompilationStatus = (
  project: ProgramDescr,
  setProject: Dispatch<SetStateAction<ProgramDescr>>,
  setLastCompiledVersion: Dispatch<SetStateAction<number>>
) => {
  const queryClient = useQueryClient()
  const compilationStatus = useQuery<ProgramDescr>({
    queryKey: ['programStatus', { program_id: project.program_id }],
    refetchInterval: data =>
      data === undefined || data.status === 'Pending' || data.status === 'CompilingSql' ? 1000 : false,
    enabled: project.program_id !== '' && (project.status === 'Pending' || project.status === 'CompilingSql')
  })

  useEffect(() => {
    if (compilationStatus.data && !compilationStatus.isLoading && !compilationStatus.isError) {
      match(compilationStatus.data.status)
        .with({ SqlError: P.select() }, () => {
          setLastCompiledVersion(project.version)
        })
        .with({ RustError: P.select() }, () => {
          setLastCompiledVersion(project.version)
        })
        .with({ SystemError: P.select() }, () => {
          setLastCompiledVersion(project.version)
        })
        .with('Pending', () => {
          // Wait
        })
        .with('CompilingSql', () => {
          // Wait
        })
        .with('CompilingRust', () => {
          setLastCompiledVersion(project.version)
        })
        .with('Success', () => {
          setLastCompiledVersion(project.version)
        })
        .with('None', () => {
          // Wait -- shouldn't it be pending?
        })
        .exhaustive()

      if (project.status !== compilationStatus.data.status) {
        setProject((prevState: ProgramDescr) => ({ ...prevState, status: compilationStatus.data.status }))
        programStatusUpdate(queryClient, project.program_id, compilationStatus.data.status)
      }
    }
  }, [
    compilationStatus,
    compilationStatus.data,
    compilationStatus.isLoading,
    compilationStatus.isError,
    project.status,
    project.version,
    project.program_id,
    setLastCompiledVersion,
    setProject,
    queryClient
  ])
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

const Editors = (props: { programId: string | null }) => {
  const { programId } = props
  const theme = useTheme()
  const [loaded, setLoaded] = useState<boolean>(false)
  const [lastCompiledVersion, setLastCompiledVersion] = useState<number>(0)
  const [state, setState] = useState<SaveIndicatorState>(props.programId ? 'isNew' : 'isUpToDate')
  const [project, setProject] = useState<ProgramDescr>({
    program_id: programId || '',
    name: '',
    description: '',
    status: 'None',
    version: 0,
    code: ''
  })
  const [formError, setFormError] = useState<FormError>({})

  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'

  useCreateProjectIfNew(state, project, setProject, setState, setFormError)
  useFetchExistingProject(
    programId,
    setProject,
    setState,
    lastCompiledVersion,
    setLastCompiledVersion,
    loaded,
    setLoaded
  )
  useUpdateProjectIfChanged(state, project, setProject, setState, setFormError)
  useCompileProjectIfChanged(state, project, setProject, lastCompiledVersion)
  usePollCompilationStatus(project, setProject, setLastCompiledVersion)

  // Mounting and callback for when code is edited
  // TODO: The IStandaloneCodeEditor type is not exposed in the react monaco
  // editor package?
  const editorRef = useRef<any /* IStandaloneCodeEditor */>(null)
  function handleEditorDidMount(editor: any) {
    editorRef.current = editor
  }
  const debouncedCodeEditStateUpdate = useDebouncedCallback(() => {
    setState('isModified')
  }, SAVE_DELAY)
  const updateCode = (value: string | undefined) => {
    setProject(prevState => ({ ...prevState, code: value || '' }))
    setState('isDebouncing')
    debouncedCodeEditStateUpdate()
  }
  useDisplayCompilerErrorsInEditor(project, editorRef)

  usePageHeader(s => s.setHeader)({ title: 'SQL Editor', subtitle: 'Define your analytics and data transformations.' })

  return (programId !== null && loaded) || programId == null ? (
    <Grid container spacing={6}>
      <Grid item xs={12}>
        <Card>
          <CardHeader title='SQL Code'></CardHeader>
          <CardContent>
            <MetadataForm project={project} setProject={setProject} setState={setState} errors={formError} />
          </CardContent>
          <CardContent>
            <Grid item xs={12}>
              {/* ids referenced by webui-tester */}
              <SaveIndicator id='save-indicator' stateToLabel={stateToEditorLabel} state={state} />
              <CompileIndicator id='compile-indicator' state={project.status} />
            </Grid>
          </CardContent>
          {/* id referenced by webui-tester */}
          <CardContent id='editor-content'>
            <Editor
              height='60vh'
              theme={vscodeTheme}
              defaultLanguage='sql'
              value={project.code || ''}
              onChange={updateCode}
              onMount={editor => handleEditorDidMount(editor)}
            />
          </CardContent>
          <Divider sx={{ m: '0 !important' }} />
        </Card>
      </Grid>
    </Grid>
  ) : (
    <>Loading...</>
  )
}

export default Editors
