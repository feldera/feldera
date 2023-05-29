// Editor for SQL programs. This is the main component for the editor page.
// It is responsible for loading the program, compiling it, and saving it.

import { useState, useEffect, useRef, Dispatch, SetStateAction, MutableRefObject } from 'react'
import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import TextField from '@mui/material/TextField'
import Divider from '@mui/material/Divider'
import { Card, CardHeader, CardContent, FormHelperText, useTheme } from '@mui/material'
import FormControl from '@mui/material/FormControl'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import PageHeader from 'src/layouts/components/page-header'
import Editor, { useMonaco } from '@monaco-editor/react'
import { match, P } from 'ts-pattern'
import { useDebouncedCallback } from 'use-debounce'

import {
  CancelError,
  CompileProgramRequest,
  NewProgramRequest,
  NewProgramResponse,
  ProgramCodeResponse,
  ProgramStatus,
  SqlCompilerMessage,
  UpdateProgramRequest,
  UpdateProgramResponse
} from 'src/types/manager'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { ProgramService } from 'src/types/manager/services/ProgramService'
import { ProgramDescr } from 'src/types/manager/models/ProgramDescr'
import CompileIndicator from './CompileIndicator'
import SaveIndicator, { SaveIndicatorState } from 'src/components/SaveIndicator'
import { PLACEHOLDER_VALUES } from 'src/utils'
import { projectQueryCacheUpdate } from 'src/types/defaultQueryFn'

// How many ms to wait until we save the project.
const SAVE_DELAY = 2000

// The error format for the editor form.
interface FormError {
  name?: { message?: string }
}

// Top level form with Name and Description TextInput elements
const MetadataForm = (props: { errors: FormError; project: ProgramState; setProject: any; setState: any }) => {
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

// This is a representation of the state of the program. It's basically
// ProjectDesc, except that program_id can be null.
interface ProgramState {
  program_id: string | null
  name: string
  description: string
  status: ProgramStatus
  version: number
  code: string
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
  project: ProgramState,
  setProject: Dispatch<SetStateAction<ProgramState>>,
  setState: Dispatch<SetStateAction<SaveIndicatorState>>,
  setFormError: Dispatch<SetStateAction<FormError>>
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate } = useMutation<NewProgramResponse, CancelError, NewProgramRequest>(ProgramService.newProgram)
  useEffect(() => {
    if (project.program_id == null) {
      if (state === 'isModified') {
        mutate(
          {
            name: project.name,
            description: project.description,
            code: project.code
          },
          {
            onSettled: () => {
              queryClient.invalidateQueries(['program'])
              queryClient.invalidateQueries(['programStatus', { program_id: project.program_id }])
            },
            onSuccess: (data: NewProgramResponse) => {
              setProject((prevState: ProgramState) => ({
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
            onError: (error: CancelError) => {
              // TODO: would be good to have error codes from the API
              if (error.message.includes('name already exists')) {
                setFormError({ name: { message: 'This name already exists. Enter a different name.' } })
                // This won't try to save again, but set the save indicator to
                // Saving... until the user changes something:
                setState('isDebouncing')
              } else {
                pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
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
  project: ProgramState,
  setProject: Dispatch<SetStateAction<ProgramState>>,
  setState: Dispatch<SetStateAction<SaveIndicatorState>>,
  lastCompiledVersion: number,
  setLastCompiledVersion: Dispatch<SetStateAction<number>>
) => {
  const codeQuery = useQuery<number, CancelError, ProgramCodeResponse>(
    ['programCode', { program_id: project.program_id }],
    { enabled: project.program_id != null }
  )
  useEffect(() => {
    if (codeQuery.data && !codeQuery.isLoading && !codeQuery.isError) {
      setProject({
        program_id: codeQuery.data.program.program_id,
        name: codeQuery.data.program.name,
        description: codeQuery.data.program.description,
        status: codeQuery.data.program.status,
        version: codeQuery.data.program.version,
        code: codeQuery.data.code
      })
      if (codeQuery.data.program.version > lastCompiledVersion && codeQuery.data.program.status !== 'None') {
        setLastCompiledVersion(codeQuery.data.program.version)
      }
      setState('isUpToDate')
    }
  }, [
    codeQuery.isLoading,
    codeQuery.isError,
    codeQuery.data,
    lastCompiledVersion,
    setProject,
    setState,
    setLastCompiledVersion
  ])
}

// Updates the project if it has changed and we have a program_id.
const useUpdateProjectIfChanged = (
  state: SaveIndicatorState,
  project: ProgramState,
  setProject: Dispatch<SetStateAction<ProgramState>>,
  setState: Dispatch<SetStateAction<SaveIndicatorState>>,
  setFormError: Dispatch<SetStateAction<FormError>>
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate, isLoading } = useMutation<UpdateProgramResponse, CancelError, UpdateProgramRequest>(
    ProgramService.updateProgram
  )
  useEffect(() => {
    if (project.program_id !== null && state === 'isModified' && !isLoading) {
      const updateRequest = {
        program_id: project.program_id,
        name: project.name,
        description: project.description,
        code: project.code
      }
      mutate(updateRequest, {
        onSettled: () => {
          queryClient.invalidateQueries(['program'])
          queryClient.invalidateQueries(['programCode', { program_id: project.program_id }])
          queryClient.invalidateQueries(['programStatus', { program_id: project.program_id }])
        },
        onSuccess: (data: UpdateProgramResponse) => {
          projectQueryCacheUpdate(queryClient, updateRequest)
          setProject((prevState: ProgramState) => ({ ...prevState, version: data.version }))
          setState('isUpToDate')
          setFormError({})
        },
        onError: (error: CancelError) => {
          // TODO: would be good to have error codes from the API
          if (error.message.includes('name already exists')) {
            setFormError({ name: { message: 'This name already exists. Enter a different name.' } })
            // This won't try to save again, but set the save indicator to
            // Saving... until the user changes something:
            setState('isDebouncing')
          } else {
            pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
          }
        }
      })
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
  project: ProgramState,
  setProject: Dispatch<SetStateAction<ProgramState>>,
  lastCompiledVersion: number
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate, isLoading, isError } = useMutation<CompileProgramRequest, CancelError, any>(
    ProgramService.compileProgram
  )
  useEffect(() => {
    if (
      !isLoading &&
      !isError &&
      state == 'isUpToDate' &&
      project.program_id !== null &&
      project.version > lastCompiledVersion &&
      project.status !== 'Pending' &&
      project.status !== 'CompilingSql'
    ) {
      //console.log('compileProject ' + project.version)
      setProject((prevState: ProgramState) => ({ ...prevState, status: 'Pending' }))
      mutate(
        { program_id: project.program_id, version: project.version },
        {
          onSettled: () => {
            queryClient.invalidateQueries(['program'])
            queryClient.invalidateQueries(['programStatus', { program_id: project.program_id }])
          },
          onError: (error: CancelError) => {
            setProject((prevState: ProgramState) => ({ ...prevState, status: 'None' }))
            pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
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
  project: ProgramState,
  setProject: Dispatch<SetStateAction<ProgramState>>,
  setLastCompiledVersion: Dispatch<SetStateAction<number>>
) => {
  const queryClient = useQueryClient()
  const compilationStatus = useQuery<ProgramDescr>({
    queryKey: ['programStatus', { program_id: project.program_id }],
    refetchInterval: data =>
      data === undefined || data.status === 'Pending' || data.status === 'CompilingSql' ? 1000 : false,
    enabled: project.program_id !== null && (project.status === 'Pending' || project.status === 'CompilingSql')
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
        // @ts-ignore: Typescript thinks compilationStatus.data can be undefined but we check it above?
        setProject((prevState: ProgramState) => ({ ...prevState, status: compilationStatus.data.status }))
        queryClient.setQueryData(['programStatus', { program_id: project.program_id }], compilationStatus.data)
        queryClient.setQueryData(['program'], (oldData: ProgramDescr[] | undefined) => {
          return oldData?.map((item: ProgramDescr) => {
            if (item.program_id === project.program_id) {
              return compilationStatus.data
            } else {
              return item
            }
          })
        })
      }
    }
  }, [
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

const useDisplayCompilerErrorsInEditor = (project: ProgramState, editorRef: MutableRefObject<any>) => {
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

const Editors = (props: { program: ProgramState }) => {
  const theme = useTheme()
  const [lastCompiledVersion, setLastCompiledVersion] = useState<number>(0)
  const [state, setState] = useState<SaveIndicatorState>(props.program.program_id ? 'isNew' : 'isUpToDate')
  const [project, setProject] = useState<ProgramState>(props.program)
  const [formError, setFormError] = useState<FormError>({})

  const vscodeTheme = theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'

  useCreateProjectIfNew(state, project, setProject, setState, setFormError)
  useFetchExistingProject(project, setProject, setState, lastCompiledVersion, setLastCompiledVersion)
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

  return (
    <Grid container spacing={6}>
      <PageHeader
        title={<Typography variant='h5'>SQL Editor</Typography>}
        subtitle={<Typography variant='body2'>Define your analytics and data transformations.</Typography>}
      />

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
              value={project.code}
              onChange={updateCode}
              onMount={editor => handleEditorDidMount(editor)}
            />
          </CardContent>
          <Divider sx={{ m: '0 !important' }} />
        </Card>
      </Grid>
    </Grid>
  )
}

export default Editors
