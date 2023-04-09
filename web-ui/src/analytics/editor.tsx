/* eslint-disable lines-around-comment */
// ** React Imports
import { useState, useEffect, useRef } from 'react'

import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import TextField from '@mui/material/TextField'
import Divider from '@mui/material/Divider'
import { Card, CardHeader, CardContent, FormHelperText } from '@mui/material'
import FormControl from '@mui/material/FormControl'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import PageHeader from 'src/layouts/components/page-header'
import Editor, { useMonaco } from '@monaco-editor/react'
import { match, P } from 'ts-pattern'
import { useDebouncedCallback } from 'use-debounce'

import { ProjectService } from 'src/types/manager/services/ProjectService'
import { ProjectDescr } from 'src/types/manager/models/ProjectDescr'
import {
  CompileProjectRequest,
  NewProjectResponse,
  ProjectStatus,
  SqlCompilerMessage,
  UpdateProjectResponse
} from 'src/types/manager'
import CompileIndicator from './CompileIndicator'
import SaveIndicator, { SaveIndicatorState } from 'src/components/SaveIndicator'

interface FormError {
  name?: { message?: string }
}

const MetadataForm = (props: { errors: FormError; project: ProjectDescr; setProject: any; setState: any }) => {
  const debouncedSaveStateUpdate = useDebouncedCallback(
    () => {
      props.setState(ProgramEditState.NameOrDescEdited)
    },

    // delay in ms
    2000
  )

  const updateName = event => {
    props.setProject((prevState: ProjectDescr) => ({ ...prevState, name: event.target.value }))
    props.setState(ProgramEditState.Debouncing)
    debouncedSaveStateUpdate()
  }

  const updateDescription = event => {
    props.setProject((prevState: ProjectDescr) => ({ ...prevState, description: event.target.value }))
    props.setState(ProgramEditState.Debouncing)
    debouncedSaveStateUpdate()
  }

  return (
    <Grid container spacing={5}>
      <Grid item xs={4}>
        <FormControl fullWidth>
          <TextField
            fullWidth
            type='text'
            label='Name'
            placeholder='Sql Program #1'
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
          type='Description'
          label='Description'
          placeholder='Average price over last 3 months.'
          value={props.project.description}
          onChange={updateDescription}
        />
      </Grid>
    </Grid>
  )
}

interface ProgramState {
  project_id: number | null
  name: string
  description: string
  status: ProjectStatus
  version: number
  code: string
}

// State machine of this website
enum ProgramEditState {
  // We loaded the existing program from the data-base (we have a valid project id)
  UpToDate,

  // We edited the code, name or description but are not updating the server yet.
  //
  // That is because we are waiting for the user to stop typing a few seconds first.
  Debouncing,

  // We edited the Name or Description.
  NameOrDescEdited,

  // We edited the code in the UI.
  CodeEdited,

  // We are recompiling the program.
  Compiling
}

const editStateToSaveState = (state: ProgramEditState, isNew: boolean): SaveIndicatorState => {
  if (isNew) {
    return 'isNew' as const
  }
  if (
    state === ProgramEditState.Debouncing ||
    state === ProgramEditState.NameOrDescEdited ||
    state === ProgramEditState.CodeEdited
  ) {
    return 'isSaving' as const
  }
  if (state === ProgramEditState.UpToDate) {
    return 'isUpToDate' as const
  }
}

const stateToEditorLabel = (state: SaveIndicatorState): string =>
  match(state)
    .with('isSaving' as const, () => {
      return 'Saving ...'
    })
    .with('isUpToDate' as const, () => {
      return 'Saved'
    })
    .with('isNew' as const, () => {
      return 'New Project'
    })
    .exhaustive()

const Editors = (props: { program: ProgramState }) => {
  const queryClient = useQueryClient()

  // Editor page state
  const [lastCompiledVersion, setLastCompiledVersion] = useState<number>(0)
  const [state, setState] = useState<ProgramEditState>(ProgramEditState.UpToDate)
  const [project, setProject] = useState<ProgramState>(props.program)
  const [formError, setFormError] = useState<FormError>({})

  // Create a project if we just happened to open a new one
  {
    const { mutate } = useMutation(['project', 'compilationStatus'], ProjectService.newProject)
    useEffect(() => {
      if (project.project_id == null) {
        let newProjectData = null
        if (state === ProgramEditState.NameOrDescEdited) {
          console.log('newProject name or desc changed')
          newProjectData = {
            name: project.name,
            description: project.description,
            code: ''
          }
        } else if (state === ProgramEditState.CodeEdited) {
          console.log('newProject code changed')
          newProjectData = {
            name: project.name,
            description: project.description,
            code: project.code
          }
        } else {
          // We don't to anything if we are not in the right state.
        }

        // Send create request if we either edited the code, the name or the description
        if (newProjectData != null) {
          mutate(newProjectData, {
            onSuccess: (data: NewProjectResponse) => {
              setProject((prevState: ProgramState) => ({
                ...prevState,
                version: data.version,
                project_id: data.project_id
              }))
              console.log('newProject name/desc or code successful')
              if (project.name === '') {
                setFormError({ name: { message: 'Enter a name for the project.' } })
              }
              setState(ProgramEditState.UpToDate)
              setFormError({})
            },
            onError: (error: unknown, _vars: unknown) => {
              //console.log(error)
              setFormError({ name: { message: 'This name already exists. Enter a different name.' } })
            }
          })
        }
      }
    }, [project.project_id, mutate, project.code, project.description, project.name, state])
  }

  // Fetch project meta-data and code
  const codeQuery = useQuery(
    ['project', 'compilationStatus', { project_id: project.project_id }],
    () => ProjectService.projectCode(project.project_id),
    { enabled: project.project_id !== null }
  )
  useEffect(() => {
    if (!codeQuery.isLoading && !codeQuery.isError) {
      console.log('set code to ', codeQuery.data.code)
      setProject({
        project_id: codeQuery.data.project.project_id,
        name: codeQuery.data.project.name,
        description: codeQuery.data.project.description,
        status: codeQuery.data.project.status,
        version: codeQuery.data.project.version,
        code: codeQuery.data.code
      })
      if (codeQuery.data.project.version > lastCompiledVersion && codeQuery.data.project.status !== 'None') {
        setLastCompiledVersion(codeQuery.data.project.version)
      }
      setState(ProgramEditState.UpToDate)
      console.log('codeQuery done')
    }
  }, [codeQuery.isLoading, codeQuery.isError, codeQuery.data, lastCompiledVersion])

  // Update project if name, description or code has changed
  {
    const { mutate, isLoading } = useMutation(ProjectService.updateProject)
    useEffect(() => {
      if (project.project_id !== null) {
        let updatedProjectData = null
        if (state === ProgramEditState.NameOrDescEdited) {
          console.log('updateProject name or desc')
          updatedProjectData = {
            project_id: project.project_id,
            name: project.name,
            description: project.description
          }
        } else if (state === ProgramEditState.CodeEdited) {
          console.log('updateProject code')
          updatedProjectData = {
            project_id: project.project_id,
            name: project.name,
            description: project.description,
            code: project.code
          }
        } else {
          // We don't to anything if we are not in the right state.
        }

        // Send update if we either edited the code, the name or the description
        if (updatedProjectData != null && !isLoading) {
          mutate(updatedProjectData, {
            onSuccess: (data: UpdateProjectResponse) => {
              setProject((prevState: ProgramState) => ({ ...prevState, version: data.version }))
              console.log('updateProject name/desc or code successful')
              setState(ProgramEditState.UpToDate)
              queryClient.invalidateQueries(['project', 'compilationStatus', { project_id: project.project_id }])
              setFormError({})
            },
            onError: (error: unknown, _vars: unknown) => {
              //console.log(error)
              setFormError({ name: { message: 'This name already exists. Enter a different name.' } })
            }
          })
        }
      }
    }, [
      mutate,
      state,
      project.project_id,
      project.description,
      project.name,
      project.code,
      setState,
      isLoading,
      queryClient
    ])
  }

  // Compile code if version has changed
  {
    const { mutate, isLoading, isError } = useMutation(
      [
        'project',
        () => {
          project_id: project.project_id
        }
      ],
      ProjectService.compileProject
    )
    useEffect(() => {
      console.log('should compileProject? ' + project.version, lastCompiledVersion, project.status)
      if (
        !isLoading &&
        !isError &&
        state === ProgramEditState.UpToDate &&
        project.version > lastCompiledVersion &&
        project.status !== 'Pending' &&
        project.status !== 'CompilingSql'
      ) {
        console.log('compileProject ' + project.version)
        setProject((prevState: ProgramState) => ({ ...prevState, status: 'Pending' }))
        mutate(
          { project_id: project.project_id, version: project.version },
          {
            onSuccess: () => {
              console.log('compileProject is now compiling...')

              //queryClient.invalidateQueries(['compilationStatus', { project_id: project.project_id }])
            },
            onError: (error: unknown, vars: CompileProjectRequest) => {
              setProject((prevState: ProgramState) => ({ ...prevState, status: 'None' }))
              console.log('updateProjectData failed with error:' + error)
              console.log('version in UI is ' + project.version)
              console.log('version returned is ' + vars.version)
            }
          }
        )
      }
    }, [
      mutate,
      isLoading,
      isError,
      state,
      project.project_id,
      project.version,
      project.status,
      lastCompiledVersion,
      queryClient
    ])
  }

  // Check the status periodically if the project is compiling
  const compilationStatus = useQuery({
    queryKey: [
      'compilationStatus',
      {
        project_id: project.project_id
      }
    ],
    queryFn: () => ProjectService.projectStatus(project.project_id),
    refetchInterval: (data, _query) =>
      data === undefined || data.status === 'Pending' || data.status === 'CompilingSql' ? 1000 : false,
    enabled: project.status === 'Pending' || project.status === 'CompilingSql'
  })

  useEffect(() => {
    if (!compilationStatus.isLoading && !compilationStatus.isError && project.project_id != null) {
      match(compilationStatus.data.status)
        .with({ SqlError: P.select() }, err => {
          //console.log('Status Query: returned sql error', err)
          setLastCompiledVersion(project.version)
        })
        .with({ RustError: P.select() }, err => {
          //console.log('Status Query: returned rust error', err)
          setLastCompiledVersion(project.version)
        })
        .with({ SystemError: P.select() }, err => {
          //console.log('Status Query: returned system error', err)
          setLastCompiledVersion(project.version)
        })
        .with('Pending', () => {
          //console.log('Status Query: Currently pending')
        })
        .with('CompilingSql', () => {
          //console.log('Status Query: Currently compiling sql')
        })
        .with('CompilingRust', () => {
          //console.log('Status Query: Currently compiling rust code')
          setLastCompiledVersion(project.version)
        })
        .with('Success', () => {
          //console.log('Status Query: we have a success')
          setLastCompiledVersion(project.version)
        })
        .with('None', () => {
          //console.log('Status Query: None, should be pending?')
        })
        .exhaustive()

      if (project.status !== compilationStatus.data.status) {
        setProject((prevState: ProgramState) => ({ ...prevState, status: compilationStatus.data.status }))
        console.log('compilationStatus status update ' + compilationStatus.data.status.toString())
      }
    }
  }, [
    compilationStatus.data,
    compilationStatus.isLoading,
    compilationStatus.isError,
    project.status,
    project.version,
    project.project_id,
    setLastCompiledVersion
  ])

  // Display errors in the editor if there are any
  const monaco = useMonaco()
  const editorRef = useRef(null)
  function handleEditorDidMount(editor: any, _monaco: any) {
    editorRef.current = editor
    console.log('handleEditorDidMount', project.code)
  }

  const debouncedCodeEditStateUpdate = useDebouncedCallback(
    () => {
      setState(ProgramEditState.CodeEdited)
    },

    // delay in ms
    2000
  )
  const updateCode = (value: string | undefined, _event: any) => {
    setProject(prevState => ({ ...prevState, code: value || '' }))
    setState(ProgramEditState.Debouncing)
    debouncedCodeEditStateUpdate()
  }

  // TODO: this doesn't yet update the status when the page is first loaded/we fetch project status from API
  useEffect(() => {
    console.log('editor status update: ' + project.status)
    if (monaco !== null && editorRef.current !== null) {
      console.log('editor status made it in the if: ' + project.status)
      match(project.status)
        .with({ SqlError: P.select() }, (err: SqlCompilerMessage[]) => {
          console.log(err)
          const monaco_markers = err.map((item, _idx, _arr) => {
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

  return (
    <Grid container spacing={6}>
      <PageHeader
        title={<Typography variant='h5'>SQL Editor</Typography>}
        subtitle={
          <Typography variant='body2'>Define your analytics and data transformation on the input pipelines.</Typography>
        }
      />

      <Grid item xs={12}>
        <Card>
          <CardHeader title='SQL Code'></CardHeader>
          <CardContent>
            <MetadataForm project={project} setProject={setProject} setState={setState} errors={formError} />
          </CardContent>
          <CardContent>
            <Grid item xs={12}>
              <SaveIndicator
                stateToLabel={stateToEditorLabel}
                state={editStateToSaveState(state, project.project_id == null)}
              />
              <CompileIndicator state={project.status} />
            </Grid>
          </CardContent>
          <CardContent>
            <Editor
              height='60vh'
              theme='vs-dark'
              defaultLanguage='sql'
              value={project.code}
              onChange={updateCode}
              onMount={(editor, monaco) => handleEditorDidMount(editor, monaco)}
            />
          </CardContent>
          <Divider sx={{ m: '0 !important' }} />
        </Card>
      </Grid>
    </Grid>
  )
}

export default Editors
