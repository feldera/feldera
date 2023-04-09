import Card from '@mui/material/Card'
import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import { DataGridPro, GridColumns, useGridApiRef } from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'
import { useRouter } from 'next/router'
import { useEffect, useRef, useState } from 'react'
import PageHeader from 'src/layouts/components/page-header'
import { ConfigDescr, ConfigId, ProjectService } from 'src/types/manager'
import { ConfigService } from 'src/types/manager/services/ConfigService'
import { parse } from 'csv-parse'
import { projectToProjectWithSchema } from 'src/types/program'

const IntrospectInputOutput = () => {
  const apiRef = useGridApiRef()
  const [rows, setRows] = useState<any[]>([])
  const [configDescr, setConfig] = useState<ConfigDescr | undefined>(undefined)
  const [headers, setHeaders] = useState<GridColumns | undefined>(undefined)
  const [configId, setConfigId] = useState<ConfigId | undefined>(undefined)
  const [viewName, setViewName] = useState<string | undefined>(undefined)
  const router = useRouter()
  const { config, view } = router.query

  const projectQuery = useQuery({
    queryKey: ['projectStatus'],
    queryFn: () => {
      if (configDescr !== undefined && configDescr.project_id !== undefined) {
        return ProjectService.projectStatus(configDescr.project_id)
      }
    },
    enabled: configDescr !== undefined && configDescr.project_id !== undefined
  })
  useEffect(() => {
    if (!projectQuery.isLoading && !projectQuery.isError && viewName) {
      if (projectQuery.data && projectQuery.data.schema) {
        const program = projectToProjectWithSchema(projectQuery.data)
        const view = program.schema['outputs'].find(v => v.name === viewName)
        if (view) {
          const id = [{ field: 'genId', headerName: 'genId', flex: 0.1 }]
          setHeaders(
            id
              .concat(
                view.fields.map((col: any) => {
                  return { field: col.name, headerName: col.name, flex: 1 }
                })
              )
              .concat([{ field: 'weight', headerName: 'weight', flex: 0.2 }])
          )
        }
      }
    }
  }, [projectQuery.isLoading, projectQuery.isError, projectQuery.data, setHeaders, viewName])

  useEffect(() => {
    if (typeof config === 'string' && parseInt(config) != configId) {
      setConfigId(parseInt(config))
    }
    if (typeof view === 'string') {
      setViewName(view)
    }
  }, [configId, setConfigId, config, view, setViewName])

  const configQuery = useQuery({
    queryKey: ['configStatus'],
    queryFn: () => {
      if (configId) {
        return ConfigService.configStatus(configId)
      }
    },
    enabled: configId !== undefined
  })
  useEffect(() => {
    if (!configQuery.isLoading && !configQuery.isError) {
      setConfig(configQuery.data)
    }
  }, [configQuery.isLoading, configQuery.isError, configQuery.data, setConfig])

  const ws = useRef<WebSocket | null>(null)
  useEffect(() => {
    if (configDescr && configDescr.pipeline && view !== undefined && headers !== undefined && apiRef.current) {
      const socket = new WebSocket('ws://localhost:' + configDescr.pipeline.port + '/output_endpoint/debug-' + view)
      socket.onopen = () => {
        console.log('opened')
      }

      socket.onclose = () => {
        console.log('closed')
      }

      socket.onmessage = event => {
        console.log('got message')
        event.data.text().then((txt: string) => {
          parse(
            txt,
            {
              delimiter: ','
            },
            (error, result: string[][]) => {
              if (error) {
                console.error(error)
              }

              // console.log('Result', result)

              const newRows: any[] = result.map(row => {
                const genId = row[0] //row.slice(0, row.length - 1).join('-')
                const weight = row[row.length - 1]
                const fields = row.slice(0, row.length - 1)

                const newRow = { genId, weight: parseInt(weight) } as any
                headers.forEach((col, i) => {
                  if (col.field !== 'genId' && col.field !== 'weight') {
                    newRow[col.field] = fields[i - 1]
                  }
                })

                return newRow
              })

              apiRef.current?.updateRows(
                newRows
                  .map(row => {
                    const curRow = apiRef.current.getRow(row.genId)
                    if (curRow !== null && curRow.weight + row.weight == 0) {
                      //return { genId: curRow.genId, _action: 'delete' as const }
                      return row
                    } else if (curRow == null && row.weight < 0) {
                      return null
                    } else {
                      return { ...row, weight: row.weight + (curRow?.weight || 0) }
                    }
                  })
                  .filter(x => x !== null)
              )
            }
          )
        })
      }
      ws.current = socket

      return () => {
        socket.close()
      }
    }
  }, [configDescr, view, apiRef, headers])

  return (
    !configQuery.isLoading &&
    !configQuery.isError &&
    headers && (
      <Grid container spacing={6} className='match-height'>
        <PageHeader
          title={
            <Typography variant='h5'>
              {' '}
              {configDescr?.name} / {viewName}
            </Typography>
          }
          subtitle={<Typography variant='body2'>Introspection</Typography>}
        />

        <Grid item xs={12}>
          <Card>
            <DataGridPro
              getRowId={(row: any) => row.genId}
              apiRef={apiRef}
              autoHeight
              columns={headers}
              rowThreshold={0}
              rows={rows}
              setRows={setRows}
            />
          </Card>
        </Grid>
      </Grid>
    )
  )
}

export default IntrospectInputOutput
