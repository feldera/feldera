// See the status of a input table or output view.
//
// Note: This is still a work in progress and currently does not work as well as
// it should or is not very flexible in displaying what a user wants.
import Card from '@mui/material/Card'
import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import { DataGridPro, GridColumns, useGridApiRef } from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'
import { useRouter } from 'next/router'
import { useEffect, useState } from 'react'
import PageHeader from 'src/layouts/components/page-header'
import { PipelineDescr, PipelineId, ProgramDescr, PipelineStatus, OpenAPI } from 'src/types/manager'
import { parse } from 'csv-parse'
import { parseProjectSchema } from 'src/types/program'

const IntrospectInputOutput = () => {
  const apiRef = useGridApiRef()
  const [configDescr, setConfig] = useState<PipelineDescr | undefined>(undefined)
  const [headers, setHeaders] = useState<GridColumns | undefined>(undefined)
  const [pipelineId, setPipelineId] = useState<PipelineId | undefined>(undefined)
  const [viewName, setViewName] = useState<string | undefined>(undefined)
  const router = useRouter()
  const { config, view } = router.query

  const projectQuery = useQuery<ProgramDescr>(['programStatus', { program_id: configDescr?.program_id }], {
    enabled: configDescr !== undefined && configDescr.program_id !== undefined
  })
  useEffect(() => {
    if (!projectQuery.isLoading && !projectQuery.isError && viewName) {
      if (projectQuery.data && projectQuery.data.schema) {
        const program = parseProjectSchema(projectQuery.data)
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
    if (typeof config === 'string') {
      setPipelineId(config)
    }
    if (typeof view === 'string') {
      setViewName(view)
    }
  }, [pipelineId, setPipelineId, config, view, setViewName])

  const configQuery = useQuery<PipelineDescr>(['pipelineStatus', { pipeline_id: pipelineId }], {
    enabled: pipelineId !== undefined
  })
  useEffect(() => {
    if (!configQuery.isLoading && !configQuery.isError) {
      setConfig(configQuery.data)
    }
  }, [configQuery.isLoading, configQuery.isError, configQuery.data, setConfig])

  useEffect(() => {
    if (
      configDescr &&
      configDescr.status == PipelineStatus.RUNNING &&
      view !== undefined &&
      headers !== undefined &&
      apiRef.current
    ) {
      const watchStream = async function (url: string) {
        const response = await fetch(url)
        const reader = response.body?.getReader()
        const decoder = new TextDecoder('utf-8')

        while (true) {
          const value = await reader?.read().then(function (result) {
            return decoder.decode(result.value)
          })
          const obj = JSON.parse(value || '{}')
          console.log(obj.text_data)
          parse(
            obj.text_data,
            {
              delimiter: ','
            },
            (error, result: string[][]) => {
              if (error) {
                console.error(error)
              }

              const newRows: any[] = result.map(row => {
                const genId = row[0]
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
        }
      }

      const url = OpenAPI.BASE + '/v0/pipelines/' + pipelineId + '/egress/' + viewName + '?format=csv'
      watchStream(url)
    }
  }, [configDescr, pipelineId, viewName, view, apiRef, headers])

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
              columnVisibilityModel={{ genId: false, weight: false }}
              getRowId={(row: any) => row.genId}
              apiRef={apiRef}
              autoHeight
              columns={headers}
              rowThreshold={0}
              rows={[]}
            />
          </Card>
        </Grid>
      </Grid>
    )
  )
}

export default IntrospectInputOutput
